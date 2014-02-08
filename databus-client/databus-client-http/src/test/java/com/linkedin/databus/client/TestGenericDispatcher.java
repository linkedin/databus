package com.linkedin.databus.client;
/*
 *
 * Copyright 2013 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/


import static org.testng.AssertJUnit.assertEquals;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.channels.Pipe;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.client.consumer.AbstractDatabusCombinedConsumer;
import com.linkedin.databus.client.consumer.AbstractDatabusStreamConsumer;
import com.linkedin.databus.client.consumer.DatabusV2ConsumerRegistration;
import com.linkedin.databus.client.consumer.DelegatingDatabusCombinedConsumer;
import com.linkedin.databus.client.consumer.MultiConsumerCallback;
import com.linkedin.databus.client.consumer.SelectingDatabusCombinedConsumer;
import com.linkedin.databus.client.consumer.StreamConsumerCallbackFactory;
import com.linkedin.databus.client.pub.CheckpointPersistenceProvider;
import com.linkedin.databus.client.pub.CheckpointPersistenceProviderAbstract;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DatabusCombinedConsumer;
import com.linkedin.databus.client.pub.DatabusStreamConsumer;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.client.pub.SCN;
import com.linkedin.databus.client.pub.mbean.ConsumerCallbackStats;
import com.linkedin.databus.client.pub.mbean.UnifiedClientStats;
import com.linkedin.databus.core.BootstrapCheckpointHandler;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DatabusComponentStatus;
import com.linkedin.databus.core.DbusClientMode;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventBuffer.AllocationPolicy;
import com.linkedin.databus.core.DbusEventBuffer.QueuePolicy;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.StreamEventsArgs;
import com.linkedin.databus.core.StreamEventsResult;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.test.DbusEventAppender;
import com.linkedin.databus.core.test.DbusEventBufferReader;
import com.linkedin.databus.core.test.DbusEventGenerator;
import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus.core.util.RangeBasedReaderWriterLock;
import com.linkedin.databus.core.util.UncaughtExceptionTrackingThread;
import com.linkedin.databus2.core.container.request.RegisterResponseEntry;
import com.linkedin.databus2.core.container.request.RegisterResponseMetadataEntry;
import com.linkedin.databus2.schemas.SchemaId;
import com.linkedin.databus2.schemas.SchemaRegistryService;
import com.linkedin.databus2.schemas.VersionedSchema;
import com.linkedin.databus2.test.ConditionCheck;
import com.linkedin.databus2.test.TestUtil;

public class TestGenericDispatcher
{
    public static final String MODULE = TestGenericDispatcher.class.getName();
    public static final Logger LOG = Logger.getLogger(MODULE);

    private DbusEventBuffer.Config _generic100KBufferConfig;
    private DbusEventBuffer.StaticConfig _generic100KBufferStaticConfig;

    private DatabusSourcesConnection.Config _genericRelayConnConfig;
    private DatabusSourcesConnection.StaticConfig _genericRelayConnStaticConfig;

    private static final String SOURCE1_SCHEMA_STR = "{\"name\":\"source1\",\"type\":\"record\",\"fields\":[{\"name\":\"s\",\"type\":\"string\"}]}";
    private static final String SOURCE2_SCHEMA_STR = "{\"name\":\"source2\",\"type\":\"record\",\"fields\":[{\"name\":\"s\",\"type\":\"string\"}]}";;
    private static final String SOURCE3_SCHEMA_STR = "{\"name\":\"source3\",\"type\":\"record\",\"fields\":[{\"name\":\"s\",\"type\":\"string\"}]}";;

    private static final String META1_SCHEMA_STR = "{\"name\":\"meta-source\",\"type\":\"record\",\"fields\":[{\"name\":\"s\",\"type\":\"string\"}]}";
    private static final String META2_SCHEMA_STR = "{\"name\":\"meta-source\",\"type\":\"record\",\"fields\":[{\"name\":\"sNew\",\"type\":\"string\"}]}";;


    private void initBufferWithEvents(DbusEventBuffer eventsBuf,
        long keyBase,
        int numEvents,
        short srcId,
        Hashtable<Long, AtomicInteger> keyCounts,
        Hashtable<Short, AtomicInteger> srcidCounts)
    {
      String schemaStr=null;
      switch (srcId)
      {
        case 1:
          schemaStr=SOURCE1_SCHEMA_STR;
          break;
        case 2:
          schemaStr=SOURCE2_SCHEMA_STR;
          break;
        case 3:
          schemaStr=SOURCE3_SCHEMA_STR;
          break;

        default:
          break;
      }
      SchemaId schemaId = schemaStr != null ? SchemaId.createWithMd5(schemaStr) : null;
      byte[] schemaBytes = schemaId != null? schemaId.getByteArray(): new byte[16];
      initBufferWithEvents(eventsBuf, keyBase, numEvents, srcId,schemaBytes, keyCounts, srcidCounts);
    }

    private void initBufferWithEvents(DbusEventBuffer eventsBuf,
            long keyBase,
            int numEvents,
            short srcId,
            byte[] schemaId,
            Hashtable<Long, AtomicInteger> keyCounts,
            Hashtable<Short, AtomicInteger> srcidCounts)
    {
        if (null != srcidCounts) srcidCounts.put(srcId, new AtomicInteger(0));

        for (long i = 0; i < numEvents; ++i)
        {
            String value = "{\"s\":\"value" + i + "\"}";
            try {
                eventsBuf.appendEvent(new DbusEventKey(keyBase + i), (short)0, (short)1, (short)0, srcId,
                        schemaId, value.getBytes("UTF-8"), false);
            } catch (UnsupportedEncodingException e) {
                //ignore
            }
            if (null != keyCounts) keyCounts.put(keyBase + i, new AtomicInteger(0));
        }
    }

    @BeforeClass
    public void beforeClass() throws Exception
    {
      TestUtil.setupLoggingWithTimestampedFile(true, "/tmp/TestGenericDispatcher_", ".log", Level.INFO);
        _generic100KBufferConfig = new DbusEventBuffer.Config();
        _generic100KBufferConfig.setAllocationPolicy(AllocationPolicy.HEAP_MEMORY.toString());
        _generic100KBufferConfig.setMaxSize(100000);
        _generic100KBufferConfig.setQueuePolicy(QueuePolicy.BLOCK_ON_WRITE.toString());
        _generic100KBufferConfig.setScnIndexSize(10000);
        _generic100KBufferConfig.setAverageEventSize(100000);
        _generic100KBufferStaticConfig = _generic100KBufferConfig.build();


        _genericRelayConnConfig = new DatabusSourcesConnection.Config();
        _genericRelayConnConfig.setConsumerParallelism(1);
        _genericRelayConnConfig.setEventBuffer(_generic100KBufferConfig);
        _genericRelayConnConfig.setConsumerTimeBudgetMs(1000);

        _genericRelayConnStaticConfig = _genericRelayConnConfig.build();
    }

    @Test(groups = {"small", "functional"})
    public void testOneWindowHappyPath()
    {
        final Logger log = Logger.getLogger("TestGenericDispatcher.testOneWindowHappyPath");
        //log.setLevel(Level.DEBUG);
        log.info("start");

        int source1EventsNum = 2;
        int source2EventsNum = 2;

        Hashtable<Long, AtomicInteger> keyCounts = new Hashtable<Long, AtomicInteger>();
        Hashtable<Short, AtomicInteger> srcidCounts = new Hashtable<Short, AtomicInteger>();

        final TestGenericDispatcherEventBuffer eventsBuf =
            new TestGenericDispatcherEventBuffer(_generic100KBufferStaticConfig);
        eventsBuf.start(0);

        final StateVerifyingStreamConsumer svsConsumer = new StateVerifyingStreamConsumer(null);
        //svsConsumer.getLog().setLevel(Level.DEBUG);
        DatabusStreamConsumer mockConsumer =
                new EventCountingConsumer(svsConsumer, keyCounts, srcidCounts);
        SelectingDatabusCombinedConsumer sdccMockConsumer = new SelectingDatabusCombinedConsumer(mockConsumer);

        List<String> sources = new ArrayList<String>();
        Map<Long, IdNamePair> sourcesMap = new HashMap<Long, IdNamePair>();
        for (int i = 1; i <= 3; ++i)
        {
            IdNamePair sourcePair = new IdNamePair((long)i, "source" + i);
            sources.add(sourcePair.getName());
            sourcesMap.put(sourcePair.getId(), sourcePair);
        }

        DatabusV2ConsumerRegistration consumerReg =
                new DatabusV2ConsumerRegistration(sdccMockConsumer, sources, null);

        List<DatabusV2ConsumerRegistration> allRegistrations =
                Arrays.asList(consumerReg);
        MultiConsumerCallback callback =
                new MultiConsumerCallback(
                        allRegistrations,
                        Executors.newSingleThreadExecutor(),
                        1000,
                        new StreamConsumerCallbackFactory(null, null),
                        null,
                        null,
                        null,
                        null);
        callback.setSourceMap(sourcesMap);

        List<DatabusSubscription> subs = DatabusSubscription.createSubscriptionList(sources);
        RelayDispatcher dispatcher =
                new RelayDispatcher("dispatcher", _genericRelayConnStaticConfig, subs,
                        new InMemoryPersistenceProvider(),
                        eventsBuf, callback, null,null,null, null, null);
        dispatcher.setSchemaIdCheck(false);

        Thread dispatcherThread = new Thread(dispatcher, "testOneWindowHappyPath-dispatcher");
        //dispatcherThread.setDaemon(true);
        dispatcherThread.start();

        HashMap<Long, List<RegisterResponseEntry>> schemaMap =
                new HashMap<Long, List<RegisterResponseEntry>>();

        List<RegisterResponseEntry> l1 = new ArrayList<RegisterResponseEntry>();
        List<RegisterResponseEntry> l2 = new ArrayList<RegisterResponseEntry>();
        List<RegisterResponseEntry> l3 = new ArrayList<RegisterResponseEntry>();

        l1.add(new RegisterResponseEntry(1L, (short)1,SOURCE1_SCHEMA_STR));
        l2.add(new RegisterResponseEntry(2L, (short)1,SOURCE2_SCHEMA_STR));
        l3.add(new RegisterResponseEntry(3L, (short)1,SOURCE3_SCHEMA_STR));

        schemaMap.put(1L, l1);
        schemaMap.put(2L, l2);
        schemaMap.put(3L, l3);

        dispatcher.enqueueMessage(SourcesMessage.createSetSourcesIdsMessage(sourcesMap.values()));
        dispatcher.enqueueMessage(SourcesMessage.createSetSourcesSchemasMessage(schemaMap));

        eventsBuf.startEvents();
        initBufferWithEvents(eventsBuf, 1, source1EventsNum, (short)1, keyCounts, srcidCounts);
        initBufferWithEvents(eventsBuf, 1 + source1EventsNum, source2EventsNum, (short)2, keyCounts, srcidCounts);
        eventsBuf.endEvents(100L,null);

        try
        {
            Thread.sleep(2000);
        }
        catch (InterruptedException ie) {}

        dispatcher.shutdown();
        Checkpoint cp = dispatcher.getDispatcherState().getLastSuccessfulCheckpoint();
        Assert.assertEquals(Checkpoint.FULLY_CONSUMED_WINDOW_OFFSET, cp.getWindowOffset());
        Assert.assertEquals(cp.getWindowScn(), cp.getPrevScn());

        for (long i = 1; i <= source1EventsNum + source2EventsNum; ++i)
        {
            assertEquals("correct amount of callbacks for key " + i,
                    1, keyCounts.get(i).intValue());
        }

        assertEquals("correct amount of callbacks for srcid 1", source1EventsNum,
                     srcidCounts.get((short)1).intValue());
        assertEquals("correct amount of callbacks for srcid 2", source2EventsNum,
                     srcidCounts.get((short)2).intValue());
        verifyNoLocks(log, eventsBuf);
        log.info("end\n");
    }

    /**
     * @param log
     * @param eventsBuf
     */
    private void verifyNoLocks(final Logger log,
                               final TestGenericDispatcherEventBuffer eventsBuf)
    {
      TestUtil.assertWithBackoff(new ConditionCheck()
      {
        @Override
        public boolean check()
        {
          if (null != log)
              log.debug("rwlocks: " + eventsBuf.getRangeLocksProvider());
          return 0 == eventsBuf.getRangeLocksProvider().getNumReaders();
        }
      }, "no locks remaining on buffer", 5000, log);
    }

    @Test(groups = {"small", "functional"})
    /**
     * Test a relatively big window which forces a checkpoint which fails. Then trigger
     * a rollback. The dispatcher should shutdown. */
    public void testRollbackFailure() throws InvalidConfigException
    {
        final Logger log = Logger.getLogger("TestGenericDispatcher.testRollbackFailure");
        //log.setLevel(Level.DEBUG);
        log.info("start");

        final TestGenericDispatcherEventBuffer eventsBuf =
            new TestGenericDispatcherEventBuffer(_generic100KBufferStaticConfig);
        eventsBuf.start(0);
        eventsBuf.startEvents();
        initBufferWithEvents(eventsBuf, 1, 100, (short)1, null, null);
        eventsBuf.endEvents(100L,null);

        RollbackFailingConsumer mockConsumer =
                new RollbackFailingConsumer(new StateVerifyingStreamConsumer(null), LOG, 2);

        List<String> sources = new ArrayList<String>();
        Map<Long, IdNamePair> sourcesMap = new HashMap<Long, IdNamePair>();
        for (int i = 1; i <= 3; ++i)
        {
            IdNamePair sourcePair = new IdNamePair((long)i, "source" + i);
            sources.add(sourcePair.getName());
            sourcesMap.put(sourcePair.getId(), sourcePair);
        }

        DatabusV2ConsumerRegistration consumerReg =
                new DatabusV2ConsumerRegistration(mockConsumer, sources, null);

        List<DatabusV2ConsumerRegistration> allRegistrations =
                Arrays.asList(consumerReg);
        MultiConsumerCallback callback =
                new MultiConsumerCallback(
                        allRegistrations,
                        Executors.newSingleThreadExecutor(),
                        1000,
                        new StreamConsumerCallbackFactory(null, null),
                        null,
                        null,
                        null,
                        null);
        callback.setSourceMap(sourcesMap);

        DatabusSourcesConnection.Config connCfgBuilder = new DatabusSourcesConnection.Config();
        connCfgBuilder.setConsumerParallelism(1);
        connCfgBuilder.setEventBuffer(_generic100KBufferConfig);
        connCfgBuilder.setFreeBufferThreshold(10000);
        connCfgBuilder.setConsumerTimeBudgetMs(1000);
        connCfgBuilder.setCheckpointThresholdPct(5.0);
        connCfgBuilder.getDispatcherRetries().setMaxRetryNum(10);

        DatabusSourcesConnection.StaticConfig connStaticCfg = connCfgBuilder.build();

        List<DatabusSubscription> subs = DatabusSubscription.createSubscriptionList(sources);
        final RelayDispatcher dispatcher =
                new RelayDispatcher("dispatcher", connStaticCfg, subs,
                        new InMemoryPersistenceProvider(),
                        eventsBuf, callback, null,null,null, null, null);

        Thread dispatcherThread = new Thread(dispatcher);
        //dispatcherThread.setDaemon(true);
        dispatcherThread.start();

        HashMap<Long, List<RegisterResponseEntry>> schemaMap =
                new HashMap<Long, List<RegisterResponseEntry>>();

        List<RegisterResponseEntry> l1 = new ArrayList<RegisterResponseEntry>();
        List<RegisterResponseEntry> l2 = new ArrayList<RegisterResponseEntry>();
        List<RegisterResponseEntry> l3 = new ArrayList<RegisterResponseEntry>();

        l1.add(new RegisterResponseEntry(1L, (short)1,SOURCE1_SCHEMA_STR));
        l2.add(new RegisterResponseEntry(2L, (short)1,SOURCE2_SCHEMA_STR));
        l3.add(new RegisterResponseEntry(3L, (short)1,SOURCE3_SCHEMA_STR));

        schemaMap.put(1L, l1);
        schemaMap.put(2L, l2);
        schemaMap.put(3L, l3);

        dispatcher.enqueueMessage(SourcesMessage.createSetSourcesIdsMessage(sourcesMap.values()));
        dispatcher.enqueueMessage(SourcesMessage.createSetSourcesSchemasMessage(schemaMap));

        try
        {
          TestUtil.assertWithBackoff(new ConditionCheck()
          {
            @Override
            public boolean check()
            {
              LOG.info(dispatcher.getStatus().getStatus().toString());
              LOG.info(String.valueOf(dispatcher.getStatus().getStatus() == DatabusComponentStatus.Status.SHUTDOWN));
              return dispatcher.getStatus().getStatus() == DatabusComponentStatus.Status.SHUTDOWN;
            }
          }, "dispatcher shtudown", 50000, LOG);

        }
        finally
        {
          dispatcher.shutdown();
        }
        verifyNoLocks(null, eventsBuf);
        log.info("end\n");
    }

    @Test(groups = {"small", "functional"})
    public void testMultiWindowsHappyPath()
    {
        final Logger log = Logger.getLogger("TestGenericDispatcher.testMultiWindowsHappyPath");
        //log.setLevel(Level.DEBUG);
        log.info("start");

        int source1EventsNum = 3;
        int source2EventsNum = 5;

        Hashtable<Long, AtomicInteger> keyCounts = new Hashtable<Long, AtomicInteger>();
        Hashtable<Short, AtomicInteger> srcidCounts = new Hashtable<Short, AtomicInteger>();


        int windowsNum = 3;

        final TestGenericDispatcherEventBuffer eventsBuf =
            new TestGenericDispatcherEventBuffer(_generic100KBufferStaticConfig);
        eventsBuf.start(0);

        int curEventNum = 1;
        for (int w = 0; w < windowsNum; ++w)
        {
            eventsBuf.startEvents();
            initBufferWithEvents(eventsBuf, curEventNum, source1EventsNum, (short)1, keyCounts, srcidCounts);
            curEventNum += source1EventsNum;
            initBufferWithEvents(eventsBuf, curEventNum, source2EventsNum, (short)2, keyCounts, srcidCounts);
            curEventNum += source2EventsNum;
            eventsBuf.endEvents(100L * (w + 1),null);
        }

        DatabusStreamConsumer mockConsumer =
                new EventCountingConsumer(new StateVerifyingStreamConsumer(null), keyCounts, srcidCounts);
        SelectingDatabusCombinedConsumer sdccMockConsumer = new SelectingDatabusCombinedConsumer(mockConsumer);

        List<String> sources = new ArrayList<String>();
        Map<Long, IdNamePair> sourcesMap = new HashMap<Long, IdNamePair>();
        for (int i = 1; i <= 3; ++i)
        {
            IdNamePair sourcePair = new IdNamePair((long)i, "source" + i);
            sources.add(sourcePair.getName());
            sourcesMap.put(sourcePair.getId(), sourcePair);
        }

        DatabusV2ConsumerRegistration consumerReg =
                new DatabusV2ConsumerRegistration(sdccMockConsumer, sources, null);

        List<DatabusV2ConsumerRegistration> allRegistrations =
                Arrays.asList(consumerReg);
        MultiConsumerCallback callback =
                new MultiConsumerCallback(
                        allRegistrations,
                        Executors.newSingleThreadExecutor(),
                        1000,
                        new StreamConsumerCallbackFactory(null, null),
                        null,
                        null,
                        null,
                        null);
        callback.setSourceMap(sourcesMap);

        List<DatabusSubscription> subs = DatabusSubscription.createSubscriptionList(sources);
        RelayDispatcher dispatcher =
                new RelayDispatcher("dispatcher", _genericRelayConnStaticConfig, subs,
                        new InMemoryPersistenceProvider(),
                        eventsBuf, callback, null,null,null,null, null);

        Thread dispatcherThread = new Thread(dispatcher);
        //dispatcherThread.setDaemon(true);
        dispatcherThread.start();

        HashMap<Long, List<RegisterResponseEntry>> schemaMap =
                new HashMap<Long, List<RegisterResponseEntry>>();

        List<RegisterResponseEntry> l1 = new ArrayList<RegisterResponseEntry>();
        List<RegisterResponseEntry> l2 = new ArrayList<RegisterResponseEntry>();
        List<RegisterResponseEntry> l3 = new ArrayList<RegisterResponseEntry>();

        l1.add(new RegisterResponseEntry(1L, (short)1,SOURCE1_SCHEMA_STR));
        l2.add(new RegisterResponseEntry(2L, (short)1,SOURCE2_SCHEMA_STR));
        l3.add(new RegisterResponseEntry(3L, (short)1,SOURCE3_SCHEMA_STR));

        schemaMap.put(1L, l1);
        schemaMap.put(2L, l2);
        schemaMap.put(3L, l3);

        dispatcher.enqueueMessage(SourcesMessage.createSetSourcesIdsMessage(sourcesMap.values()));
        dispatcher.enqueueMessage(SourcesMessage.createSetSourcesSchemasMessage(schemaMap));

        try
        {
            Thread.sleep(2000);
        }
        catch (InterruptedException ie) {}

        dispatcher.shutdown();

        for (long i = 1; i < curEventNum; ++i)
        {
            assertEquals("correct amount of callbacks for key " + i,
                    1, keyCounts.get(i).intValue());
        }

        assertEquals("correct amount of callbacks for srcid 1", windowsNum * source1EventsNum,
                srcidCounts.get((short)1).intValue());
        assertEquals("correct amount of callbacks for srcid 2", windowsNum * source2EventsNum,
                srcidCounts.get((short)2).intValue());
        verifyNoLocks(null, eventsBuf);
        log.info("end\n");
    }

    @Test(groups = {"small", "functional"})
    public void testDispatcherDiscrepancy()
    {
        final Logger log = Logger.getLogger("TestGenericDispatcher.testDispatcherDiscrepancy");
        log.setLevel(Level.INFO);
        log.info("start");
        final Level saveLevel = Logger.getLogger("com.linkedin.databus.client").getLevel();
        //Logger.getLogger("com.linkedin.databus.client").setLevel(Level.DEBUG);

        log.info("Creating sourcesMap");
        List<String> sources = new ArrayList<String>();
        Map<Long, IdNamePair> sourcesMap = new HashMap<Long, IdNamePair>();
        for (int i = 1; i <= 3; ++i)
        {
            IdNamePair sourcePair = new IdNamePair((long)i, "source" + i);
            sources.add(sourcePair.getName());
            sourcesMap.put(sourcePair.getId(), sourcePair);
        }
        HashMap<Long, List<RegisterResponseEntry>> schemaMap =
                new HashMap<Long, List<RegisterResponseEntry>>();

        List<RegisterResponseEntry> l1 = new ArrayList<RegisterResponseEntry>();
        List<RegisterResponseEntry> l2 = new ArrayList<RegisterResponseEntry>();
        List<RegisterResponseEntry> l3 = new ArrayList<RegisterResponseEntry>();

        l1.add(new RegisterResponseEntry(1L, (short)1,SOURCE1_SCHEMA_STR));

        // Not setting the ID for the schema
        RegisterResponseEntry re = new RegisterResponseEntry();
        re.setSchema(SOURCE2_SCHEMA_STR);
        re.setVersion((short)1);
        l2.add(re);

        // Should add the third schema correctly
        l3.add(new RegisterResponseEntry(3L, (short)1,SOURCE3_SCHEMA_STR));

        schemaMap.put(1L, l1);
        schemaMap.put(2L, l2);
        schemaMap.put(3L, l3);

        log.info("Switch to start dispatch events");
        DispatcherState ds = DispatcherState.create().addSources(sourcesMap.values());
        ds.getSchemaSet().clear();
        long initSize = 0, finalSize = 0;
        try
        {
            // the schemaSet inside DispatcherState is a static
            log.info("===Printing the decoder object's schema set\n" + ds.getEventDecoder().getSchemaSet());
            log.info("===Printing the decoder object's schema set basenames\n" + ds.getEventDecoder().getSchemaSet().getSchemaBaseNames());
            initSize = ds.getEventDecoder().getSchemaSet().size();
            log.info("initSize = " + initSize + " Schema base names = " + ds.getEventDecoder().getSchemaSet().getSchemaBaseNames());
        } catch (Exception e){}


        ds.addSchemas(schemaMap);
        log.info("Schemas have been refreshed");
        finalSize = ds.getEventDecoder().getSchemaSet().getSchemaBaseNames().size();
        log.info("===Printing the decoder object's schema set\n" + ds.getEventDecoder().getSchemaSet());
        log.info("===Printing the decoder object's schema set basenames\n" + ds.getEventDecoder().getSchemaSet().getSchemaBaseNames());
        Assert.assertEquals(finalSize, 2 + initSize);
        Logger.getLogger("com.linkedin.databus.client").setLevel(saveLevel);
        log.info("end\n");
    }

    @Test(groups = {"small", "functional"})
    public void testOneWindowTwoIndependentConsumersHappyPath()
    {
      final Logger log = Logger.getLogger("TestGenericDispatcher.testOneWindowTwoIndependentConsumersHappyPath");
      log.setLevel(Level.INFO);
      log.info("start");
      final Level saveLevel = Logger.getLogger("com.linkedin.databus.client").getLevel();
      //Logger.getLogger("com.linkedin.databus.client").setLevel(Level.DEBUG);

        final int source1EventsNum = 2;
        final int source2EventsNum = 2;

        final Hashtable<Long, AtomicInteger> keyCounts = new Hashtable<Long, AtomicInteger>();
        final Hashtable<Short, AtomicInteger> srcidCounts = new Hashtable<Short, AtomicInteger>();

        final TestGenericDispatcherEventBuffer eventsBuf =
            new TestGenericDispatcherEventBuffer(_generic100KBufferStaticConfig);
        eventsBuf.start(0);
        eventsBuf.startEvents();
        initBufferWithEvents(eventsBuf, 1, source1EventsNum, (short)1, keyCounts, srcidCounts);
        initBufferWithEvents(eventsBuf, 1 + source1EventsNum, source2EventsNum, (short)2, keyCounts,
                srcidCounts);
        eventsBuf.endEvents(100L);

        Hashtable<Long, AtomicInteger> keyCounts2 = new Hashtable<Long, AtomicInteger>();
        Hashtable<Short, AtomicInteger> srcidCounts2 = new Hashtable<Short, AtomicInteger>();
        for (Long key: keyCounts.keySet())
        {
            keyCounts2.put(key, new AtomicInteger(0));
        }
        for (Short srcid: srcidCounts.keySet())
        {
            srcidCounts2.put(srcid, new AtomicInteger(0));
        }

        DatabusStreamConsumer mockConsumer =
                new EventCountingConsumer(new StateVerifyingStreamConsumer(null), keyCounts, srcidCounts);
        DatabusStreamConsumer mockConsumer2 =
                new EventCountingConsumer(new StateVerifyingStreamConsumer(null), keyCounts2, srcidCounts2);

        SelectingDatabusCombinedConsumer sdccMockConsumer = new SelectingDatabusCombinedConsumer(mockConsumer);
        SelectingDatabusCombinedConsumer sdccMockConsumer2 = new SelectingDatabusCombinedConsumer(mockConsumer2);

        List<String> sources = new ArrayList<String>();
        Map<Long, IdNamePair> sourcesMap = new HashMap<Long, IdNamePair>();
        for (int i = 1; i <= 3; ++i)
        {
            IdNamePair sourcePair = new IdNamePair((long)i, "source" + i);
            sources.add(sourcePair.getName());
            sourcesMap.put(sourcePair.getId(), sourcePair);
        }

        DatabusV2ConsumerRegistration consumerReg =
                new DatabusV2ConsumerRegistration(sdccMockConsumer, sources, null);

        DatabusV2ConsumerRegistration consumer2Reg =
                new DatabusV2ConsumerRegistration(sdccMockConsumer2, sources, null);

        List<DatabusV2ConsumerRegistration> allRegistrations =
                Arrays.asList(consumerReg, consumer2Reg);
        MultiConsumerCallback callback =
                new MultiConsumerCallback(
                        allRegistrations,
                        Executors.newFixedThreadPool(2),
                        1000,
                        new StreamConsumerCallbackFactory(null, null),
                        null,
                        null,
                        null,
                        null);
        callback.setSourceMap(sourcesMap);

        List<DatabusSubscription> subs = DatabusSubscription.createSubscriptionList(sources);
        final RelayDispatcher dispatcher =
                new RelayDispatcher("dispatcher", _genericRelayConnStaticConfig, subs,
                        new InMemoryPersistenceProvider(),
                        eventsBuf, callback, null,null,null,null, null);

        Thread dispatcherThread = new Thread(dispatcher);
        dispatcherThread.setDaemon(true);
        log.info("starting dispatcher thread");
        dispatcherThread.start();

        HashMap<Long, List<RegisterResponseEntry>> schemaMap =
                new HashMap<Long, List<RegisterResponseEntry>>();

        List<RegisterResponseEntry> l1 = new ArrayList<RegisterResponseEntry>();
        List<RegisterResponseEntry> l2 = new ArrayList<RegisterResponseEntry>();
        List<RegisterResponseEntry> l3 = new ArrayList<RegisterResponseEntry>();

        l1.add(new RegisterResponseEntry(1L, (short)1,SOURCE1_SCHEMA_STR));
        l2.add(new RegisterResponseEntry(2L, (short)1,SOURCE2_SCHEMA_STR));
        l3.add(new RegisterResponseEntry(3L, (short)1,SOURCE3_SCHEMA_STR));

        schemaMap.put(1L, l1);
        schemaMap.put(2L, l2);
        schemaMap.put(3L, l3);

        dispatcher.enqueueMessage(SourcesMessage.createSetSourcesIdsMessage(sourcesMap.values()));
        dispatcher.enqueueMessage(SourcesMessage.createSetSourcesSchemasMessage(schemaMap));

        log.info("starting event dispatch");

        TestUtil.assertWithBackoff(new ConditionCheck()
        {
          @Override
          public boolean check()
          {
            return null != dispatcher.getDispatcherState().getEventsIterator() &&
                   !dispatcher.getDispatcherState().getEventsIterator().hasNext();
          }
        }, "all events processed", 5000, log);
        dispatcher.shutdown();


        log.info("all events processed");
        for (long i = 1; i <= source1EventsNum + source2EventsNum; ++i)
        {
            assertEquals("correct amount of callbacks for key " + i, 1, keyCounts.get(i).intValue());
            assertEquals("correct amount of callbacks for key " + i, 1, keyCounts2.get(i).intValue());
        }

        assertEquals("correct amount of callbacks for srcid 1", source1EventsNum,
                srcidCounts.get((short)1).intValue());
        assertEquals("correct amount of callbacks for srcid 2", source2EventsNum,
                srcidCounts.get((short)2).intValue());
        assertEquals("correct amount of callbacks for srcid 1", source1EventsNum,
                srcidCounts2.get((short)1).intValue());
        assertEquals("correct amount of callbacks for srcid 2", source2EventsNum,
                srcidCounts2.get((short)2).intValue());

        verifyNoLocks(null, eventsBuf);

        Logger.getLogger("com.linkedin.databus.client").setLevel(saveLevel);
        log.info("end\n");
    }

    @Test(groups = {"small", "functional"})
    public void testOneWindowTwoGroupedConsumersHappyPath()
    {
      final Logger log = Logger.getLogger("TestGenericDispatcher.testOneWindowTwoGroupedConsumersHappyPath");
      log.info("start");
        int source1EventsNum = 2;
        int source2EventsNum = 2;

        Hashtable<Long, AtomicInteger> keyCounts = new Hashtable<Long, AtomicInteger>();
        Hashtable<Short, AtomicInteger> srcidCounts = new Hashtable<Short, AtomicInteger>();

        final TestGenericDispatcherEventBuffer eventsBuf =
            new TestGenericDispatcherEventBuffer(_generic100KBufferStaticConfig);
        eventsBuf.start(0);
        eventsBuf.startEvents();
        initBufferWithEvents(eventsBuf, 1, source1EventsNum, (short)1, keyCounts, srcidCounts);
        initBufferWithEvents(eventsBuf, 1 + source1EventsNum, source2EventsNum, (short)2, keyCounts,
                srcidCounts);
        eventsBuf.endEvents(100L);

        DatabusStreamConsumer mockConsumer =
                new EventCountingConsumer(new StateVerifyingStreamConsumer(null), keyCounts, srcidCounts);
        DatabusStreamConsumer mockConsumer2 =
                new EventCountingConsumer(new StateVerifyingStreamConsumer(null), keyCounts, srcidCounts);

        DatabusCombinedConsumer sdccMockConsumer = new SelectingDatabusCombinedConsumer(mockConsumer);
        DatabusCombinedConsumer sdccMockConsumer2 = new SelectingDatabusCombinedConsumer(mockConsumer2);

        List<String> sources = new ArrayList<String>();
        Map<Long, IdNamePair> sourcesMap = new HashMap<Long, IdNamePair>();
        for (int i = 1; i <= 3; ++i)
        {
            IdNamePair sourcePair = new IdNamePair((long)i, "source" + i);
            sources.add(sourcePair.getName());
            sourcesMap.put(sourcePair.getId(), sourcePair);
        }

        DatabusV2ConsumerRegistration consumerReg =
                new DatabusV2ConsumerRegistration(Arrays.asList(sdccMockConsumer, sdccMockConsumer2),
                        sources, null);

        List<DatabusV2ConsumerRegistration> allRegistrations =
                Arrays.asList(consumerReg);
        MultiConsumerCallback callback =
                new MultiConsumerCallback(
                        allRegistrations,
                        Executors.newFixedThreadPool(2),
                        1000,
                        new StreamConsumerCallbackFactory(null, null),
                        null,
                        null,
                        null,
                        null);
        callback.setSourceMap(sourcesMap);

        List<DatabusSubscription> subs = DatabusSubscription.createSubscriptionList(sources);
        RelayDispatcher dispatcher =
                new RelayDispatcher("dispatcher", _genericRelayConnStaticConfig, subs,
                        new InMemoryPersistenceProvider(),
                        eventsBuf, callback, null, null,null,null, null);

        Thread dispatcherThread = new Thread(dispatcher);
        //dispatcherThread.setDaemon(true);
        dispatcherThread.start();

        HashMap<Long, List<RegisterResponseEntry>> schemaMap =
                new HashMap<Long, List<RegisterResponseEntry>>();

        List<RegisterResponseEntry> l1 = new ArrayList<RegisterResponseEntry>();
        List<RegisterResponseEntry> l2 = new ArrayList<RegisterResponseEntry>();
        List<RegisterResponseEntry> l3 = new ArrayList<RegisterResponseEntry>();

        l1.add(new RegisterResponseEntry(1L, (short)1,SOURCE1_SCHEMA_STR));
        l2.add(new RegisterResponseEntry(2L, (short)1,SOURCE2_SCHEMA_STR));
        l3.add(new RegisterResponseEntry(3L, (short)1,SOURCE3_SCHEMA_STR));

        schemaMap.put(1L, l1);
        schemaMap.put(2L, l2);
        schemaMap.put(3L, l3);

        dispatcher.enqueueMessage(SourcesMessage.createSetSourcesIdsMessage(sourcesMap.values()));
        dispatcher.enqueueMessage(SourcesMessage.createSetSourcesSchemasMessage(schemaMap));

        try
        {
            Thread.sleep(2000);
        }
        catch (InterruptedException ie) {}

        dispatcher.shutdown();

        for (long i = 1; i <= source1EventsNum + source2EventsNum; ++i)
        {
            assertEquals("correct amount of callbacks for key " + i, 1, keyCounts.get(i).intValue());
        }

        assertEquals("correct amount of callbacks for srcid 1", source1EventsNum,
                srcidCounts.get((short)1).intValue());
        assertEquals("correct amount of callbacks for srcid 2", source2EventsNum,
                srcidCounts.get((short)2).intValue());
        verifyNoLocks(null, eventsBuf);
        log.info("end\n");
    }

    @Test(groups = {"small", "functional"})
    public void testTwoWindowEventCallbackFailure()
    {
      final Logger log = Logger.getLogger("TestGenericDispatcher.testTwoWindowEventCallbackFailure");
      log.info("start");
        int source1EventsNum = 2;
        int source2EventsNum = 2;

        Hashtable<Long, AtomicInteger> keyCounts = new Hashtable<Long, AtomicInteger>();
        Hashtable<Short, AtomicInteger> srcidCounts = new Hashtable<Short, AtomicInteger>();

        final TestGenericDispatcherEventBuffer eventsBuf =
            new TestGenericDispatcherEventBuffer(_generic100KBufferStaticConfig);
        eventsBuf.start(0);
        eventsBuf.startEvents();
        initBufferWithEvents(eventsBuf, 1, source1EventsNum, (short)1, keyCounts, srcidCounts);
        eventsBuf.endEvents(100L);
        eventsBuf.startEvents();
        initBufferWithEvents(eventsBuf, 1 + source1EventsNum, source2EventsNum, (short)2, keyCounts, srcidCounts);
        eventsBuf.endEvents(200L);

        DatabusStreamConsumer mockConsumer =
                new EventCountingConsumer(
                        new StateVerifyingStreamConsumer(
                                new DataEventFailingStreamConsumer((short)2)), keyCounts, srcidCounts);
        SelectingDatabusCombinedConsumer sdccMockConsumer = new SelectingDatabusCombinedConsumer(mockConsumer);

        List<String> sources = new ArrayList<String>();
        Map<Long, IdNamePair> sourcesMap = new HashMap<Long, IdNamePair>();
        for (int i = 1; i <= 3; ++i)
        {
            IdNamePair sourcePair = new IdNamePair((long)i, "source" + i);
            sources.add(sourcePair.getName());
            sourcesMap.put(sourcePair.getId(), sourcePair);
        }

        DatabusV2ConsumerRegistration consumerReg =
                new DatabusV2ConsumerRegistration(sdccMockConsumer, sources, null);

        List<DatabusV2ConsumerRegistration> allRegistrations =
                Arrays.asList(consumerReg);
        MultiConsumerCallback callback =
                new MultiConsumerCallback(
                        allRegistrations,
                        Executors.newSingleThreadExecutor(),
                        1000,
                        new StreamConsumerCallbackFactory(null, null),
                        null,
                        null,
                        null,
                        null);
        callback.setSourceMap(sourcesMap);

        List<DatabusSubscription> subs = DatabusSubscription.createSubscriptionList(sources);
        RelayDispatcher dispatcher =
                new RelayDispatcher("dispatcher", _genericRelayConnStaticConfig, subs,
                        new InMemoryPersistenceProvider(),
                        eventsBuf, callback, null,null,null,null, null);

        Thread dispatcherThread = new Thread(dispatcher);
        //dispatcherThread.setDaemon(true);
        dispatcherThread.start();

        HashMap<Long, List<RegisterResponseEntry>> schemaMap =
                new HashMap<Long, List<RegisterResponseEntry>>();

        List<RegisterResponseEntry> l1 = new ArrayList<RegisterResponseEntry>();
        List<RegisterResponseEntry> l2 = new ArrayList<RegisterResponseEntry>();
        List<RegisterResponseEntry> l3 = new ArrayList<RegisterResponseEntry>();

        l1.add(new RegisterResponseEntry(1L, (short)1,SOURCE1_SCHEMA_STR));
        l2.add(new RegisterResponseEntry(2L, (short)1,SOURCE2_SCHEMA_STR));
        l3.add(new RegisterResponseEntry(3L, (short)1,SOURCE3_SCHEMA_STR));

        schemaMap.put(1L, l1);
        schemaMap.put(2L, l2);
        schemaMap.put(3L, l3);

        dispatcher.enqueueMessage(SourcesMessage.createSetSourcesIdsMessage(sourcesMap.values()));
        dispatcher.enqueueMessage(SourcesMessage.createSetSourcesSchemasMessage(schemaMap));

        try
        {
            Thread.sleep(2000);
        }
        catch (InterruptedException ie) {}

        dispatcher.shutdown();

        for (long i = 1; i <= source1EventsNum; ++i)
        {
            assertEquals("correct amount of callbacks for key " + i,
                    1, keyCounts.get(i).intValue());
        }

        for (long i = source2EventsNum + 1; i <= source1EventsNum + source2EventsNum; ++i)
        {
            assert keyCounts.get(1L + source1EventsNum).intValue() > 1 :
                "correct amount of callbacks for key " + i + ":" + keyCounts.get(i).intValue();
        }
        verifyNoLocks(null, eventsBuf);
        log.info("end\n");
    }


    @Test(groups = {"small", "functional"})
    public void testTwoWindowEventIndependentConsumersCallbackFailure()
    {
      final Logger log = Logger.getLogger("TestGenericDispatcher.testTwoWindowEventIndependentConsumersCallbackFailure");
      log.info("start");
        int source1EventsNum = 4;
        int source2EventsNum = 5;

        Hashtable<Long, AtomicInteger> keyCounts = new Hashtable<Long, AtomicInteger>();
        Hashtable<Short, AtomicInteger> srcidCounts = new Hashtable<Short, AtomicInteger>();

        final TestGenericDispatcherEventBuffer eventsBuf =
            new TestGenericDispatcherEventBuffer(_generic100KBufferStaticConfig);
        eventsBuf.start(0);
        eventsBuf.startEvents();
        initBufferWithEvents(eventsBuf, 1, source1EventsNum, (short)1, keyCounts, srcidCounts);
        eventsBuf.endEvents(100L);
        eventsBuf.startEvents();
        initBufferWithEvents(eventsBuf, 1 + source1EventsNum, source2EventsNum, (short)2, keyCounts, srcidCounts);
        eventsBuf.endEvents(200L);

        DatabusStreamConsumer mockConsumer =
                new EventCountingConsumer(
                        new StateVerifyingStreamConsumer(
                                new DataSourceFailingStreamConsumer("source2")),
                                keyCounts, srcidCounts);
        SelectingDatabusCombinedConsumer sdccMockConsumer = new SelectingDatabusCombinedConsumer(mockConsumer);

        Hashtable<Long, AtomicInteger> keyCounts2 = new Hashtable<Long, AtomicInteger>();
        Hashtable<Short, AtomicInteger> srcidCounts2 = new Hashtable<Short, AtomicInteger>();
        for (Long key: keyCounts.keySet())
        {
            keyCounts2.put(key, new AtomicInteger(0));
        }
        for (Short srcid: srcidCounts.keySet())
        {
            srcidCounts2.put(srcid, new AtomicInteger(0));
        }

        DatabusStreamConsumer mockConsumer2 =
                new EventCountingConsumer(new StateVerifyingStreamConsumer(null),
                        keyCounts2, srcidCounts2);
        SelectingDatabusCombinedConsumer sdccMockConsumer2 = new SelectingDatabusCombinedConsumer(mockConsumer2);

        List<String> sources = new ArrayList<String>();
        Map<Long, IdNamePair> sourcesMap = new HashMap<Long, IdNamePair>();
        for (int i = 1; i <= 3; ++i)
        {
            IdNamePair sourcePair = new IdNamePair((long)i, "source" + i);
            sources.add(sourcePair.getName());
            sourcesMap.put(sourcePair.getId(), sourcePair);
        }

        DatabusV2ConsumerRegistration consumerReg =
                new DatabusV2ConsumerRegistration(sdccMockConsumer, sources, null);

        DatabusV2ConsumerRegistration consumerReg2 =
                new DatabusV2ConsumerRegistration(sdccMockConsumer2, sources, null);
        List<DatabusV2ConsumerRegistration> allRegistrations =
                Arrays.asList(consumerReg, consumerReg2);
        MultiConsumerCallback callback =
                new MultiConsumerCallback(
                        allRegistrations,
                        Executors.newSingleThreadExecutor(),
                        1000,
                        new StreamConsumerCallbackFactory(null, null),
                        null,
                        null,
                        null,
                        null);
        callback.setSourceMap(sourcesMap);

        List<DatabusSubscription> subs = DatabusSubscription.createSubscriptionList(sources);
        RelayDispatcher dispatcher =
                new RelayDispatcher("dispatcher", _genericRelayConnStaticConfig, subs,
                        new InMemoryPersistenceProvider(),
                        eventsBuf, callback, null,null,null,null, null);

        Thread dispatcherThread = new Thread(dispatcher);
        //dispatcherThread.setDaemon(true);
        dispatcherThread.start();

        HashMap<Long, List<RegisterResponseEntry>> schemaMap =
                new HashMap<Long, List<RegisterResponseEntry>>();

        List<RegisterResponseEntry> l1 = new ArrayList<RegisterResponseEntry>();
        List<RegisterResponseEntry> l2 = new ArrayList<RegisterResponseEntry>();
        List<RegisterResponseEntry> l3 = new ArrayList<RegisterResponseEntry>();

        l1.add(new RegisterResponseEntry(1L, (short)1,SOURCE1_SCHEMA_STR));
        l2.add(new RegisterResponseEntry(2L, (short)1,SOURCE2_SCHEMA_STR));
        l3.add(new RegisterResponseEntry(3L, (short)1,SOURCE3_SCHEMA_STR));

        schemaMap.put(1L, l1);
        schemaMap.put(2L, l2);
        schemaMap.put(3L, l3);

        dispatcher.enqueueMessage(SourcesMessage.createSetSourcesIdsMessage(sourcesMap.values()));
        dispatcher.enqueueMessage(SourcesMessage.createSetSourcesSchemasMessage(schemaMap));

        try
        {
            Thread.sleep(2000);
        }
        catch (InterruptedException ie) {}

        dispatcher.shutdown();

        for (long i = 1; i <= source1EventsNum; ++i)
        {
            assertEquals("correct amount of callbacks for key " + i,
                    1, keyCounts.get(i).intValue());
            assertEquals("correct amount of callbacks for key " + i,
                    1, keyCounts2.get(i).intValue());
        }

        for (long i = source2EventsNum + 1; i <= source1EventsNum + source2EventsNum; ++i)
        {
            assert keyCounts.get(1L + source1EventsNum).intValue() == 0 :
                "correct amount of callbacks for key " + i + ":" + keyCounts.get(i).intValue();
        }
        verifyNoLocks(null, eventsBuf);
        log.info("end\n");
    }


    @Test(groups = {"small", "functional"})
    public void testLargeWindowCheckpointFrequency() throws Exception
    {
      final Logger log = Logger.getLogger("TestGenericDispatcher.testLargeWindowCheckpointFrequency");
      log.info("start");
            /* Consumer creation */
            int timeTakenForEventInMs = 1;
            DatabusStreamConsumer tConsumer = new TimeoutTestConsumer(timeTakenForEventInMs);
            DatabusCombinedConsumer sdccTConsumer = new SelectingDatabusCombinedConsumer(tConsumer);
            HashMap<Long, List<RegisterResponseEntry>> schemaMap =
                    new HashMap<Long, List<RegisterResponseEntry>>();

            short srcId=1;
            List<RegisterResponseEntry> l1 = new ArrayList<RegisterResponseEntry>();
            l1.add(new RegisterResponseEntry(1L, srcId,SOURCE1_SCHEMA_STR));

            schemaMap.put(1L, l1);

            Map<Long, IdNamePair> sourcesMap = new HashMap<Long, IdNamePair>();
            List<String> sources = new ArrayList<String>();
            for (int i = 1; i <= 1; ++i)
            {
                IdNamePair sourcePair = new IdNamePair((long)i, "source" + i);
                sources.add(sourcePair.getName());
                sourcesMap.put(sourcePair.getId(), sourcePair);
            }

            DatabusV2ConsumerRegistration consumerReg = new DatabusV2ConsumerRegistration(sdccTConsumer, sources, null);
            List<DatabusV2ConsumerRegistration> allRegistrations =  Arrays.asList(consumerReg);
            MultiConsumerCallback mConsumer = new MultiConsumerCallback(allRegistrations,Executors.newFixedThreadPool(2),
                    1000, new StreamConsumerCallbackFactory(null, null), null, null, null, null);

            /* Source configuration */
            double thresholdChkptPct = 50.0;
            DatabusSourcesConnection.Config conf = new DatabusSourcesConnection.Config();
            conf.setCheckpointThresholdPct(thresholdChkptPct);
            DatabusSourcesConnection.StaticConfig connConfig = conf.build();

            /* Generate events **/
            Vector<DbusEvent> srcTestEvents = new Vector<DbusEvent>();
            Vector<Short> srcIdList = new Vector<Short> ();
            srcIdList.add(srcId);
            int numEvents = 100;
            int payloadSize = 20;
            int maxWindowSize = 80;
            DbusEventGenerator evGen = new DbusEventGenerator(0,srcIdList);
            Assert.assertTrue(evGen.generateEvents(numEvents, maxWindowSize, 512, payloadSize, srcTestEvents) > 0);

            int size=0;
            for (DbusEvent e : srcTestEvents)
            {
                if (!e.isControlMessage()) {
                    size = e.size();
                    break;
                }
            }

            //make buffer large enough to hold data
            int producerBufferSize = (int) (numEvents*size*1.1);
            int individualBufferSize = producerBufferSize;
            int indexSize = producerBufferSize / 10;
            int stagingBufferSize = producerBufferSize;

            /*Event Buffer creation */
            final TestGenericDispatcherEventBuffer dataEventsBuffer=
                new TestGenericDispatcherEventBuffer(
                    getConfig(producerBufferSize, individualBufferSize, indexSize ,
                              stagingBufferSize, AllocationPolicy.HEAP_MEMORY,QueuePolicy.BLOCK_ON_WRITE));

            List<DatabusSubscription> subs = DatabusSubscription.createSubscriptionList(sources);
            /* Generic Dispatcher creation */
            TestDispatcher<DatabusCombinedConsumer> dispatcher = new TestDispatcher<DatabusCombinedConsumer>("freqCkpt",
                    connConfig,
                    subs,
                    new InMemoryPersistenceProvider(),
                    dataEventsBuffer,
                    mConsumer,
                    true);

            /* Launch writer */
            DbusEventAppender eventProducer = new DbusEventAppender(srcTestEvents, dataEventsBuffer, null) ;
            Thread tEmitter = new Thread(eventProducer);
            tEmitter.start();
            tEmitter.join();

            /* Launch dispatcher */
            Thread tDispatcher = new Thread(dispatcher);
            tDispatcher.start();

            /* Now initialize this damn state machine */
            dispatcher.enqueueMessage(SourcesMessage.createSetSourcesIdsMessage(sourcesMap.values()));
            dispatcher.enqueueMessage(SourcesMessage.createSetSourcesSchemasMessage(schemaMap));

            Thread.sleep(2000);

            System.out.println("Number of checkpoints = " + dispatcher.getNumCheckPoints());
            Assert.assertTrue(dispatcher.getNumCheckPoints()==3);
            dispatcher.shutdown();
            verifyNoLocks(null, dataEventsBuffer);
        log.info("end\n");
    }

    @Test(groups = {"small", "functional"})
    public void testControlEventsRemoval() throws Exception
    {
      final Logger log = Logger.getLogger("TestGenericDispatcher.testControlEventsRemoval");
      log.info("start");
        //DDSDBUS-559
            /* Consumer creation */
            int timeTakenForEventInMs = 10;
            TimeoutTestConsumer tConsumer = new TimeoutTestConsumer(timeTakenForEventInMs);
            HashMap<Long, List<RegisterResponseEntry>> schemaMap =
                    new HashMap<Long, List<RegisterResponseEntry>>();

            short srcId=1;
            List<RegisterResponseEntry> l1 = new ArrayList<RegisterResponseEntry>();
            l1.add(new RegisterResponseEntry(1L, srcId,SOURCE1_SCHEMA_STR));

            schemaMap.put(1L, l1);

            Map<Long, IdNamePair> sourcesMap = new HashMap<Long, IdNamePair>();
            List<String> sources = new ArrayList<String>();
            for (int i = 1; i <= 1; ++i)
            {
                IdNamePair sourcePair = new IdNamePair((long)i, "source" + i);
                sources.add(sourcePair.getName());
                sourcesMap.put(sourcePair.getId(), sourcePair);
            }

            DatabusV2ConsumerRegistration consumerReg = new DatabusV2ConsumerRegistration(tConsumer, sources, null);
            List<DatabusV2ConsumerRegistration> allRegistrations =  Arrays.asList(consumerReg);
            MultiConsumerCallback mConsumer = new MultiConsumerCallback(allRegistrations,Executors.newFixedThreadPool(2),
                    1000, new StreamConsumerCallbackFactory(null, null), null, null, null, null);

            /* Source configuration */
            double thresholdChkptPct = 10.0;
            DatabusSourcesConnection.Config conf = new DatabusSourcesConnection.Config();
            conf.setCheckpointThresholdPct(thresholdChkptPct);
            int freeBufferThreshold = conf.getFreeBufferThreshold();
            DatabusSourcesConnection.StaticConfig connConfig = conf.build();

            /* Generate events **/
            Vector<DbusEvent> srcTestEvents = new Vector<DbusEvent>();
            Vector<Short> srcIdList = new Vector<Short> ();
            srcIdList.add(srcId);
            int numEvents = 100;
            int payloadSize = 20;
            int maxWindowSize =1;
            DbusEventGenerator evGen = new DbusEventGenerator(0,srcIdList);
            Assert.assertTrue(evGen.generateEvents(numEvents, maxWindowSize, payloadSize+62, payloadSize, srcTestEvents) > 0);

            long lastWindowScn = srcTestEvents.get(srcTestEvents.size()-1).sequence();
            int size = 0;
            for (DbusEvent e : srcTestEvents)
            {
                if (e.size() > size) size = e.size();
            }

            //make buffer large enough to hold data
            int numWindows = (numEvents/maxWindowSize) + 1;
            int producerBufferSize = (numEvents+numWindows)*size + freeBufferThreshold;
            int individualBufferSize = producerBufferSize;
            int indexSize = producerBufferSize / 10;
            int stagingBufferSize = producerBufferSize;

            /*Event Buffer creation */
            final TestGenericDispatcherEventBuffer dataEventsBuffer=
                new TestGenericDispatcherEventBuffer(
                    getConfig(producerBufferSize, individualBufferSize, indexSize ,
                              stagingBufferSize, AllocationPolicy.HEAP_MEMORY,
                              QueuePolicy.BLOCK_ON_WRITE));

            List<DatabusSubscription> subs = DatabusSubscription.createSubscriptionList(sources);
            /* Generic Dispatcher creation */
            TestDispatcher<DatabusCombinedConsumer> dispatcher = new TestDispatcher<DatabusCombinedConsumer>("freqCkpt",
                    connConfig,
                    subs,
                    new InMemoryPersistenceProvider(),
                    dataEventsBuffer,
                    mConsumer,
                    true);

            /* Launch writer */
            /* write events all of which  are empty windows */
            DbusEventAppender eventProducer = new DbusEventAppender(srcTestEvents, dataEventsBuffer, null,1.0,true,0) ;
            Thread tEmitter = new Thread(eventProducer);
            tEmitter.start();
            tEmitter.join();
            long freeSpaceBefore = dataEventsBuffer.getBufferFreeSpace();
            /* Launch dispatcher */
            Thread tDispatcher = new Thread(dispatcher);
            tDispatcher.start();

            /* Now initialize  state machine */
            dispatcher.enqueueMessage(SourcesMessage.createSetSourcesIdsMessage(sourcesMap.values()));
            dispatcher.enqueueMessage(SourcesMessage.createSetSourcesSchemasMessage(schemaMap));

            tDispatcher.join(5000);
            LOG.warn("Free Space After=" + dataEventsBuffer.getBufferFreeSpace() +
                     " tConsumer=" + tConsumer + " expected last window=" + lastWindowScn +
                     " last Window = " + dataEventsBuffer.lastWrittenScn());

            Assert.assertTrue(dataEventsBuffer.lastWrittenScn()==lastWindowScn);
            Assert.assertTrue(freeSpaceBefore < dataEventsBuffer.getBufferFreeSpace());

            dispatcher.shutdown();
            verifyNoLocks(null, dataEventsBuffer);
        log.info("end\n");
    }


    protected void runDispatcherRollback(int numEvents,int maxWindowSize,int numFailDataEvent,int numFailCheckpointEvent,int numFailEndWindow)
    throws Exception
    {
        runDispatcherRollback(numEvents, maxWindowSize, numFailDataEvent, numFailCheckpointEvent, numFailEndWindow,90.0,false,1,0,1,1,false);
    }

    protected void runDispatcherRollback(int numEvents,int maxWindowSize,int numFailDataEvent,int numFailCheckpointEvent,int numFailEndWindow,double pct)
    throws Exception
    {
        runDispatcherRollback(numEvents, maxWindowSize, numFailDataEvent, numFailCheckpointEvent, numFailEndWindow,pct,false,1,0,1,1,false);
    }

    protected void runDispatcherRollback(int numEvents,int maxWindowSize,int numFailDataEvent,int numFailCheckpointEvent,int numFailEndWindow,double
            thresholdPct,boolean negativeTest,int numFailures,int bootstrapCheckpointsPerWindow) throws Exception
    {
        runDispatcherRollback(numEvents, maxWindowSize, numFailDataEvent,
                              numFailCheckpointEvent, numFailEndWindow,thresholdPct,negativeTest,
                              numFailures,bootstrapCheckpointsPerWindow,1,1,false);
    }



    /**
     *
     * @param numEvents  number of events that will be written out in the test
     * @param maxWindowSize  size of window expressed as #events
     * @param numFailDataEvent  the nth data event at which failure occurs; 0 == no failures
     * @param numFailCheckpointEvent  the nth checkpoint event at which failure occurs; 0 == no failures
     * @param numFailEndWindow  the nth end-of-window at which failure occurs; 0 == no failures
     * @param thresholdPct  checkpointThresholdPct - forcible checkpoint before end-of-window
     * @param negativeTest  is this test supposed to fail
     * @param numFailures  number of failures expected (across all error types); in effect controls number of rollbacks
     * @param bootstrapCheckpointsPerWindow  k bootstrap checkpoint events are written for every one end-of-window event
     * @param timeTakenForDataEventInMs  time taken for processing data events
     * @param timeTakenForControlEventInMs  time taken for processing control events
     * @param wrapAround  use a smaller producer buffer so that events will wrap around
     */
    protected void runDispatcherRollback(int numEvents,int maxWindowSize,int numFailDataEvent,int numFailCheckpointEvent,int numFailEndWindow,double
            thresholdPct,boolean negativeTest,int numFailures,int bootstrapCheckpointsPerWindow,
            long timeTakenForDataEventInMs,long timeTakenForControlEventInMs,boolean wrapAround) throws Exception
    {
            LOG.info("Running dispatcher rollback with: " + "numEvents=" + numEvents + " maxWindowSize=" + maxWindowSize
                    + " numFailDataEvent=" + numFailDataEvent + " numFailCheckpoint=" + numFailCheckpointEvent
                    + " numFailEndWindow=" + numFailEndWindow + " thresholdPct=" + thresholdPct
                    + " negativeTest=" + negativeTest + " numFailures=" + numFailures
                    + " bootstrapCheckpointsPerWindow=" + bootstrapCheckpointsPerWindow
                    + " timeTakenForDataEventsInMs=" + timeTakenForDataEventInMs
                    + " timeTakenForControlEventsInMs=" + timeTakenForControlEventInMs + " wrapAround=" + wrapAround);
            /* Experiment setup */
            int payloadSize = 20;
            int numCheckpoints = numEvents/maxWindowSize;

            /* Consumer creation */
            // set up consumer to fail on data callback at the nth event
            TimeoutTestConsumer tConsumer = new TimeoutTestConsumer(timeTakenForDataEventInMs,
                                                                    timeTakenForControlEventInMs,
                                                                    numFailCheckpointEvent,
                                                                    numFailDataEvent,
                                                                    numFailEndWindow,
                                                                    numFailures);

            HashMap<Long, List<RegisterResponseEntry>> schemaMap =
                    new HashMap<Long, List<RegisterResponseEntry>>();

            short srcId=1;
            List<RegisterResponseEntry> l1 = new ArrayList<RegisterResponseEntry>();
            l1.add(new RegisterResponseEntry(1L, srcId,SOURCE1_SCHEMA_STR));

            schemaMap.put(1L, l1);

            Map<Long, IdNamePair> sourcesMap = new HashMap<Long, IdNamePair>();
            List<String> sources = new ArrayList<String>();
            for (int i = 1; i <= 1; ++i)
            {
                IdNamePair sourcePair = new IdNamePair((long)i, "source" + i);
                sources.add(sourcePair.getName());
                sourcesMap.put(sourcePair.getId(), sourcePair);
            }

            long consumerTimeBudgetMs = 60*1000;
            DatabusV2ConsumerRegistration consumerReg = new DatabusV2ConsumerRegistration(tConsumer, sources, null);
            List<DatabusV2ConsumerRegistration> allRegistrations =  Arrays.asList(consumerReg);
            final UnifiedClientStats unifiedStats = new UnifiedClientStats(0, "test", "test.unified");
            // single-threaded execution of consumer
            MultiConsumerCallback mConsumer =
                new MultiConsumerCallback(allRegistrations,
                                          Executors.newFixedThreadPool(1),
                                          consumerTimeBudgetMs,
                                          new StreamConsumerCallbackFactory(null, unifiedStats),
                                          null,
                                          unifiedStats,
                                          null,
                                          null);

            /* Generate events */
            Vector<DbusEvent> srcTestEvents = new Vector<DbusEvent>();
            Vector<Short> srcIdList = new Vector<Short> ();
            srcIdList.add(srcId);

            DbusEventGenerator evGen = new DbusEventGenerator(0,srcIdList);
            Assert.assertTrue(evGen.generateEvents(numEvents, maxWindowSize, 512, payloadSize, srcTestEvents) > 0);

            int totalSize=0;
            int maxSize=0;
            for (DbusEvent e : srcTestEvents)
            {
                totalSize += e.size();
                maxSize = (e.size() > maxSize) ? e.size():maxSize;
            }

            /* Source configuration */
            double thresholdChkptPct = thresholdPct;
            DatabusSourcesConnection.Config conf = new DatabusSourcesConnection.Config();
            conf.setCheckpointThresholdPct(thresholdChkptPct);
            conf.getDispatcherRetries().setMaxRetryNum(10);
            conf.setFreeBufferThreshold(maxSize);
            conf.setConsumerTimeBudgetMs(consumerTimeBudgetMs);
            int freeBufferThreshold = conf.getFreeBufferThreshold();
            DatabusSourcesConnection.StaticConfig connConfig = conf.build();

            // make buffer large enough to hold data; the control events are large that contain checkpoints
            int producerBufferSize = wrapAround ? totalSize : totalSize*2 + numCheckpoints*10*maxSize*5 + freeBufferThreshold;
            int individualBufferSize = producerBufferSize;
            int indexSize = producerBufferSize / 10;
            int stagingBufferSize = producerBufferSize;

            /* Event Buffer creation */
            TestGenericDispatcherEventBuffer dataEventsBuffer=
                new TestGenericDispatcherEventBuffer(
                    getConfig(producerBufferSize, individualBufferSize, indexSize ,
                              stagingBufferSize, AllocationPolicy.HEAP_MEMORY,
                              QueuePolicy.BLOCK_ON_WRITE));

            List<DatabusSubscription> subs = DatabusSubscription.createSubscriptionList(sources);
            /* Generic Dispatcher creation */
            TestDispatcher<DatabusCombinedConsumer> dispatcher =
                new TestDispatcher<DatabusCombinedConsumer>("rollBackcheck",
                                                            connConfig,
                                                            subs,
                                                            new InMemoryPersistenceProvider(),
                                                            dataEventsBuffer,
                                                            mConsumer,
                                                            bootstrapCheckpointsPerWindow == 0);

            /* Launch writer */
            DbusEventAppender eventProducer =
                new DbusEventAppender(srcTestEvents, dataEventsBuffer,bootstrapCheckpointsPerWindow ,null);
            Thread tEmitter = new Thread(eventProducer);
            tEmitter.start();

            /* Launch dispatcher */
            Thread tDispatcher = new Thread(dispatcher);
            tDispatcher.start();

            /* Now initialize this state machine */
            dispatcher.enqueueMessage(SourcesMessage.createSetSourcesIdsMessage(sourcesMap.values()));
            dispatcher.enqueueMessage(SourcesMessage.createSetSourcesSchemasMessage(schemaMap));

            // be generous; use worst case for num control events
            long waitTimeMs  = (numEvents*timeTakenForDataEventInMs + numEvents*timeTakenForControlEventInMs) * 4;
            tEmitter.join(waitTimeMs);
            // wait for dispatcher to finish reading the events
            tDispatcher.join(waitTimeMs);
            Assert.assertFalse(tEmitter.isAlive());
            System.out.println("tConsumer: " + tConsumer);

            int windowBeforeDataFail=(numFailDataEvent/maxWindowSize);
            int expectedDataFaults = numFailDataEvent == 0 ? 0:numFailures;

            int expectedCheckPointFaults =
                (numFailCheckpointEvent==0 || (expectedDataFaults!=0 && numFailCheckpointEvent==windowBeforeDataFail)) ?
                0 : numFailures;

            // Dispatcher/Library perspective
            // check if all windows were logged by dispatcher; in online case;
            if (bootstrapCheckpointsPerWindow == 0)
            {
                Assert.assertTrue(dispatcher.getNumCheckPoints() >= (numCheckpoints-expectedCheckPointFaults));
            }

            // Consumer prespective
            // 1 or 0 faults  injected in data callbacks; success (store) differs callback by 1
            Assert.assertEquals("Mismatch between callbacks and stored data on consumer.",
                                expectedDataFaults, tConsumer.getDataCallbackCount()-tConsumer.getStoredDataCount());
            Assert.assertTrue(tConsumer.getStoredDataCount() >= tConsumer.getNumUniqStoredEvents());
            Assert.assertEquals("Consumer failed to store expected number of checkpoints.",
                                dispatcher.getNumCheckPoints(), tConsumer.getStoredCheckpointCount());

            // Equality would be nice, but each "real" error (as seen by MultiConsumerCallback) triggers a non-
            // deterministic number of cancelled callbacks, each of which is treated as another consumer error
            // (as seen by StreamConsumerCallbackFactory or BootstrapConsumerCallbackFactory).  So an inequality
            // is the best we can do at the moment, barring a change in how numConsumerErrors is accounted.
            // Special case:  non-zero numFailCheckpointEvent doesn't actually trigger a callback error; instead
            // it's converted to ConsumerCallbackResult.SKIP_CHECKPOINT and therefore not seen by client metrics.
            if (expectedCheckPointFaults == 0 || expectedDataFaults > 0 || negativeTest)
            {
              Assert.assertTrue("Unexpected error count in consumer metrics (" + unifiedStats.getNumConsumerErrors() +
                                "); should be greater than or equal to numFailures (" + numFailures + ").",
                                unifiedStats.getNumConsumerErrors() >= numFailures);
            }
            else
            {
              Assert.assertEquals("Unexpected error count in consumer metrics; checkpoint errors shouldn't count. ",
                                  // unless negativeTest ...
                                  0, unifiedStats.getNumConsumerErrors());
            }

            // rollback behaviour; were all events re-sent?
            if (!negativeTest)
            {
                Assert.assertTrue(tConsumer.getNumUniqStoredEvents()==numEvents);
            }
            else
            {
                Assert.assertTrue(tConsumer.getNumUniqStoredEvents() < numEvents);
            }

            dispatcher.shutdown();
            verifyNoLocks(null, dataEventsBuffer);
    }



    @Test(groups = {"small", "functional"})
    public void testRollback() throws Exception
    {
      final Logger log = Logger.getLogger("TestGenericDispatcher.testRollback");
      log.info("start");
        //runDispatcherRollback(int numEvents,int maxWindowSize,int numFailDataEvent,int numFailCheckpointEvent,int numFailEndWindow)

        //data event failure rollbacks
        runDispatcherRollback(100, 20, 99, 0,0);
        runDispatcherRollback(100,20,3,0,0);

        //checkpoint event failure
        runDispatcherRollback(100,20,0,1,0);
        runDispatcherRollback(100,20,0,3,0);

        //mixed mode: fail checkpoint event in window 2, but fail 45th event
        runDispatcherRollback(100,20,45,2,0);
        runDispatcherRollback(100,20,45,1,0);

        //endofWindow event failure
        runDispatcherRollback(100,20,0,0,1);
        runDispatcherRollback(100,20,0,0,3);

        //mixed mode: fail checkpoint and fail subsequent end of window
        runDispatcherRollback(100,20,50,2,3);

        //large window recoverable failure
        runDispatcherRollback(100,40,55,0,0,10.0);


        //large window unrecoverable failure : fail first checkpoint
         runDispatcherRollback(100,80,0,1,1,30.0,true,1,0);

        //onCheckpoint always returns null; forces removal of events; but lastSuccessful iterator is null; DDSDBUS-653
        runDispatcherRollback(100,120,0,-2,0,10.0);

        //recoverable failure - DDSDBUS-1659 : with successive rollbacks ;fail before first checkpoint; fail twice; ensure that
        //rollback is triggered twice; and iterators are found in buffer to rollback to
        runDispatcherRollback(100,20,7,0,0,10.0,false,2,0);

        //bootstrap sends control events that are not end-of-window; ensure that rollback logic works with them - DDSDBUS-1776
        runDispatcherRollback(100,10,18,0,0,1.8,false,1,2);

       //negative test: bootstrap sends control events that are not end-of-window;
        // onCheckpoint returns false on first system event; however - that window is sufficient to trigger flush
        // then a subsequent data event failure triggers a rollBack attempt - that fails due to iterator invalidation
        runDispatcherRollback(100,10,18,1,0,1.8,true,1,2);

        log.info("end\n");
    }

    @Test
    public void testBootstrapBufferCorruption() throws Exception
    {
      final Logger log = Logger.getLogger("TestGenericDispatcher.testBootstrapBufferCorruption");
      log.info("start");
        //bootstrap event buffer corruption - DDSDBUS-1820
        //try and get the call to flush forcibly on a control-event ; wrap around the buffer
        runDispatcherRollback(100,5,0,0,0,2.0,false,0,2,100,5,true);
        log.info("end\n");
    }

    @Test
    public void testNumConsumerErrorsMetric() throws Exception
    {
      final Logger log = Logger.getLogger("TestGenericDispatcher.testNumConsumerErrorsMetric");
      log.info("start");

      // UnifiedClientStats - DDSDBUS-2815
      int numEvents = 100;
      int maxWindowSize = 10;
      int numFailDataEvent = 17;  // every 17th onDataEvent() call (including retries after rollback!), with total of 3
      int numFailCheckpointEvent = 0;
      int numFailEndWindow = 0;
      double thresholdPct = 90.0;
      boolean negativeTest = false;
      int numFailures = 3;
      int bootstrapCheckpointsPerWindow = 0;
      long timeTakenForDataEventInMs = 1;
      long timeTakenForControlEventInMs = 1;
      boolean wrapAround = false;

      runDispatcherRollback(numEvents, maxWindowSize, numFailDataEvent, numFailCheckpointEvent, numFailEndWindow,
                            thresholdPct, negativeTest, numFailures, bootstrapCheckpointsPerWindow,
                            timeTakenForDataEventInMs, timeTakenForControlEventInMs, wrapAround);

      log.info("end\n");
    }

    /**
     *
     * @param numEvents : number of events in buffer
     * @param maxWindowSize : window size expressed as number of events
     * @param numFailWindow : nth end-of-window that will fail
     * @throws Exception
     */
    void runPartialWindowCheckpointPersistence(int numEvents,int maxWindowSize,int numFailWindow) throws Exception
    {
        /* Experiment setup */
        int payloadSize = 20;
        int numCheckpoints = numEvents/maxWindowSize;

        /* Consumer creation */
        //setup consumer to fail on data callback at the nth event
        int timeTakenForDataEventInMs = 1;
        int timeTakenForControlEventInMs=1;
        int numFailCheckpointEvent = 0;
        int numFailDataEvent = 0;
        int numFailEndWindow = numFailWindow;
        int numFailures = 1;
        //fail at the specified window; no retries; so the dispatcher should stop having written one window; but having checkpointed the other
        //thanks to a very small checkpoint frequency threshold
        TimeoutTestConsumer tConsumer = new TimeoutTestConsumer(timeTakenForDataEventInMs,timeTakenForControlEventInMs,
                numFailCheckpointEvent,numFailDataEvent,numFailEndWindow,numFailures);

        HashMap<Long, List<RegisterResponseEntry>> schemaMap =
                new HashMap<Long, List<RegisterResponseEntry>>();

        short srcId=1;
        List<RegisterResponseEntry> l1 = new ArrayList<RegisterResponseEntry>();
        l1.add(new RegisterResponseEntry(1L, srcId,SOURCE1_SCHEMA_STR));

        schemaMap.put(1L, l1);

        Map<Long, IdNamePair> sourcesMap = new HashMap<Long, IdNamePair>();
        List<String> sources = new ArrayList<String>();
        for (int i = 1; i <= 1; ++i)
        {
            IdNamePair sourcePair = new IdNamePair((long)i, "source" + i);
            sources.add(sourcePair.getName());
            sourcesMap.put(sourcePair.getId(), sourcePair);
        }

        long consumerTimeBudgetMs = 60*1000;
        DatabusV2ConsumerRegistration consumerReg = new DatabusV2ConsumerRegistration(tConsumer, sources, null);
        List<DatabusV2ConsumerRegistration> allRegistrations =  Arrays.asList(consumerReg);
        //Single threaded execution of consumer
        MultiConsumerCallback mConsumer = new MultiConsumerCallback(allRegistrations,Executors.newFixedThreadPool(1),
                consumerTimeBudgetMs, new StreamConsumerCallbackFactory(null, null), null, null, null, null);

        /* Generate events **/
        Vector<DbusEvent> srcTestEvents = new Vector<DbusEvent>();
        Vector<Short> srcIdList = new Vector<Short> ();
        srcIdList.add(srcId);

        DbusEventGenerator evGen = new DbusEventGenerator(15000,srcIdList);
        //Assumption: generates events with  non-decreasing timestamps
        Assert.assertTrue(evGen.generateEvents(numEvents, maxWindowSize, 512, payloadSize,true, srcTestEvents) > 0);

        int totalSize=0; int maxSize=0;
        for (DbusEvent e : srcTestEvents)
        {
            totalSize += e.size();
            maxSize = (e.size() > maxSize) ? e.size():maxSize;
        }

        /* Source configuration */
        double thresholdChkptPct = 5.0;
        DatabusSourcesConnection.Config conf = new DatabusSourcesConnection.Config();
        conf.setCheckpointThresholdPct(thresholdChkptPct);
        conf.getDispatcherRetries().setMaxRetryNum(0);
        conf.setFreeBufferThreshold(maxSize);
        conf.setConsumerTimeBudgetMs(consumerTimeBudgetMs);
        int freeBufferThreshold = conf.getFreeBufferThreshold();
        DatabusSourcesConnection.StaticConfig connConfig = conf.build();

        //make buffer large enough to hold data; the control events are large that contain checkpoints
        int producerBufferSize = totalSize*2 + numCheckpoints*10*maxSize*5 + freeBufferThreshold;
        int individualBufferSize = producerBufferSize;
        int indexSize = producerBufferSize / 10;
        int stagingBufferSize = producerBufferSize;

        /*Event Buffer creation */
        TestGenericDispatcherEventBuffer dataEventsBuffer=
            new TestGenericDispatcherEventBuffer(
                getConfig(producerBufferSize, individualBufferSize, indexSize ,
                          stagingBufferSize, AllocationPolicy.HEAP_MEMORY,
                          QueuePolicy.BLOCK_ON_WRITE));

        List<DatabusSubscription> subs = DatabusSubscription.createSubscriptionList(sources);
        /* Generic Dispatcher creation */
        InMemoryPersistenceProvider cpPersister = new InMemoryPersistenceProvider();
        TestDispatcher<DatabusCombinedConsumer> dispatcher = new TestDispatcher<DatabusCombinedConsumer>("OnlinePartialWindowCheckpointPersistence",
                connConfig,
                subs,
                cpPersister,
                dataEventsBuffer,
                mConsumer,
                true);

        /* Launch writer */
        DbusEventAppender eventProducer = new DbusEventAppender(srcTestEvents, dataEventsBuffer,0,null) ;
        Thread tEmitter = new Thread(eventProducer);
      //be generous ; use worst case for num control events
        long waitTimeMs  = (numEvents*timeTakenForDataEventInMs + numEvents*timeTakenForControlEventInMs) * 4;

        tEmitter.start();
        tEmitter.join(waitTimeMs);


        /* Launch dispatcher */
        Thread tDispatcher = new Thread(dispatcher);
        tDispatcher.start();

        /* Now initialize this  state machine */
        dispatcher.enqueueMessage(SourcesMessage.createSetSourcesIdsMessage(sourcesMap.values()));
        dispatcher.enqueueMessage(SourcesMessage.createSetSourcesSchemasMessage(schemaMap));

        //wait for dispatcher to finish reading the events;
        tDispatcher.join(waitTimeMs);
        Assert.assertFalse(tEmitter.isAlive());
        Assert.assertFalse(tDispatcher.isAlive());

        LOG.info("tConsumer: " + tConsumer);
        HashMap<List<String>,Checkpoint> cps = cpPersister.getCheckpoints();
        for (Map.Entry<List<String>,Checkpoint> i : cps.entrySet())
        {
            Checkpoint cp = i.getValue();
            LOG.info("checkpoint="+ cp);


            Assert.assertEquals(cp.getWindowOffset().longValue() , -1L);
            //check if lastSeenCheckpoint by consumer is higher than scn persisted
            Assert.assertTrue(tConsumer.getLastSeenCheckpointScn() > cp.getWindowScn());
            //the latest event seen should be newer (or at least as new) as the checkpoint
            Assert.assertTrue(tConsumer.getLastTsInNanosOfEvent() >= tConsumer.getLastTsInNanosOfWindow());

            if (tConsumer.getLastSeenWindowScn() > 0)
            {
              Assert.assertEquals(cp.getWindowScn(),tConsumer.getLastSeenWindowScn());
              //check if the timestamp in checkpoint is the same as checkpoint of last completed window (ts of last event of the window)
              Assert.assertEquals(tConsumer.getLastTsInNanosOfWindow(),cp.getTsNsecs());
            }
            else
            {
              //not even one window was processed before error; expect uninitialized timestamp
              Assert.assertEquals(Checkpoint.UNSET_TS_NSECS,cp.getTsNsecs());
            }
        }

    }


    @Test
    public void testMetadataSchema()
    {
        final Logger log = Logger.getLogger("TestGenericDispatcher.testMetadataSchema");
        //log.setLevel(Level.DEBUG);
        log.info("start");

        int source1EventsNum = 2;
        int source2EventsNum = 2;

        Hashtable<Long, AtomicInteger> keyCounts = new Hashtable<Long, AtomicInteger>();
        Hashtable<Short, AtomicInteger> srcidCounts = new Hashtable<Short, AtomicInteger>();

        final TestGenericDispatcherEventBuffer eventsBuf =
            new TestGenericDispatcherEventBuffer(_generic100KBufferStaticConfig);
        eventsBuf.start(0);

        final StateVerifyingStreamConsumer svsConsumer = new StateVerifyingStreamConsumer(null);
        //svsConsumer.getLog().setLevel(Level.DEBUG);
        DatabusStreamConsumer mockConsumer =
                new EventCountingConsumer(svsConsumer, keyCounts, srcidCounts);
        SelectingDatabusCombinedConsumer sdccMockConsumer = new SelectingDatabusCombinedConsumer(mockConsumer);

        List<String> sources = new ArrayList<String>();
        Map<Long, IdNamePair> sourcesMap = new HashMap<Long, IdNamePair>();
        for (int i = 1; i <= 3; ++i)
        {
            IdNamePair sourcePair = new IdNamePair((long)i, "source" + i);
            sources.add(sourcePair.getName());
            sourcesMap.put(sourcePair.getId(), sourcePair);
        }

        DatabusV2ConsumerRegistration consumerReg =
                new DatabusV2ConsumerRegistration(sdccMockConsumer, sources, null);

        List<DatabusV2ConsumerRegistration> allRegistrations =
                Arrays.asList(consumerReg);
        MultiConsumerCallback callback =
                new MultiConsumerCallback(
                        allRegistrations,
                        Executors.newSingleThreadExecutor(),
                        1000,
                        new StreamConsumerCallbackFactory(null, null),
                        null,
                        null,
                        null,
                        null);
        callback.setSourceMap(sourcesMap);

        List<DatabusSubscription> subs = DatabusSubscription.createSubscriptionList(sources);
        RelayDispatcher dispatcher =
                new RelayDispatcher("dispatcher", _genericRelayConnStaticConfig, subs,
                        new InMemoryPersistenceProvider(),
                        eventsBuf, callback, null,null,null, null, null);

        Thread dispatcherThread = new Thread(dispatcher, "testMetadataSchema-dispatcher");
        //dispatcherThread.setDaemon(true);
        dispatcherThread.start();

        HashMap<Long, List<RegisterResponseEntry>> schemaMap =
                new HashMap<Long, List<RegisterResponseEntry>>();

        List<RegisterResponseEntry> l1 = new ArrayList<RegisterResponseEntry>();
        List<RegisterResponseEntry> l2 = new ArrayList<RegisterResponseEntry>();
        List<RegisterResponseEntry> l3 = new ArrayList<RegisterResponseEntry>();

        l1.add(new RegisterResponseEntry(1L, (short)1,SOURCE1_SCHEMA_STR));
        l2.add(new RegisterResponseEntry(2L, (short)1,SOURCE2_SCHEMA_STR));
        l3.add(new RegisterResponseEntry(3L, (short)1,SOURCE3_SCHEMA_STR));

        schemaMap.put(1L, l1);
        schemaMap.put(2L, l2);
        schemaMap.put(3L, l3);

        //add meta data schema
        byte[] crc32 = {0x01,0x02,0x03,0x04};

        List<RegisterResponseMetadataEntry> lMeta = new ArrayList<RegisterResponseMetadataEntry>();
        lMeta.add(new RegisterResponseMetadataEntry((short) 1 , META1_SCHEMA_STR,crc32));
        lMeta.add(new RegisterResponseMetadataEntry((short) 2 , META2_SCHEMA_STR,crc32));


        dispatcher.enqueueMessage(SourcesMessage.createSetSourcesIdsMessage(sourcesMap.values()));
        dispatcher.enqueueMessage(SourcesMessage.createSetSourcesSchemasMessage(schemaMap,lMeta));
        eventsBuf.startEvents();
        initBufferWithEvents(eventsBuf, 1, source1EventsNum, (short)1, keyCounts, srcidCounts);
        initBufferWithEvents(eventsBuf, 1 + source1EventsNum, source2EventsNum, (short)2, keyCounts, srcidCounts);
        eventsBuf.endEvents(100L,null);

        //check standard execution of callbacks
        try
        {
            Thread.sleep(2000);
        }
        catch (InterruptedException ie) {}

        dispatcher.shutdown();

        for (long i = 1; i <= source1EventsNum + source2EventsNum; ++i)
        {
            assertEquals("correct amount of callbacks for key " + i,
                    1, keyCounts.get(i).intValue());
        }

        assertEquals("incorrect amount of callbacks for srcid 1", source1EventsNum,
                     srcidCounts.get((short)1).intValue());
        assertEquals("incorrect amount of callbacks for srcid 2", source2EventsNum,
                     srcidCounts.get((short)2).intValue());

        //check metadata schemas

        EventCountingConsumer myCons = (EventCountingConsumer) mockConsumer;
        VersionedSchema metadataSchema = myCons.getMetadataSchema();
        Assert.assertTrue(null != metadataSchema);

        log.info("Metadata VersionedSchema = " + metadataSchema);
        Assert.assertEquals(metadataSchema.getVersion(),2);
        Assert.assertEquals(metadataSchema.getSchemaBaseName(), SchemaRegistryService.DEFAULT_METADATA_SCHEMA_SOURCE);

        verifyNoLocks(log, eventsBuf);
        log.info("end\n");
    }

    @Test
    public void testOnlinePartialWindowCheckpointPersistence() throws Exception
    {
        final Logger log = Logger.getLogger("TestGenericDispatcher.testOnlinePartialWindowCheckpointPersistence");
        //log.setLevel(Level.DEBUG);
        log.info("start");

        //DDSDBUS-1889: Ensure relay does save scn of partial window
        //checks case where very partial window occurs without any complete window having been processed
        runPartialWindowCheckpointPersistence(100, 25, 1);

        runPartialWindowCheckpointPersistence(100, 25, 2);
      log.info("end\n");
    }

    @Test
    public void testBootstrapPartialWindowScnOrdering() throws Exception
    {
        final Logger log = Logger.getLogger("TestGenericDispatcher.testBootstrapPartialWindowScnOrdering");
        //log.setLevel(Level.DEBUG);
        log.info("start");

        //DDSDBUS-1889: Ensure bootstrap onCheckpoint() callback receives bootstrapSinceScn - not some scn.
        int numEvents=100;
        int maxWindowSize = 25;
        /* Experiment setup */
        int payloadSize = 20;
        int numCheckpoints = numEvents/maxWindowSize;

        /* Consumer creation */
        //setup consumer to fail on end of first full window
        int timeTakenForDataEventInMs = 1;
        int timeTakenForControlEventInMs=1;
        int numFailCheckpointEvent = 0;
        int numFailDataEvent = 0;
        int numFailEndWindow = 1;
        int numFailures = 1;
        //fail at the specified window; no retries; so the dispatcher should stop having written one window; but having checkpointed the other
        //thanks to a very small checkpoint frequency threshold
        TimeoutTestConsumer tConsumer = new TimeoutTestConsumer(timeTakenForDataEventInMs,timeTakenForControlEventInMs,
                numFailCheckpointEvent,numFailDataEvent,numFailEndWindow,numFailures);

        HashMap<Long, List<RegisterResponseEntry>> schemaMap =
                new HashMap<Long, List<RegisterResponseEntry>>();

        short srcId=1;
        List<RegisterResponseEntry> l1 = new ArrayList<RegisterResponseEntry>();
        l1.add(new RegisterResponseEntry(1L, srcId,SOURCE1_SCHEMA_STR));

        schemaMap.put(1L, l1);

        Map<Long, IdNamePair> sourcesMap = new HashMap<Long, IdNamePair>();
        List<String> sources = new ArrayList<String>();
        for (int i = 1; i <= 1; ++i)
        {
            IdNamePair sourcePair = new IdNamePair((long)i, "source" + i);
            sources.add(sourcePair.getName());
            sourcesMap.put(sourcePair.getId(), sourcePair);
        }

        long consumerTimeBudgetMs = 60*1000;
        DatabusV2ConsumerRegistration consumerReg = new DatabusV2ConsumerRegistration(tConsumer, sources, null);
        List<DatabusV2ConsumerRegistration> allRegistrations =  Arrays.asList(consumerReg);
        //Single threaded execution of consumer
        MultiConsumerCallback mConsumer = new MultiConsumerCallback(allRegistrations,Executors.newFixedThreadPool(1),
                consumerTimeBudgetMs, new StreamConsumerCallbackFactory(null, null), null, null, null, null);

        /* Generate events **/
        Vector<DbusEvent> srcTestEvents = new Vector<DbusEvent>();
        Vector<Short> srcIdList = new Vector<Short> ();
        srcIdList.add(srcId);

        DbusEventGenerator evGen = new DbusEventGenerator(15000,srcIdList);
        Assert.assertTrue(evGen.generateEvents(numEvents, maxWindowSize, 512, payloadSize,true, srcTestEvents) > 0);

        int totalSize=0; int maxSize=0;
        for (DbusEvent e : srcTestEvents)
        {
            totalSize += e.size();
            maxSize = (e.size() > maxSize) ? e.size():maxSize;
        }

        /* Source configuration */
        double thresholdChkptPct = 5.0;
        DatabusSourcesConnection.Config conf = new DatabusSourcesConnection.Config();
        conf.setCheckpointThresholdPct(thresholdChkptPct);
        conf.getDispatcherRetries().setMaxRetryNum(0);
        conf.setFreeBufferThreshold(maxSize);
        conf.setConsumerTimeBudgetMs(consumerTimeBudgetMs);
        int freeBufferThreshold = conf.getFreeBufferThreshold();
        DatabusSourcesConnection.StaticConfig connConfig = conf.build();

        //make buffer large enough to hold data; the control events are large that contain checkpoints
        int producerBufferSize = totalSize*2 + numCheckpoints*10*maxSize*5 + freeBufferThreshold;
        int individualBufferSize = producerBufferSize;
        int indexSize = producerBufferSize / 10;
        int stagingBufferSize = producerBufferSize;

        /*Event Buffer creation */
        TestGenericDispatcherEventBuffer dataEventsBuffer=
            new TestGenericDispatcherEventBuffer(
                getConfig(producerBufferSize, individualBufferSize, indexSize ,
                          stagingBufferSize, AllocationPolicy.HEAP_MEMORY,
                          QueuePolicy.BLOCK_ON_WRITE));

        List<DatabusSubscription> subs = DatabusSubscription.createSubscriptionList(sources);
        /* Generic Dispatcher creation */
        InMemoryPersistenceProvider cpPersister = new InMemoryPersistenceProvider();
        BootstrapDispatcher dispatcher = new BootstrapDispatcher("bootstrapPartialWindowCheckpointPersistence",
                connConfig,
                subs,
                cpPersister,
                dataEventsBuffer,
                mConsumer,
                null, //relaypuller
                null, //mbean server
                null, //ClientImpl
                null, //registrationId
                null // logger
                );
        dispatcher.setSchemaIdCheck(false);

        BootstrapCheckpointHandler cptHandler = new BootstrapCheckpointHandler("source1");
        long sinceScn=15000L;
        long startTsNsecs = System.nanoTime();
        final Checkpoint initCheckpoint = cptHandler.createInitialBootstrapCheckpoint(null, sinceScn);
        initCheckpoint.setBootstrapStartNsecs(startTsNsecs);
        initCheckpoint.setBootstrapStartScn(0L);

        /* Launch writer */
        //numBootstrapCheckpoint - number of checkpoints before writing end of period
        int numBootstrapCheckpoint=4;
        DbusEventAppender eventProducer = new DbusEventAppender(srcTestEvents, dataEventsBuffer,numBootstrapCheckpoint,null) ;
        //simulate bootstrap server; use this checkpoint as init checkpoint
        eventProducer.setBootstrapCheckpoint(initCheckpoint);
        Thread tEmitter = new Thread(eventProducer);
      //be generous ; use worst case for num control events
        long waitTimeMs  = (numEvents*timeTakenForDataEventInMs + numEvents*timeTakenForControlEventInMs) * 4;

        tEmitter.start();
        tEmitter.join(waitTimeMs);


        /* Launch dispatcher */
        Thread tDispatcher = new Thread(dispatcher);
        tDispatcher.start();

        /* Now initialize this  state machine */
        dispatcher.enqueueMessage(SourcesMessage.createSetSourcesIdsMessage(sourcesMap.values()));
        dispatcher.enqueueMessage(SourcesMessage.createSetSourcesSchemasMessage(schemaMap));

        //expect dispatcher to fail - at end of window
        tDispatcher.join(waitTimeMs);

        Assert.assertFalse(tEmitter.isAlive());
        Assert.assertFalse(tDispatcher.isAlive());

        LOG.info("tConsumer: " + tConsumer);
        HashMap<List<String>,Checkpoint> cps = cpPersister.getCheckpoints();
        Assert.assertTrue(cps.size() > 0);
        for (Map.Entry<List<String>,Checkpoint> i : cps.entrySet())
        {
            Checkpoint cp = i.getValue();
            LOG.info("checkpoint="+ cp);
            Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.BOOTSTRAP_SNAPSHOT);
            //check if progress has been made during bootstrap
            Assert.assertTrue(cp.getSnapshotOffset() > 0);
            //these two values should be unchanged during the course of bootstrap
            Assert.assertEquals(sinceScn,cp.getBootstrapSinceScn().longValue());
            Assert.assertEquals(startTsNsecs,cp.getBootstrapStartNsecs());
            //the tsNsec normally udpdated by client at end of window should be a no-op during bootstrap
            Assert.assertEquals(Checkpoint.UNSET_TS_NSECS,cp.getTsNsecs());
            //the scn passed to consumers during onCheckpoint should be the sinceSCN and not any other interim value
            Assert.assertEquals(cp.getBootstrapSinceScn().longValue(),tConsumer.getLastSeenCheckpointScn());
        }
        log.info("end\n");
    }

    //This is a negative test for DDSDBUS-3421. We expect dispatcher to fail without dataEvents being called.
    @Test
    public void testAbsentSchemaTest() throws Exception
    {
      runAbsentSchemaTest(true);
      runAbsentSchemaTest(false);
    }

    void runAbsentSchemaTest(boolean setSchemaCheck) throws Exception
    {
      /* Experiment setup */
      int numEvents=100; int maxWindowSize=20;
      int payloadSize = 20;
      int numCheckpoints = numEvents/maxWindowSize;

      /* Consumer creation */
      //setup consumer to fail on data callback at the nth event
      DataDecodingConsumer tConsumer = new DataDecodingConsumer();

      HashMap<Long, List<RegisterResponseEntry>> schemaMap =
              new HashMap<Long, List<RegisterResponseEntry>>();

      short srcId=1;
      List<RegisterResponseEntry> l1 = new ArrayList<RegisterResponseEntry>();
      l1.add(new RegisterResponseEntry(1L, srcId,SOURCE1_SCHEMA_STR));

      schemaMap.put(1L, l1);

      Map<Long, IdNamePair> sourcesMap = new HashMap<Long, IdNamePair>();
      List<String> sources = new ArrayList<String>();
      for (int i = 1; i <= 1; ++i)
      {
          IdNamePair sourcePair = new IdNamePair((long)i, "source" + i);
          sources.add(sourcePair.getName());
          sourcesMap.put(sourcePair.getId(), sourcePair);
      }

      long consumerTimeBudgetMs = 60*1000;
      DatabusV2ConsumerRegistration consumerReg = new DatabusV2ConsumerRegistration(tConsumer, sources, null);
      List<DatabusV2ConsumerRegistration> allRegistrations =  Arrays.asList(consumerReg);
      //Single threaded execution of consumer
      MultiConsumerCallback mConsumer = new MultiConsumerCallback(allRegistrations,Executors.newFixedThreadPool(1),
              consumerTimeBudgetMs,new StreamConsumerCallbackFactory(null,null),null,null, null, null);



      /* Generate events **/
      Vector<DbusEvent> srcTestEvents = new Vector<DbusEvent>();
      Vector<Short> srcIdList = new Vector<Short> ();
      srcIdList.add(srcId);

      DbusEventGenerator evGen = new DbusEventGenerator(0,srcIdList);
      //the schemaIds generated here are random. They will not be the same as those computed in the dispatcher.
      //The result is either the processing will fail early (desired behaviour) or during event decoding in the onDataEvent()
      Assert.assertTrue(evGen.generateEvents(numEvents, maxWindowSize, 512, payloadSize, srcTestEvents) > 0);

      int totalSize=0; int maxSize=0;
      for (DbusEvent e : srcTestEvents)
      {
          totalSize += e.size();
          maxSize = (e.size() > maxSize) ? e.size():maxSize;
      }

      /* Source configuration */
      DatabusSourcesConnection.Config conf = new DatabusSourcesConnection.Config();
      conf.getDispatcherRetries().setMaxRetryNum(1);
      conf.setFreeBufferThreshold(maxSize);
      conf.setConsumerTimeBudgetMs(consumerTimeBudgetMs);
      int freeBufferThreshold = conf.getFreeBufferThreshold();
      DatabusSourcesConnection.StaticConfig connConfig = conf.build();

      //make buffer large enough to hold data; the control events are large that contain checkpoints
      int producerBufferSize =  totalSize*2 + numCheckpoints*10*maxSize*5 + freeBufferThreshold;
      int individualBufferSize = producerBufferSize;
      int indexSize = producerBufferSize / 10;
      int stagingBufferSize = producerBufferSize;

      /*Event Buffer creation */
      TestGenericDispatcherEventBuffer dataEventsBuffer=
          new TestGenericDispatcherEventBuffer(
              getConfig(producerBufferSize, individualBufferSize, indexSize ,
                        stagingBufferSize, AllocationPolicy.HEAP_MEMORY,
                        QueuePolicy.BLOCK_ON_WRITE));

      List<DatabusSubscription> subs = DatabusSubscription.createSubscriptionList(sources);
      /* Generic Dispatcher creation */
      TestDispatcher<DatabusCombinedConsumer> dispatcher = new TestDispatcher<DatabusCombinedConsumer>("rollBackcheck",
              connConfig,
              subs,
              new InMemoryPersistenceProvider(),
              dataEventsBuffer,
              mConsumer,
              false);
      //DDSDBUS-3421; set schema check to true
      dispatcher.setSchemaIdCheck(setSchemaCheck);

      /* Launch writer */
      DbusEventAppender eventProducer = new DbusEventAppender(srcTestEvents, dataEventsBuffer,0 ,null) ;
      Thread tEmitter = new Thread(eventProducer);
      tEmitter.start();

      /* Launch dispatcher */
      Thread tDispatcher = new Thread(dispatcher);
      tDispatcher.start();

      /* Now initialize this  state machine */
      dispatcher.enqueueMessage(SourcesMessage.createSetSourcesIdsMessage(sourcesMap.values()));
      dispatcher.enqueueMessage(SourcesMessage.createSetSourcesSchemasMessage(schemaMap));

      //be generous ; use worst case for num control events
      long waitTimeMs  = (numEvents*1 + numEvents*1) * 4;
      tEmitter.join(waitTimeMs);
      //wait for dispatcher to finish reading the events;
      tDispatcher.join(waitTimeMs);
      Assert.assertFalse(tEmitter.isAlive());

      //asserts
      if (!setSchemaCheck)
      {
        //decoding fails many errors show up;
        Assert.assertTrue(tConsumer.getNumDataEvents() > 0);
        Assert.assertTrue(tConsumer.getNumErrors() > 0);
      }
      else
      {
        //never gets to decoding; but error shows up (exactly one - dispatcher retries set to 1);
        Assert.assertEquals(0, tConsumer.getNumDataEvents());
        Assert.assertEquals(1,tConsumer.getNumErrors());
      }
    }

    DbusEventBuffer.StaticConfig getConfig(long maxEventBufferSize, int maxIndividualBufferSize, int maxIndexSize,
                                           int maxReadBufferSize, AllocationPolicy allocationPolicy, QueuePolicy policy)
    throws InvalidConfigException
    {
        DbusEventBuffer.Config config = new DbusEventBuffer.Config();
        config.setMaxSize(maxEventBufferSize);
        config.setMaxIndividualBufferSize(maxIndividualBufferSize);
        config.setScnIndexSize(maxIndexSize);
        config.setAverageEventSize(maxReadBufferSize);
        config.setAllocationPolicy(allocationPolicy.name());
        config.setQueuePolicy(policy.toString());
        return config.build();
    }

    /** Expose some package-level testing info */
    static class TestGenericDispatcherEventBuffer extends DbusEventBuffer
    {
      public TestGenericDispatcherEventBuffer(DbusEventBuffer.StaticConfig conf)
      {
        super(conf);
      }

      RangeBasedReaderWriterLock getRangeLocksProvider()
      {
        return _rwLockProvider;
      }
    }

    @Test(groups = {"small", "functional"})
    /**
     *
     * 1. Dispatcher is dispatching 2 window of events.
     * 2. First window consumption is successfully done.
     * 3. The second window's onStartDataEventSequence() of the callback registered is blocked (interruptible),
     *    causing the dispatcher to wait.
     * 4. At this instant the dispatcher is shut down. The callback is made to return Failure status which would
     *    cause rollback in normal scenario.
     * 5. As the shutdown message is passed, the blocked callback is expected to be interrupted and no rollback
     *    calls MUST be made.
     */
    public void testShutdownBeforeRollback()
      throws Exception
    {
      final Logger log = Logger.getLogger("TestGenericDispatcher.testShutdownBeforeRollback");
      log.setLevel(Level.INFO);
      //log.getRoot().setLevel(Level.DEBUG);
      LOG.info("start");

      // generate events
      Vector<Short> srcIdList = new Vector<Short> ();
      srcIdList.add((short)1);

      DbusEventGenerator evGen = new DbusEventGenerator(0,srcIdList);
      Vector<DbusEvent> srcTestEvents = new Vector<DbusEvent>();
      final int numEvents = 8;
      final int numEventsPerWindow = 4;
      final int payloadSize = 200;
      Assert.assertTrue(evGen.generateEvents(numEvents, numEventsPerWindow, 500, payloadSize, srcTestEvents) > 0);

      // find out how much data we need to stream for the failure
      int win1Size= payloadSize - 1; // account for the EOW event which is < payload size
      for (DbusEvent e : srcTestEvents)
      {
        win1Size += e.size();
      }

      //serialize the events to a buffer so they can be sent to the client
      final TestGenericDispatcherEventBuffer srcEventsBuf =
          new TestGenericDispatcherEventBuffer(_generic100KBufferStaticConfig);

      DbusEventAppender eventProducer = new DbusEventAppender(srcTestEvents, srcEventsBuf,null, true) ;
      Thread tEmitter = new Thread(eventProducer);
      tEmitter.start();

      //Create destination (client) buffer
      final TestGenericDispatcherEventBuffer destEventsBuf =
          new TestGenericDispatcherEventBuffer(_generic100KBufferStaticConfig);

      /**
       *
       *  Consumer with ability to wait for latch during onStartDataEventSequence()
       */
      class TimeoutDESConsumer
      extends TimeoutTestConsumer
      {
        private final CountDownLatch latch = new CountDownLatch(1);
        private int _countStartWindow = 0;
        private final int _failedRequestNumber;

        public TimeoutDESConsumer(int failedRequestNumber) {
          super(1,1,0, 0, 0, 0);
          _failedRequestNumber = failedRequestNumber;
        }

        public CountDownLatch getLatch()
        {
          return latch;
        }

        @Override
        public ConsumerCallbackResult onStartDataEventSequence(SCN startScn) {

          _countStartWindow++;

          if ( _countStartWindow == _failedRequestNumber)
          {
            try { latch.await(); } catch (InterruptedException e) {} // Wait for the latch to open
            return ConsumerCallbackResult.ERROR_FATAL;
          }

          return super.onStartDataEventSequence(startScn);
        }

        @Override
        public ConsumerCallbackResult onDataEvent(DbusEvent e,
                                                  DbusEventDecoder eventDecoder) {
          return ConsumerCallbackResult.SUCCESS;
        }

        public int getNumBeginWindowCalls()
        {
          return _countStartWindow;
        }
      }
      //Create dispatcher
      final TimeoutDESConsumer mockConsumer = new TimeoutDESConsumer(2); //fail on second window

      SelectingDatabusCombinedConsumer sdccMockConsumer =
          new SelectingDatabusCombinedConsumer((DatabusStreamConsumer)mockConsumer);

      List<String> sources = new ArrayList<String>();
      Map<Long, IdNamePair> sourcesMap = new HashMap<Long, IdNamePair>();
      for (int i = 1; i <= 3; ++i)
      {
        IdNamePair sourcePair = new IdNamePair((long)i, "source" + i);
        sources.add(sourcePair.getName());
        sourcesMap.put(sourcePair.getId(), sourcePair);
      }

      DatabusV2ConsumerRegistration consumerReg =
          new DatabusV2ConsumerRegistration(sdccMockConsumer, sources, null);

      List<DatabusV2ConsumerRegistration> allRegistrations = Arrays.asList(consumerReg);
      final ConsumerCallbackStats callbackStats = new ConsumerCallbackStats(0, "test", "test", true, false, null);
      final UnifiedClientStats unifiedStats = new UnifiedClientStats(0, "test", "test.unified");
      MultiConsumerCallback callback =
          new MultiConsumerCallback(allRegistrations,
                                    Executors.newFixedThreadPool(2),
                                    100, // 100 ms budget
                                    new StreamConsumerCallbackFactory(callbackStats, unifiedStats),
                                    callbackStats,
                                    unifiedStats,
                                    null,
                                    null);
      callback.setSourceMap(sourcesMap);
      List<DatabusSubscription> subs = DatabusSubscription.createSubscriptionList(sources);
      final RelayDispatcher dispatcher =
          new RelayDispatcher("dispatcher", _genericRelayConnStaticConfig, subs,
                              new InMemoryPersistenceProvider(),
                              destEventsBuf, callback, null,null,null,null,null);

      final Thread dispatcherThread = new Thread(dispatcher);
      log.info("starting dispatcher thread");
      dispatcherThread.start();

      // Generate RegisterRespone for schema
      HashMap<Long, List<RegisterResponseEntry>> schemaMap =
          new HashMap<Long, List<RegisterResponseEntry>>();

      List<RegisterResponseEntry> l1 = new ArrayList<RegisterResponseEntry>();
      List<RegisterResponseEntry> l2 = new ArrayList<RegisterResponseEntry>();
      List<RegisterResponseEntry> l3 = new ArrayList<RegisterResponseEntry>();

      l1.add(new RegisterResponseEntry(1L, (short)1,SOURCE1_SCHEMA_STR));
      l2.add(new RegisterResponseEntry(2L, (short)1,SOURCE2_SCHEMA_STR));
      l3.add(new RegisterResponseEntry(3L, (short)1,SOURCE3_SCHEMA_STR));

      schemaMap.put(1L, l1);
      schemaMap.put(2L, l2);
      schemaMap.put(3L, l3);

      // Enqueue Necessary messages before starting dispatch
      dispatcher.enqueueMessage(SourcesMessage.createSetSourcesIdsMessage(sourcesMap.values()));
      dispatcher.enqueueMessage(SourcesMessage.createSetSourcesSchemasMessage(schemaMap));

      log.info("starting event dispatch");
      //comm channels between reader and writer
      Pipe pipe = Pipe.open();
      Pipe.SinkChannel writerStream = pipe.sink();
      Pipe.SourceChannel readerStream = pipe.source();
      writerStream.configureBlocking(true);
      /*
       *  Needed for DbusEventBuffer.readEvents() to exit their loops when no more data is available.
       *  With Pipe mimicking ChunkedBodyReadableByteChannel, we need to make Pipe non-blocking on the
       *  reader side to achieve the behavior that ChunkedBodyReadableByte channel provides.
       */
      readerStream.configureBlocking(false);

      //Event writer - Relay in the real world
      Checkpoint cp = Checkpoint.createFlexibleCheckpoint();

      //Event readers - Clients in the real world
      //Checkpoint pullerCheckpoint = Checkpoint.createFlexibleCheckpoint();
      DbusEventsStatisticsCollector clientStats = new DbusEventsStatisticsCollector(0, "client", true, false, null);
      DbusEventBufferReader reader = new DbusEventBufferReader(destEventsBuf, readerStream, null, clientStats);
      UncaughtExceptionTrackingThread tReader = new UncaughtExceptionTrackingThread(reader,"Reader");
      tReader.setDaemon(true);
      tReader.start();
      try
      {
        log.info("send both windows");
        StreamEventsResult streamRes = srcEventsBuf.streamEvents(cp, writerStream, new StreamEventsArgs(win1Size));
        Assert.assertEquals("num events streamed should equal total number of events plus 2", // EOP events, presumably?
                            numEvents + 2, streamRes.getNumEventsStreamed());

        TestUtil.assertWithBackoff(new ConditionCheck()
        {
          @Override
          public boolean check()
          {
            return 2 == mockConsumer.getNumBeginWindowCalls();
          }
        }, "second window processing started", 5000, log);

        dispatcher.shutdown();
        mockConsumer.getLatch().countDown(); // remove the barrier

        TestUtil.assertWithBackoff(new ConditionCheck()
        {
          @Override
          public boolean check()
          {
            return ! dispatcherThread.isAlive();
          }
        }, "Ensure Dispatcher thread is shutdown", 5000, log);

        TestUtil.assertWithBackoff(new ConditionCheck()
        {
          @Override
          public boolean check()
          {
            return 0 == mockConsumer.getNumRollbacks();
          }
        }, "Ensure No Rollback is called", 10, log);
      }
      finally
      {
        reader.stop();
      }
      log.info("end\n");
    }

    @Test(groups = {"small", "functional"})
    /**
     * Tests the case where the dispatcher exits the main processing loop in {@link GenericDispatcher#doDispatchEvents()}
     * with a partial window and the flushing of the outstanding callbacks fails. We want to make sure that a rollback
     * is correctly triggered.
     *
     * The test simulates the following case: e1_1 e1_2 e1_3 <EOW> e2_1 e2_2 e2_3 <EOW> ... with a failure in the e2_2
     * callback.
     *
     * 1) Read full first window: e1_1 e1_2 e1_3 <EOW>
     * 2) Read partial second window: e2_1 e2_2
     * 3) The above should fail -- verify that rollback is called
     * 4) Read the rest
     */
    public void testPartialWindowRollback() throws Exception
    {
      final Logger log = Logger.getLogger("TestGenericDispatcher.testPartialWindowRollback");
      //log.setLevel(Level.INFO);
      log.info("start");
      final Level saveLevel = Logger.getLogger("com.linkedin.databus.client").getLevel();
      //Logger.getLogger("com.linkedin.databus.client").setLevel(Level.DEBUG);

      // generate events
      Vector<Short> srcIdList = new Vector<Short> ();
      srcIdList.add((short)1);

      DbusEventGenerator evGen = new DbusEventGenerator(0,srcIdList);
      Vector<DbusEvent> srcTestEvents = new Vector<DbusEvent>();
      final int numEvents = 9;
      final int numOfFailureEvent = 5; //1-based number of the event callback to fail
      final int numEventsPerWindow = 3;
      final int payloadSize = 200;
      final int numWindows = (int)Math.ceil(1.0 * numEvents / numEventsPerWindow);
      Assert.assertTrue(evGen.generateEvents(numEvents, numEventsPerWindow, 500, payloadSize, srcTestEvents) > 0);

      // find out how much data we need to stream for the failure
      int win1Size= payloadSize - 1; // account for the EOW event which is < payload size
      int win2Size = 0;
      int eventN = 0;
      for (DbusEvent e : srcTestEvents)
      {
        eventN++;
        if (eventN <= numEventsPerWindow)
        {
          win1Size += e.size();
        }
        else if (eventN <= numOfFailureEvent)
        {
          win2Size += e.size();
        }
      }

      //serialize the events to a buffer so they can be sent to the client
      final TestGenericDispatcherEventBuffer srcEventsBuf =
          new TestGenericDispatcherEventBuffer(_generic100KBufferStaticConfig);

      DbusEventAppender eventProducer = new DbusEventAppender(srcTestEvents, srcEventsBuf,null, true) ;
      Thread tEmitter = new Thread(eventProducer);
      tEmitter.start();

      //Create destination (client) buffer
      final TestGenericDispatcherEventBuffer destEventsBuf =
          new TestGenericDispatcherEventBuffer(_generic100KBufferStaticConfig);

      //Create dispatcher
      final TimeoutTestConsumer mockConsumer = new TimeoutTestConsumer(100, 10, 0, numOfFailureEvent, 0, 1);

      SelectingDatabusCombinedConsumer sdccMockConsumer =
          new SelectingDatabusCombinedConsumer((DatabusStreamConsumer)mockConsumer);

      List<String> sources = new ArrayList<String>();
      Map<Long, IdNamePair> sourcesMap = new HashMap<Long, IdNamePair>();
      for (int i = 1; i <= 3; ++i)
      {
          IdNamePair sourcePair = new IdNamePair((long)i, "source" + i);
          sources.add(sourcePair.getName());
          sourcesMap.put(sourcePair.getId(), sourcePair);
      }

      DatabusV2ConsumerRegistration consumerReg =
              new DatabusV2ConsumerRegistration(sdccMockConsumer, sources, null);

      List<DatabusV2ConsumerRegistration> allRegistrations = Arrays.asList(consumerReg);
      final ConsumerCallbackStats callbackStats = new ConsumerCallbackStats(0, "test", "test", true, false, null);
      final UnifiedClientStats unifiedStats = new UnifiedClientStats(0, "test", "test.unified");
      MultiConsumerCallback callback =
          new MultiConsumerCallback(allRegistrations,
                                    Executors.newFixedThreadPool(2),
                                    1000,
                                    new StreamConsumerCallbackFactory(callbackStats, unifiedStats),
                                    callbackStats,
                                    unifiedStats,
                                    null,
                                    null);
      callback.setSourceMap(sourcesMap);


      List<DatabusSubscription> subs = DatabusSubscription.createSubscriptionList(sources);
      final RelayDispatcher dispatcher =
              new RelayDispatcher("dispatcher", _genericRelayConnStaticConfig, subs,
                      new InMemoryPersistenceProvider(),
                      destEventsBuf, callback, null,null,null,null,null);
      dispatcher.setSchemaIdCheck(false);

      Thread dispatcherThread = new Thread(dispatcher);
      dispatcherThread.setDaemon(true);
      log.info("starting dispatcher thread");
      dispatcherThread.start();

      HashMap<Long, List<RegisterResponseEntry>> schemaMap =
              new HashMap<Long, List<RegisterResponseEntry>>();

      List<RegisterResponseEntry> l1 = new ArrayList<RegisterResponseEntry>();
      List<RegisterResponseEntry> l2 = new ArrayList<RegisterResponseEntry>();
      List<RegisterResponseEntry> l3 = new ArrayList<RegisterResponseEntry>();

      l1.add(new RegisterResponseEntry(1L, (short)1,SOURCE1_SCHEMA_STR));
      l2.add(new RegisterResponseEntry(2L, (short)1,SOURCE2_SCHEMA_STR));
      l3.add(new RegisterResponseEntry(3L, (short)1,SOURCE3_SCHEMA_STR));

      schemaMap.put(1L, l1);
      schemaMap.put(2L, l2);
      schemaMap.put(3L, l3);

      dispatcher.enqueueMessage(SourcesMessage.createSetSourcesIdsMessage(sourcesMap.values()));
      dispatcher.enqueueMessage(SourcesMessage.createSetSourcesSchemasMessage(schemaMap));

      log.info("starting event dispatch");

      //stream the events from the source buffer without the EOW

      //comm channels between reader and writer
      Pipe pipe = Pipe.open();
      Pipe.SinkChannel writerStream = pipe.sink();
      Pipe.SourceChannel readerStream = pipe.source();
      writerStream.configureBlocking(true);
      readerStream.configureBlocking(false);

      //Event writer - Relay in the real world
      Checkpoint cp = Checkpoint.createFlexibleCheckpoint();

      //Event readers - Clients in the real world
      //Checkpoint pullerCheckpoint = Checkpoint.createFlexibleCheckpoint();
      DbusEventsStatisticsCollector clientStats = new DbusEventsStatisticsCollector(0, "client", true, false, null);
      DbusEventBufferReader reader = new DbusEventBufferReader(destEventsBuf, readerStream, null, clientStats);
      UncaughtExceptionTrackingThread tReader = new UncaughtExceptionTrackingThread(reader,"Reader");
      tReader.setDaemon(true);
      tReader.start();

      try
      {
        log.info("send first window -- that one should be OK");
        StreamEventsResult streamRes = srcEventsBuf.streamEvents(cp, writerStream, new StreamEventsArgs(win1Size));
        Assert.assertEquals(numEventsPerWindow + 1, streamRes.getNumEventsStreamed());

        TestUtil.assertWithBackoff(new ConditionCheck()
        {
          @Override
          public boolean check()
          {
            return 1 == callbackStats.getNumSysEventsProcessed();
          }
        }, "first window processed", 5000, log);

        log.info("send the second partial window -- that one should cause an error");
        streamRes = srcEventsBuf.streamEvents(cp, writerStream, new StreamEventsArgs(win2Size));
        Assert.assertEquals(numOfFailureEvent - numEventsPerWindow, streamRes.getNumEventsStreamed());

        log.info("wait for dispatcher to finish");
        TestUtil.assertWithBackoff(new ConditionCheck()
        {
          @Override
          public boolean check()
          {
            log.info("events received: " + callbackStats.getNumDataEventsReceived());
            return numOfFailureEvent <= callbackStats.getNumDataEventsProcessed();
          }
        }, "all events until the error processed", 5000, log);

        log.info("all data events have been received but no EOW");
        Assert.assertEquals(numOfFailureEvent, clientStats.getTotalStats().getNumDataEvents());
        Assert.assertEquals(1, clientStats.getTotalStats().getNumSysEvents());
        //at least one failing event therefore < numOfFailureEvent events can be processed
        Assert.assertTrue(numOfFailureEvent <= callbackStats.getNumDataEventsProcessed());
        //onDataEvent callbacks for e2_1 and e2_2 get cancelled
        Assert.assertEquals(2, callbackStats.getNumDataErrorsProcessed());
        //only one EOW
        Assert.assertEquals(1, callbackStats.getNumSysEventsProcessed());

        log.info("Send the remainder of the window");
        streamRes = srcEventsBuf.streamEvents(cp, writerStream, new StreamEventsArgs(100000));
        //remaining events + EOWs
        Assert.assertEquals(srcTestEvents.size() + numWindows - (numOfFailureEvent + 1),
                            streamRes.getNumEventsStreamed());

        log.info("wait for the rollback");
        TestUtil.assertWithBackoff(new ConditionCheck()
        {
          @Override
          public boolean check()
          {
            return 1 == mockConsumer.getNumRollbacks();
          }
        }, "rollback seen", 5000, log);

        log.info("wait for dispatcher to finish after the rollback");
        TestUtil.assertWithBackoff(new ConditionCheck()
        {
          @Override
          public boolean check()
          {
            log.info("num windows processed: " + callbackStats.getNumSysEventsProcessed());
            return numWindows == callbackStats.getNumSysEventsProcessed();
          }
        }, "all events processed", 5000, log);
      }
      finally
      {
        reader.stop();
        dispatcher.shutdown();

        log.info("all events processed");

        verifyNoLocks(null, srcEventsBuf);
        verifyNoLocks(null, destEventsBuf);
      }

      Logger.getLogger("com.linkedin.databus.client").setLevel(saveLevel);
      log.info("end\n");
    }



  // TODO Change this class to behave like bootstrap dispatcher or relay dispatcher depending on what we are
  // testing. If bootstrap dispatcher, then we need to override processSysEvents to construct checkpoint when
  // a checkpoint event is received (or better, initialize checkpoint in ctor), and then override createCheckpoint
  // method to call onEvent on the saved checkpoint.
class TestDispatcher<C> extends GenericDispatcher<C>
{
    private final boolean _isRelayDispatcher;

    public TestDispatcher(String name,
                          DatabusSourcesConnection.StaticConfig connConfig,
                          List<DatabusSubscription> subsList,
                          CheckpointPersistenceProvider checkpointPersistor,
                          DbusEventBuffer dataEventsBuffer,
                          MultiConsumerCallback asyncCallback,
                          boolean isRelayDispatcher) {
        super(name, connConfig, subsList, checkpointPersistor, dataEventsBuffer,
                asyncCallback, null);
        _isRelayDispatcher = isRelayDispatcher;
        //disable schemaIdCheck at onStartSource() by default, in the interest of many unit tests written without paying attention to same schemaIds being present in events
        _schemaIdCheck=false;
    }

    @Override
    protected Checkpoint createCheckpoint(DispatcherState curState,
            DbusEvent event) {
      if (_isRelayDispatcher)
      {
       return createOnlineConsumptionCheckpoint(_lastWindowScn, _lastEowTsNsecs, curState,event);
      }
      else
      {
        // TODO for bootstrap dispatcher: Update the prev checkpoint.
        return createOnlineConsumptionCheckpoint(_lastWindowScn, _lastEowTsNsecs, curState, event);
      }
    }

}


static class StateVerifyingStreamConsumer extends DelegatingDatabusCombinedConsumer
{
    public enum State
    {
        START_CONSUMPTION,
        START_DATA_EVENT_SEQUENCE,
        START_SOURCE,
        DATA_EVENT,
        END_SOURCE,
        END_DATA_EVENT_SEQUENCE,
        STOP_CONSUMPTION
    };

    private State _curState;
    private final HashSet<State> _expectedStates = new HashSet<State>();
    final Logger _log;

    public StateVerifyingStreamConsumer(DatabusStreamConsumer delegate)
    {
        this(delegate, null);
    }

    public StateVerifyingStreamConsumer(DatabusStreamConsumer delegate, Logger log)
    {
        super(delegate, null);
        _expectedStates.add(State.START_CONSUMPTION);
        _curState = null;
        _log = null != log ? log : Logger.getLogger(StateVerifyingStreamConsumer.class);
    }

    private void expectStates(State... states)
    {
      _expectedStates.clear();
      _expectedStates.add(State.STOP_CONSUMPTION); //always expect stop consumption in case of an error
      for (State state: states)
      {
        _expectedStates.add(state);
      }
    }

    @Override
    public ConsumerCallbackResult onStartConsumption()
    {
        assertState(State.START_CONSUMPTION);
        expectStates(State.START_DATA_EVENT_SEQUENCE);

        return super.onStartConsumption();
    }

    @Override
    public ConsumerCallbackResult onStopConsumption()
    {
        assertState(State.STOP_CONSUMPTION);
        _expectedStates.clear();

        return super.onStopConsumption();
    }

    @Override
    public ConsumerCallbackResult onStartDataEventSequence(SCN startScn)
    {
        assertState(State.START_DATA_EVENT_SEQUENCE);
        expectStates(State.END_DATA_EVENT_SEQUENCE, State.START_SOURCE);

        return super.onStartDataEventSequence(startScn);
    }

    @Override
    public ConsumerCallbackResult onEndDataEventSequence(SCN endScn)
    {
        assertState(State.END_DATA_EVENT_SEQUENCE);
        expectStates(State.START_DATA_EVENT_SEQUENCE);

        return super.onEndDataEventSequence(endScn);
    }

    @Override
    public ConsumerCallbackResult onRollback(SCN startScn)
    {
        _expectedStates.clear();
        expectStates(State.START_DATA_EVENT_SEQUENCE);

        return super.onRollback(startScn);
    }

    @Override
    public ConsumerCallbackResult onStartSource(String source, Schema sourceSchema)
    {
        assertState(State.START_SOURCE);
        expectStates(State.DATA_EVENT, State.END_SOURCE);

        return super.onStartSource(source, sourceSchema);
    }

    @Override
    public ConsumerCallbackResult onEndSource(String source, Schema sourceSchema)
    {
        assertState(State.END_SOURCE);
        expectStates(State.START_SOURCE, State.END_DATA_EVENT_SEQUENCE);

        return super.onEndSource(source, sourceSchema);
    }

    @Override
    public synchronized ConsumerCallbackResult onDataEvent(DbusEvent e, DbusEventDecoder eventDecoder)
    {
        assertState(State.DATA_EVENT);
        expectStates(State.DATA_EVENT, State.END_SOURCE);

        return super.onDataEvent(e, eventDecoder);
    }

    @Override
    public ConsumerCallbackResult onCheckpoint(SCN checkpointScn)
    {
        expectStates(State.DATA_EVENT, State.START_SOURCE, State.END_SOURCE,
                     State.START_DATA_EVENT_SEQUENCE, State.END_DATA_EVENT_SEQUENCE);

        return super.onCheckpoint(checkpointScn);
    }

    private synchronized void assertState(State curState)
    {
      _log.debug("in state: " + curState + "; expected states: " + _expectedStates);
        assert _expectedStates.contains(curState) : "Unexpected state: " + curState + " expected: "
                + _expectedStates;
        _curState = curState;
    }


    public State getCurState()
    {
        return _curState;
    }

    @Override
    public Logger getLog()
    {
      return _log;
    }

}

class RollbackFailingConsumer extends DelegatingDatabusCombinedConsumer
{

  private boolean _checkpointSeen = false;
  private int _rollbackCnt = 0;

  public RollbackFailingConsumer(DatabusCombinedConsumer delegate, Logger log,
                                 int maxRollbacks)
  {
    super(delegate, log);
  }

  @Override
  public ConsumerCallbackResult onRollback(SCN rollbackScn)
  {
    ++_rollbackCnt;
    return super.onRollback(rollbackScn);
  }

  @Override
  public ConsumerCallbackResult onDataEvent(DbusEvent e, DbusEventDecoder eventDecoder)
  {
    return _checkpointSeen ? ConsumerCallbackResult.ERROR : super.onDataEvent(e, eventDecoder);
  }

  @Override
  public ConsumerCallbackResult onCheckpoint(SCN checkpointScn)
  {
    _checkpointSeen = true;
    return ConsumerCallbackResult.SKIP_CHECKPOINT;
  }

  public int getRollbackCnt()
  {
    return _rollbackCnt;
  }

}

class EventCountingConsumer extends DelegatingDatabusCombinedConsumer
{

    private final Hashtable<Long, AtomicInteger> _keyCounts;
    private final Hashtable<Short, AtomicInteger> _srcidCounts;
    private VersionedSchema _metadataSchema = null ;

    public EventCountingConsumer(DatabusStreamConsumer delegate,
            Hashtable<Long, AtomicInteger> keyCounts,
            Hashtable<Short, AtomicInteger> srcidCounts)
    {
        super(delegate, null);
        _keyCounts = keyCounts;
        _srcidCounts = srcidCounts;
    }

    @Override
    public ConsumerCallbackResult onDataEvent(DbusEvent e, DbusEventDecoder eventDecoder)
    {
        assert null != e : "Null event";

        //System.err.println("e=" + e.toString());

        AtomicInteger counter = _keyCounts.get(e.key());
        assert null != counter : "No counter for key: " + e.key();
        counter.incrementAndGet();

        AtomicInteger srcidCount = _srcidCounts.get((short)e.getSourceId());
        assert null != srcidCount : "No counter for source:" + e.getSourceId();
        srcidCount.incrementAndGet();
        _metadataSchema = ((DbusEventAvroDecoder)eventDecoder).getLatestMetadataSchema();
        return super.onDataEvent(e, eventDecoder);
    }

    public VersionedSchema getMetadataSchema()
    {
        return _metadataSchema;
    }
}

class DataEventFailingStreamConsumer extends AbstractDatabusStreamConsumer
{
    private final short _failingSrcid;

    public DataEventFailingStreamConsumer()
    {
        this((short)-1);
    }

    public DataEventFailingStreamConsumer(short failingSrcid)
    {
        _failingSrcid = failingSrcid;
    }

    @Override
    public ConsumerCallbackResult onDataEvent(DbusEvent e, DbusEventDecoder eventDecoder)
    {
        return (_failingSrcid > 0) && (_failingSrcid != (short)e.getSourceId()) ? ConsumerCallbackResult.SUCCESS
                : ConsumerCallbackResult.ERROR;
    }

}

class DataDecodingConsumer extends AbstractDatabusCombinedConsumer
{
    private int _numDataEvents=0;
    private int _numErrors=0;

    public  DataDecodingConsumer()
    {
      // TODO Auto-generated constructor stub
    }

    @Override
    public ConsumerCallbackResult onDataEvent(DbusEvent e, DbusEventDecoder eventDecoder)
    {
       try
       {
         _numDataEvents++;
         GenericRecord r = eventDecoder.getGenericRecord(e, null);
         return ConsumerCallbackResult.SUCCESS;
       }
       catch (Exception ex)
       {
         LOG.error("Error in processing event: " + ex);
       }
       return ConsumerCallbackResult.ERROR;
    }

    @Override
    public ConsumerCallbackResult onError(Throwable err)
    {
      _numErrors++;
      LOG.error("Error received: " + err.getMessage());
      return ConsumerCallbackResult.SUCCESS;
    }

    public int getNumDataEvents()
    {
      return _numDataEvents;
    }

    public int getNumErrors()
    {
      return _numErrors;
    }

}

class DataSourceFailingStreamConsumer extends AbstractDatabusStreamConsumer
{
    private final String _failingSrc;

    public DataSourceFailingStreamConsumer()
    {
        this(null);
    }

    public DataSourceFailingStreamConsumer(String failingSrc)
    {
        _failingSrc = failingSrc;
    }

    @Override
    public ConsumerCallbackResult onStartSource(String source, Schema sourceSchema)
    {
        boolean result = _failingSrc != null && ! source.equals(_failingSrc);

        //System.err.println("startSource[" + source + "]:" + result);

        return result ? ConsumerCallbackResult.SUCCESS : ConsumerCallbackResult.ERROR;
    }

}

class InMemoryPersistenceProvider extends CheckpointPersistenceProviderAbstract
{
    private final HashMap<List<String>, Checkpoint> _checkpoints;

    public InMemoryPersistenceProvider()
    {
        _checkpoints = new HashMap<List<String>, Checkpoint>();
    }

    @Override
    public void storeCheckpoint(List<String> sourceNames, Checkpoint checkpoint) throws IOException
    {
        _checkpoints.put(sourceNames, checkpoint);
    }

    @Override
    public Checkpoint loadCheckpoint(List<String> sourceNames)
    {
        return _checkpoints.get(sourceNames);
    }

    @Override
    public void removeCheckpoint(List<String> sourceNames)
    {
        _checkpoints.remove(sourceNames);
    }

    public HashMap<List<String>,Checkpoint> getCheckpoints()
    {
        return _checkpoints;
    }
}

/******* Timeout consumer ***********/
class TimeoutTestConsumer implements DatabusCombinedConsumer {

    private final long _timeoutInMs;
    private final long _controlEventTimeoutInMs;
    private final int _failCheckPoint;
    private final int _failData;
    private int _countCheckPoint;
    private int _countData;
    private int _countEndWindow;
    private int _storeData;
    private int _storeCheckpoint;
    private final int _failEndWindow;
    private final int _numFailureTimes;
    private int _countFailureTimes;
    private int _curWindowCount;
    private int _curDataCount;
    private int _curCkptCount;
    private final HashSet<Integer> _events;
    private long _lastSeenCheckpointScn = -1;
    private long _lastSeenWindowScn = -1;
    private int _numRollbacks = 0;
    private long _lastTsInNanosOfEvent=-1;
    private  long _lastTsInNanosOfWindow=-1;

    public TimeoutTestConsumer(long timeoutMs)
    {
        this(timeoutMs,0,0,0,0,0);
    }

    public TimeoutTestConsumer(long timeoutMs,int failCheckPoint,int failData,int failEndWindow,int numFailureTimes)
    {
        this(timeoutMs,timeoutMs,failCheckPoint,failData,failEndWindow,numFailureTimes);
    }

    /**
     *
     * @param timeoutMs : delay added for each call back
     * @param failCheckPoint : nth checkpoint at which failure occurs ; if negative failures occurs |failCheckpoint| times
     * @param failData : nth data event at which failure occurs
     * @param failEndWindow : nth end of window event at which failure occurs
     * @param numFailureTimes :  total number of failures allowed across all events before success
     */
    public TimeoutTestConsumer(long timeoutMs, long controlEventTimeoutInMs, int failCheckPoint,
                               int failData, int failEndWindow, int numFailureTimes)
    {
        _timeoutInMs = timeoutMs;
        _controlEventTimeoutInMs = controlEventTimeoutInMs;
        _failCheckPoint = failCheckPoint;
        _failData = failData;
        _failEndWindow=failEndWindow;
        _storeCheckpoint = 0;
        _storeData = 0;
        _countData = 0;
        _countCheckPoint = 0;
        _countEndWindow = 0;
        _countFailureTimes = 0;
        _numFailureTimes = numFailureTimes;
        _events = new HashSet<Integer>();
        _curWindowCount = 0;
        _curDataCount = 0;
        _curCkptCount = 0;
        if (failCheckPoint == 0 && failData == 0 && failEndWindow == 0)
        {
          Assert.assertEquals("Can't fail without specifying at least one non-zero failure type.",
                              0, numFailureTimes);
        }
    }

    @Override
    public ConsumerCallbackResult onStartConsumption() {
        return ConsumerCallbackResult.SUCCESS;
    }

    @Override
    public ConsumerCallbackResult onStopConsumption() {
        return ConsumerCallbackResult.SUCCESS;
    }

    @Override
    public ConsumerCallbackResult onStartDataEventSequence(SCN startScn) {
        return ConsumerCallbackResult.SUCCESS;
    }

    @Override
    public ConsumerCallbackResult onEndDataEventSequence(SCN endScn) {
        _countEndWindow++;
        _curWindowCount++;
        try {
            Thread.sleep(_controlEventTimeoutInMs);
        } catch (InterruptedException e1) {
        }
        if (_curWindowCount == _failEndWindow)
        {
            _countFailureTimes++;
            if (_countFailureTimes < _numFailureTimes)
            {
                //reset countData to simulate another failure;
                _curWindowCount = 0;
            }
            return ConsumerCallbackResult.ERROR;
        }
        _lastSeenWindowScn = ((SingleSourceSCN) endScn).getSequence();
        _lastTsInNanosOfWindow=_lastTsInNanosOfEvent;
        return ConsumerCallbackResult.SUCCESS;
    }

    @Override
    public ConsumerCallbackResult onRollback(SCN rollbackScn) {
        System.out.println("Rollback called on Scn = "  + rollbackScn);
        ++_numRollbacks;
        return ConsumerCallbackResult.SUCCESS;
    }

    @Override
    public ConsumerCallbackResult onStartSource(String source,
            Schema sourceSchema) {
        return ConsumerCallbackResult.SUCCESS;
    }

    @Override
    public ConsumerCallbackResult onEndSource(String source, Schema sourceSchema) {
        return ConsumerCallbackResult.SUCCESS;
    }

    @Override
    public ConsumerCallbackResult onDataEvent(DbusEvent e,
            DbusEventDecoder eventDecoder) {
        _countData++;
        _curDataCount++;
        _lastTsInNanosOfEvent = e.timestampInNanos();
        if (!e.isValid())
        {
            return ConsumerCallbackResult.ERROR;
        }
        try {
            Thread.sleep(_timeoutInMs);
        } catch (InterruptedException e1) {
        }

        if ((_failData != 0 ) && (_curDataCount ==_failData))
        {
            _countFailureTimes++;
            if (_countFailureTimes < _numFailureTimes)
            {
                //reset countData to simulate another failure;
                _curDataCount = 0;
            }
            return ConsumerCallbackResult.ERROR ;
        }
        else
        {
            _events.add(e.hashCode());
            _storeData++;
            return ConsumerCallbackResult.SUCCESS;
        }
    }
    @Override
    public ConsumerCallbackResult onCheckpoint(SCN checkpointScn) {
        _countCheckPoint++;
        _curCkptCount++;
        try {
            Thread.sleep(_controlEventTimeoutInMs);
        } catch (InterruptedException e1) {
        }
        if  ((_failCheckPoint != 0) && ((_curCkptCount == _failCheckPoint) || (_curCkptCount <= -_failCheckPoint)))
        {
            _countFailureTimes++;
            if (_countFailureTimes < _numFailureTimes)
            {
                //reset countData to simulate another failure;
                _curCkptCount = 0;
            }
            return ConsumerCallbackResult.ERROR ;
        } else {
            _storeCheckpoint++;
            _lastSeenCheckpointScn = ((SingleSourceSCN) checkpointScn).getSequence();
            return ConsumerCallbackResult.SUCCESS;
        }
    }


    @Override
    public ConsumerCallbackResult onError(Throwable err) {
        return ConsumerCallbackResult.SUCCESS;
    }

    public int getDataCallbackCount()
    {
        return _countData;
    }

    public int getWindowCount()
    {
        return _countEndWindow;
    }

    public int getCheckpointCallbackCount()
    {
        return _countCheckPoint;
    }

    public long getLastSeenWindowScn()
    {
        return _lastSeenWindowScn;
    }

    public long getLastSeenCheckpointScn()
    {
        return _lastSeenCheckpointScn;
    }

    public int getStoredDataCount()
    {
        return _storeData;
    }

    public int getStoredCheckpointCount()
    {
        return _storeCheckpoint;
    }

    public int getNumUniqStoredEvents()
    {
        return _events.size();
    }

    public int getNumFailedTimes()
    {
        return _countFailureTimes;
    }

    public long getLastTsInNanosOfEvent()
    {
      return _lastTsInNanosOfEvent;
    }

    public long getLastTsInNanosOfWindow()
    {
      return _lastTsInNanosOfWindow;
    }

    @Override
    public String toString()
    {
        return " Data callbacks = " + getDataCallbackCount()
                + " Windows = " + getWindowCount()
                + " Checkpoint callbacks = " + getCheckpointCallbackCount()
                + " Stored Data count = " + getStoredDataCount()
                + " Stored Checkpoint count = " + getStoredCheckpointCount()
                + " Unique stored data count = " + getNumUniqStoredEvents()
                + " Number of times failed = " + getNumFailedTimes()
                + " Last seen window scn = " + getLastSeenWindowScn()
                + " Last seen checkpoint scn = " + getLastSeenCheckpointScn();
    }

    @Override
    public ConsumerCallbackResult onStartBootstrap()
    {
        return ConsumerCallbackResult.SUCCESS;
    }

    @Override
    public ConsumerCallbackResult onStopBootstrap()
    {
        return ConsumerCallbackResult.SUCCESS;
    }

    @Override
    public ConsumerCallbackResult onStartBootstrapSequence(SCN startScn)
    {
        return onStartDataEventSequence(startScn);
    }

    @Override
    public ConsumerCallbackResult onEndBootstrapSequence(SCN endScn)
    {
        return onEndDataEventSequence(endScn);
    }

    @Override
    public ConsumerCallbackResult onStartBootstrapSource(String sourceName,
            Schema sourceSchema)
    {
       return onStartSource(sourceName, sourceSchema);
    }

    @Override
    public ConsumerCallbackResult onEndBootstrapSource(String name,
            Schema sourceSchema)
    {
        return onEndSource(name, sourceSchema);
    }

    @Override
    public ConsumerCallbackResult onBootstrapEvent(DbusEvent e,
            DbusEventDecoder eventDecoder)
    {
        return onDataEvent(e, eventDecoder);
    }

    @Override
    public ConsumerCallbackResult onBootstrapRollback(SCN batchCheckpointScn)
    {
        return onRollback(batchCheckpointScn);
    }

    @Override
    public ConsumerCallbackResult onBootstrapCheckpoint(SCN checkpointScn)
    {
        return onCheckpoint(checkpointScn);
    }

    @Override
    public ConsumerCallbackResult onBootstrapError(Throwable err)
    {
        return onError(err);
    }

    @Override
    public boolean canBootstrap()
    {
        return true;
    }

    /**
     * @return the numRollbacks
     */
    public int getNumRollbacks()
    {
      return _numRollbacks;
    }
}


}

