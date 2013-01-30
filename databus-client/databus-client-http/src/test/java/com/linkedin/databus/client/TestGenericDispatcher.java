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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;

import org.apache.avro.Schema;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

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
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DatabusComponentStatus;
import com.linkedin.databus.core.DbusClientMode;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventBuffer.AllocationPolicy;
import com.linkedin.databus.core.DbusEventBuffer.QueuePolicy;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.util.DbusEventAppender;
import com.linkedin.databus.core.util.DbusEventGenerator;
import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus.core.util.RangeBasedReaderWriterLock;
import com.linkedin.databus2.core.container.request.RegisterResponseEntry;
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

    private void initBufferWithEvents(DbusEventBuffer eventsBuf,
            long keyBase,
            int numEvents,
            short srcId,
            Hashtable<Long, AtomicInteger> keyCounts,
            Hashtable<Short, AtomicInteger> srcidCounts)
    {
        if (null != srcidCounts) srcidCounts.put(srcId, new AtomicInteger(0));

        for (long i = 0; i < numEvents; ++i)
        {
            String value = "{\"s\":\"value" + i + "\"}";
            try {
				eventsBuf.appendEvent(new DbusEventKey(keyBase + i), (short)0, (short)1, (short)0, srcId,
				        new byte[16], value.getBytes("UTF-8"), false);
			} catch (UnsupportedEncodingException e) {
				//ignore
			}
            if (null != keyCounts) keyCounts.put(keyBase + i, new AtomicInteger(0));
        }
    }

    @BeforeClass
    public void beforeClass() throws Exception
    {
      TestUtil.setupLogging(true, null, Level.ERROR);
        _generic100KBufferConfig = new DbusEventBuffer.Config();
        _generic100KBufferConfig.setAllocationPolicy(AllocationPolicy.HEAP_MEMORY.toString());
        _generic100KBufferConfig.setMaxSize(100000);
        _generic100KBufferConfig.setQueuePolicy(QueuePolicy.BLOCK_ON_WRITE.toString());

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

        log.info("starting");
        int source1EventsNum = 2;
        int source2EventsNum = 2;

        Hashtable<Long, AtomicInteger> keyCounts = new Hashtable<Long, AtomicInteger>();
        Hashtable<Short, AtomicInteger> srcidCounts = new Hashtable<Short, AtomicInteger>();

        final TestGenericDispatcherEventBuffer eventsBuf =
            new TestGenericDispatcherEventBuffer(_generic100KBufferStaticConfig);
        eventsBuf.start(0);
        eventsBuf.startEvents();
        initBufferWithEvents(eventsBuf, 1, source1EventsNum, (short)1, keyCounts, srcidCounts);
        initBufferWithEvents(eventsBuf, 1 + source1EventsNum, source2EventsNum, (short)2, keyCounts, srcidCounts);
        eventsBuf.endEvents(100L,null);

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
        MultiConsumerCallback<DatabusCombinedConsumer> callback =
                new MultiConsumerCallback<DatabusCombinedConsumer>(
                        allRegistrations,
                        Executors.newSingleThreadExecutor(),
                        1000,
                        new StreamConsumerCallbackFactory(),null);
        callback.setSourceMap(sourcesMap);

        List<DatabusSubscription> subs = DatabusSubscription.createSubscriptionList(sources);
        RelayDispatcher dispatcher =
                new RelayDispatcher("dispatcher", _genericRelayConnStaticConfig, subs,
                        new InMemoryPersistenceProvider(),
                        eventsBuf, callback, null,null,null, null);

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

        dispatcher.enqueueMessage(
                DispatcherState.create().switchToStartDispatchEvents(sourcesMap,
                        schemaMap, eventsBuf));

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

        assertEquals("correct amount of callbacks for srcid 1", source1EventsNum,
                     srcidCounts.get((short)1).intValue());
        assertEquals("correct amount of callbacks for srcid 2", source2EventsNum,
                     srcidCounts.get((short)2).intValue());
        verifyNoLocks(log, eventsBuf);
        log.info("done");
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
        MultiConsumerCallback<DatabusCombinedConsumer> callback =
                new MultiConsumerCallback<DatabusCombinedConsumer>(
                        allRegistrations,
                        Executors.newSingleThreadExecutor(),
                        1000,
                        new StreamConsumerCallbackFactory(),null);
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
                        eventsBuf, callback, null,null,null, null);

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

        dispatcher.enqueueMessage(
                DispatcherState.create().switchToStartDispatchEvents(sourcesMap,
                        schemaMap, eventsBuf));
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
    }

    @Test(groups = {"small", "functional"})
    public void testMultiWindowsHappyPath()
    {
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
        MultiConsumerCallback<DatabusCombinedConsumer> callback =
                new MultiConsumerCallback<DatabusCombinedConsumer>(
                        allRegistrations,
                        Executors.newSingleThreadExecutor(),
                        1000,
                        new StreamConsumerCallbackFactory(),
                        null
                        );
        callback.setSourceMap(sourcesMap);

        List<DatabusSubscription> subs = DatabusSubscription.createSubscriptionList(sources);
        RelayDispatcher dispatcher =
                new RelayDispatcher("dispatcher", _genericRelayConnStaticConfig, subs,
                        new InMemoryPersistenceProvider(),
                        eventsBuf, callback, null,null,null,null);

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

        dispatcher.enqueueMessage(
                DispatcherState.create().switchToStartDispatchEvents(sourcesMap,
                        schemaMap, eventsBuf));

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
    }

    @Test(groups = {"small", "functional"})
    public void testOneWindowTwoIndependentConsumersHappyPath()
    {
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
        MultiConsumerCallback<DatabusCombinedConsumer> callback =
                new MultiConsumerCallback<DatabusCombinedConsumer>(
                        allRegistrations,
                        Executors.newFixedThreadPool(2),
                        1000,
                        new StreamConsumerCallbackFactory(),
                        null);
        callback.setSourceMap(sourcesMap);

        List<DatabusSubscription> subs = DatabusSubscription.createSubscriptionList(sources);
        RelayDispatcher dispatcher =
                new RelayDispatcher("dispatcher", _genericRelayConnStaticConfig, subs,
                        new InMemoryPersistenceProvider(),
                        eventsBuf, callback, null,null,null,null);

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

        dispatcher.enqueueMessage(
                DispatcherState.create().switchToStartDispatchEvents(sourcesMap,
                        schemaMap, eventsBuf));

        try
        {
            Thread.sleep(2500);
        }
        catch (InterruptedException ie) {}

        dispatcher.shutdown();

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
    }

    @Test(groups = {"small", "functional"})
    public void testOneWindowTwoGroupedConsumersHappyPath()
    {
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
        MultiConsumerCallback<DatabusCombinedConsumer> callback =
                new MultiConsumerCallback<DatabusCombinedConsumer>(
                        allRegistrations,
                        Executors.newFixedThreadPool(2),
                        1000,
                        new StreamConsumerCallbackFactory(),null);
        callback.setSourceMap(sourcesMap);

        List<DatabusSubscription> subs = DatabusSubscription.createSubscriptionList(sources);
        RelayDispatcher dispatcher =
                new RelayDispatcher("dispatcher", _genericRelayConnStaticConfig, subs,
                        new InMemoryPersistenceProvider(),
                        eventsBuf, callback, null, null,null,null);

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

        dispatcher.enqueueMessage(
                DispatcherState.create().switchToStartDispatchEvents(sourcesMap,
                        schemaMap, eventsBuf));

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
    }

    @Test(groups = {"small", "functional"})
    public void testTwoWindowEventCallbackFailure()
    {
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
        MultiConsumerCallback<DatabusCombinedConsumer> callback =
                new MultiConsumerCallback<DatabusCombinedConsumer>(
                        allRegistrations,
                        Executors.newSingleThreadExecutor(),
                        1000,
                        new StreamConsumerCallbackFactory(),null);
        callback.setSourceMap(sourcesMap);

        List<DatabusSubscription> subs = DatabusSubscription.createSubscriptionList(sources);
        RelayDispatcher dispatcher =
                new RelayDispatcher("dispatcher", _genericRelayConnStaticConfig, subs,
                        new InMemoryPersistenceProvider(),
                        eventsBuf, callback, null,null,null,null);

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

        dispatcher.enqueueMessage(
                DispatcherState.create().switchToStartDispatchEvents(sourcesMap,
                        schemaMap, eventsBuf));

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
    }

    @Test(groups = {"small", "functional"})
    public void testTwoWindowEventIndependentConsumersCallbackFailure()
    {
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
        MultiConsumerCallback<DatabusCombinedConsumer> callback =
                new MultiConsumerCallback<DatabusCombinedConsumer>(
                        allRegistrations,
                        Executors.newSingleThreadExecutor(),
                        1000,
                        new StreamConsumerCallbackFactory(),null);
        callback.setSourceMap(sourcesMap);

        List<DatabusSubscription> subs = DatabusSubscription.createSubscriptionList(sources);
        RelayDispatcher dispatcher =
                new RelayDispatcher("dispatcher", _genericRelayConnStaticConfig, subs,
                        new InMemoryPersistenceProvider(),
                        eventsBuf, callback, null,null,null,null);

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

        dispatcher.enqueueMessage(
                DispatcherState.create().switchToStartDispatchEvents(sourcesMap,
                        schemaMap, eventsBuf));

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
    }


    @Test(groups = {"small", "functional"})
    public void testLargeWindowCheckpointFrequency()
    {
        try
        {
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
            MultiConsumerCallback<DatabusCombinedConsumer> mConsumer = new MultiConsumerCallback<DatabusCombinedConsumer>(allRegistrations,Executors.newFixedThreadPool(2),
            		1000,new StreamConsumerCallbackFactory(),null);

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
            if (evGen.generateEvents(numEvents,maxWindowSize,512,payloadSize,srcTestEvents) <= 0) {
                assert(false);
                return;
            }
            int size=0;
            for (DbusEvent e:srcTestEvents)
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
                    mConsumer);

            /* Launch writer */
            DbusEventAppender eventProducer = new DbusEventAppender(srcTestEvents, dataEventsBuffer, null) ;
            Thread tEmitter = new Thread(eventProducer);
            tEmitter.start();
            tEmitter.join();

            /* Launch dispatcher */
            Thread tDispatcher = new Thread(dispatcher);
            tDispatcher.start();

            /* Now initialize this damn state machine */
            dispatcher.enqueueMessage(
                    DispatcherState.create().switchToStartDispatchEvents(sourcesMap,
                            schemaMap, dataEventsBuffer));

            tDispatcher.join(2000);
            System.out.println("Number of checkpoints = " + dispatcher.getNumCheckPoints());
            assert(dispatcher.getNumCheckPoints()==3);
            dispatcher.shutdown();
            verifyNoLocks(null, dataEventsBuffer);
        }
        catch (InterruptedException e)
        {
            assert(false);
        }
        catch (InvalidConfigException e)
        {
            assert(false);
        }
    }

    @Test(groups = {"small", "functional"})
    public void testControlEventsRemoval()
    {
    	//DDSDBUS-559
        try
        {
            /* Consumer creation */
            int timeTakenForEventInMs = 1;
            TimeoutTestConsumer tConsumer = new TimeoutTestConsumer(timeTakenForEventInMs);
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
            MultiConsumerCallback<DatabusCombinedConsumer> mConsumer = new MultiConsumerCallback<DatabusCombinedConsumer>(allRegistrations,Executors.newFixedThreadPool(2),
            		1000,new StreamConsumerCallbackFactory(),null);

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
            int maxWindowSize =1;
            DbusEventGenerator evGen = new DbusEventGenerator(0,srcIdList);
            if (evGen.generateEvents(numEvents,maxWindowSize,512,payloadSize,srcTestEvents) <= 0) {
                assert(false);
                return;
            }
            int size=61;

            long lastWindowScn = srcTestEvents.get(srcTestEvents.size()-1).sequence();

            //make buffer large enough to hold data
            int producerBufferSize = (int) (numEvents*size*1.1);
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
                    mConsumer);

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

            /* Now initialize this damn state machine */
            dispatcher.enqueueMessage(
                    DispatcherState.create().switchToStartDispatchEvents(sourcesMap,
                            schemaMap, dataEventsBuffer));

            tDispatcher.join(5000);
            System.out.println("Free Space After=" + dataEventsBuffer.getBufferFreeSpace() +
            		" tConsumer=" + tConsumer + " expected last window=" + lastWindowScn +
            		" last Window = " + dataEventsBuffer.lastWrittenScn());

            Assert.assertTrue(dataEventsBuffer.lastWrittenScn()==lastWindowScn);
            Assert.assertTrue(freeSpaceBefore < dataEventsBuffer.getBufferFreeSpace());

            dispatcher.shutdown();
            verifyNoLocks(null, dataEventsBuffer);
        }
        catch (InterruptedException e)
        {
            assert(false);
        }
        catch (InvalidConfigException e)
        {
            assert(false);
        }
    }

    protected void runDispatcherRollback(int numEvents,int maxWindowSize,int numFailDataEvent,int numFailCheckpointEvent,int numFailEndWindow)
    {
        runDispatcherRollback(numEvents, maxWindowSize, numFailDataEvent, numFailCheckpointEvent, numFailEndWindow,90.0,false);
    }

    protected void runDispatcherRollback(int numEvents,int maxWindowSize,int numFailDataEvent,int numFailCheckpointEvent,int numFailEndWindow,double pct)
    {
        runDispatcherRollback(numEvents, maxWindowSize, numFailDataEvent, numFailCheckpointEvent, numFailEndWindow,pct,false);
    }

    protected void runDispatcherRollback(int numEvents,int maxWindowSize,int numFailDataEvent,int numFailCheckpointEvent,int numFailEndWindow,double
            thresholdPct,boolean negativeTest)
    {
        try
        {
            /* Experiment setup */
            int timeTakenForEventInMs = 1;
            int payloadSize = 20;
            int numCheckpoints = numEvents/maxWindowSize;

            /* Consumer creation */
            //setup consumer to fail on data callback at the nth event
            TimeoutTestConsumer tConsumer = new TimeoutTestConsumer(timeTakenForEventInMs,numFailCheckpointEvent,numFailDataEvent,numFailEndWindow);
            SelectingDatabusCombinedConsumer sdccTConsumer = new SelectingDatabusCombinedConsumer(tConsumer);

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
            //Single threaded execution of consumer
            MultiConsumerCallback<DatabusCombinedConsumer> mConsumer = new MultiConsumerCallback<DatabusCombinedConsumer>(allRegistrations,Executors.newFixedThreadPool(1),
                    1000,new StreamConsumerCallbackFactory(),null);

            /* Source configuration */
            double thresholdChkptPct = thresholdPct;
            DatabusSourcesConnection.Config conf = new DatabusSourcesConnection.Config();
            conf.setCheckpointThresholdPct(thresholdChkptPct);
            conf.getDispatcherRetries().setMaxRetryNum(10);
            DatabusSourcesConnection.StaticConfig connConfig = conf.build();

            /* Generate events **/
            Vector<DbusEvent> srcTestEvents = new Vector<DbusEvent>();
            Vector<Short> srcIdList = new Vector<Short> ();
            srcIdList.add(srcId);

            DbusEventGenerator evGen = new DbusEventGenerator(0,srcIdList);
            if (evGen.generateEvents(numEvents,maxWindowSize,512,payloadSize,srcTestEvents) <= 0) {
                assert(false);
                return;
            }
            int size=0;
            for (DbusEvent e:srcTestEvents)
            {
                if (!e.isControlMessage()) {
                    size = e.size();
                    break;
                }
            }

            //make buffer large enough to hold data
            int producerBufferSize = numEvents*size*2;
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
                    mConsumer);

            /* Launch writer */
            DbusEventAppender eventProducer = new DbusEventAppender(srcTestEvents, dataEventsBuffer, null) ;
            Thread tEmitter = new Thread(eventProducer);
            tEmitter.start();
            tEmitter.join();

            /* Launch dispatcher */
            Thread tDispatcher = new Thread(dispatcher);
            tDispatcher.start();

            /* Now initialize this  state machine */
            dispatcher.enqueueMessage(
                    DispatcherState.create().switchToStartDispatchEvents(sourcesMap,
                            schemaMap, dataEventsBuffer));

            tDispatcher.join(2000);
            System.out.println("tConsumer: " + tConsumer);

            int windowBeforeDataFail=(numFailDataEvent/maxWindowSize);
            int expectedDataFaults = numFailDataEvent == 0 ? 0:1;

            int expectedCheckPointFaults = (numFailCheckpointEvent==0 || (expectedDataFaults!=0 && (numFailCheckpointEvent==windowBeforeDataFail))) ? 0:1;
            //Dispatcher/Library perspective
            //check if all windows were logged by dispatcher

            assert(dispatcher.getNumCheckPoints()>=(numCheckpoints-expectedCheckPointFaults));

            //Consumer prespective
            //1 or 0 faults  injected in data callbacks; success (store) differs callback by 1
            assert((tConsumer.getDataCallbackCount()-tConsumer.getStoredDataCount())==expectedDataFaults);
            assert(tConsumer.getStoredDataCount() >= tConsumer.getNumUniqStoredEvents());
            assert(tConsumer.getStoredCheckpointCount()==dispatcher.getNumCheckPoints());
            //rollback behaviour;were all events  re-sent?
            if (!negativeTest)
            {
                assert(tConsumer.getNumUniqStoredEvents()==numEvents);
            }
            else
            {
                assert(tConsumer.getNumUniqStoredEvents() < numEvents);
            }

            dispatcher.shutdown();
            verifyNoLocks(null, dataEventsBuffer);
        }
        catch (InterruptedException e)
        {
            assert(false);
        }
        catch (InvalidConfigException e)
        {
            assert(false);
        }
    }

    @Test(groups = {"small", "functional"})
    public void testRollback()
    {
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

        //large window irrecoverable failure : fail first checkpoint
         runDispatcherRollback(100,80,0,1,1,30.0,true);

    	//onCheckpoint always returns null; forces removal of events; but lastSuccessful iterator is null  ; DDSDBUS-653
    	runDispatcherRollback(100,120,0,-2,0,10.0);

    }


    DbusEventBuffer.StaticConfig getConfig(long maxEventBufferSize, int maxIndividualBufferSize, int maxIndexSize,
            int maxReadBufferSize, AllocationPolicy allocationPolicy, QueuePolicy policy) throws InvalidConfigException
            {
        DbusEventBuffer.Config config = new DbusEventBuffer.Config();
        config.setMaxSize(maxEventBufferSize);
        config.setMaxIndividualBufferSize(maxIndividualBufferSize);
        config.setScnIndexSize(maxIndexSize);
        config.setReadBufferSize(maxReadBufferSize);
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


}

class TestDispatcher<C> extends GenericDispatcher<C>
{

    public TestDispatcher(String name, DatabusSourcesConnection.StaticConfig connConfig,
            List<DatabusSubscription> subsList,
            CheckpointPersistenceProvider checkpointPersistor,
            DbusEventBuffer dataEventsBuffer,
            MultiConsumerCallback<C> asyncCallback) {
        super(name, connConfig, subsList, checkpointPersistor, dataEventsBuffer,
                asyncCallback);
    }

    @Override
    protected Checkpoint createCheckpoint(DispatcherState curState,
            DbusEvent event) {
        Checkpoint cp = new Checkpoint();
        cp.setConsumptionMode(DbusClientMode.ONLINE_CONSUMPTION);
        cp.setWindowScn(event.sequence());
        cp.setWindowOffset(-1);
        return cp;
    }

}


class StateVerifyingStreamConsumer extends DelegatingDatabusCombinedConsumer
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

    public StateVerifyingStreamConsumer(DatabusStreamConsumer delegate)
    {
        super(delegate, null);
        _expectedStates.add(State.START_CONSUMPTION);
        _curState = null;
    }

    @Override
    public ConsumerCallbackResult onStartConsumption()
    {
        assertState(State.START_CONSUMPTION);
        _expectedStates.clear();
        _expectedStates.add(State.START_DATA_EVENT_SEQUENCE);
        _expectedStates.add(State.STOP_CONSUMPTION);

        return super.onStartConsumption();
    }

    @Override
    public ConsumerCallbackResult onStopConsumption()
    {
        assertState(State.STOP_CONSUMPTION);
        _expectedStates.clear();
        _expectedStates.add(State.STOP_CONSUMPTION);

        return super.onStopConsumption();
    }

    @Override
    public ConsumerCallbackResult onStartDataEventSequence(SCN startScn)
    {
        assertState(State.START_DATA_EVENT_SEQUENCE);
        _expectedStates.clear();
        _expectedStates.add(State.END_DATA_EVENT_SEQUENCE);
        _expectedStates.add(State.START_SOURCE);

        return super.onStartDataEventSequence(startScn);
    }

    @Override
    public ConsumerCallbackResult onEndDataEventSequence(SCN endScn)
    {
        assertState(State.END_DATA_EVENT_SEQUENCE);
        _expectedStates.clear();
        _expectedStates.add(State.START_DATA_EVENT_SEQUENCE);
        _expectedStates.add(State.STOP_CONSUMPTION);

        return super.onEndDataEventSequence(endScn);
    }

    @Override
    public ConsumerCallbackResult onRollback(SCN startScn)
    {
        _expectedStates.clear();
        _expectedStates.add(State.START_DATA_EVENT_SEQUENCE);
        _expectedStates.add(State.STOP_CONSUMPTION);

        return super.onRollback(startScn);
    }

    @Override
    public ConsumerCallbackResult onStartSource(String source, Schema sourceSchema)
    {
        assertState(State.START_SOURCE);
        _expectedStates.clear();
        _expectedStates.add(State.DATA_EVENT);
        _expectedStates.add(State.END_SOURCE);

        return super.onStartSource(source, sourceSchema);
    }

    @Override
    public ConsumerCallbackResult onEndSource(String source, Schema sourceSchema)
    {
        assertState(State.END_SOURCE);
        _expectedStates.clear();
        _expectedStates.add(State.START_SOURCE);
        _expectedStates.add(State.END_DATA_EVENT_SEQUENCE);

        return super.onEndSource(source, sourceSchema);
    }

    @Override
    public ConsumerCallbackResult onDataEvent(DbusEvent e, DbusEventDecoder eventDecoder)
    {
        assertState(State.DATA_EVENT);
        _expectedStates.clear();
        _expectedStates.add(State.DATA_EVENT);
        _expectedStates.add(State.END_SOURCE);

        return super.onDataEvent(e, eventDecoder);
    }

    @Override
    public ConsumerCallbackResult onCheckpoint(SCN checkpointScn)
    {
        _expectedStates.clear();
        _expectedStates.add(State.DATA_EVENT);
        _expectedStates.add(State.START_SOURCE);
        _expectedStates.add(State.END_SOURCE);
        _expectedStates.add(State.START_DATA_EVENT_SEQUENCE);
        _expectedStates.add(State.END_DATA_EVENT_SEQUENCE);
        _expectedStates.add(State.STOP_CONSUMPTION);

        return super.onCheckpoint(checkpointScn);
    }

    private void assertState(State curState)
    {
        assert _expectedStates.contains(curState) : "Unexpected state: " + curState + " expected: "
                + _expectedStates;
        _curState = curState;
    }


    public State getCurState()
    {
        return _curState;
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
    return ConsumerCallbackResult.ERROR;
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

        AtomicInteger srcidCount = _srcidCounts.get(e.srcId());
        assert null != srcidCount : "No counter for source:" + e.srcId();
        srcidCount.incrementAndGet();

        return super.onDataEvent(e, eventDecoder);
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
        return (_failingSrcid > 0) && (_failingSrcid != e.srcId()) ? ConsumerCallbackResult.SUCCESS
                : ConsumerCallbackResult.ERROR;
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
}

/******* Timeout consumer ***********/
class TimeoutTestConsumer implements DatabusStreamConsumer {

    private long _timeoutInMs;
    private int _failCheckPoint;
    private int _failData;
    private int _countCheckPoint;
    private int _countData;
    private int _countEndWindow;
    private int _storeData;
    private int _storeCheckpoint;
    private int _failEndWindow;
    private HashSet<Integer> _events;

    public TimeoutTestConsumer(long timeoutMs)
    {
        this(timeoutMs,0,0,0);
    }

    /*! Specify the modulo n instance of call that should fail */
    /* if failCheckPoint is negative; then fail on every checkPoint until |failCheckPoint| */
    public TimeoutTestConsumer(long timeoutMs,int failCheckPoint,int failData,int failEndWindow)
    {
        _timeoutInMs = timeoutMs;
        _failCheckPoint = failCheckPoint;
        _failData = failData;
        _failEndWindow=failEndWindow;
        _storeCheckpoint = 0;
        _storeData = 0;
        _countData = 0;
        _countCheckPoint = 0;
        _countEndWindow = 0;
        _events = new HashSet<Integer>();
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
    	System.out.println("start: ");
        return ConsumerCallbackResult.SUCCESS;
    }

    @Override
    public ConsumerCallbackResult onEndDataEventSequence(SCN endScn) {
        _countEndWindow++;
        return (_countEndWindow == _failEndWindow) ? ConsumerCallbackResult.ERROR: ConsumerCallbackResult.SUCCESS;
    }

    @Override
    public ConsumerCallbackResult onRollback(SCN rollbackScn) {
        System.out.println("Rollback called on Scn = "  + rollbackScn);
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
        try {
            Thread.sleep(_timeoutInMs);
        } catch (InterruptedException e1) {
        }
        if ((_failData != 0 ) && (_countData ==_failData))
        {
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
        if  ((_failCheckPoint != 0) && (_countCheckPoint == _failCheckPoint) || (_countCheckPoint <= -_failCheckPoint))
        {
            return ConsumerCallbackResult.ERROR ;
        } else {
            _storeCheckpoint++;
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

    @Override
    public String toString()
    {
        return " Data callbacks = " + getDataCallbackCount()
        		+ " Windows = " + getWindowCount()
                + " Checkpoint callbacks = " + getCheckpointCallbackCount()
                + " Stored Data count = " + getStoredDataCount()
                + " Stored Checkpoint count = " + getStoredCheckpointCount()
                + " Unique stored data count = " + getNumUniqStoredEvents();
    }
}


