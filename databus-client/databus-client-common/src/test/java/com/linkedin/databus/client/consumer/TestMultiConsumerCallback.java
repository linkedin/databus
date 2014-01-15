package com.linkedin.databus.client.consumer;
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

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import com.linkedin.databus.client.SingleSourceSCN;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DatabusCombinedConsumer;
import com.linkedin.databus.client.pub.DatabusStreamConsumer;
import com.linkedin.databus.client.pub.mbean.ConsumerCallbackStats;
import com.linkedin.databus.client.pub.mbean.UnifiedClientStats;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventBuffer.AllocationPolicy;
import com.linkedin.databus.core.DbusEventBuffer.QueuePolicy;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus2.test.TestUtil;


public class TestMultiConsumerCallback
{
  public static final String MODULE = TestMultiConsumerCallback.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private DbusEventBuffer.Config _generic100KBufferConfig;
  private DbusEventBuffer.StaticConfig _generic100KBufferStaticConfig;

  static
  {
    TestUtil.setupLogging(true, "/tmp/TestMultiConsumerCallback.txt", Level.OFF);
  }

  private void initMockStreamConsumer3EventFullLifecycle(DatabusStreamConsumer mockConsumer,
                                                         DbusEvent event1,
                                                         DbusEvent event2,
                                                         DbusEvent event3,
                                                         Hashtable<Long, AtomicInteger> keyCounts)
  {
    EasyMock.makeThreadSafe(mockConsumer, true);
     EasyMock.expect(mockConsumer.onStartConsumption()).andAnswer(
         new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG,
             "startConsumption() called"));
     EasyMock.expect(mockConsumer.onStartDataEventSequence(null)).andAnswer(
         new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG,
             "startDataEventSequence() called"));
     EasyMock.expect(mockConsumer.onStartSource("source1", null)).andAnswer(
         new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG,
             "startDataEventSequence() called"));
     EasyMock.expect(mockConsumer.onDataEvent(event1, null)).andAnswer(
         new EventCountingAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS,
             keyCounts.get(1L)));
     EasyMock.expect(mockConsumer.onEndSource("source1", null)).andAnswer(
         new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG,
             "endDataEventSequence() called")).times(0, 1);
     EasyMock.expect(mockConsumer.onStartSource("source3", null)).andAnswer(
         new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG,
             "startDataEventSequence() called"));
     EasyMock.expect(mockConsumer.onDataEvent(event2, null)).andAnswer(
         new EventCountingAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS,
             keyCounts.get(2L)));
     EasyMock.expect(mockConsumer.onDataEvent(event3, null)).andAnswer(
         new EventCountingAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS,
             keyCounts.get(3L)));
     EasyMock.expect(mockConsumer.onEndSource("source3", null)).andAnswer(
         new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG,
             "endDataEventSequence() called"));
     EasyMock.expect(mockConsumer.onEndDataEventSequence(null)).andAnswer(
         new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG,
             "endDataEventSequence() called"));
     EasyMock.expect(mockConsumer.onStopConsumption()).andAnswer(
         new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG,
             "stopConsumption() called"));
     EasyMock.replay(mockConsumer);
  }

  private void initMockExceptionStreamConsumer3EventFullLifecycle(
      DatabusStreamConsumer mockConsumer,
      DbusEvent event1,
      DbusEvent event2,
      DbusEvent event3,
      Hashtable<Long, AtomicInteger> keyCounts,
      Throwable exception)
  {
    EasyMock.makeThreadSafe(mockConsumer, true);
    EasyMock.expect(mockConsumer.onStartConsumption()).andAnswer(new LoggedAnswer<ConsumerCallbackResult>(
        ConsumerCallbackResult.SUCCESS,
        LOG,
        Level.DEBUG,
        "startConsumption() called"));
    EasyMock.expect(mockConsumer.onStartDataEventSequence(null)).andAnswer(
        new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG, "startDataEventSequence() called"));
    EasyMock.expect(mockConsumer.onStartSource("source1", null)).andAnswer(
        new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG, "startSource() called"));
//    EasyMock.expect(mockConsumer.onDataEvent(event1, null)).andAnswer(
//        new EventCountingAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.ERROR, keyCounts.get(1L)));
    EasyMock.expect(mockConsumer.onDataEvent(event1, null)).andAnswer(new ExceptionAnswer<ConsumerCallbackResult>(exception));
//        new ExceptionAnswer<ConsumerCallbackResult>(new LoggedAnswer<ConsumerCallbackResult>(
//            ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG, "onEvent() called")));
    EasyMock.expect(mockConsumer.onEndSource("source1", null)).andAnswer(new LoggedAnswer<ConsumerCallbackResult>(
        ConsumerCallbackResult.SUCCESS,
        LOG,
        Level.DEBUG,
        "endSource() called")).times(0, 1);
    EasyMock.expect(mockConsumer.onStartSource("source3", null)).andAnswer(
        new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG, "startSource() called"));
    EasyMock.expect(mockConsumer.onDataEvent(event2, null)).andAnswer(
        new EventCountingAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, keyCounts.get(2L)));
    EasyMock.expect(mockConsumer.onDataEvent(event3, null)).andAnswer(
        new EventCountingAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, keyCounts.get(3L)));
    EasyMock.expect(mockConsumer.onEndSource("source3", null)).andAnswer(
        new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG, "endSource() called"));
    EasyMock.expect(mockConsumer.onEndDataEventSequence(null)).andAnswer(
        new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG, "endDataEventSequence() called"));
    EasyMock.expect(mockConsumer.onStopConsumption()).andAnswer(
        new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG, "stopConsumption() called"));
    EasyMock.replay(mockConsumer);
  }

  private void initMockFailingStreamConsumer3EventFullLifecycle(
      DatabusStreamConsumer mockConsumer,
      DbusEvent event1,
      DbusEvent event2,
      DbusEvent event3,
      Hashtable<Long, AtomicInteger> keyCounts)
  {
    EasyMock.makeThreadSafe(mockConsumer, true);
     EasyMock.expect(mockConsumer.onStartConsumption()).andAnswer(
         new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG, "startConsumption() called"));
     EasyMock.expect(mockConsumer.onStartDataEventSequence(null)).andAnswer(
         new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG, "startDataEventSequence() called"));
     EasyMock.expect(mockConsumer.onStartSource("source1", null)).andAnswer(
         new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG, "startDataEventSequence() called"));
     EasyMock.expect(mockConsumer.onDataEvent(event1, null)).andAnswer(
         new EventCountingAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.ERROR, keyCounts.get(1L)));
     EasyMock.expect(mockConsumer.onEndSource("source1", null)).andAnswer(
         new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG, "endDataEventSequence() called")).times(0, 1);
     EasyMock.expect(mockConsumer.onStartSource("source3", null)).andAnswer(
         new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG, "startDataEventSequence() called"));
     EasyMock.expect(mockConsumer.onDataEvent(event2, null)).andAnswer(new EventCountingAnswer<ConsumerCallbackResult>(
         ConsumerCallbackResult.SUCCESS,
         keyCounts.get(2L)));
     EasyMock.expect(mockConsumer.onDataEvent(event3, null)).andAnswer(
         new EventCountingAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, keyCounts.get(3L)));
     EasyMock.expect(mockConsumer.onEndSource("source3", null)).andAnswer(
         new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG, "endDataEventSequence() called"));
     EasyMock.expect(mockConsumer.onEndDataEventSequence(null)).andAnswer(
         new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG, "endDataEventSequence() called"));
     EasyMock.expect(mockConsumer.onStopConsumption()).andAnswer(
         new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG, "stopConsumption() called"));
     EasyMock.replay(mockConsumer);
  }

  private void initMockStreamConsumer3OptEventFullLifecycle(DatabusStreamConsumer mockConsumer,
                                                         DbusEvent event1,
                                                         DbusEvent event2,
                                                         DbusEvent event3,
                                                         Hashtable<Long, AtomicInteger> keyCounts)
  {
    EasyMock.makeThreadSafe(mockConsumer, true);
     EasyMock.expect(mockConsumer.onStartConsumption()).andAnswer(
         new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG,
             "startConsumption() called"));
     EasyMock.expect(mockConsumer.onStartDataEventSequence(null)).andAnswer(
         new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG,
             "startDataEventSequence() called"));
     EasyMock.expect(mockConsumer.onStartSource("source1", null)).andAnswer(
         new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG,
             "startDataEventSequence() called"));
     EasyMock.expect(mockConsumer.onDataEvent(event1, null)).andAnswer(new EventCountingAnswer<ConsumerCallbackResult>(
         ConsumerCallbackResult.SUCCESS,
         keyCounts.get(1L))).anyTimes();
     EasyMock.expect(mockConsumer.onEndSource("source1", null)).andAnswer(new LoggedAnswer<ConsumerCallbackResult>(
         ConsumerCallbackResult.SUCCESS,
         LOG,
         Level.DEBUG,
         "endDataEventSequence() called"));
     EasyMock.expect(mockConsumer.onStartSource("source3", null)).andAnswer(
         new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG,
             "startDataEventSequence() called"));
     EasyMock.expect(mockConsumer.onDataEvent(event2, null)).andAnswer(new EventCountingAnswer<ConsumerCallbackResult>(
         ConsumerCallbackResult.SUCCESS,
         keyCounts.get(2L))).anyTimes();
     /*EasyMock.expect(mockConsumer.dataEvent(event3, null)).andAnswer(
         new EventCountingAnswer<Boolean>(true, keyCounts.get(3L))).anyTimes();*/
     EasyMock.expect(mockConsumer.onEndSource("source3", null)).andAnswer(
         new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG,
             "endDataEventSequence() called"));
     EasyMock.expect(mockConsumer.onEndDataEventSequence(null)).andAnswer(
         new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG,
             "endDataEventSequence() called"));
     EasyMock.expect(mockConsumer.onStopConsumption()).andAnswer(
         new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG,
             "stopConsumption() called"));
     EasyMock.replay(mockConsumer);
  }

  private void initMockFailingStreamConsumer3OptEventFullLifecycle(
      DatabusStreamConsumer mockConsumer,
      DbusEvent event1,
      DbusEvent event2,
      DbusEvent event3,
      Hashtable<Long, AtomicInteger> keyCounts)
  {
    EasyMock.makeThreadSafe(mockConsumer, true);
     EasyMock.expect(mockConsumer.onStartConsumption()).andAnswer(
         new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG, "startConsumption() called"));
     EasyMock.expect(mockConsumer.onStartDataEventSequence(null)).andAnswer(
         new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG, "startDataEventSequence() called"));
     EasyMock.expect(mockConsumer.onStartSource("source1", null)).andAnswer(
         new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG, "startDataEventSequence() called"));
     EasyMock.expect(mockConsumer.onDataEvent(event1, null)).andAnswer(
         new EventCountingAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.ERROR, keyCounts.get(1L))).anyTimes();
     EasyMock.expect(mockConsumer.onEndSource("source1", null)).andAnswer(new LoggedAnswer<ConsumerCallbackResult>(
         ConsumerCallbackResult.SUCCESS,
         LOG,
         Level.DEBUG,
         "endDataEventSequence() called")).times(0, 1);
     EasyMock.expect(mockConsumer.onStartSource("source3", null)).andAnswer(
         new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG, "startDataEventSequence() called"));
     EasyMock.expect(mockConsumer.onDataEvent(event2, null)).andAnswer(
         new EventCountingAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, keyCounts.get(2L))).anyTimes();
     /*EasyMock.expect(mockConsumer.dataEvent(event3, null)).andAnswer(
         new EventCountingAnswer<Boolean>(true, keyCounts.get(3L))).anyTimes();*/
     EasyMock.expect(mockConsumer.onEndSource("source3", null)).andAnswer(
         new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG, "endDataEventSequence() called"));
     EasyMock.expect(mockConsumer.onEndDataEventSequence(null)).andAnswer(
         new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG, "endDataEventSequence() called"));
     EasyMock.expect(mockConsumer.onStopConsumption()).andAnswer(
         new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG, "stopConsumption() called"));
     EasyMock.replay(mockConsumer);
  }

  private void initBufferWithEvents(DbusEventBuffer eventsBuf,
                                    long keyBase,
                                    int numEvents,
                                    short srcId,
                                    Hashtable<Long, AtomicInteger> keyCounts)
  {
    for (long i = 0; i < numEvents; ++i)
    {
      try {
        eventsBuf.appendEvent(new DbusEventKey(keyBase + i), (short) 0, (short)1, (short)0, srcId,
                              new byte[16], "value1".getBytes("UTF-8"), false);
      } catch (UnsupportedEncodingException e) {
        //ignore
      }
      keyCounts.put(keyBase + i, new AtomicInteger(0));
    }
  }

  private void assert3EventFullLifecycle(MultiConsumerCallback callback,
                                         DbusEvent event1, DbusEvent event2, DbusEvent event3)
  {
    assert ConsumerCallbackResult.isSuccess(callback.onStartConsumption()) : "startConsumption() failed";
    assert ConsumerCallbackResult.isSuccess(callback.onStartDataEventSequence(null)) : "startDataEventSequence() failed";
    assert ConsumerCallbackResult.isSuccess(callback.onStartSource("source1", null)) : "startSource(source1) failed";
    assert ConsumerCallbackResult.isSuccess(callback.onDataEvent(event1, null)) : "dataEvent() failed";
    assert ConsumerCallbackResult.isSuccess(callback.onEndSource("source1", null)) : "endsSource(source1) failed";
    assert ConsumerCallbackResult.isSuccess(callback.onStartSource("source3", null)) : "startSource(source3) failed";
    assert ConsumerCallbackResult.isSuccess(callback.onDataEvent(event2, null)) : "dataEvent() failed";
    assert ConsumerCallbackResult.isSuccess(callback.onDataEvent(event3, null)) : "dataEvent() failed";
    assert ConsumerCallbackResult.isSuccess(callback.onEndSource("source3", null)) : "endsSource(source3) failed";
    assert ConsumerCallbackResult.isSuccess(callback.onEndDataEventSequence(null)) : "endDataEventSequence() failed";
    assert ConsumerCallbackResult.isSuccess(callback.onStopConsumption()) : "stopConsumption() failed";
  }

  private void assert3EventFullLifecycleWithFailure(
      MultiConsumerCallback callback,
      DbusEvent event1, DbusEvent event2, DbusEvent event3)
  {
    assert ConsumerCallbackResult.isSuccess(callback.onStartConsumption()) : "startConsumption() failed";
    assert ConsumerCallbackResult.isSuccess(callback.onStartDataEventSequence(null)) : "startDataEventSequence() failed";
    assert ConsumerCallbackResult.isSuccess(callback.onStartSource("source1", null)) : "startSource(source1) failed";
    boolean dataEventSuccess = !ConsumerCallbackResult.isSuccess(callback.onDataEvent(event1, null));
    boolean endSourceSuccess = ConsumerCallbackResult.isSuccess(callback.onEndSource("source1", null));
    //either onDataEvent should fail immediately or the next (barrier) call will
    assert (!dataEventSuccess || !endSourceSuccess) : "dataEvent() failed";
    assert ConsumerCallbackResult.isSuccess(callback.onStartSource("source3", null)) : "startSource(source3) failed";
    assert ConsumerCallbackResult.isSuccess(callback.onDataEvent(event2, null)) : "dataEvent() failed";
    assert ConsumerCallbackResult.isSuccess(callback.onDataEvent(event3, null)) : "dataEvent() failed";
    assert ConsumerCallbackResult.isSuccess(callback.onEndSource("source3", null)) : "endsSource(source3) failed";
    assert ConsumerCallbackResult.isSuccess(callback.onEndDataEventSequence(null)) : "endDataEventSequence() failed";
    assert ConsumerCallbackResult.isSuccess(callback.onStopConsumption()) : "stopConsumption() failed";
  }

  @Test
  public void testPerf() throws Exception
  {
    LOG.info("\n\nstarting testPerf()");

    List<String> sources = new ArrayList<String>();
    Map<Long, IdNamePair> sourcesMap = new HashMap<Long, IdNamePair>();
    for (int i = 1; i <= 3; ++i)
    {
      IdNamePair sourcePair = new IdNamePair((long)i, "source" + i);
      sources.add(sourcePair.getName());
      sourcesMap.put(sourcePair.getId(), sourcePair);
    }

    Hashtable<Long, AtomicInteger> keyCounts = new Hashtable<Long, AtomicInteger>();

    DbusEventBuffer eventsBuf = new DbusEventBuffer(_generic100KBufferStaticConfig);
    eventsBuf.start(0);
    eventsBuf.startEvents();
    initBufferWithEvents(eventsBuf, 1, 1, (short)1, keyCounts);
    initBufferWithEvents(eventsBuf, 2, 2, (short)3, keyCounts);
    eventsBuf.endEvents(100L);

    DbusEventBuffer.DbusEventIterator iter = eventsBuf.acquireIterator("myIter1");
    assert iter.hasNext() : "unable to read event";
    iter.next();
    DbusEvent event1 = iter.next();

    DatabusStreamConsumer logConsumer = new LoggingConsumer();
    SelectingDatabusCombinedConsumer sdccLogConsumer = new SelectingDatabusCombinedConsumer(logConsumer);

    DatabusV2ConsumerRegistration consumerReg =
        new DatabusV2ConsumerRegistration(sdccLogConsumer, sources, null);
    ConsumerCallbackStats consumerStatsCollector = new ConsumerCallbackStats(1, "test","test", true,false, null);
    UnifiedClientStats unifiedStatsCollector = new UnifiedClientStats(1, "test","test.unified");

    List<DatabusV2ConsumerRegistration> allRegistrations =
        Arrays.asList(consumerReg);
    ThreadPoolExecutor executor = (ThreadPoolExecutor)Executors.newCachedThreadPool();
    MultiConsumerCallback callback =
        new MultiConsumerCallback(
            allRegistrations,
            executor,
            60000,
            new StreamConsumerCallbackFactory(consumerStatsCollector, unifiedStatsCollector),
            consumerStatsCollector,
            unifiedStatsCollector,
            null);
    callback.setSourceMap(sourcesMap);

    callback.onStartConsumption();
    callback.onStartDataEventSequence(new SingleSourceSCN(1, 1));
    for (int i = 0; i < 10000; ++i)
    {
      callback.onDataEvent(event1, null);
    }
    callback.onEndDataEventSequence(new SingleSourceSCN(1, 1));
    callback.onStopConsumption();

    System.out.println("max threads=" + executor.getLargestPoolSize() + " task count=" + executor.getTaskCount());
    System.out.println("dataEventsReceived=" + consumerStatsCollector.getNumDataEventsReceived() +
                       " sysEventsReceived=" + consumerStatsCollector.getNumSysEventsReceived()  +
                       " dataEventsProcessed=" + consumerStatsCollector.getNumDataEventsProcessed() +
                       " latencyEventsProcessed=" + consumerStatsCollector.getLatencyEventsProcessed());
    long dataEvents = consumerStatsCollector.getNumDataEventsReceived();
    assert(consumerStatsCollector.getNumDataEventsProcessed()==dataEvents);

  }

  @Test(groups = {"small", "functional"})
  public void test1StreamConsumerHappyPath()
  {
    LOG.info("\n\nstarting test1StreamConsumerHappyPath()");

    Hashtable<Long, AtomicInteger> keyCounts = new Hashtable<Long, AtomicInteger>();

    DbusEventBuffer eventsBuf = new DbusEventBuffer(_generic100KBufferStaticConfig);
    eventsBuf.start(0);
    eventsBuf.startEvents();
    initBufferWithEvents(eventsBuf, 1, 1, (short)1, keyCounts);
    initBufferWithEvents(eventsBuf, 2, 2, (short) 3, keyCounts);
    eventsBuf.endEvents(100L);

    DatabusStreamConsumer mockConsumer = EasyMock.createStrictMock(DatabusStreamConsumer.class);
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
            Executors.newCachedThreadPool(),
            60000,
            new StreamConsumerCallbackFactory(null, null),
            null,
            null,
            null);
    callback.setSourceMap(sourcesMap);

    DbusEventBuffer.DbusEventIterator iter = eventsBuf.acquireIterator("myIter1");
    assert iter.hasNext() : "unable to read event";
    DbusEvent event1 = iter.next();
    assert iter.hasNext() : "unable to read event";
    DbusEvent event2 = iter.next();
    assert iter.hasNext() : "unable to read event";
    DbusEvent event3 = iter.next();

    initMockStreamConsumer3EventFullLifecycle(mockConsumer, event1, event2, event3, keyCounts);

    assert3EventFullLifecycle(callback, event1, event2, event3);

    EasyMock.verify(mockConsumer);
    assert keyCounts.get(1L).get() == 1 : "invalid number of event(1) calls: " + keyCounts.get(1L).get();
    assert keyCounts.get(2L).get() == 1 : "invalid number of event(2) calls:" + keyCounts.get(2L).get();
    assert keyCounts.get(3L).get() == 1 : "invalid number of event(3) calls:" + keyCounts.get(3L).get();
  }

  @Test(groups = {"small", "functional"})
  public void test1StreamConsumerCallFailure()
  {
    LOG.info("\n\nstarting test1StreamConsumerCallFailure()");

    Hashtable<Long, AtomicInteger> keyCounts = new Hashtable<Long, AtomicInteger>();

    DbusEventBuffer eventsBuf = new DbusEventBuffer(_generic100KBufferStaticConfig);
    eventsBuf.start(0);
    eventsBuf.startEvents();
    initBufferWithEvents(eventsBuf, 1, 1, (short)1, keyCounts);
    initBufferWithEvents(eventsBuf, 2, 2, (short)3, keyCounts);
    eventsBuf.endEvents(100L);

    DatabusStreamConsumer mockConsumer = EasyMock.createStrictMock(DatabusStreamConsumer.class);
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
            Executors.newCachedThreadPool(),
            1000,
            new StreamConsumerCallbackFactory(null, null),
            null,
            null,
            null);
    callback.setSourceMap(sourcesMap);

    DbusEventBuffer.DbusEventIterator iter = eventsBuf.acquireIterator("myIter1");
    assert iter.hasNext() : "unable to read event";
    DbusEvent event1 = iter.next();
    assert iter.hasNext() : "unable to read event";
    DbusEvent event2 = iter.next();
    assert iter.hasNext() : "unable to read event";
    DbusEvent event3 = iter.next();

    initMockFailingStreamConsumer3EventFullLifecycle(mockConsumer, event1, event2, event3, keyCounts);

    assert3EventFullLifecycleWithFailure(callback, event1, event2, event3);

    EasyMock.verify(mockConsumer);
    assert keyCounts.get(1L).get() == 1 : "invalid number of event(1) calls: " + keyCounts.get(1L).get();
    assert keyCounts.get(2L).get() == 1 : "invalid number of event(2) calls:" + keyCounts.get(2L).get();
    assert keyCounts.get(3L).get() == 1 : "invalid number of event(3) calls:" + keyCounts.get(3L).get();
  }

  // Test the cases where the application throws some random exception (or times out), and make
  // sure we field it correctly and return ERROR.
  @Test(groups = {"small", "functional"})
  public void testConsumersWithException()
  {
    LOG.info("\n\nstarting testConsumersWithException()");

    testConsumersWithException(new SomeApplicationException());
    testConsumersWithException(new TimeoutException("Application timed out processing some call but refused to field it"));
  }

  private void testConsumersWithException(Throwable exception)
  {
    Hashtable<Long, AtomicInteger> keyCounts = new Hashtable<Long, AtomicInteger>();

    DbusEventBuffer eventsBuf = new DbusEventBuffer(_generic100KBufferStaticConfig);
    eventsBuf.start(0);
    eventsBuf.startEvents();
    initBufferWithEvents(eventsBuf, 1, 1, (short)1, keyCounts);
    initBufferWithEvents(eventsBuf, 2, 2, (short)3, keyCounts);
    eventsBuf.endEvents(100L);

    DatabusStreamConsumer mockConsumer = EasyMock.createStrictMock(DatabusStreamConsumer.class);
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
            Executors.newCachedThreadPool(),
            1000,
            new StreamConsumerCallbackFactory(null, null),
            null,
            null,
            null);
    callback.setSourceMap(sourcesMap);

    DbusEventBuffer.DbusEventIterator iter = eventsBuf.acquireIterator("myIter1");
    assert iter.hasNext() : "unable to read event";
    DbusEvent event1 = iter.next();
    assert iter.hasNext() : "unable to read event";
    DbusEvent event2 = iter.next();
    assert iter.hasNext() : "unable to read event";
    DbusEvent event3 = iter.next();

    initMockExceptionStreamConsumer3EventFullLifecycle(mockConsumer, event1, event2, event3, keyCounts, exception);

    assert3EventFullLifecycleWithFailure(callback, event1, event2, event3);

    EasyMock.verify(mockConsumer);
//    assert keyCounts.get(1L).get() == 1 : "invalid number of event(1) calls: " + keyCounts.get(1L).get();
    assert keyCounts.get(2L).get() == 1 : "invalid number of event(2) calls:" + keyCounts.get(2L).get();
    assert keyCounts.get(3L).get() == 1 : "invalid number of event(3) calls:" + keyCounts.get(3L).get();
  }

  @Test(groups = {"small", "functional"})
  public void test3IndependentStreamConsumersHappyPath()
  {
    LOG.info("\n\nstarting test3IndependentStreamConsumersHappyPath()");

    Hashtable<Long, AtomicInteger> keyCounts = new Hashtable<Long, AtomicInteger>();

    DbusEventBuffer eventsBuf = new DbusEventBuffer(_generic100KBufferStaticConfig);
    eventsBuf.start(0);
    eventsBuf.startEvents();
    initBufferWithEvents(eventsBuf, 1, 1, (short)1, keyCounts);
    initBufferWithEvents(eventsBuf, 2, 2, (short)3, keyCounts);
    eventsBuf.endEvents(100L);

    DatabusStreamConsumer mockConsumer1 = EasyMock.createStrictMock("consumer1",
                                                                    DatabusStreamConsumer.class);
    SelectingDatabusCombinedConsumer sdccMockConsumer1 = new SelectingDatabusCombinedConsumer(mockConsumer1);
    EasyMock.makeThreadSafe(mockConsumer1, true);

    DatabusStreamConsumer mockConsumer2 = EasyMock.createStrictMock("consumer2",
                                                                    DatabusStreamConsumer.class);
    SelectingDatabusCombinedConsumer sdccMockConsumer2 = new SelectingDatabusCombinedConsumer(mockConsumer2);
    EasyMock.makeThreadSafe(mockConsumer2, true);

    DatabusStreamConsumer mockConsumer3 = EasyMock.createStrictMock("consumer3",
                                                                    DatabusStreamConsumer.class);
    SelectingDatabusCombinedConsumer sdccMockConsumer3 = new SelectingDatabusCombinedConsumer(mockConsumer3);
    EasyMock.makeThreadSafe(mockConsumer3, true);

    List<String> sources = new ArrayList<String>();
    Map<Long, IdNamePair> sourcesMap = new HashMap<Long, IdNamePair>();
    for (int i = 1; i <= 3; ++i)
    {
      IdNamePair sourcePair = new IdNamePair((long)i, "source" + i);
      sources.add(sourcePair.getName());
      sourcesMap.put(sourcePair.getId(), sourcePair);
    }

    DatabusV2ConsumerRegistration consumerReg1 =
        new DatabusV2ConsumerRegistration(sdccMockConsumer1, sources, null);
    DatabusV2ConsumerRegistration consumerReg2 =
      new DatabusV2ConsumerRegistration(sdccMockConsumer2, sources, null);
    DatabusV2ConsumerRegistration consumerReg3 =
      new DatabusV2ConsumerRegistration(sdccMockConsumer3, sources, null);

    List<DatabusV2ConsumerRegistration> allRegistrations =
        Arrays.asList(consumerReg1, consumerReg2, consumerReg3);
    MultiConsumerCallback callback =
        new MultiConsumerCallback(
            allRegistrations,
            Executors.newCachedThreadPool(),
            1000,
            new StreamConsumerCallbackFactory(null, null),
            null,
            null,
            null);
    callback.setSourceMap(sourcesMap);

    DbusEventBuffer.DbusEventIterator iter = eventsBuf.acquireIterator("myIter1");
    assert iter.hasNext() : "unable to read event";
    DbusEvent event1 = iter.next();
    assert iter.hasNext() : "unable to read event";
    DbusEvent event2 = iter.next();
    assert iter.hasNext() : "unable to read event";
    DbusEvent event3 = iter.next();

    initMockStreamConsumer3EventFullLifecycle(mockConsumer1, event1, event2, event3, keyCounts);
    initMockStreamConsumer3EventFullLifecycle(mockConsumer2, event1, event2, event3, keyCounts);
    initMockStreamConsumer3EventFullLifecycle(mockConsumer3, event1, event2, event3, keyCounts);

    assert3EventFullLifecycle(callback, event1, event2, event3);

    EasyMock.verify(mockConsumer1);
    EasyMock.verify(mockConsumer2);
    EasyMock.verify(mockConsumer3);

    assert keyCounts.get(1L).get() == 3 : "invalid number of event(1) calls: " + keyCounts.get(1L).get();
    assert keyCounts.get(2L).get() == 3 : "invalid number of event(2) calls:" + keyCounts.get(2L).get();
    assert keyCounts.get(3L).get() == 3 : "invalid number of event(3) calls:" + keyCounts.get(3L).get();
  }

  @Test(groups = {"small", "functional"})
  public void test3IndependentStreamConsumersWithFailure()
  {
    LOG.info("\n\nstarting test3IndependentStreamConsumersWithFailure()");

    Hashtable<Long, AtomicInteger> keyCounts = new Hashtable<Long, AtomicInteger>();

    DbusEventBuffer eventsBuf = new DbusEventBuffer(_generic100KBufferStaticConfig);
    eventsBuf.start(0);
    eventsBuf.startEvents();
    initBufferWithEvents(eventsBuf, 1, 1, (short)1, keyCounts);
    initBufferWithEvents(eventsBuf, 2, 2, (short)3, keyCounts);
    eventsBuf.endEvents(100L);

    DatabusStreamConsumer mockConsumer1 = EasyMock.createStrictMock("consumer1",
                                                                    DatabusStreamConsumer.class);
    EasyMock.makeThreadSafe(mockConsumer1, true);
    DatabusStreamConsumer mockConsumer2 = EasyMock.createStrictMock("consumer2",
                                                                    DatabusStreamConsumer.class);
    EasyMock.makeThreadSafe(mockConsumer2, true);
    DatabusStreamConsumer mockConsumer3 = EasyMock.createStrictMock("consumer3",
                                                                    DatabusStreamConsumer.class);
    EasyMock.makeThreadSafe(mockConsumer3, true);

    SelectingDatabusCombinedConsumer sdccMockConsumer1 = new SelectingDatabusCombinedConsumer(mockConsumer1);
    SelectingDatabusCombinedConsumer sdccMockConsumer2 = new SelectingDatabusCombinedConsumer(mockConsumer2);
    SelectingDatabusCombinedConsumer sdccMockConsumer3 = new SelectingDatabusCombinedConsumer(mockConsumer3);

    List<String> sources = new ArrayList<String>();
    Map<Long, IdNamePair> sourcesMap = new HashMap<Long, IdNamePair>();
    for (int i = 1; i <= 3; ++i)
    {
      IdNamePair sourcePair = new IdNamePair((long)i, "source" + i);
      sources.add(sourcePair.getName());
      sourcesMap.put(sourcePair.getId(), sourcePair);
    }

    DatabusV2ConsumerRegistration consumerReg1 =
        new DatabusV2ConsumerRegistration(sdccMockConsumer1, sources, null);
    DatabusV2ConsumerRegistration consumerReg2 =
      new DatabusV2ConsumerRegistration(sdccMockConsumer2, sources, null);
    DatabusV2ConsumerRegistration consumerReg3 =
      new DatabusV2ConsumerRegistration(sdccMockConsumer3, sources, null);

    List<DatabusV2ConsumerRegistration> allRegistrations =
        Arrays.asList(consumerReg1, consumerReg2, consumerReg3);
    MultiConsumerCallback callback =
        new MultiConsumerCallback(
            allRegistrations,
            Executors.newCachedThreadPool(),
            1000,
            new StreamConsumerCallbackFactory(null, null),
            null,
            null,
            null);
    callback.setSourceMap(sourcesMap);

    DbusEventBuffer.DbusEventIterator iter = eventsBuf.acquireIterator("myIter1");
    assert iter.hasNext() : "unable to read event";
    DbusEvent event1 = iter.next();
    assert iter.hasNext() : "unable to read event";
    DbusEvent event2 = iter.next();
    assert iter.hasNext() : "unable to read event";
    DbusEvent event3 = iter.next();

    initMockStreamConsumer3EventFullLifecycle(mockConsumer1, event1, event2, event3, keyCounts);
    initMockFailingStreamConsumer3EventFullLifecycle(mockConsumer2, event1, event2, event3, keyCounts);
    initMockStreamConsumer3EventFullLifecycle(mockConsumer3, event1, event2, event3, keyCounts);

    assert3EventFullLifecycleWithFailure(callback, event1, event2, event3);

    EasyMock.verify(mockConsumer1);
    EasyMock.verify(mockConsumer2);
    EasyMock.verify(mockConsumer3);

    assert keyCounts.get(1L).get() == 3 : "invalid number of event(1) calls: " + keyCounts.get(1L).get();
    assert keyCounts.get(2L).get() == 3 : "invalid number of event(2) calls:" + keyCounts.get(2L).get();
    assert keyCounts.get(3L).get() == 3 : "invalid number of event(3) calls:" + keyCounts.get(3L).get();
  }

  @Test(groups = {"small", "functional"})
  public void test3GroupedStreamConsumersHappyPath()
  {
    LOG.info("\n\nstarting test3GroupedStreamConsumersHappyPath()");

    Hashtable<Long, AtomicInteger> keyCounts = new Hashtable<Long, AtomicInteger>();

    DbusEventBuffer eventsBuf = new DbusEventBuffer(_generic100KBufferStaticConfig);
    eventsBuf.start(0);
    eventsBuf.startEvents();
    initBufferWithEvents(eventsBuf, 1, 1, (short)1, keyCounts);
    initBufferWithEvents(eventsBuf, 2, 2, (short)3, keyCounts);
    eventsBuf.endEvents(100L);

    DatabusStreamConsumer mockConsumer1 = EasyMock.createStrictMock("consumer1",
                                                                    DatabusStreamConsumer.class);
    EasyMock.makeThreadSafe(mockConsumer1, true);
    DatabusStreamConsumer mockConsumer2 = EasyMock.createStrictMock("consumer2",
                                                                    DatabusStreamConsumer.class);
    EasyMock.makeThreadSafe(mockConsumer2, true);
    DatabusStreamConsumer mockConsumer3 = EasyMock.createStrictMock("consumer3",
                                                                    DatabusStreamConsumer.class);
    EasyMock.makeThreadSafe(mockConsumer3, true);

    DatabusCombinedConsumer sdccMockConsumer1 = new SelectingDatabusCombinedConsumer(mockConsumer1);
    DatabusCombinedConsumer sdccMockConsumer2 = new SelectingDatabusCombinedConsumer(mockConsumer2);
    DatabusCombinedConsumer sdccMockConsumer3 = new SelectingDatabusCombinedConsumer(mockConsumer3);

    List<String> sources = new ArrayList<String>();
    Map<Long, IdNamePair> sourcesMap = new HashMap<Long, IdNamePair>();
    for (int i = 1; i <= 3; ++i)
    {
      IdNamePair sourcePair = new IdNamePair((long)i, "source" + i);
      sources.add(sourcePair.getName());
      sourcesMap.put(sourcePair.getId(), sourcePair);
    }

    DatabusV2ConsumerRegistration consumerReg1 =
        new DatabusV2ConsumerRegistration(
            Arrays.asList(sdccMockConsumer1, sdccMockConsumer2, sdccMockConsumer3), sources, null);

    List<DatabusV2ConsumerRegistration> allRegistrations =
        Arrays.asList(consumerReg1);
    MultiConsumerCallback callback =
        new MultiConsumerCallback(
            allRegistrations,
            Executors.newCachedThreadPool(),
            1000,
            new StreamConsumerCallbackFactory(null, null),
            null,
            null,
            null);
    callback.setSourceMap(sourcesMap);

    DbusEventBuffer.DbusEventIterator iter = eventsBuf.acquireIterator("myIter1");
    assert iter.hasNext() : "unable to read event";
    DbusEvent event1 = iter.next();
    assert iter.hasNext() : "unable to read event";
    DbusEvent event2 = iter.next();
    assert iter.hasNext() : "unable to read event";
    DbusEvent event3 = iter.next();

    initMockStreamConsumer3OptEventFullLifecycle(mockConsumer1, event1, event2, event3, keyCounts);
    initMockStreamConsumer3OptEventFullLifecycle(mockConsumer2, event1, event2, event3, keyCounts);
    initMockStreamConsumer3OptEventFullLifecycle(mockConsumer3, event1, event2, event3, keyCounts);

    assert3EventFullLifecycle(callback, event1, event2, event3);

    EasyMock.verify(mockConsumer1);
    EasyMock.verify(mockConsumer2);
    EasyMock.verify(mockConsumer3);

    assert (keyCounts.get(1L).get() + keyCounts.get(2L).get() + keyCounts.get(3L).get()) == 3
           : "invalid number of calls: " + keyCounts.get(1L).get() + "," + keyCounts.get(2L).get()
           + "," + keyCounts.get(3L).get();
  }

  @Test(groups = {"small", "functional"})
  public void test3GroupedStreamConsumersWithFailure()
  {
    LOG.info("\n\nstarting test3GroupedStreamConsumersWithFailure()");

    Hashtable<Long, AtomicInteger> keyCounts = new Hashtable<Long, AtomicInteger>();

    DbusEventBuffer eventsBuf = new DbusEventBuffer(_generic100KBufferStaticConfig);
    eventsBuf.start(0);
    eventsBuf.startEvents();
    initBufferWithEvents(eventsBuf, 1, 1, (short)1, keyCounts);
    initBufferWithEvents(eventsBuf, 2, 2, (short)3, keyCounts);
    eventsBuf.endEvents(100L);

    DatabusStreamConsumer mockConsumer1 = EasyMock.createStrictMock("consumer1",
                                                                    DatabusStreamConsumer.class);
    EasyMock.makeThreadSafe(mockConsumer1, true);
    DatabusStreamConsumer mockConsumer2 = EasyMock.createStrictMock("consumer2",
                                                                    DatabusStreamConsumer.class);
    EasyMock.makeThreadSafe(mockConsumer2, true);
    DatabusStreamConsumer mockConsumer3 = EasyMock.createStrictMock("consumer3",
                                                                    DatabusStreamConsumer.class);
    EasyMock.makeThreadSafe(mockConsumer3, true);

    DatabusCombinedConsumer sdccMockConsumer1 = new SelectingDatabusCombinedConsumer(mockConsumer1);
    DatabusCombinedConsumer sdccMockConsumer2 = new SelectingDatabusCombinedConsumer(mockConsumer2);
    DatabusCombinedConsumer sdccMockConsumer3 = new SelectingDatabusCombinedConsumer(mockConsumer3);

    List<String> sources = new ArrayList<String>();
    Map<Long, IdNamePair> sourcesMap = new HashMap<Long, IdNamePair>();
    for (int i = 1; i <= 3; ++i)
    {
      IdNamePair sourcePair = new IdNamePair((long)i, "source" + i);
      sources.add(sourcePair.getName());
      sourcesMap.put(sourcePair.getId(), sourcePair);
    }

    DatabusV2ConsumerRegistration consumerReg1 =
        new DatabusV2ConsumerRegistration(
            Arrays.asList(sdccMockConsumer1, sdccMockConsumer2, sdccMockConsumer3), sources, null);

    List<DatabusV2ConsumerRegistration> allRegistrations =
        Arrays.asList(consumerReg1);
    MultiConsumerCallback callback =
        new MultiConsumerCallback(
            allRegistrations,
            Executors.newCachedThreadPool(),
            1000,
            new StreamConsumerCallbackFactory(null, null),
            null,
            null,
            null);
    callback.setSourceMap(sourcesMap);

    DbusEventBuffer.DbusEventIterator iter = eventsBuf.acquireIterator("myIter1");
    assert iter.hasNext() : "unable to read event";
    DbusEvent event1 = iter.next();
    assert iter.hasNext() : "unable to read event";
    DbusEvent event2 = iter.next();
    assert iter.hasNext() : "unable to read event";
    DbusEvent event3 = iter.next();

    initMockFailingStreamConsumer3OptEventFullLifecycle(mockConsumer1, event1, event2, event3,
                                                        keyCounts);
    initMockFailingStreamConsumer3OptEventFullLifecycle(mockConsumer2, event1, event2, event3,
                                                        keyCounts);
    initMockFailingStreamConsumer3OptEventFullLifecycle(mockConsumer3, event1, event2, event3,
                                                        keyCounts);

    assert3EventFullLifecycleWithFailure(callback, event1, event2, event3);

    EasyMock.verify(mockConsumer1);
    EasyMock.verify(mockConsumer2);
    EasyMock.verify(mockConsumer3);

    assert (keyCounts.get(1L).get() + keyCounts.get(2L).get() + keyCounts.get(3L).get()) == 3
           : "invalid number of calls: " + keyCounts.get(1L).get() + "," + keyCounts.get(2L).get()
           + "," + keyCounts.get(3L).get();
  }

  @Test
  public void test1ConsumerTimeout()
  {
    LOG.info("\n\nstarting test1ConsumerTimeout()");

    //create dummy events
    Hashtable<Long, AtomicInteger> keyCounts = new Hashtable<Long, AtomicInteger>();

    DbusEventBuffer eventsBuf = new DbusEventBuffer(_generic100KBufferStaticConfig);
    eventsBuf.start(0);
    eventsBuf.startEvents();
    initBufferWithEvents(eventsBuf, 1, 2, (short)1, keyCounts);
    initBufferWithEvents(eventsBuf, 3, 1, (short)2, keyCounts);
    eventsBuf.endEvents(100L);

    DbusEventBuffer.DbusEventIterator iter = eventsBuf.acquireIterator("myIter1");
    Assert.assertTrue(iter.hasNext(), "unable to read event");
    iter.next(); //skip over the first system event
    Assert.assertTrue(iter.hasNext(), "unable to read event");
    DbusEvent event1 = iter.next().createCopy();
    Assert.assertTrue(iter.hasNext(), "unable to read event");
    DbusEvent event2 = iter.next().createCopy();
    Assert.assertTrue(iter.hasNext(), "unable to read event");
    DbusEvent event3 = iter.next().createCopy();

    //make up some sources
    List<String> sources = new ArrayList<String>();
    Map<Long, IdNamePair> sourcesMap = new HashMap<Long, IdNamePair>();
    for (int i = 1; i <= 3; ++i)
    {
      IdNamePair sourcePair = new IdNamePair((long)i, "source" + i);
      sources.add(sourcePair.getName());
      sourcesMap.put(sourcePair.getId(), sourcePair);
    }

    //create the consumer mock up
    DatabusStreamConsumer mockConsumer1 = EasyMock.createStrictMock("consumer1",
                                                                    DatabusStreamConsumer.class);
    SelectingDatabusCombinedConsumer sdccMockConsumer1 = new SelectingDatabusCombinedConsumer(mockConsumer1);
    EasyMock.makeThreadSafe(mockConsumer1, true);

    DatabusV2ConsumerRegistration consumerReg =
        new DatabusV2ConsumerRegistration(sdccMockConsumer1, sources, null);

    EasyMock.expect(mockConsumer1.onStartConsumption()).andAnswer(
        new SleepingAnswer<ConsumerCallbackResult>(new LoggedAnswer<ConsumerCallbackResult>(
            ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG, "startConsumption() called"),
            150));
    EasyMock.expect(mockConsumer1.onStartDataEventSequence(null)).andAnswer(
        new SleepingAnswer<ConsumerCallbackResult>(new LoggedAnswer<ConsumerCallbackResult>(
            ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG, "onStartDataEventSequence() called"),
            110));
    EasyMock.expect(mockConsumer1.onStartSource("source1", null)).andAnswer(
        new SleepingAnswer<ConsumerCallbackResult>(
            new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG,
                                                     Level.DEBUG,
                                                     "onStartSource() called"),
            40));
    EasyMock.expect(mockConsumer1.onDataEvent(event1, null)).andAnswer(
        new SleepingAnswer<ConsumerCallbackResult>(
            new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG,
                                                    "onDataEvet(1) called"),
           50));
    EasyMock.expect(mockConsumer1.onDataEvent(event2, null)).andAnswer(
        new SleepingAnswer<ConsumerCallbackResult>(
            new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG,
                                                    "onDataEvet(2) called"),
           210));
    EasyMock.expect(mockConsumer1.onDataEvent(event1, null)).andAnswer(
        new SleepingAnswer<ConsumerCallbackResult>(
            new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG,
                                                     "onDataEvet(1) called"),
            40));
    EasyMock.expect(mockConsumer1.onEndSource("source1", null)).andAnswer(
        new SleepingAnswer<ConsumerCallbackResult>(
            new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG,
                                                     "onStartSource() called"),
            50));
    EasyMock.replay(mockConsumer1);
    ConsumerCallbackStats consumerStatsCollector = new ConsumerCallbackStats(1, "test","test", true,false, null);
    UnifiedClientStats unifiedStatsCollector = new UnifiedClientStats(1, "test","test.unified");

    //Create and fire up callbacks
    List<DatabusV2ConsumerRegistration> allRegistrations =
        Arrays.asList(consumerReg);
    MultiConsumerCallback callback =
        new MultiConsumerCallback(
            allRegistrations,
            Executors.newCachedThreadPool(),
            100,
            new StreamConsumerCallbackFactory(consumerStatsCollector, unifiedStatsCollector),
            consumerStatsCollector,
            unifiedStatsCollector,
>>>>>>> github-merge
            null);
    callback.setSourceMap(sourcesMap);

    ConsumerCallbackResult startConsumptionRes = callback.onStartConsumption();
    Assert.assertTrue(ConsumerCallbackResult.isFailure(startConsumptionRes),
                      "startConsumption() failed");
    ConsumerCallbackResult startWindowRes = callback.onStartDataEventSequence(null);
    Assert.assertTrue(ConsumerCallbackResult.isFailure(startWindowRes),
                      "startDataEventSequence() failed");
    ConsumerCallbackResult startSourceRes = callback.onStartSource("source1", null);
    Assert.assertTrue(ConsumerCallbackResult.isSuccess(startSourceRes),
                      "startSources(source1) succeeded");
    ConsumerCallbackResult event1Res = callback.onDataEvent(event1, null);
    Assert.assertTrue(ConsumerCallbackResult.isSuccess(event1Res),
                      "onDataEvent(1) succeeded");
    ConsumerCallbackResult event2Res = callback.onDataEvent(event2, null);
    Assert.assertTrue(ConsumerCallbackResult.isSuccess(event2Res),
                       "onDataEvent(2) queued up");
    ConsumerCallbackResult event3Res = callback.onDataEvent(event1, null);
    Assert.assertTrue(ConsumerCallbackResult.isSuccess(event3Res),
                      "onDataEvent(1) queued up");
    ConsumerCallbackResult endSourceRes = callback.onEndSource("source1", null);
    Assert.assertTrue(ConsumerCallbackResult.isFailure(endSourceRes),
                       "onEndSource fails because of timeout in onDataEvent(2)");

    EasyMock.reset(mockConsumer1);
    EasyMock.makeThreadSafe(mockConsumer1, true);
    EasyMock.expect(mockConsumer1.onStartSource("source2", null)).andAnswer(
        new SleepingAnswer<ConsumerCallbackResult>(
            new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG,
                                                     "onStartSource() called"),
            150)).times(0, 1);
    EasyMock.expect(mockConsumer1.onDataEvent(event3, null)).andAnswer(
        new SleepingAnswer<ConsumerCallbackResult>(
            new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG,
                                                     "onDataEvet(3) called"),
        40));
    EasyMock.expect(mockConsumer1.onEndSource("source2", null)).andAnswer(
        new SleepingAnswer<ConsumerCallbackResult>(
            new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG,
                                                     "onStartSource() called"),
        60));
    EasyMock.replay(mockConsumer1);


    startSourceRes = callback.onStartSource("source2", null);
    Assert.assertTrue(ConsumerCallbackResult.isFailure(startSourceRes),
        "startSources(source2) fails");

    event1Res = callback.onDataEvent(event3, null);
    Assert.assertTrue(ConsumerCallbackResult.isSuccess(event1Res),
                      "onDataEvent(3) succeeded");
    endSourceRes = callback.onEndSource("source2", null);
    Assert.assertTrue(ConsumerCallbackResult.isSuccess(endSourceRes),
                       "onEndSource succeeds");

    long eventsErrProcessed = consumerStatsCollector.getNumErrorsProcessed();
    long totalEvents  = consumerStatsCollector.getNumEventsReceived();
    long totalEventsProcessed = consumerStatsCollector.getNumEventsProcessed();

    System.out.println("eventsReceived = " + consumerStatsCollector.getNumEventsReceived() + " eventsProcessed=" + consumerStatsCollector.getNumEventsProcessed());
    System.out.println("eventsErrProcessed =" + consumerStatsCollector.getNumErrorsProcessed() + " eventsErrReceived=" + consumerStatsCollector.getNumErrorsReceived()
                       + " totalEvents=" + consumerStatsCollector.getNumEventsReceived() + " totalEventsProcessed=" + totalEventsProcessed);

    //FIXME
    Assert.assertTrue(totalEvents >= totalEventsProcessed+eventsErrProcessed);
    Assert.assertTrue(eventsErrProcessed > 0);
    Assert.assertTrue(totalEventsProcessed < totalEvents);

    //NOTE: We don't verify because all the canceled callbacks are not detected by EasyMock
    //EasyMock.verify(mockConsumer1);
  }


  @Test
  public void test2ConsumerTimeout()
  {
    Logger log = Logger.getLogger("TestMultiConsumerCallback.test2ConsumerTimeout");

    //Logger.getRootLogger().setLevel(Level.INFO);
    log.info("\n\nstarting test2ConsumerTimeout()");

    log.info("create dummy events");
    Hashtable<Long, AtomicInteger> keyCounts = new Hashtable<Long, AtomicInteger>();

    DbusEventBuffer eventsBuf = new DbusEventBuffer(_generic100KBufferStaticConfig);
    eventsBuf.start(0);
    eventsBuf.startEvents();
    initBufferWithEvents(eventsBuf, 1, 2, (short)1, keyCounts);
    initBufferWithEvents(eventsBuf, 3, 1, (short)2, keyCounts);
    eventsBuf.endEvents(100L);

    DbusEventBuffer.DbusEventIterator iter = eventsBuf.acquireIterator("myIter1");
    Assert.assertTrue(iter.hasNext(), "unable to read event");
    iter.next(); //skip over the first system event
    Assert.assertTrue(iter.hasNext(), "unable to read event");
    DbusEvent event1 = iter.next().createCopy();
    Assert.assertTrue(iter.hasNext(), "unable to read event");
    DbusEvent event2 = iter.next().createCopy();
    Assert.assertTrue(iter.hasNext(), "unable to read event");


    log.info("make up some sources");
    List<String> sources = new ArrayList<String>();
    Map<Long, IdNamePair> sourcesMap = new HashMap<Long, IdNamePair>();
    for (int i = 1; i <= 3; ++i)
    {
      IdNamePair sourcePair = new IdNamePair((long)i, "source" + i);
      sources.add(sourcePair.getName());
      sourcesMap.put(sourcePair.getId(), sourcePair);
    }

    log.info("create the consumer mock up");
    DatabusStreamConsumer mockConsumer1 = EasyMock.createStrictMock("consumer1",
                                                                    DatabusStreamConsumer.class);
    SelectingDatabusCombinedConsumer sdccMockConsumer1 = new SelectingDatabusCombinedConsumer(mockConsumer1);
    EasyMock.makeThreadSafe(mockConsumer1, true);

    DatabusV2ConsumerRegistration consumerReg =
        new DatabusV2ConsumerRegistration(sdccMockConsumer1, sources, null);

    EasyMock.expect(mockConsumer1.onStartConsumption()).andAnswer(
        new SleepingAnswer<ConsumerCallbackResult>(new LoggedAnswer<ConsumerCallbackResult>(
            ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG, "startConsumption() called"),
            1));
    EasyMock.expect(mockConsumer1.onStartDataEventSequence(null)).andAnswer(
        new SleepingAnswer<ConsumerCallbackResult>(new LoggedAnswer<ConsumerCallbackResult>(
            ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG, "onStartDataEventSequence() called"),
            1));
    EasyMock.expect(mockConsumer1.onStartSource("source1", null)).andAnswer(
        new SleepingAnswer<ConsumerCallbackResult>(
            new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG,
                                                     Level.DEBUG,
                                                     "onStartSource() called"),
            1));
    EasyMock.expect(mockConsumer1.onDataEvent(event1, null)).andAnswer(
        new SleepingAnswer<ConsumerCallbackResult>(
            new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG,
                                                    "onDataEvet(1) called"),
           1));
    EasyMock.expect(mockConsumer1.onDataEvent(event2, null)).andAnswer(
        new SleepingAnswer<ConsumerCallbackResult>(
            new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG,
                                                    "onDataEvet(2) called"),
           1));
    EasyMock.expect(mockConsumer1.onDataEvent(event1, null)).andAnswer(
        new SleepingAnswer<ConsumerCallbackResult>(
            new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG,
                                                     "onDataEvet(1) called"),
            1));
    EasyMock.expect(mockConsumer1.onEndSource("source1", null)).andAnswer(
        new SleepingAnswer<ConsumerCallbackResult>(
            new LoggedAnswer<ConsumerCallbackResult>(ConsumerCallbackResult.SUCCESS, LOG, Level.DEBUG,
                                                     "onStartSource() called"),
            1));
    EasyMock.replay(mockConsumer1);
    ConsumerCallbackStats consumerStatsCollector = new ConsumerCallbackStats(1, "test","test", true,false, null);
    UnifiedClientStats unifiedStatsCollector = new UnifiedClientStats(1, "test","test.unified");

    log.info("Create and fire up callbacks");
    List<DatabusV2ConsumerRegistration> allRegistrations =
        Arrays.asList(consumerReg);
    TimingOutMultiConsumerCallback callback =
        new TimingOutMultiConsumerCallback(
            allRegistrations,
            Executors.newCachedThreadPool(),
            300,
            new StreamConsumerCallbackFactory(consumerStatsCollector, unifiedStatsCollector),
            consumerStatsCollector,
            unifiedStatsCollector,
            3);
    callback.setSourceMap(sourcesMap);

    ConsumerCallbackResult startConsumptionRes = callback.onStartConsumption();
    Assert.assertTrue(ConsumerCallbackResult.isSuccess(startConsumptionRes),
                      "startConsumption() succeeded: " + startConsumptionRes);
    ConsumerCallbackResult startWindowRes = callback.onStartDataEventSequence(null);
    Assert.assertTrue(ConsumerCallbackResult.isSuccess(startWindowRes),
                      "startDataEventSequence() succeeded");
    ConsumerCallbackResult startSourceRes = callback.onStartSource("source1", null);
    Assert.assertTrue(ConsumerCallbackResult.isSuccess(startSourceRes),
                      "startSources(source1) succeeded");
    ConsumerCallbackResult event1Res = callback.onDataEvent(event1, null);
    Assert.assertTrue(ConsumerCallbackResult.isSuccess(event1Res),
                      "onDataEvent(1) succeeded");
    ConsumerCallbackResult event2Res = callback.onDataEvent(event2, null);
    Assert.assertTrue(ConsumerCallbackResult.isSuccess(event2Res),
                       "onDataEvent(2) queued up");
    ConsumerCallbackResult event3Res = callback.onDataEvent(event1, null);
    Assert.assertTrue(ConsumerCallbackResult.isSuccess(event3Res),
                      "onDataEvent(1) queued up");
    ConsumerCallbackResult endSourceRes = callback.onEndSource("source1", null);
    Assert.assertTrue(ConsumerCallbackResult.isFailure(endSourceRes),
                       "onEndSource fails because of timeout in onDataEvent(2)");

    EasyMock.reset(mockConsumer1);
    log.info("test2ConsumerTimeout: end");
  }
  @BeforeMethod
  public void beforeMethod()
  {
  }

  @AfterMethod
  public void afterMethod()
  {
  }

  @BeforeClass
  public void beforeClass() throws Exception
  {
    _generic100KBufferConfig = new DbusEventBuffer.Config();
    _generic100KBufferConfig.setAllocationPolicy(AllocationPolicy.HEAP_MEMORY.toString());
    _generic100KBufferConfig.setMaxSize(100000);
    _generic100KBufferConfig.setQueuePolicy(QueuePolicy.BLOCK_ON_WRITE.toString());
    _generic100KBufferConfig.setEnableScnIndex(false);

    _generic100KBufferStaticConfig = _generic100KBufferConfig.build();
  }

  @AfterClass
  public void afterClass()
  {
  }

  @BeforeTest
  public void beforeTest()
  {
  }

  @AfterTest
  public void afterTest()
  {
  }

  @BeforeSuite
  public void beforeSuite()
  {
  }

  @AfterSuite
  public void afterSuite()
  {
  }


  class EventCountingAnswer<T> implements IAnswer<T>
  {
    private final T _result;
    private final AtomicInteger _keyCounter;

    public EventCountingAnswer(T result, AtomicInteger keyCounter)
    {
      super();
      _result = result;
      _keyCounter = keyCounter;
    }


    @Override
    public T answer() throws Throwable
    {
      _keyCounter.incrementAndGet();
      return _result;
    }
  }

  class TimingOutMultiConsumerCallback extends MultiConsumerCallback
  {

    private int _failOnCall;

    public TimingOutMultiConsumerCallback(
        List<DatabusV2ConsumerRegistration> consumers,
        ExecutorService executorService, long timeBudgetMs,
        ConsumerCallbackFactory<DatabusCombinedConsumer> callbackFactory,
        ConsumerCallbackStats consumerStats,
        UnifiedClientStats unifiedClientStats,
        int errorCallNumber) {
      super(consumers, executorService, timeBudgetMs, callbackFactory, consumerStats, unifiedClientStats, null);
      _failOnCall = errorCallNumber;
    }


    public TimingOutMultiConsumerCallback(
        List<DatabusV2ConsumerRegistration> consumers,
        ExecutorService executorService, long timeBudgetMs,
        ConsumerCallbackFactory<DatabusCombinedConsumer> callbackFactory,
        int errorCallNumber) {
      super(consumers, executorService, timeBudgetMs, callbackFactory);
      _failOnCall = errorCallNumber;
    }

    @Override
    protected long getEstimatedTimeout(long timeBudget,
           long curTime,
           TimestampedFuture<ConsumerCallbackResult> top )
    {
      _failOnCall--;

      if (_failOnCall == -1 )
        return 0;
      return super.getEstimatedTimeout(timeBudget, curTime, top);
    }

    public void setFailOnCall(int failOnCall)
    {
      _failOnCall = failOnCall;
    }
  }

  class LoggedAnswer<T> implements IAnswer<T>
  {
    private final Logger _logger;
    private final T _result;
    private final Level _level;
    private final String _message;

    public LoggedAnswer(T result, Logger logger, Level level, String message)
    {
      super();
      _result = result;
      _level = level;
      _message = message;
      _logger = logger;
    }


    @Override
    public T answer() throws Throwable
    {
      _logger.log(_level, _message);
      return _result;
    }

  }

  class SleepingAnswer<T> implements IAnswer<T>
  {
    private final IAnswer<T> _delegate;
    private final long _sleepMs;
    private final T _defaultResult;

    public SleepingAnswer(IAnswer<T> delegate, long sleepMs)
    {
      _delegate = delegate;
      _sleepMs = sleepMs;
      _defaultResult = null;
    }

    public SleepingAnswer(T defaultResult, long sleepMs)
    {
      _delegate = null;
      _sleepMs = sleepMs;
      _defaultResult = defaultResult;
    }

    @Override
    public T answer() throws Throwable
    {
      try {Thread.sleep(_sleepMs); } catch (InterruptedException ie) {}
      return null == _delegate ? _defaultResult : _delegate.answer();
    }
  }

  class ExceptionAnswer<T> implements IAnswer<T>
  {
    private final Throwable _exception;
    public ExceptionAnswer(Throwable exception) {
      _exception = exception;
    }

    @Override
    public T answer() throws Throwable
    {
      throw _exception;
    }
  }

  class SomeApplicationException extends RuntimeException {
    SomeApplicationException() {
      super("Some Application Exception");
    }
  }
}
