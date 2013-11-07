package com.linkedin.databus.core;
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
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.Pipe;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.core.DbusEventBuffer.AllocationPolicy;
import com.linkedin.databus.core.DbusEventBuffer.DbusEventIterator;
import com.linkedin.databus.core.DbusEventBuffer.QueuePolicy;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsTotalStats;
import com.linkedin.databus.core.test.DbusEventAppender;
import com.linkedin.databus.core.test.DbusEventBufferConsumer;
import com.linkedin.databus.core.test.DbusEventBufferReader;
import com.linkedin.databus.core.test.DbusEventBufferReflector;
import com.linkedin.databus.core.test.DbusEventBufferWriter;
import com.linkedin.databus.core.test.DbusEventCorrupter;
import com.linkedin.databus.core.test.DbusEventCorrupter.EventCorruptionType;
import com.linkedin.databus.core.test.DbusEventGenerator;
import com.linkedin.databus.core.util.BufferPosition;
import com.linkedin.databus.core.util.BufferPositionParser;
import com.linkedin.databus.core.util.EventBufferConsumer;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus.core.util.RngUtils;
import com.linkedin.databus.core.util.UncaughtExceptionTrackingThread;
import com.linkedin.databus.core.util.Utils;
import com.linkedin.databus2.core.AssertLevel;
import com.linkedin.databus2.test.TestUtil;

public class TestDbusEventBuffer
{

  public static final String MODULE = TestDbusEventBuffer.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);
  public static final long NANOSECONDS = 1000*1000*1000;
  public static final long MILLISECONDS = 1000;

  @BeforeClass
  public void setupClass()
  {
    TestUtil.setupLoggingWithTimestampedFile(true, "/tmp/TestDbusEventBuffer_", ".log", Level.WARN);
  }

  public class KeyValue
  {
    private final DbusEventKey key;
    private final String value;

    KeyValue(DbusEventKey key, String value)
    {
      this.key = key;
      this.value = value;
    }

    public DbusEventKey getKey()
    {
      return key;
    }

    public String getValue()
    {
      return value;
    }
  }

  private final long key = 12345L;
  private final long timeStamp = System.nanoTime();
  private final short pPartitionId = 0;
  private final short lPartitionId = 30;
  private final String value = "foobar";
  private final short srcId = 15;
  private final byte[] schemaId = "abcdefghijklmnop".getBytes();


  static DbusEventBuffer.StaticConfig getConfig(long maxEventBufferSize, int maxIndividualBufferSize,
                                           int maxIndexSize, int maxReadBufferSize,
                                           AllocationPolicy allocationPolicy, QueuePolicy policy,
                                           AssertLevel assertLevel) throws InvalidConfigException
  {
    DbusEventBuffer.Config config = new DbusEventBuffer.Config();
    config.setMaxSize(maxEventBufferSize);
    config.setMaxIndividualBufferSize(maxIndividualBufferSize);
    config.setScnIndexSize(maxIndexSize);
    config.setAverageEventSize(maxReadBufferSize);
    config.setAllocationPolicy(allocationPolicy.name());
    config.setQueuePolicy(policy.toString());
    config.setAssertLevel(null != assertLevel ? assertLevel.toString(): AssertLevel.NONE.toString());
    return config.build();
  }

  /**
   * Base readEvents case: single empty buffer and enough capacity to fit everything;
   * BLOCK_ON_WRITE
   */
  @Test
  public void testReadEventsSingleLargeEmptyBufferBlockOnWrite() throws Exception
  {
    final ReadEventsTestParams params = new ReadEventsTestParams()
        .testName("testReadEventsSingleLarEmptyBufferBlockOnWrite")
        .startScn(10)
        .srcBufferSize(100000)
        .numSrcEvents(50)
        .maxWindowSize(5)
        .destBufferSize(100000)
        .destIndividualBufferSize(1000000)
        .destStgBufferSize(100000)
        .destQueuePolicy(QueuePolicy.BLOCK_ON_WRITE)
        //.logLevel(Level.DEBUG)
        //.debuggingMode(true)
        ;

    params.runReadEventsTests();
  }

  /**
   * Base readEvents case: single empty buffer and enough capacity to fit everything;
   * OVERWRITE_ON_WRITE
   */
  @Test
  public void testReadEventsSingleLargeEmptyBufferOverwriteOnWrite() throws Exception
  {
    final ReadEventsTestParams params = new ReadEventsTestParams()
        .testName("testReadEventsSingleLarEmptyBufferOverwriteOnWrite")
        .startScn(10)
        .srcBufferSize(100000)
        .numSrcEvents(50)
        .maxWindowSize(5)
        .destBufferSize(100000)
        .destIndividualBufferSize(1000000)
        .destStgBufferSize(100000)
        .destQueuePolicy(QueuePolicy.BLOCK_ON_WRITE);

    params.runReadEventsTests();
  }

  /** Base readEvents case: many empty buffers with size < the max size needed; BLOCK_ON_WRITE */
  @Test
  public void testReadEventsManySmallEmptyBuffersBlockOnWrite() throws Exception
  {
    final ReadEventsTestParams params = new ReadEventsTestParams();
    params
    .testName("testReadEventsManySmallEmptyBuffersBlockOnWrite")
    .startScn(10)
    .srcBufferSize(100000)
    .numSrcEvents(20)
    .maxWindowSize(5)
    .destBufferSize(100000)
    .destIndividualBufferSize(params._numSrcEvents * 150 / 5)
    .destStgBufferSize(100000)
    .destQueuePolicy(QueuePolicy.BLOCK_ON_WRITE);

    params.runReadEventsTests();
  }

  /** Base readEvents case: many empty buffers with size < the max size needed; BLOCK_ON_WRITE */
  @Test
  public void testReadEventsManySmallEmptyBuffersOverwriteOnWrite() throws Exception
  {
    final ReadEventsTestParams params = new ReadEventsTestParams();
    params
    .testName("testReadEventsManySmallEmptyBuffersOverwriteOnWrite")
    .startScn(10)
    .srcBufferSize(100000)
    .numSrcEvents(20)
    .maxWindowSize(5)
    .destBufferSize(100000)
    .destIndividualBufferSize(params._numSrcEvents * 150 / 5)
    .destStgBufferSize(100000)
    .destQueuePolicy(QueuePolicy.OVERWRITE_ON_WRITE);

    params.runReadEventsTests();
  }

  /**
   * Base readEvents case: single empty buffer and enough capacity to fit everything; small
   * staging buffer; BLOCK_ON_WRITE
   */
  @Test
  public void testReadEventsSmallStgBufferBlockOnWrite() throws Exception
  {
    final ReadEventsTestParams params = new ReadEventsTestParams();
    params
    .testName("testReadEventsManySmallEmptyBuffersOverwriteOnWrite")
    .startScn(10)
    .srcBufferSize(100000)
    .numSrcEvents(20)
    .maxWindowSize(5)
    .destBufferSize(100000)
    .destIndividualBufferSize(1000000)
    .destStgBufferSize(params._numSrcEvents * 150 / 5)
    .destQueuePolicy(QueuePolicy.BLOCK_ON_WRITE);

    params.runReadEventsTests();
  }

  /**
   * Base readEvents case: single empty buffer and enough capacity to fit everything; small
   * staging buffer; BLOCK_ON_WRITE
   */
  @Test
  public void testReadEventsSmallStgBufferOverwriteOnWrite() throws Exception
  {
    final ReadEventsTestParams params = new ReadEventsTestParams();
    params
    .testName("testReadEventsSmallStgBufferOverwriteOnWrite")
    .startScn(10)
    .srcBufferSize(100000)
    .numSrcEvents(20)
    .maxWindowSize(5)
    .destBufferSize(100000)
    .destIndividualBufferSize(1000000)
    .destStgBufferSize(params._numSrcEvents * 150 / 5)
    .destQueuePolicy(QueuePolicy.OVERWRITE_ON_WRITE);

    params.runReadEventsTests();
  }

  /**
   * Base readEvents case: many empty buffers with size < the max size needed; small staging
   * buffer; BLOCK_ON_WRITE
   */
  @Test
  public void testReadEventsManySmallBuffersSmallStgBlock() throws Exception
  {
    final ReadEventsTestParams params = new ReadEventsTestParams();
    params
    .testName("testReadEventsManySmallBuffersSmallStgBlock")
    .startScn(10)
    .srcBufferSize(100000)
    .numSrcEvents(20)
    .maxWindowSize(5)
    .destBufferSize(100000)
    .destIndividualBufferSize(params._numSrcEvents * 150 / 5)
    .destStgBufferSize(params._numSrcEvents * 150 / 10)
    .destQueuePolicy(QueuePolicy.BLOCK_ON_WRITE);

    params.runReadEventsTests();
  }

  /**
   * Base readEvents case: many empty buffers with size < the max size needed; small staging
   * buffer; BLOCK_ON_WRITE
   */
  @Test
  public void testReadEventsManySmallBuffersSmallStgOverwrite() throws Exception
  {
    final ReadEventsTestParams params = new ReadEventsTestParams();
    params
    .testName("testReadEventsManySmallBuffersSmallStgOverwrite")
    .startScn(10)
    .srcBufferSize(100000)
    .numSrcEvents(20)
    .maxWindowSize(5)
    .destBufferSize(100000)
    .destIndividualBufferSize(params._numSrcEvents * 150 / 5)
    .destStgBufferSize(params._numSrcEvents * 150 / 10)
    .destQueuePolicy(QueuePolicy.OVERWRITE_ON_WRITE);

    params.runReadEventsTests();
  }

  /**
   * Base readEvents case:single small event buffer which should cause multiple wrap-arounds while
   * reading; OVERWRITE_ON_WRITE
   */
  @Test
  public void testReadEventsSingleBufferWrapAroundOverwrite() throws Exception
  {
    final ReadEventsTestParams params = new ReadEventsTestParams();
    params
    .testName("testReadEventsSingleBufferWrapAroundOverwrite")
    .startScn(10)
    .srcBufferSize(100000)
    .numSrcEvents(200)
    .maxWindowSize(5)
    .destBufferSize(params._numSrcEvents * 150 / 3)
    .destIndividualBufferSize(1000000)
    .destStgBufferSize(params._numSrcEvents * 150 / 10)
    .destQueuePolicy(QueuePolicy.OVERWRITE_ON_WRITE)
    .dataValidation(false) /* we can't validate the exact events because some of them have
                              been overwritten; we'll just validate errors and counts */
    //.debuggingMode(true)
     ;

    params.runReadEventsTests();
  }

  /**
   * Tests a case with an error in the middle of readEvents and dropOldEvents enabled.
   * The scenario is as follows. Relay1: events1 events2 error events3
   * Relay2: events2 events3.
   * We expect the clients to see: events1 events2 events3.
   */
  @Test
  public void testReadEventsMidErrorDropOldEvents() throws Exception
  {
    final int eventBatchSize = 50;

    final ReadEventsTestParams params1 = new ReadEventsTestParams();
    params1.testName("testReadEventsMidErrorDropOldEvents")
           .startScn(10)
           .srcBufferSize(100000)
           .numSrcEvents(eventBatchSize)
           .maxWindowSize(5);
    params1.setup();
    params1.generateAndAppendEvents();
    params1.streamEvents();

    params1._log.info("events2 bytes");
    final ReadEventsTestParams params2 = new ReadEventsTestParams();
    params2.testName("testReadEventsMidErrorDropOldEvents")
           .startScn(params1._srcBuf.lastWrittenScn() + 1)
           .srcBufferSize(100000)
           .numSrcEvents(eventBatchSize)
           .maxWindowSize(5);
    params2.setup();
    params2.generateAndAppendEvents();
    params2.streamEvents();

    params1._log.info("events3 bytes");
    final ReadEventsTestParams params3 = new ReadEventsTestParams();
    params3.testName("testReadEventsMidErrorDropOldEvents")
           .startScn(params2._srcBuf.lastWrittenScn() + 1)
           .srcBufferSize(100000)
           .numSrcEvents(eventBatchSize)
           .maxWindowSize(5);
    params3.setup();
    params3.generateAndAppendEvents();
    params3.streamEvents();

    params1._log.info("simulate relay1 stream: events1 events2 error events3");
    final ReadEventsTestParams paramsRead1 = new ReadEventsTestParams();
    paramsRead1.testName("testReadEventsMidErrorDropOldEvents")
               .numSrcEvents(2 * eventBatchSize)
               .destBufferSize(100000)
               .destIndividualBufferSize(100000)
               .destStgBufferSize(100000)
               .destQueuePolicy(QueuePolicy.BLOCK_ON_WRITE)
               .expectReadError(true)
//             .debuggingMode(true)
               ;
    paramsRead1.setup();
    paramsRead1._destBuf.setDropOldEvents(true);

    paramsRead1._srcByteStr = new ByteArrayOutputStream();
    paramsRead1._srcByteStr.write(params1._srcByteStr.toByteArray());
    paramsRead1._srcByteStr.write(params2._srcByteStr.toByteArray());
    paramsRead1._srcByteStr.write("DEADBEEF".getBytes());
    paramsRead1._srcByteStr.write(params3._srcByteStr.toByteArray());

    paramsRead1.readDataAtDestination();

    params1._log.info("simulate relay2 stream: events1 events2 error events3");
    final ReadEventsTestParams paramsRead2 = new ReadEventsTestParams();
    paramsRead2.testName("testReadEventsMidErrorDropOldEvents")
          .srcBufferSize(100000)
          .numSrcEvents(2 * eventBatchSize)
          .destBufferSize(100000)
          .destIndividualBufferSize(100000)
          .destStgBufferSize(100000)
          .destQueuePolicy(QueuePolicy.BLOCK_ON_WRITE)
          .expectReadError(false);
    paramsRead2.setup();
    paramsRead2._destBuf.setDropOldEvents(true);

    paramsRead2._srcEvents = new Vector<DbusEvent>();
    paramsRead2._srcEvents.addAll(params2._srcEvents);
    paramsRead2._srcEvents.addAll(params3._srcEvents);
    paramsRead2.appendGeneratedEvents();
    paramsRead2.streamEvents();

    paramsRead2.runAndValidateReadEventsCall();

  }

  /**
   * Tests the dropping of events with an old scn. We generate 2 sets of events: events1 and
   * events2. Then send events1 events1 events2. The client should only see events1 events2.
   * @throws InvalidConfigException
   * @throws OffsetNotFoundException
   * @throws ScnNotFoundException
   */
  @Test
  public void testReadEventsDropOld() throws Exception
  {
    final int eventBatchSize = 123;

    final ReadEventsTestParams params1 = new ReadEventsTestParams();
    params1.testName("testReadEventsDropOld")
           .startScn(10)
           .srcBufferSize(100000)
           .numSrcEvents(eventBatchSize)
           .maxWindowSize(10);
    params1.setup();
    params1.generateAndAppendEvents();
    params1.streamEvents();

    params1._log.info("events1 bytes");

    params1._log.info("events2 bytes");
    final ReadEventsTestParams params2 = new ReadEventsTestParams();
    params2.testName("testReadEventsDropOld")
           .startScn(params1._srcBuf.lastWrittenScn() + 1)
           .srcBufferSize(100000)
           .numSrcEvents(eventBatchSize)
           .maxWindowSize(5);
    params2.setup();
    params2.generateAndAppendEvents();
    params2.streamEvents();

    params1._log.info("simulate relay2 stream: events1 events2 error events3");
    final ReadEventsTestParams paramsRead2 = new ReadEventsTestParams();
    paramsRead2.testName("testReadEventsMidErrorDropOldEvents")
               .srcBufferSize(100000)
               .numSrcEvents(2 * eventBatchSize)
               .destBufferSize(100000)
               .destIndividualBufferSize(100000)
               .destStgBufferSize(100000)
               .destQueuePolicy(QueuePolicy.BLOCK_ON_WRITE)
               .expectReadError(false)
//             .debuggingMode(true)
               ;
    paramsRead2.setup();
    paramsRead2._destBuf.setDropOldEvents(true);

    paramsRead2._srcByteStr = new ByteArrayOutputStream();
    paramsRead2._srcByteStr.write(params1._srcByteStr.toByteArray());
    paramsRead2._srcByteStr.write(params1._srcByteStr.toByteArray());
    paramsRead2._srcByteStr.write(params2._srcByteStr.toByteArray());
    paramsRead2._numStreamedEvents = 2 * params1._numStreamedEvents + params2._numStreamedEvents;

    //reset the expected events
    paramsRead2._srcEvents = new Vector<DbusEvent>();
    paramsRead2._srcEvents.addAll(params1._srcEvents);
    paramsRead2._srcEvents.addAll(params2._srcEvents);

    paramsRead2.runAndValidateReadEventsCall();
  }

  @Test
  public void testReadEventsBlockingAutoStart()
  throws Exception
  {
    readEventsBlocking(false);
  }

  @Test
  public void testReadEventsBlocking()
  throws Exception
  {
    readEventsBlocking(true);
  }

  @Test
  /**
   * A class of tests to verify the automatic growing of the event staging buffer in readEvents().
   * The test varies the event buffer sizes and the event sizes */
  public void testReadEventsGrowStgBuffer() throws Exception
  {
    final int K = 1024;
    final int baseSize = 150 * K;

    final int numEvents = 5;
    for (int maxSize = baseSize + 20 * K; maxSize < 2 * baseSize ; maxSize += 20 * K)
    {
      for (int eventSize = 567; eventSize <= maxSize / (int)(numEvents * 1.5); eventSize = eventSize * 2 + 1 )
      {
        final ReadEventsTestParams params = new ReadEventsTestParams();
        params
        .testName("testReadEventsGrowStgBuffer_" + maxSize + "_" + eventSize)
        .startScn(10)
        .srcBufferSize((eventSize + 300) * numEvents)
        .numSrcEvents(numEvents)
        .maxWindowSize(1)
        .eventSize(eventSize)
        .destBufferSize(maxSize)
        .destIndividualBufferSize(baseSize)
        .destStgBufferSize(1 * K)
        .destQueuePolicy(QueuePolicy.OVERWRITE_ON_WRITE)
        .dataValidation(true)
        //.logLevel(Level.INFO)
        //.debuggingMode(true)
        ;

        params.runReadEventsTests();
      }
    }

  }

  private void readEventsBlocking (boolean invokeStartOnBuffer)
  throws Exception
  {
    // Src Event producer
    Vector<DbusEvent> srcTestEvents = new Vector<DbusEvent>();
    // Dest Event consumer
    Vector<DbusEvent> dstTestEvents = new Vector<DbusEvent>();
    EventBufferTestInput blockingCapacityTest = new EventBufferTestInput();
    final int numEvents = 5000;
    // set sharedBufferSize to a value much smaller than total size required
    blockingCapacityTest.setNumEvents(numEvents)
                        .setWindowSize(numEvents/10)
                        .setSharedBufferSize(numEvents/5)
                        .setStagingBufferSize(numEvents/10)
                        .setIndexSize(numEvents/10)
                        .setIndividualBufferSize(numEvents)
                        .setBatchSize(numEvents/2)
                        .setProducerBufferSize(numEvents*2)
                        .setConsQueuePolicy(QueuePolicy.BLOCK_ON_WRITE)
                        .setPayloadSize(100)
                        .setDeleteInterval(1)
                        .setTestName("readEventsBlocking(" + invokeStartOnBuffer +")");

    // set window size > shared buffer size ; shared buffer is still lower  than total capacity
    EventBufferTestInput block2 = new EventBufferTestInput();
    block2.setNumEvents(numEvents)
          .setWindowSize(numEvents/4)
          .setSharedBufferSize(numEvents/5)
          .setStagingBufferSize(numEvents/5)
          .setIndexSize(numEvents/10)
          .setIndividualBufferSize(numEvents)
          .setBatchSize(numEvents)
          .setProducerBufferSize(numEvents*2)
          .setConsQueuePolicy(QueuePolicy.BLOCK_ON_WRITE)
          .setPayloadSize(100)
          .setDeleteInterval(1);

    // set staging buffer size = shared buffer size ; shared buffer is still lower  than total capacity ;
    EventBufferTestInput block3 = new EventBufferTestInput();
    block3.setNumEvents(numEvents)
          .setWindowSize(numEvents/20)
          .setSharedBufferSize(numEvents/5)
          .setStagingBufferSize(numEvents/5)
          .setIndexSize(numEvents/10)
          .setIndividualBufferSize(numEvents)
          .setBatchSize(numEvents)
          .setProducerBufferSize(numEvents*2)
          .setConsQueuePolicy(QueuePolicy.BLOCK_ON_WRITE)
          .setPayloadSize(100)
          .setDeleteInterval(1);

    // Test configurations;
    Vector<EventBufferTestInput> tests = new Vector<EventBufferTestInput>();
    tests.add(blockingCapacityTest);
    tests.add(block2);
    tests.add(block3);

    DbusEventsStatisticsCollector emitterStats = new DbusEventsStatisticsCollector(1,"appenderStats",true,true,null);
    DbusEventsStatisticsCollector streamStats = new DbusEventsStatisticsCollector(1,"streamStats",true,true,null);
    DbusEventsStatisticsCollector clientStats = new DbusEventsStatisticsCollector(1,"clientStats",true,true,null);

    int testId = 0;
    for (Iterator<EventBufferTestInput> it=tests.iterator(); it.hasNext(); )
    {
      EventBufferTestInput testInput = it.next();

      srcTestEvents.clear();
      dstTestEvents.clear();
      emitterStats.reset();
      streamStats.reset();
      clientStats.reset();

      assertEquals(0, dstTestEvents.size());
      boolean result = runConstEventsReaderWriter(srcTestEvents, dstTestEvents, testInput, emitterStats,
                                                  streamStats, clientStats, invokeStartOnBuffer);
      LOG.info(String.format("TestId=%d Test=%s  result=%b size of dst events=%d \n",
                             testId, testInput.toString(), result, dstTestEvents.size()));
      assertTrue(result);
      checkEvents(srcTestEvents, dstTestEvents, numEvents);

      ++testId;
    }
  }

  @Test
  public void testMaxEventSize() throws Exception
  {
    DbusEventBuffer dbusBuf =
        new DbusEventBuffer(getConfig(1144, 500, 100, 500, AllocationPolicy.HEAP_MEMORY,
                                      QueuePolicy.OVERWRITE_ON_WRITE, AssertLevel.ALL));
    DbusEventGenerator generator = new DbusEventGenerator();

    // Generate one event that equals the size of the first buffer. We won't be able to
    // append that to the buffer.
    Vector<DbusEvent> events = new Vector<DbusEvent>();
    generator.generateEvents(1, 2, 500, 439, events);
    DbusEventAppender appender = new DbusEventAppender(events,dbusBuf,null, false);
    int eventCount = 0;
    boolean exceptionCaught = false;

    dbusBuf.startEvents();
    try
    {
      eventCount = appender.addEventToBuffer(events.get(0), eventCount);
    }
    catch(DatabusRuntimeException e)
    {
      exceptionCaught = true;
    }
    Assert.assertTrue(exceptionCaught);
    Assert.assertEquals(0, eventCount);
    DbusEventBufferReflector reflector = appender.getDbusEventReflector();
    Assert.assertEquals(0, reflector.getBuffer(0).limit());
    Assert.assertEquals(0, reflector.getBuffer(1).limit());
    Assert.assertEquals(0, reflector.getBuffer(2).limit());

    events.clear();
    dbusBuf =
        new DbusEventBuffer(getConfig(1144, 500, 100, 500, AllocationPolicy.HEAP_MEMORY,
                                      QueuePolicy.OVERWRITE_ON_WRITE, AssertLevel.ALL));
    appender = new DbusEventAppender(events,dbusBuf,null, false);
    generator.generateEvents(1, 20, 500, 438, events);  // total event size = 499
    generator.generateEvents(1, 20, 500, 10, events);   // total event size = 71
    generator.generateEvents(1, 20, 500, 367, events);   // total event size = 428
    generator.generateEvents(1, 1, 500, 10, events);   // event + EOP = 132.

    // We should be able to append the first three events above, filling the two byte buffers completely.
    // And then the last event along with EOP marker in the last byte buffer, filling that to complete
    // one window.
    appender.run();

    // Now try to add one event. Because ScnIndex still has head as -1 for this buffer,
    // we end up getting a DatabusRuntimeException and not being able to add an event.
    // Since this is a rare case, it is TBD whether we need to support this case or not.
    events.clear();
    generator.generateEvents(1, 20, 500, 10, events);   // event size = 71, can append.
    exceptionCaught = false;
    try
    {
      appender.run();
    }
    catch(DatabusRuntimeException e)
    {
      exceptionCaught = true;
    }
    Assert.assertTrue(exceptionCaught);
  }

  // Add a full window plus one event to the buffer, leaving room at the end, and then roll back.
  // The write pointer should come back to the end of the event.
  @Test
  public void testBasicRollback() throws Exception
  {
    // We have 7 byte buffers now, the last one having 200 bytes, others with 500 bytes.
    DbusEventBuffer dbusBuf = new DbusEventBuffer(getConfig(3200, 500, 100, 500, AllocationPolicy.HEAP_MEMORY,
                                      QueuePolicy.OVERWRITE_ON_WRITE, AssertLevel.ALL));
    DbusEventGenerator generator = new DbusEventGenerator();
    Vector<DbusEvent> events = new Vector<DbusEvent>();
    DbusEventAppender appender = new DbusEventAppender(events,dbusBuf,null, false);
    events.clear();

    // Same test case as before, with the first 3 byte buffers having data now.
    generator.generateEvents(1, 20, 500, 438, events);  // total event size = 499
    generator.generateEvents(1, 20, 500, 10, events);   // total event size = 71
    generator.generateEvents(1, 20, 500, 367, events);   // total event size = 428
    generator.generateEvents(1, 1, 500, 10, events);   // event + EOP = 132.
    appender.run();
    DbusEventBufferReflector reflector = appender.getDbusEventReflector();

    // Now we should be able to add an event and roll it back as well.
    events.clear();
    generator.generateEvents(1, 20, 500, 10, events);   // event size = 71, can append.

    long cwp1 = reflector.getCurrentWritePosition().getPosition();
    dbusBuf.startEvents();
    int evCount = 0;
    evCount = appender.addEventToBuffer(events.firstElement(), evCount);
    Assert.assertEquals(1, evCount);
    long cwp2 = reflector.getCurrentWritePosition().getPosition();
    Assert.assertTrue("Cwp should differ event write", cwp1 != cwp2);

    // Roll back.
    dbusBuf.rollbackEvents();
    Assert.assertEquals(cwp1, reflector.getCurrentWritePosition().getPosition());
  }

  // Add a bunch of events to the buffer without EOP, and then issue a rollback.
  // Verify that the buffer is now empty, and we can add the same events with EOP again.
  @Test
  public void testCompleteRollback() throws Exception
  {
    DbusEventBuffer dbusBuf = new DbusEventBuffer(getConfig(1144, 500, 100, 500, AllocationPolicy.HEAP_MEMORY,
                                                            QueuePolicy.OVERWRITE_ON_WRITE, AssertLevel.ALL));
    DbusEventGenerator generator = new DbusEventGenerator();
    Vector<DbusEvent> events = new Vector<DbusEvent>();
    DbusEventAppender appender = new DbusEventAppender(events,dbusBuf,null, false);
    events.clear();

    // Same test case as before, with the first 3 byte buffers having data now.
    generator.generateEvents(1, 20, 500, 438, events);  // total event size = 499
    generator.generateEvents(1, 20, 500, 10, events);   // total event size = 71
    generator.generateEvents(1, 20, 500, 367, events);   // total event size = 428
    generator.generateEvents(1, 20, 500, 10, events);   // event = 71.

    dbusBuf.startEvents();
    int evCount = 0;
    for (DbusEvent e : events)
    {
      evCount = appender.addEventToBuffer(e, evCount);
    }
    Assert.assertEquals(events.size(), evCount);

    DbusEventBufferReflector reflector = appender.getDbusEventReflector();
    dbusBuf.rollbackEvents();
    Assert.assertEquals(0, reflector.getCurrentWritePosition().getPosition());

    // Now we should be able to add these events, plus an EOP marker at the end.
    appender.run();
    long cwp1 = reflector.getCurrentWritePosition().getPosition();
    Assert.assertEquals(1156, cwp1);
    long tail = reflector.getTail().getPosition();
    Assert.assertEquals(1156, tail);
    Assert.assertEquals(0, reflector.getHead().getPosition());
  }

  @Test
  /**
   * For DDSDBUS-502. The bug will manifest as an "Error in BufferOffsetException"
   *
   * The problem was when we the next event to append is bigger than the available space in the current byte buffer,
   * we are not moving the head properly which causes this exception
   *
   * @throws Exception
   */
  public void testAppendEventBufferJump()
  throws Exception
  {
    // Multi byte-buffer EVB case
    {
      //Logger.getRootLogger().setLevel(Level.INFO);
      final DbusEventBuffer dbusBuf =
          new DbusEventBuffer(getConfig(1144, 500, 100, 500, AllocationPolicy.HEAP_MEMORY,
                                        QueuePolicy.OVERWRITE_ON_WRITE, AssertLevel.ALL));
      BufferPositionParser parser = dbusBuf.getBufferPositionParser();
      LOG.info("New Batch append 1");
      DbusEventGenerator generator = new DbusEventGenerator();
      Vector<DbusEvent> events = new Vector<DbusEvent>();
      generator.generateEvents(9, 1, 120, 39, events);

      // Add events to the EventBuffer. Now the buffer is full
      DbusEventAppender appender = new DbusEventAppender(events,dbusBuf,null);
      //Logger.getRootLogger().setLevel(Level.ALL);
      appender.run(); // running in the same thread

      LOG.info("Head:" + parser.toString(dbusBuf.getHead()) + ",Tail:" + parser.toString(dbusBuf.getTail()));
      LOG.info("Num buffers :" + dbusBuf.getBuffer().length);
      LOG.info("Buffer :" + Arrays.toString(dbusBuf.getBuffer()));

      long headPos = dbusBuf.getHead();
      long tailPos = dbusBuf.getTail();
      long headGenId = parser.bufferGenId(headPos);
      long headIndexId = parser.bufferIndex(headPos);
      long headOffset = parser.bufferOffset(headPos);
      long tailGenId = parser.bufferGenId(tailPos);
      long tailIndexId = parser.bufferIndex(tailPos);
      long tailOffset = parser.bufferOffset(tailPos);

      assertEquals("Head GenId", 0, headGenId);
      assertEquals("Head Index", 1, headIndexId);
      assertEquals("Head Offset", 222, headOffset);
      assertEquals("Tail GenId", 1, tailGenId);
      assertEquals("Tail Index", 0, tailIndexId);
      assertEquals("Tail Offset", 483, tailOffset);


      LOG.info("New Batch append 2");
      generator = new DbusEventGenerator(100);
      events = new Vector<DbusEvent>();
      generator.generateEvents(1, 1, 80, 10, events);

      // Add events to the EventBuffer. Now the buffer is full
      appender = new DbusEventAppender(events,dbusBuf,null);
      //Logger.getRootLogger().setLevel(Level.ALL);
      appender.run();
      LOG.info("Head:" + parser.toString(dbusBuf.getHead()) + ",Tail:" + parser.toString(dbusBuf.getTail()));
      LOG.info("Num buffers :" + dbusBuf.getBuffer().length);
      LOG.info("Buffer :" + Arrays.toString(dbusBuf.getBuffer()));
      headPos = dbusBuf.getHead();
      tailPos = dbusBuf.getTail();
      headGenId = parser.bufferGenId(headPos);
      headIndexId = parser.bufferIndex(headPos);
      headOffset = parser.bufferOffset(headPos);
      tailGenId = parser.bufferGenId(tailPos);
      tailIndexId = parser.bufferIndex(tailPos);
      tailOffset = parser.bufferOffset(tailPos);
      assertEquals("Head GenId", 0, headGenId);
      assertEquals("Head Index", 1, headIndexId);
      assertEquals("Head Offset", 222, headOffset);
      assertEquals("Tail GenId", 1, tailGenId);
      assertEquals("Tail Index", 1, tailIndexId);
      assertEquals("Tail Offset", 193, tailOffset);


      LOG.info("New Batch append 3");
      generator = new DbusEventGenerator(200);
      events = new Vector<DbusEvent>();
      generator.generateEvents(1, 1, 400, 320, events);

      // Add events to the EventBuffer. Now the buffer is full
      appender = new DbusEventAppender(events,dbusBuf,null);
      //Logger.getRootLogger().setLevel(Level.ALL);
      appender.run();
      LOG.info("Head:" + parser.toString(dbusBuf.getHead()) + ",Tail:" + parser.toString(dbusBuf.getTail()));
      LOG.info("Num buffers :" + dbusBuf.getBuffer().length);
      LOG.info("Buffer :" + Arrays.toString(dbusBuf.getBuffer()));
      headPos = dbusBuf.getHead();
      tailPos = dbusBuf.getTail();
      headGenId = parser.bufferGenId(headPos);
      headIndexId = parser.bufferIndex(headPos);
      headOffset = parser.bufferOffset(headPos);
      tailGenId = parser.bufferGenId(tailPos);
      tailIndexId = parser.bufferIndex(tailPos);
      tailOffset = parser.bufferOffset(tailPos);
      assertEquals("Head GenId", 1, headGenId);
      assertEquals("Head Index", 1, headIndexId);
      assertEquals("Head Offset", 61, headOffset);
      assertEquals("Tail GenId", 2, tailGenId);
      assertEquals("Tail Index", 0, tailIndexId);
      assertEquals("Tail Offset", 442, tailOffset);
    }


    // Single byte-buffer EVB case
    {
      //Logger.getRootLogger().setLevel(Level.INFO);
      final DbusEventBuffer dbusBuf =
          new DbusEventBuffer(getConfig(2144, 5000, 200, 500, AllocationPolicy.HEAP_MEMORY,
                                        QueuePolicy.OVERWRITE_ON_WRITE, AssertLevel.ALL));
      BufferPositionParser parser = dbusBuf.getBufferPositionParser();
      LOG.info("New Batch append 1");
      DbusEventGenerator generator = new DbusEventGenerator();
      Vector<DbusEvent> events = new Vector<DbusEvent>();
      generator.generateEvents(28, 2, 180, 39, events);

      // Add events to the EventBuffer. Now the buffer is full
      DbusEventAppender appender = new DbusEventAppender(events,dbusBuf,null);
      //Logger.getRootLogger().setLevel(Level.ALL);
      appender.run(); // running in the same thread

      LOG.info("Head:" + parser.toString(dbusBuf.getHead()) + ",Tail:" + parser.toString(dbusBuf.getTail()));
      LOG.info("Num buffers :" + dbusBuf.getBuffer().length);
      LOG.info("Buffer :" + Arrays.toString(dbusBuf.getBuffer()));
      long headPos = dbusBuf.getHead();
      long tailPos = dbusBuf.getTail();
      long headGenId = parser.bufferGenId(headPos);
      long headIndexId = parser.bufferIndex(headPos);
      long headOffset = parser.bufferOffset(headPos);
      long tailGenId = parser.bufferGenId(tailPos);
      long tailIndexId = parser.bufferIndex(tailPos);
      long tailOffset = parser.bufferOffset(tailPos);

      assertEquals("Head GenId", 0, headGenId);
      assertEquals("Head Index", 0, headIndexId);
      assertEquals("Head Offset", 1627, headOffset);
      assertEquals("Tail GenId", 1, tailGenId);
      assertEquals("Tail Index", 0, tailIndexId);
      assertEquals("Tail Offset", 1627, tailOffset);

      LOG.info("New Batch append 2");
      generator = new DbusEventGenerator(200);
      events = new Vector<DbusEvent>();
      generator.generateEvents(1, 1, 80, 10, events);

      // Add events to the EventBuffer. Now the buffer is full
      appender = new DbusEventAppender(events,dbusBuf,null);
      //Logger.getRootLogger().setLevel(Level.ALL);
      appender.run();
      LOG.info("Head:" + parser.toString(dbusBuf.getHead()) + ",Tail:" + parser.toString(dbusBuf.getTail()));
      LOG.info("Num buffers :" + dbusBuf.getBuffer().length);
      LOG.info("Buffer :" + Arrays.toString(dbusBuf.getBuffer()));
      headPos = dbusBuf.getHead();
      tailPos = dbusBuf.getTail();
      headGenId = parser.bufferGenId(headPos);
      headIndexId = parser.bufferIndex(headPos);
      headOffset = parser.bufferOffset(headPos);
      tailGenId = parser.bufferGenId(tailPos);
      tailIndexId = parser.bufferIndex(tailPos);
      tailOffset = parser.bufferOffset(tailPos);

      assertEquals("Head GenId", 0, headGenId);
      assertEquals("Head Index", 0, headIndexId);
      assertEquals("Head Offset", 1888, headOffset);
      assertEquals("Tail GenId", 1, tailGenId);
      assertEquals("Tail Index", 0, tailIndexId);
      assertEquals("Tail Offset", 1820, tailOffset);

      LOG.info("New Batch append 3");
      generator = new DbusEventGenerator(300);
      events = new Vector<DbusEvent>();
      generator.generateEvents(1, 1, 400, 330, events);

      // Add events to the EventBuffer. Now the buffer is full
      appender = new DbusEventAppender(events,dbusBuf,null);
      //Logger.getRootLogger().setLevel(Level.ALL);
      appender.run();
      LOG.info("Head:" + parser.toString(dbusBuf.getHead()) + ",Tail:" + parser.toString(dbusBuf.getTail()));
      LOG.info("Num buffers :" + dbusBuf.getBuffer().length);
      LOG.info("Buffer :" + Arrays.toString(dbusBuf.getBuffer()));
      headPos = dbusBuf.getHead();
      tailPos = dbusBuf.getTail();
      headGenId = parser.bufferGenId(headPos);
      headIndexId = parser.bufferIndex(headPos);
      headOffset = parser.bufferOffset(headPos);
      tailGenId = parser.bufferGenId(tailPos);
      tailIndexId = parser.bufferIndex(tailPos);
      tailOffset = parser.bufferOffset(tailPos);

      assertEquals("Head GenId", 1, headGenId);
      assertEquals("Head Index", 0, headIndexId);
      assertEquals("Head Offset", 583, headOffset);
      assertEquals("Tail GenId", 2, tailGenId);
      assertEquals("Tail Index", 0, tailIndexId);
      assertEquals("Tail Offset", 452, tailOffset);
    }
  }

  /*
   * This test-case is to recreate the bug where appendEvents incorrectly writes event without
   *  moving head.
   *
   *  The following is the issue.
   *
   *  head is at location x and tail is at location y
   *  The next event window is such that after adding "some" number of events in the window, both head
   *  and currentWritePosition is same (differing only in genId). Now adding the next event corrupts the
   *  buffer (and scnIndex) as head is not moved ahead.
   *
   */
  /**
   * Recreate the head and tail position such that the eventBuffer is in the below state:
   *
   * <pre>
   *                       NETBW
   *                      |-----|
   * ------------------------------------------------------------
   * ^      ^             ^                                     ^
   * |      |             |                                     |
   * 0      tail       CWP,head                                 capacity
   *
   * CWP : Current Write Position
   * NETBW : Next Event to be written
   * </pre>
   *
   * In this case, all pointers except head will be at (n+1)th generation while head
   * is at nth generation.
   *
   * Two test cases are covered here:
   *   1. n = 0
   *   2. n > 0
   */
  @Test
  public void testAppendEventOverlapNeq0() throws Exception
    // Case n = 0;
  {
    final DbusEventBuffer dbusBuf =
        new DbusEventBuffer(getConfig(1145,5000,100,500,AllocationPolicy.HEAP_MEMORY,
                                      QueuePolicy.OVERWRITE_ON_WRITE,
                                      AssertLevel.ALL));
    BufferPositionParser parser = dbusBuf.getBufferPositionParser();
    DbusEventGenerator generator = new DbusEventGenerator();
    Vector<DbusEvent> events = new Vector<DbusEvent>();
    generator.generateEvents(9, 3, 120, 39, events);

    // Add events to the EventBuffer. Now the buffer is full
    DbusEventAppender appender = new DbusEventAppender(events,dbusBuf,null);
    //Logger.getRootLogger().setLevel(Level.ALL);
    appender.run(); // running in the same thread

    LOG.info("Head:" + parser.toString(dbusBuf.getHead()) +
             ",Tail:" + parser.toString(dbusBuf.getTail()));

    long headPos = dbusBuf.getHead();
    long tailPos = dbusBuf.getTail();
    long scnIndexHead = dbusBuf.getScnIndex().getHead();
    long scnIndexTail = dbusBuf.getScnIndex().getTail();
    long headGenId = parser.bufferGenId(headPos);
    long headIndexId = parser.bufferIndex(headPos);
    long headOffset = parser.bufferOffset(headPos);
    long tailGenId = parser.bufferGenId(tailPos);
    long tailIndexId = parser.bufferIndex(tailPos);
    long tailOffset = parser.bufferOffset(tailPos);

    assertEquals("Head GenId", 0, headGenId);
    assertEquals("Head Index", 0, headIndexId);
    assertEquals("Head Offset", 0, headOffset);
    assertEquals("Tail GenId", 0, tailGenId);
    assertEquals("Tail Index", 0, tailIndexId);
    assertEquals("Tail Offset", 1144, tailOffset);
    assertEquals("SCNIndex Head",0,scnIndexHead);
    assertEquals("SCNIndex Tail",80,scnIndexTail);

    LOG.info("ScnIndex Head is :" + scnIndexHead + ", ScnIndex Tail is :" + scnIndexTail);


    events = new Vector<DbusEvent>();
    generator = new DbusEventGenerator(100);
    /*
     * The event size is carefully created such that after adding 2nd
     * event CWP and tail points to the same location. Now the 3rd event corrupts the EVB and index (in the presence of bug).
     */

    generator.generateEvents(3, 2, 150, 89, events);

    appender = new DbusEventAppender(events,dbusBuf,null);
    appender.run(); // running in the same thread

    LOG.info("Head:" + parser.toString(dbusBuf.getHead()) + ",Tail:" + parser.toString(dbusBuf.getTail()));

    headPos = dbusBuf.getHead();
    tailPos = dbusBuf.getTail();
    headGenId = parser.bufferGenId(headPos);
    headIndexId = parser.bufferIndex(headPos);
    headOffset = parser.bufferOffset(headPos);
    tailGenId = parser.bufferGenId(tailPos);
    tailIndexId = parser.bufferIndex(tailPos);
    tailOffset = parser.bufferOffset(tailPos);
    scnIndexHead = dbusBuf.getScnIndex().getHead();
    scnIndexTail = dbusBuf.getScnIndex().getTail();
    assertEquals("Head GenId", 0, headGenId);
    assertEquals("Head Index", 0, headIndexId);
    assertEquals("Head Offset", 783, headOffset);
    assertEquals("Tail GenId", 1, tailGenId);
    assertEquals("Tail Index", 0, tailIndexId);
    assertEquals("Tail Offset", 633, tailOffset);
    assertEquals("SCNIndex Head",64,scnIndexHead);
    assertEquals("SCNIndex Tail",48,scnIndexTail);
  }

  //Case when n> 0
  @Test
  public void testAppendEventOverlapNgt0() throws Exception
  {
    final DbusEventBuffer dbusBuf =
        new DbusEventBuffer(getConfig(1145,5000,100,500,AllocationPolicy.HEAP_MEMORY,
                                      QueuePolicy.OVERWRITE_ON_WRITE, AssertLevel.ALL));
    BufferPositionParser parser = dbusBuf.getBufferPositionParser();
    DbusEventGenerator generator = new DbusEventGenerator();
    Vector<DbusEvent> events = new Vector<DbusEvent>();
    generator.generateEvents(9, 3, 120, 39, events);

    // Add events to the EventBuffer. Now the buffer is full
    DbusEventAppender appender = new DbusEventAppender(events,dbusBuf,null);
    appender.run(); // running in the same thread

    LOG.info("Head:" + parser.toString(dbusBuf.getHead()) + ",Tail:" + parser.toString(dbusBuf.getTail()));

    long headPos = dbusBuf.getHead();
    long tailPos = dbusBuf.getTail();
    long scnIndexHead = dbusBuf.getScnIndex().getHead();
    long scnIndexTail = dbusBuf.getScnIndex().getTail();
    long headGenId = parser.bufferGenId(headPos);
    long headIndexId = parser.bufferIndex(headPos);
    long headOffset = parser.bufferOffset(headPos);
    long tailGenId = parser.bufferGenId(tailPos);
    long tailIndexId = parser.bufferIndex(tailPos);
    long tailOffset = parser.bufferOffset(tailPos);

    assertEquals("Head GenId", 0, headGenId);
    assertEquals("Head Index", 0, headIndexId);
    assertEquals("Head Offset", 0, headOffset);
    assertEquals("Tail GenId", 0, tailGenId);
    assertEquals("Tail Index", 0, tailIndexId);
    assertEquals("Tail Offset", 1144, tailOffset);
    assertEquals("SCNIndex Head",0,scnIndexHead);
    assertEquals("SCNIndex Tail",80,scnIndexTail);


    headPos = parser.setGenId(headPos, 300);
    tailPos = parser.setGenId(tailPos, 300);
    dbusBuf.setHead(headPos);
    dbusBuf.setTail(tailPos);
    dbusBuf.recreateIndex();

    events = new Vector<DbusEvent>();
    generator = new DbusEventGenerator(1000);
    /*
     * The event size is carefully created such that after adding 2nd
     * event CWP and tail points to the same location. Now the 3rd event corrupts the EVB and index (in the presence of bug).
     */
    generator.generateEvents(3, 2, 150, 89, events);

    appender = new DbusEventAppender(events,dbusBuf,null);
    LOG.info("1");
    //Logger.getRootLogger().setLevel(Level.ALL);
    appender.run(); // running in the same thread
    LOG.info("2");

    LOG.info("Head:" + parser.toString(dbusBuf.getHead()) + ",Tail:" + parser.toString(dbusBuf.getTail()));

    headPos = dbusBuf.getHead();
    tailPos = dbusBuf.getTail();
    headGenId = parser.bufferGenId(headPos);
    headIndexId = parser.bufferIndex(headPos);
    headOffset = parser.bufferOffset(headPos);
    tailGenId = parser.bufferGenId(tailPos);
    tailIndexId = parser.bufferIndex(tailPos);
    tailOffset = parser.bufferOffset(tailPos);
    scnIndexHead = dbusBuf.getScnIndex().getHead();
    scnIndexTail = dbusBuf.getScnIndex().getTail();
    assertEquals("Head GenId", 300, headGenId);
    assertEquals("Head Index", 0, headIndexId);
    assertEquals("Head Offset", 783, headOffset);
    assertEquals("Tail GenId", 301, tailGenId);
    assertEquals("Tail Index", 0, tailIndexId);
    assertEquals("Tail Offset", 633, tailOffset);
    assertEquals("SCNIndex Head",64,scnIndexHead);
    assertEquals("SCNIndex Tail",48,scnIndexTail);

  }

  //Case where we dump lot of events while reaching the error case many times during this process.
  @Test
  public void testAppendEventOverlapMany() throws Exception
  {
    final DbusEventBuffer dbusBuf =
        new DbusEventBuffer(getConfig(1145,5000,100,500,AllocationPolicy.HEAP_MEMORY,
                                      QueuePolicy.OVERWRITE_ON_WRITE, AssertLevel.ALL));
    BufferPositionParser parser = dbusBuf.getBufferPositionParser();
    DbusEventGenerator generator = new DbusEventGenerator();
    Vector<DbusEvent> events = new Vector<DbusEvent>();
    generator.generateEvents(9, 3, 120, 39, events);

    // Add events to the EventBuffer. Now the buffer is full
    DbusEventAppender appender = new DbusEventAppender(events,dbusBuf,null);
    appender.run(); // running in the same thread

    LOG.info("Head:" + parser.toString(dbusBuf.getHead()) + ",Tail:" + parser.toString(dbusBuf.getTail()));

    long headPos = dbusBuf.getHead();
    long tailPos = dbusBuf.getTail();
    long scnIndexHead = dbusBuf.getScnIndex().getHead();
    long scnIndexTail = dbusBuf.getScnIndex().getTail();
    long headGenId = parser.bufferGenId(headPos);
    long headIndexId = parser.bufferIndex(headPos);
    long headOffset = parser.bufferOffset(headPos);
    long tailGenId = parser.bufferGenId(tailPos);
    long tailIndexId = parser.bufferIndex(tailPos);
    long tailOffset = parser.bufferOffset(tailPos);


    assertEquals("Head GenId", 0, headGenId);
    assertEquals("Head Index", 0, headIndexId);
    assertEquals("Head Offset", 0, headOffset);
    assertEquals("Tail GenId", 0, tailGenId);
    assertEquals("Tail Index", 0, tailIndexId);
    assertEquals("Tail Offset", 1144, tailOffset);
    assertEquals("SCNIndex Head",0,scnIndexHead);
    assertEquals("SCNIndex Tail",80,scnIndexTail);

    LOG.info("ScnIndex Head is :" + scnIndexHead + ", ScnIndex Tail is :" + scnIndexTail);


    /*
     * Dump lots of events
     */
    events = new Vector<DbusEvent>();
    generator = new DbusEventGenerator(100);
    generator.generateEvents(655, 3, 150, 89, events);
    appender = new DbusEventAppender(events,dbusBuf,null);
    appender.run(); // running in the same thread
    LOG.info("Head:" + parser.toString(dbusBuf.getHead()) + ",Tail:" + parser.toString(dbusBuf.getTail()));

    headPos = dbusBuf.getHead();
    tailPos = dbusBuf.getTail();
    headGenId = parser.bufferGenId(headPos);
    headIndexId = parser.bufferIndex(headPos);
    headOffset = parser.bufferOffset(headPos);
    tailGenId = parser.bufferGenId(tailPos);
    tailIndexId = parser.bufferIndex(tailPos);
    tailOffset = parser.bufferOffset(tailPos);
    scnIndexHead = dbusBuf.getScnIndex().getHead();
    scnIndexTail = dbusBuf.getScnIndex().getTail();
    assertEquals("Head GenId", 109, headGenId);
    assertEquals("Head Index", 0, headIndexId);
    assertEquals("Head Offset", 511, headOffset);
    assertEquals("Tail GenId", 110, tailGenId);
    assertEquals("Tail Index", 0, tailIndexId);
    assertEquals("Tail Offset", 211, tailOffset);
    assertEquals("SCNIndex Head",32,scnIndexHead);
    assertEquals("SCNIndex Tail",16,scnIndexTail);

    /*
     * The event size is carefully created such that after adding 2nd
     * event CWP and tail points to the same location.
     */
    events = new Vector<DbusEvent>();
    generator = new DbusEventGenerator(10000);
    generator.generateEvents(3, 5, 100, 28, events);

    appender = new DbusEventAppender(events,dbusBuf,null);
    appender.run(); // running in the same thread

    LOG.info("Head:" + parser.toString(dbusBuf.getHead()) + ",Tail:" + parser.toString(dbusBuf.getTail()));

    events = new Vector<DbusEvent>();
    generator = new DbusEventGenerator(10000);
    generator.generateEvents(3, 3, 120, 19, events);

    headPos = dbusBuf.getHead();
    tailPos = dbusBuf.getTail();
    headGenId = parser.bufferGenId(headPos);
    headIndexId = parser.bufferIndex(headPos);
    headOffset = parser.bufferOffset(headPos);
    tailGenId = parser.bufferGenId(tailPos);
    tailIndexId = parser.bufferIndex(tailPos);
    tailOffset = parser.bufferOffset(tailPos);
    scnIndexHead = dbusBuf.getScnIndex().getHead();
    scnIndexTail = dbusBuf.getScnIndex().getTail();
    assertEquals("Head GenId", 110, headGenId);
    assertEquals("Head Index", 0, headIndexId);
    assertEquals("Head Offset", 0, headOffset);
    assertEquals("Tail GenId", 110, tailGenId);
    assertEquals("Tail Index", 0, tailIndexId);
    assertEquals("Tail Offset", 600, tailOffset);
    assertEquals("SCNIndex Head",0,scnIndexHead);
    assertEquals("SCNIndex Tail",32,scnIndexTail);

  }

  @Test
  /*
   * This testcase is to recreate the bug where pull thread incorrectly writes to the head of
   * the iterator when in BLOCK_ON_WRITE mode. The error was because readEvents() incorrectly
   * relies on remaining() to give an accurate value.
   */
  public void testReadEventOverlap()
    throws Exception
  {
    /*
     * Recreate the head and tail position such that the eventBuffer is in the below state
     * --------------------------------------------------------------
     * ^      ^                                              ^      ^
     * |      |                                              |      |
     * 0      head                                           tail   capacity
     *
     * The space between tail and capacity is such that with no internal fragmentation, the free space
     * will be sufficient to store 2 events but with internal fragmentation, the free space will not be
     * enough. In this case, the readEvents should block until an event is removed by the other thread.
     */
    final DbusEventBuffer dbusBuf =
        new DbusEventBuffer(getConfig(1000,1000,100,500,AllocationPolicy.HEAP_MEMORY,
                                      QueuePolicy.BLOCK_ON_WRITE, AssertLevel.NONE));
    BufferPositionParser parser = dbusBuf.getBufferPositionParser();
    DbusEventGenerator generator = new DbusEventGenerator();
    Vector<DbusEvent> events = new Vector<DbusEvent>();
    generator.generateEvents(11, 11, 100, 10, events);

    // Add events to the EventBuffer
    DbusEventAppender appender = new DbusEventAppender(events,dbusBuf,null);
    appender.run(); // running in the same thread

    LOG.info("Head:" + dbusBuf.getHead() + ",Tail:" +dbusBuf.getTail());
    assertEquals("Head Check",0,dbusBuf.getHead());
    assertEquals("Tail Check",903,dbusBuf.getTail());

    // Remove the first event
    DbusEventIterator itr = dbusBuf.acquireIterator("dummy");
    assertTrue(itr.hasNext());
    DbusEvent event = itr.next();
    assertTrue(event.isValid());
    itr.remove();
    LOG.info("Head:" + parser.toString(dbusBuf.getHead()) + ",Tail:" + parser.toString(dbusBuf.getTail()));
    assertEquals("Head Check",61,dbusBuf.getHead());
    assertEquals("Tail Check",903,dbusBuf.getTail());

    for (DbusEvent e : events)
    {
      assertTrue("invalid event", e.isValid());
    }

    // set up the ReadChannel with 2 events
    ByteArrayOutputStream oStream = new ByteArrayOutputStream();
    WritableByteChannel oChannel = Channels.newChannel(oStream);
    for (int i = 0; i < 2; ++i)
    {
      ((DbusEventInternalReadable)events.get(i)).writeTo(oChannel,Encoding.BINARY);
    }
    byte[] writeBytes = oStream.toByteArray();
    ByteArrayInputStream iStream = new ByteArrayInputStream(writeBytes);
    final ReadableByteChannel rChannel = Channels.newChannel(iStream);

    // Create a Thread to call readEvents on the channel
    Runnable writer = new Runnable() {
      @Override
      public void run()
      {
        try
        {
          dbusBuf.readEvents(rChannel);
        } catch (InvalidEventException ie) {
          ie.printStackTrace();
          throw new RuntimeException(ie);
        }
      }
    };

    Thread writerThread = new Thread(writer);
    writerThread.start();

    //Check if the thread is alive (blocked) and head/tail is not overlapped
    trySleep(1000);
    assertTrue(writerThread.isAlive());
    LOG.info("Head:" + parser.toString(dbusBuf.getHead()) + ",Tail:" + parser.toString(dbusBuf.getTail()));
    assertEquals("Head Check",61,dbusBuf.getHead());
    assertEquals("Tail Check",2048,dbusBuf.getTail()); //GenId set here but tail is not yet overlapped

    //Read the next event to unblock the writer
    event = itr.next();
    assertTrue(event.isValid());
    itr.remove();
    try
    {
      writerThread.join(1000);
    } catch (InterruptedException ie) {
      ie.printStackTrace();
    }
    LOG.info("Head:" + parser.toString(dbusBuf.getHead()) + ",Tail:" + parser.toString(dbusBuf.getTail()));

    assertFalse(writerThread.isAlive());
    assertEquals("Head Check",132,dbusBuf.getHead());
    assertEquals("Tail Check",2119,dbusBuf.getTail());

    while (itr.hasNext())
    {
      assertTrue(itr.next().isValid(true));
      itr.remove();
    }
    LOG.info("Head:" + parser.toString(dbusBuf.getHead()) + ",Tail:" + parser.toString(dbusBuf.getTail()));
    assertEquals("Head Check",dbusBuf.getHead(),dbusBuf.getTail());
  }

  @Test
  /*
   * ReadBuffer Size is bigger than the overall EVB size.
   * A large read happens to EVB which is bigger than its size.
   */
  public void testBigReadEventBuffer()
    throws Exception
  {
    final Logger log = Logger.getLogger("TestDbusEventBuffer.testBigReadEventBuffer");
    //log.setLevel(Level.INFO);
    log.info("starting");

    final DbusEventBuffer dbusBuf = new DbusEventBuffer(
      getConfig(4000,4000,100,500,AllocationPolicy.HEAP_MEMORY,
                QueuePolicy.BLOCK_ON_WRITE, AssertLevel.ALL));

    final DbusEventBuffer dbusBuf2 = new DbusEventBuffer(
      getConfig(1000,1000,100,3000,AllocationPolicy.HEAP_MEMORY,
                QueuePolicy.BLOCK_ON_WRITE, AssertLevel.NONE));
    //dbusBuf2.getLog().setLevel(Level.DEBUG);

    BufferPositionParser parser = dbusBuf.getBufferPositionParser();
    final BufferPositionParser parser2 = dbusBuf2.getBufferPositionParser();

    DbusEventGenerator generator = new DbusEventGenerator();
    Vector<DbusEvent> events = new Vector<DbusEvent>();
    generator.generateEvents(24, 24, 100, 10, events);
    log.info("Num Events :" + events.size());

    // Add events to the EventBuffer
    DbusEventAppender appender = new DbusEventAppender(events, dbusBuf, null);
    appender.run();

    final AtomicBoolean stopReader = new AtomicBoolean(false);
    log.info("dbusBuf : Head:" + parser.toString(dbusBuf.getHead()) +
             ",Tail:" + parser.toString(dbusBuf.getTail()));

    class EvbReader implements Runnable
    {
      private int _count = 0;
      public EvbReader()
      {
        _count = 0;
      }

      public int getCount()
      {
        return _count;
      }

      @Override
      public void run()
      {
        try
        {
          Thread.sleep(5*1000);
        }
        catch (InterruptedException ie)
        {
        }
        DbusEventBuffer.DbusEventIterator itr = dbusBuf2.acquireIterator("dummy");
        log.info("Reader iterator:" + itr);
        while (!stopReader.get() || itr.hasNext())
        {
          while (itr.hasNext())
          {
            itr.next();
            itr.remove();
            _count++;
          }
          itr.await(100, TimeUnit.MILLISECONDS);
        }
        log.info("Reader Thread: dbusBuf2 : Head:" + parser2.toString(dbusBuf2.getHead()) +
                 ",Tail:" + parser2.toString(dbusBuf2.getTail()));
        ByteBuffer[] buf = dbusBuf2.getBuffer();
        log.info("Reader Thread : dbusBuf2 : Buffer :" + buf[0]);
        log.info("Count is :" + _count);
        log.info("Reader iterator:" + itr);
      }
    };

    EvbReader reader = new EvbReader();
    Thread t = new Thread(reader, "BigReadEventReader");
    ByteBuffer[] buf = dbusBuf.getBuffer();
    byte[] b = new byte[(int)dbusBuf.getTail()];
    buf[0].position(0);
    buf[0].get(b);
    ReadableByteChannel rChannel = Channels.newChannel(new ByteArrayInputStream(b));
    t.start();
    dbusBuf2.readEvents(rChannel);
    stopReader.set(true);
    t.join(20000);
    Assert.assertTrue(!t.isAlive());

    DbusEventBuffer.DbusEventIterator itr2 = dbusBuf2.acquireIterator("dummy");
    int count = 0;
    while (itr2.hasNext())
    {
      itr2.next();
      itr2.remove();
      count++;
    }
    log.info("Total Count :" + (count + reader.getCount()));
    log.info("Head :" + dbusBuf2.getHead() + ", Tail :" + dbusBuf2.getTail());
    assertEquals("Total Count", 26,  (count + reader.getCount()));
    assertEquals("Head == Tail", dbusBuf2.getHead(), dbusBuf2.getTail());
    assertEquals("Head Check:", 2890, dbusBuf2.getHead());
    log.info("done");
  }

  /**
   * TestCase to recreate the bug (DDSDBUS-387) where SCNIndex.head and EVB.Head do not match.
   */
  @Test
  public void testHeadDrift()
      throws Exception
  {
    //DbusEventBuffer.LOG.setLevel(Level.DEBUG);

    DbusEventBuffer dbusBuf =
        new DbusEventBuffer(getConfig(10000, 10000, 320, 500, AllocationPolicy.HEAP_MEMORY,
                                      QueuePolicy.OVERWRITE_ON_WRITE, AssertLevel.ALL));

    DbusEventGenerator generator = new DbusEventGenerator();
    Vector<DbusEvent> events = new Vector<DbusEvent>();
    generator.generateEvents(215, 5, 100, 10, events);

    // Add events to the EventBuffer
    DbusEventAppender appender = new DbusEventAppender(events,dbusBuf,null);
    appender.run();

    LOG.info("Dbus Event Buffer is :" + dbusBuf);
    LOG.info("SCNIndex is :" + dbusBuf.getScnIndex());

    assertEquals("ScnIndex Head Location", 256, dbusBuf.getScnIndex().getHead());
    assertEquals("ScnIndex Tail Location", 256, dbusBuf.getScnIndex().getTail());
    assertEquals("EVB Head Location", 8381, dbusBuf.getHead());
    long oldEVBTail = 40733;
    assertEquals("EVB Tail Location", oldEVBTail, dbusBuf.getTail());

    dbusBuf.getScnIndex().printVerboseString(LOG, Level.DEBUG);

    long lastScn = events.get(events.size() - 1).sequence();
    generator = new DbusEventGenerator(lastScn + 1);
    events = new Vector<DbusEvent>();
    generator.generateEvents(3, 3, 80, 10, events);
    appender = new DbusEventAppender(events,dbusBuf,null);
    appender.run();

    // Ensure SCNINdex tail did not move.
    assertEquals("ScnIndex Head Location", 256, dbusBuf.getScnIndex().getHead());
    assertEquals("ScnIndex Tail Location", 256, dbusBuf.getScnIndex().getTail());
    assertEquals("EVB Head Location", 8381, dbusBuf.getHead());
    long newEVBTail = 41068;
    assertEquals("EVB Tail Location", newEVBTail, dbusBuf.getTail());
    //Make sure the EVB Tail did move. Old EVB Tail belongs to SCNINdex blockNumber 15 and new EVB tail to blockNumber 16
    assertEquals("Old EVB Tail's index block", 15, dbusBuf.getScnIndex().getBlockNumber(oldEVBTail) );
    assertEquals("New EVB Tail's index block", 16, dbusBuf.getScnIndex().getBlockNumber(newEVBTail) );
  }

  /**
   * Test to reproduce DDSDBUS-388.
   */
  @Test
  public void testReadEventsAssertSpanError()
      throws Exception
  {
    final Logger log = Logger.getLogger("TestDbusEventBuffer.testReadEventsAssertSpanError");
    //log.setLevel(Level.INFO);
    log.info("start");

    DbusEventBuffer dbusBuf =
        new DbusEventBuffer(getConfig(10000, 10000, 320, 5000, AllocationPolicy.HEAP_MEMORY,
                            QueuePolicy.OVERWRITE_ON_WRITE, AssertLevel.ALL));
    //dbusBuf.getLog().setLevel(Level.DEBUG);

    DbusEventGenerator generator = new DbusEventGenerator();
    Vector<DbusEvent> events = new Vector<DbusEvent>();
    //event size:  10 + 61 = 71
    //window size:  5 * 71 + 61 = 416
    //layout: [61 + 23 * 416 + 5 * 71 = 9984 ] <WrapAround> [ 61 + 22 * 416 + 2 * 71 + 61 = 9416 ]
    //      [ H=Gen:0,Ofs:0,    T=Gen:0,Ofs:9984, L=10000 ]
    //      [ H=Gen:0,Ofs:9629, T=Gen:1,Ofs:9416, L=9984 ]
    generator.generateEvents(232, 5, 100, 10, events);

    // Add events to the EventBuffer
    DbusEventAppender appender = new DbusEventAppender(events,dbusBuf,null);
    appender.run();

    log.info("Dbus Event Buffer is :" + dbusBuf);
    log.info("SCNIndex is :" + dbusBuf.getScnIndex());

    final BufferPosition headPos = new BufferPosition(dbusBuf.getHead(),
                                                      dbusBuf.getBufferPositionParser(),
                                                      dbusBuf.getBuffer());
    final BufferPosition tailPos = new BufferPosition(dbusBuf.getTail(),
                                                      dbusBuf.getBufferPositionParser(),
                                                      dbusBuf.getBuffer());
    Assert.assertEquals(0, headPos.bufferGenId());
    Assert.assertEquals(0, headPos.bufferIndex());
    Assert.assertEquals(9629, headPos.bufferOffset());
    Assert.assertEquals(1, tailPos.bufferGenId());
    Assert.assertEquals(0, tailPos.bufferIndex());
    Assert.assertEquals(9416, tailPos.bufferOffset());
    Assert.assertEquals(9984, dbusBuf.getBuffer()[0].limit());
    dbusBuf.getScnIndex().assertHeadPosition(dbusBuf.getHead());

    final BufferPosition lastWinStart = new BufferPosition(61 + 22 * 416,
                                                           dbusBuf.getBufferPositionParser(),
                                                           dbusBuf.getBuffer());
    dbusBuf.getScnIndex().assertLastWrittenPos(lastWinStart);

    long lastScn = events.get(events.size() - 1).sequence();
    generator = new DbusEventGenerator(lastScn - 1);
    events.clear();
    //event size: 10 + 61 = 71
    generator.generateEvents(2, 2, 75, 10, events);
    //intended layout: append 2 * 71  = 142 bytes
    //      [ H=Gen:0,Ofs:9629, T=Gen:1,Ofs:9558, L=9984 ]

    lastScn = events.get(events.size() - 1).sequence();
    generator = new DbusEventGenerator(lastScn - 1 );
    //event size: 500 + 61 = 561
    //partial window size: 561  = 622
    generator.generateEvents(1, 1, 800, 500, events);
    //intended layout: append 561 bytes;
    //      [ H=Gen:1,Ofs:893, T=Gen:2,Ofs:561, L=9558 ]
    // head moves to first window after 561: 61 + 2 * 416 = 893 > 561

    //Set up the ReadChannel with new events
    ByteArrayOutputStream oStream = new ByteArrayOutputStream();
    WritableByteChannel oChannel = Channels.newChannel(oStream);

    for (DbusEvent e : events)
    {
      assertTrue("invalid event", e.isValid());
      ((DbusEventInternalReadable)e).writeTo(oChannel,Encoding.BINARY);
      log.debug("event size is: " + e.size());
    }

    byte[] writeBytes = oStream.toByteArray();
    ByteArrayInputStream iStream = new ByteArrayInputStream(writeBytes);
    final ReadableByteChannel rChannel = Channels.newChannel(iStream);

    dbusBuf.readEvents(rChannel);

    log.info("Dbus Event Buffer is :" + dbusBuf);
    log.info("SCNIndex is :" + dbusBuf.getScnIndex());
    //expected layout:
    //      [ H=Gen:1,Ofs:893, T=Gen:2,Ofs:561, L=9558 ]

    headPos.setPosition(dbusBuf.getHead());
    Assert.assertEquals(1, headPos.bufferGenId());
    Assert.assertEquals(0, headPos.bufferIndex());
    Assert.assertEquals(893, headPos.bufferOffset());
    tailPos.setPosition(dbusBuf.getTail());
    Assert.assertEquals(2, tailPos.bufferGenId());
    Assert.assertEquals(0, tailPos.bufferIndex());
    Assert.assertEquals(561, tailPos.bufferOffset());
    Assert.assertEquals(9558, dbusBuf.getBuffer()[0].limit());

    dbusBuf.getBufferPositionParser().assertSpan(dbusBuf.getHead(), dbusBuf.getTail(), true);
    dbusBuf.getScnIndex().assertHeadPosition(dbusBuf.getHead());
    //the second write did not add EOP so the SCN index tail should not have changed
    dbusBuf.getScnIndex().assertLastWrittenPos(lastWinStart);
  }

  /*
   * TestCase to recreate the BufferOverFlowException issue tracked in DDS-793
   */
  @Test
  public void testBufferOverFlow() throws Exception
  {
    //DbusEventBuffer.LOG.setLevel(Level.DEBUG);
    final Logger log = Logger.getLogger("TestDbusEventBuffer.testBufferOverflow");
    //log.setLevel(Level.INFO);
    log.info("starting");

    DbusEventBuffer dbusBuf =
        new DbusEventBuffer(getConfig(1000, 1000, 100, 500, AllocationPolicy.HEAP_MEMORY,
                                      QueuePolicy.BLOCK_ON_WRITE, AssertLevel.ALL));

    final DbusEventBuffer dbusBuf2 =
        new DbusEventBuffer(getConfig(2000, 2000, 100, 1000, AllocationPolicy.HEAP_MEMORY,
                            QueuePolicy.BLOCK_ON_WRITE, AssertLevel.NONE));

    BufferPositionParser parser = dbusBuf.getBufferPositionParser();
    final BufferPositionParser parser2 = dbusBuf2.getBufferPositionParser();

    DbusEventGenerator generator = new DbusEventGenerator();
    Vector<DbusEvent> events = new Vector<DbusEvent>();
    generator.generateEvents(12, 12, 100, 10, events);

    log.info("generate sample events to the EventBuffer");
    DbusEventAppender appender = new DbusEventAppender(events, dbusBuf, null);
    appender.run();

    log.info("dbusBuf : Head:" + parser.toString(dbusBuf.getHead()) + ",Tail:" + parser.toString(dbusBuf.getTail()));
    ByteBuffer[] buf = dbusBuf.getBuffer();
    byte[] b = new byte[(int)dbusBuf.getTail()];
    buf[0].position(0);
    buf[0].get(b);

    log.info("copy data to the destination buffer: 1");
    ReadableByteChannel rChannel = Channels.newChannel(new ByteArrayInputStream(b));

    dbusBuf2.readEvents(rChannel);
    log.info("dbusBuf2 : Head:" + parser2.toString(dbusBuf2.getHead()) + ",Tail:" + parser2.toString(dbusBuf2.getTail()));
    rChannel.close();

    log.info("copy data to the destination buffer: 2");
    rChannel = Channels.newChannel(new ByteArrayInputStream(b));
    dbusBuf2.readEvents(rChannel);
    log.info("dbusBuf2 : Head:" + parser2.toString(dbusBuf2.getHead()) + ",Tail:" + parser2.toString(dbusBuf2.getTail()));
    log.info("Buffer Size is :" + dbusBuf2.getBuffer().length);
    rChannel.close();

    log.info("process data in destination buffer: 1");
    DbusEventBuffer.DbusEventIterator itr = dbusBuf2.acquireIterator("dummy1");

    for (int i = 0 ; i < 15; i++)
    {
      itr.next();
      itr.remove();
    }
    itr.close();
    log.info("dbusBuf2 : Head:" + parser2.toString(dbusBuf2.getHead()) + ",Tail:" + parser2.toString(dbusBuf2.getTail()));

    log.info("copy data to the destination buffer: 3");
    rChannel = Channels.newChannel(new ByteArrayInputStream(b));
    dbusBuf2.readEvents(rChannel);
    ByteBuffer[] buf2 = dbusBuf2.getBuffer();
    log.info("dbusBuf2 : Head:" + parser2.toString(dbusBuf2.getHead()) + ",Tail:" + parser2.toString(dbusBuf2.getTail()));
    log.info("dbusBuf2 : Buffer :" + buf2[0]);
    rChannel.close();

    log.info("process data in destination buffer: 2");
    itr = dbusBuf2.acquireIterator("dummy2");
    for (int i = 0 ; i < 15; i++)
    {
      itr.next();
      itr.remove();
    }
    itr.close();
    log.info("dbusBuf2 : Head:" + parser2.toString(dbusBuf2.getHead()) +
             ",Tail:" + parser2.toString(dbusBuf2.getTail()));
    log.info("dbusBuf2 : Buffer :" + buf2[0]);

    log.info("generate more sample events to the EventBuffer");
    dbusBuf = new DbusEventBuffer(
    getConfig(2000,2000,100,500,AllocationPolicy.HEAP_MEMORY,
              QueuePolicy.BLOCK_ON_WRITE, AssertLevel.ALL));
    events = new Vector<DbusEvent>();
    generator.generateEvents(8, 9, 150, 52, events);
    log.info("Events Size is :" + events.get(0).size());
    appender = new DbusEventAppender(events,dbusBuf,null);
    appender.run();

    final AtomicBoolean stopReader = new AtomicBoolean(false);

    Runnable reader = new Runnable()
    {
      @Override
      public void run()
      {
        try
        {
          Thread.sleep(5*1000);
        }
        catch (InterruptedException ie)
        {
        }
        DbusEventBuffer.DbusEventIterator itr =  dbusBuf2.acquireIterator("dummy3");
        log.info("Reader iterator:" + itr);
        while (!stopReader.get() || itr.hasNext())
        {
          while (itr.hasNext())
          {
            itr.next();
            itr.remove();
          }
          itr.await(100, TimeUnit.MILLISECONDS);
        }
        itr.close();
        log.info("Reader Thread: dbusBuf2 : Head:" +
                 parser2.toString(dbusBuf2.getHead()) +
                 ",Tail:" + parser2.toString(dbusBuf2.getTail()));
        ByteBuffer[] buf = dbusBuf2.getBuffer();
        log.info("Reader Tread : dbusBuf2 : Buffer :" + buf[0]);
        log.info("Reader iterator:" + itr);
      }
    };

    log.info("generate sample events to the EventBuffer");
    Thread t = new Thread(reader, "BufferOverflowReader");

    b = new byte[(int)dbusBuf.getTail()];
    buf = dbusBuf.getBuffer();
    buf[0].position(0);
    buf[0].get(b);

    log.info("copy data to the destination buffer: 4");
    log.info("Size is :" + b.length);
    rChannel = Channels.newChannel(new ByteArrayInputStream(b));
    dbusBuf2.readEvents(rChannel);    // <=== Overflow happened at this point
    rChannel.close();

    log.info("dbusBuf2 : Head:" + parser2.toString(dbusBuf2.getHead()) +
             ",Tail:" + parser2.toString(dbusBuf2.getTail()));
    log.info("dbusBuf2 : Buffer :" + buf2[0]);

    log.info("test if the readEvents can allow reader to proceed while it is blocked");
    rChannel = Channels.newChannel(new ByteArrayInputStream(b));
    log.info("start reader thread");
    t.start();
    log.info("copy data to the destination buffer: 5");
    dbusBuf2.readEvents(rChannel);
    rChannel.close();
    log.info("data copied to the destination buffer: 5");
    stopReader.set(true);

    t.join(20000);
    log.info("check if dbusBuf2 is empty");
    Assert.assertTrue(!t.isAlive());
    if (!dbusBuf2.empty())
    {
      log.error("dbusBuf2 not empty: " + dbusBuf2);
    }
    Assert.assertTrue(dbusBuf2.toString(), dbusBuf2.empty());

    log.info("dbusBuf2 : Head:" + parser2.toString(dbusBuf2.getHead()) +
             ",Tail:" + parser2.toString(dbusBuf2.getTail()));
    log.info("dbusBuf2 : Buffer :" + buf2[0]);
    log.info("done");
  }

  @Test
  public void testAppendEvent() throws Exception
  {
    DbusEventBuffer dbuf =
        new DbusEventBuffer(getConfig(10000000, DbusEventBuffer.Config.DEFAULT_INDIVIDUAL_BUFFER_SIZE,
                                      100000, 1000000, AllocationPolicy.HEAP_MEMORY,
                                      QueuePolicy.OVERWRITE_ON_WRITE, AssertLevel.ALL));
    assertTrue(dbuf.getScnIndex().getUpdateOnNext()); // DDSDBUS-1109
    dbuf.startEvents();
    assertTrue(dbuf.appendEvent(new DbusEventKey(key), pPartitionId, lPartitionId, timeStamp,
                                srcId, schemaId, value.getBytes(), false));
  }

  @Test
  public void testAppendEventStats() throws Exception
  {
    DbusEventsStatisticsCollector collector = new DbusEventsStatisticsCollector(1,"appenderStats",true,true,null);
    DbusEventBuffer dbuf = new DbusEventBuffer(getConfig(10000000,
                                                         DbusEventBuffer.Config.DEFAULT_INDIVIDUAL_BUFFER_SIZE,
                                                         100000, 1000000, AllocationPolicy.HEAP_MEMORY,
                                                         QueuePolicy.OVERWRITE_ON_WRITE, AssertLevel.ALL));

    assertTrue(dbuf.getScnIndex().getUpdateOnNext()); // DDSDBUS-1109
    dbuf.startEvents();
    long now = System.currentTimeMillis();
    final int sleepTime = 100;
    Thread.sleep(sleepTime);
    assertTrue(dbuf.appendEvent(new DbusEventKey(key),
                                pPartitionId,
                                lPartitionId,
                                now * 1000000,
                                srcId,
                                schemaId,
                                value.getBytes(),
                                false));
    dbuf.endEvents(true, 0x100000001L, false, false, collector);
    assertTrue(collector.getTotalStats().getTimeLag() + "," + sleepTime, collector.getTotalStats().getTimeLag() >= sleepTime);
    assertTrue(collector.getTotalStats().getMinTimeLag() + "," + sleepTime,collector.getTotalStats().getMinTimeLag() >= sleepTime);
    assertTrue(collector.getTotalStats().getMaxTimeLag() + "," + sleepTime, collector.getTotalStats().getMaxTimeLag() >= sleepTime);
  }

  @Test
  public void testMultiAppendEvent() throws Exception
  {
    DbusEventBuffer dbuf =
        new DbusEventBuffer(getConfig(10000000, DbusEventBuffer.Config.DEFAULT_INDIVIDUAL_BUFFER_SIZE,
                                      100000, 1000000, AllocationPolicy.HEAP_MEMORY,
                                      QueuePolicy.OVERWRITE_ON_WRITE, AssertLevel.ALL));
    dbuf.startEvents();
    for (int i=0; i < 1000; ++i)
    {
      //LOG.info("Iteration:"+i);
      assertTrue(dbuf.appendEvent(new DbusEventKey(key), pPartitionId, lPartitionId, timeStamp, srcId, schemaId, value.getBytes(), false));
    }
  }


  @Test
  public void testOpCode()
      throws UnsupportedKeyException, InvalidConfigException
  {
    testOpCode("STRING");
    testOpCode("LONG");
  }

  /**
   * Test if the UPSERT/DELETE opcodes are correctly set in the event buffer.
   * The test appends and upsert and a delete event and iterates though them to see if they have been
   * inserted with the correct opcode.
   */
  public void testOpCode(String type)
      throws InvalidConfigException, UnsupportedKeyException
  {

    DbusEventBuffer dbuf = new DbusEventBuffer(getConfig(10000000,
                                                         DbusEventBuffer.Config.DEFAULT_INDIVIDUAL_BUFFER_SIZE,
                                                         100000, 1000000, AllocationPolicy.HEAP_MEMORY,
                                                         QueuePolicy.OVERWRITE_ON_WRITE, AssertLevel.ALL));
    dbuf.start(0);

    //Write the upsert event
    dbuf.startEvents();
    String value = RngUtils.randomString(20);
    DbusOpcode opCode = DbusOpcode.UPSERT;
    DbusEventInfo dbusEventInfo = new DbusEventInfo(opCode,
                                                    1234,    //scn
                                                    (short)2600,   //pconfigId
                                                    (short) 1,  //Partition id
                                                    timeStamp,
                                                    (short)2601, //logical source id
                                                    schemaId,
                                                    value.getBytes(),
                                                    false,
                                                    false);

    DbusEventKey key;
    long longKey = RngUtils.randomLong();
    String stringKey = RngUtils.randomString(10);
    if(type.equals("STRING"))
    {
      key = new DbusEventKey((Object)stringKey);
    }
    else if(type.equals("LONG"))
    {
      key = new DbusEventKey((Object)longKey);
    }
    else
    {
      throw new DatabusRuntimeException("Unknown key type provided for the test");
    }

    dbuf.appendEvent(key, dbusEventInfo, null);
    dbuf.endEvents(1234);


    //Write the delete event
    dbuf.startEvents();
    opCode = DbusOpcode.DELETE;
    dbusEventInfo = new DbusEventInfo(opCode,
                                      1235,    //scn
                                      (short)2600,   //pconfigId
                                      (short) 1,  //Partition id
                                      timeStamp,
                                      (short)2601, //logical source id
                                      schemaId,
                                      value.getBytes(),
                                      false,
                                      false);

    dbuf.appendEvent(key, dbusEventInfo, null);
    dbuf.endEvents(1235);

    //Iterate and verify if the both the upsert and delete events exist
    DbusEventIterator eventIterator = dbuf.acquireIterator("eventIterator");

    //Read EOP
    assert (eventIterator.hasNext());
    eventIterator.next();

    //Read upsert event
    assert (eventIterator.hasNext());
    DbusEventInternalWritable e = eventIterator.next();
    assertEquals(DbusOpcode.UPSERT,e.getOpcode());
    if(type.equals("STRING"))
      assertEquals(stringKey.getBytes(),e.keyBytes());
    else if(type.equals("LONG"))
      assertEquals(longKey,e.key());
    assertEquals(1234,e.sequence());

    //Read EOP
    assert (eventIterator.hasNext());
    eventIterator.next();

    //Read delete event
    assert(eventIterator.hasNext());
    e = eventIterator.next();
    assertEquals(DbusOpcode.DELETE, e.getOpcode());
    if(type.equals("STRING"))
      assertEquals(stringKey.getBytes(),e.keyBytes());
    else if(type.equals("LONG"))
      assertEquals(longKey,e.key());
    assertEquals(1235,e.sequence());


    //Read EOP
    assert (eventIterator.hasNext());
    eventIterator.next();
  }

  /**
   * Add events to a buffer and verify that they are in there in the correct order.
   * Reset the buffer and ensure that there are no events and minScn is back to -1.
   * Add more events to the buffer and ensure that the only entries present are the
   * new ones added.
   */
  @Test
  public void testResetBuffer() throws Exception
  {
    int numEntries = 10;
    HashMap<Long, KeyValue> testDataMap = new HashMap<Long, KeyValue>(50);
    DbusEventBuffer dbuf = new DbusEventBuffer(getConfig(10000000,
                                                         DbusEventBuffer.Config.DEFAULT_INDIVIDUAL_BUFFER_SIZE,
                                                         100000, 1000000, AllocationPolicy.HEAP_MEMORY,
                                                         QueuePolicy.OVERWRITE_ON_WRITE, AssertLevel.ALL));
    int valueLength = 20;
    // Fill 10 entries in buffer starting with 0, and verify they are placed in order.
    dbuf.start(0);
    for (long i=1; i < numEntries; ++i)
    {
      // LOG.info("Iteration:"+i);
      DbusEventKey key = new DbusEventKey(RngUtils.randomLong());
      String value = RngUtils.randomString(valueLength);
      dbuf.startEvents();
      long ts = timeStamp + (i*1000*1000);
      testDataMap.put(i, new KeyValue(key, value));
      assertTrue(dbuf.appendEvent(key, pPartitionId, lPartitionId,ts, srcId, schemaId, value.getBytes(), false));
      dbuf.endEvents(i);
    }

    long minDbusEventBufferScn = dbuf.getMinScn();
    long expectedScn = minDbusEventBufferScn;
    DbusEventIterator eventIterator = dbuf.acquireIterator("eventIterator");
    DbusEventInternalWritable e= null;
    int state = 0; // searching for min scn
    long entryNum = 1;
    while (eventIterator.hasNext())
    {
      e = eventIterator.next();
      if (state == 0 && (e.sequence() >= minDbusEventBufferScn))
      {
        state = 1; // found min scn
      }

      if (state == 1)
      {
        assertEquals(expectedScn, e.sequence());
        long ts = e.timestampInNanos();
        assertEquals(ts, timeStamp + entryNum*1000*1000);
        byte[] eventBytes = new byte[e.valueLength()];
        e.value().get(eventBytes);
        assertEquals(eventBytes, testDataMap.get(entryNum).value.getBytes());
        entryNum++;
        e = eventIterator.next();
        assertTrue(e.isEndOfPeriodMarker());
        assertEquals(expectedScn, e.sequence());
        expectedScn++;
      }
    }
    dbuf.releaseIterator(eventIterator);
    Assert.assertEquals(entryNum, numEntries);

    // Reset the buffer with prevScn = 3, and verify
    final long prevScn = 3;
    dbuf.reset(prevScn);
    assertTrue(dbuf.empty());
    assertEquals(-1, dbuf.getMinScn());
    assertEquals(prevScn, dbuf.getPrevScn());
    testDataMap.clear();
    long currentScn = prevScn;

    // Add more events and make sure that the new events are in the buffer.
    //dbuf.start(0);
    valueLength = 25;
    numEntries = 5;
    // Now add entries to and make sure that they appear.
    for (long i=1; i < numEntries; ++i)
    {
      // LOG.info("Iteration:"+i);
      DbusEventKey key = new DbusEventKey(RngUtils.randomLong());
      String value = RngUtils.randomString(valueLength);
      dbuf.startEvents();
      long ts = timeStamp + (i*1200*1000);
      assertTrue(dbuf.appendEvent(key, pPartitionId, lPartitionId,ts, srcId, schemaId, value.getBytes(), false));
      testDataMap.put(i, new KeyValue(key, value));
      dbuf.endEvents(currentScn + i);
    }

    minDbusEventBufferScn = dbuf.getMinScn();
    assertEquals(minDbusEventBufferScn, prevScn+1);
    expectedScn = minDbusEventBufferScn;
    eventIterator = dbuf.acquireIterator("eventIterator2");
    state = 0; // searching for min scn
    entryNum = 1;
    while (eventIterator.hasNext())
    {
      e = eventIterator.next();
      if (state == 0)
      {
        if (e.sequence() >= minDbusEventBufferScn)
        {
          state = 1; // found min scn
        }
      }

      if (state == 1)
      {
        assertEquals(expectedScn, e.sequence());
        long ts = e.timestampInNanos();
        assertEquals(ts, timeStamp + entryNum*1200*1000);
        byte[] eventBytes = new byte[e.valueLength()];
        e.value().get(eventBytes);
        assertEquals(eventBytes, testDataMap.get(entryNum).value.getBytes());
        entryNum++;
        e = eventIterator.next();
        assertTrue(e.isEndOfPeriodMarker());
        assertEquals(expectedScn, e.sequence());
        expectedScn++;
      }
    }
    dbuf.releaseIterator(eventIterator);
    Assert.assertEquals(entryNum, numEntries);
  }

  @Test
  public void testIteration() throws IOException, InvalidConfigException
  {
    int numEntries = 50000;
    HashMap<Long, KeyValue> testDataMap = new HashMap<Long, KeyValue>(20000);
    DbusEventBuffer dbuf =
        new DbusEventBuffer(getConfig(10000000,
                                      DbusEventBuffer.Config.DEFAULT_INDIVIDUAL_BUFFER_SIZE,
                                      100000, 1000000, AllocationPolicy.HEAP_MEMORY,
                                      QueuePolicy.OVERWRITE_ON_WRITE,
                                      AssertLevel.ALL));

    dbuf.start(0);
    for (long i=1; i < numEntries; ++i)
    {
      //LOG.info("Iteration:"+i);
      DbusEventKey key = new DbusEventKey(RngUtils.randomLong());
      String value = RngUtils.randomString(20);
      dbuf.startEvents();
      long ts = timeStamp + (i*1000*1000);
      assertTrue(dbuf.appendEvent(key, pPartitionId, lPartitionId,ts, srcId, schemaId, value.getBytes(), false));
      testDataMap.put(i, new KeyValue(key, value));
      dbuf.endEvents(i);
    }

    long minDbusEventBufferScn = dbuf.getMinScn();
    long expectedScn = minDbusEventBufferScn;
    DbusEventIterator eventIterator = dbuf.acquireIterator("eventIterator");
    DbusEventInternalWritable e= null;
    int state = 0; // searching for min scn
    while (eventIterator.hasNext())
    {
      e = eventIterator.next();
      if (state == 0 && (e.sequence() >= minDbusEventBufferScn))
      {
        state = 1; // found min scn
      }

      if (state == 1)
      {
        assertEquals(expectedScn, e.sequence());
        long ts = e.timestampInNanos();
        e = eventIterator.next();
        assertTrue(e.isEndOfPeriodMarker());
        assertEquals(expectedScn, e.sequence());
        assertEquals(ts, e.timestampInNanos());
        expectedScn = (expectedScn + 1)%numEntries;
      }
    }

    dbuf.releaseIterator(eventIterator);
  }

  @Test
  public void testCopyIterator() throws Exception
  {
    DbusEventBuffer dbuf =
        new DbusEventBuffer(getConfig(10000000, DbusEventBuffer.Config.DEFAULT_INDIVIDUAL_BUFFER_SIZE,
                                      100000, 1000000, AllocationPolicy.HEAP_MEMORY,
                                      QueuePolicy.OVERWRITE_ON_WRITE, AssertLevel.ALL));
    int numEntries = 50000;
    HashMap<Long, KeyValue> testDataMap = new HashMap<Long, KeyValue>(20000);
    dbuf.start(0);
    for (long i=1; i < numEntries; ++i)
    {
      //LOG.info("Iteration:"+i);
      DbusEventKey key = new DbusEventKey(RngUtils.randomLong());
      String value = RngUtils.randomString(20);
      dbuf.startEvents();
      assertTrue(dbuf.appendEvent(key, pPartitionId, lPartitionId, timeStamp, srcId, schemaId, value.getBytes(), false));
      testDataMap.put(i, new KeyValue(key, value));
      dbuf.endEvents(i);
    }

    final long minDbusEventBufferScn = dbuf.getMinScn();
    long expectedScn = minDbusEventBufferScn;
    DbusEventIterator eventIterator = dbuf.acquireIterator("eventIterator");
    DbusEventInternalWritable e= null;
    int state = 0; // searching for min scn
    DbusEventIterator copyIterator = eventIterator.copy(null, "copyIterator");
    while (eventIterator.hasNext())
    {
      e = eventIterator.next();
      if (state == 0 && (e.sequence() >= minDbusEventBufferScn))
      {
        state = 1; // found min scn
      }

      if (state == 1)
      {
        assertEquals(expectedScn, e.sequence());
        e = eventIterator.next();
        assertTrue(e.isEndOfPeriodMarker());
        assertEquals(expectedScn, e.sequence());
        expectedScn = (expectedScn + 1)%numEntries;
      }
    }

    dbuf.releaseIterator(eventIterator);

    state = 0;
    expectedScn = minDbusEventBufferScn;
    while (copyIterator.hasNext())
    {
      e = copyIterator.next();
      if (state == 0 && (e.sequence() >= minDbusEventBufferScn))
      {
        state = 1; // found min scn
      }

      if (state == 1)
      {
        assertEquals(expectedScn, e.sequence());
        e = copyIterator.next();
        assertTrue(e.isEndOfPeriodMarker());
        assertEquals(expectedScn, e.sequence());
        expectedScn = (expectedScn + 1)%numEntries;
      }
    }

    dbuf.releaseIterator(copyIterator);
  }

  @Test
  public void testWaitForFreeReadSpace() throws Exception
  {
    DbusEventBuffer dbuf =
        new DbusEventBuffer(getConfig(10000000, DbusEventBuffer.Config.DEFAULT_INDIVIDUAL_BUFFER_SIZE,
                                      100000, 1000000, AllocationPolicy.HEAP_MEMORY,
                                      QueuePolicy.OVERWRITE_ON_WRITE, AssertLevel.ALL));

    final class FreeSpaceWaiter implements Runnable
    {
      private final long _freeSpaceToWaitFor;
      private final AtomicBoolean _waiting;
      private final DbusEventBuffer _eventBuffer;

      FreeSpaceWaiter(long freeSpaceToWaitFor, AtomicBoolean waiting, DbusEventBuffer eventBuffer)
      {
        _freeSpaceToWaitFor = freeSpaceToWaitFor;
        _waiting = waiting;
        _eventBuffer = eventBuffer;
      }

      @Override
      public void run()
      {
        _waiting.set(true);
        _eventBuffer.waitForFreeSpaceUninterruptibly(_freeSpaceToWaitFor);
        _waiting.set(false);
      }
    }

    long freeSpace = dbuf.getBufferFreeReadSpace();
    AtomicBoolean waiting = new AtomicBoolean(false);
    FreeSpaceWaiter waiter = new FreeSpaceWaiter(freeSpace+1, waiting, dbuf);
    Thread freeSpaceWaiter = new Thread(waiter);
    freeSpaceWaiter.start();
    try
    {
      Thread.sleep(2000);
    }
    catch (InterruptedException e)
    {
    }
    finally
    {
      assertTrue(waiting.get());
    }

    try
    {
      freeSpaceWaiter.interrupt();
    }
    catch (Exception e)
    {
      LOG.info("Caught expected exception " + e);
    }

    waiting.set(false);
    FreeSpaceWaiter waiter2 = new FreeSpaceWaiter(freeSpace-1, waiting, dbuf);
    Thread freeSpaceWaiter2 = new Thread(waiter2);
    freeSpaceWaiter2.start();
    try
    {
      Thread.sleep(2000);
    }
    catch (InterruptedException e)
    {
    }
    finally
    {
      assertFalse(waiting.get());
    }

    try
    {
      freeSpaceWaiter2.interrupt();
    }
    catch (Exception e)
    {
      LOG.info("Caught expected exception " + e);
    }
  }

  @Test
  public void testGetStreamedEvents() throws Exception
  {
    DbusEventBuffer dbuf =
        new DbusEventBuffer(getConfig(10000000, DbusEventBuffer.Config.DEFAULT_INDIVIDUAL_BUFFER_SIZE,
                                      100000, 1000000, AllocationPolicy.HEAP_MEMORY,
                                      QueuePolicy.OVERWRITE_ON_WRITE, AssertLevel.ALL));
    int numEntries = 50000;
    int eventWindowSize = 20;
    HashMap<Long, KeyValue> testDataMap = new HashMap<Long, KeyValue>(20000);
    dbuf.start(0);

    for (long i=1; i < numEntries; ++i)
    {
      //LOG.info("Iteration:"+i);
      DbusEventKey key = new DbusEventKey(RngUtils.randomLong());
      String value = RngUtils.randomString(20);
      dbuf.startEvents();
      long baseTsForWindow = timeStamp + ((RngUtils.randomPositiveInt()%numEntries)+1)*1000*1000; //ms offset for nanosecond base
      for (int j=0; j < eventWindowSize; ++j)
      {
        long ts = baseTsForWindow + ((RngUtils.randomPositiveInt()%eventWindowSize)+1)*1000* 1000; //ms offset for nanosecond base
        assertTrue(dbuf.appendEvent(key, pPartitionId, lPartitionId, ts, srcId, schemaId,
                                    value.getBytes(), false));
        testDataMap.put(i, new KeyValue(key, value));
        ++i;
      }
      dbuf.endEvents(i);
    }

    for (int i=0; i < 2; ++i)
    {
      //TODO (medium) try out corner cases, more batches, etc.
      int batchFetchSize = 5000;
      Checkpoint cp = new Checkpoint();
      cp.setFlexible();

      ByteArrayOutputStream baos = new ByteArrayOutputStream();

      WritableByteChannel writeChannel = Channels.newChannel(baos);
      //File directory = new File(".");
      //File writeFile = File.createTempFile("test", ".dbus", directory);
      int streamedEvents = 0;

      final DbusEventsStatisticsCollector streamStats =
          new DbusEventsStatisticsCollector(1, "stream", true, false, null);
      //writeChannel = Utils.openChannel(writeFile, true);
      StreamEventsArgs args = new StreamEventsArgs(batchFetchSize).setStatsCollector(streamStats);
      streamedEvents = dbuf.streamEvents(cp, writeChannel, args).getNumEventsStreamed();

      writeChannel.close();
      final byte[] eventBytes = baos.toByteArray();
      Assert.assertTrue(eventBytes.length > 0);
      Assert.assertTrue(streamedEvents > 0);

      final DbusEventsStatisticsCollector inputStats =
          new DbusEventsStatisticsCollector(1, "input", true, false, null);

      ByteArrayInputStream bais = new ByteArrayInputStream(eventBytes);
      ReadableByteChannel readChannel = Channels.newChannel(bais);
      DbusEventBuffer checkDbusEventBuffer =
          new DbusEventBuffer(getConfig(5000000, DbusEventBuffer.
                                        Config.DEFAULT_INDIVIDUAL_BUFFER_SIZE, 100000, 4000,
                                        AllocationPolicy.HEAP_MEMORY,
                                        QueuePolicy.OVERWRITE_ON_WRITE,
                                        AssertLevel.ALL));
      int messageSize = 0;
      int numEvents =0;
      checkDbusEventBuffer.clear();
      numEvents = checkDbusEventBuffer.readEvents(readChannel, inputStats);
      long ts= 0;
      for (DbusEventInternalWritable e : checkDbusEventBuffer)
      {
        ts = Math.max(e.timestampInNanos(),ts);
        messageSize += e.size();
        if (e.isEndOfPeriodMarker())
        {
          //check if of eop has timestamp of most recent data event in the window
          assertEquals(ts, e.timestampInNanos());
          LOG.debug("EOP:"+e.sequence() + " ts=" + e.timestampInNanos());
          ts=0;
        }
        else
        {
          LOG.debug("DAT:"+ e.sequence() + " ts=" + e.timestampInNanos());
        }
      }
      assertEquals("Events Count Check", streamedEvents, numEvents );
      assertTrue(messageSize <= batchFetchSize);
      assertEquals(streamStats.getTotalStats().getNumDataEvents(),
                   inputStats.getTotalStats().getNumDataEvents());
      assertEquals(streamStats.getTotalStats().getNumSysEvents(),
                   inputStats.getTotalStats().getNumSysEvents());

      LOG.debug("BatchFetchSize = " + batchFetchSize + " messagesSize = " + messageSize + " numEvents = " + numEvents);
    }
  }

  @Test
  public void testGetStreamedEventsWithRegression() throws IOException, InvalidEventException, InvalidConfigException, OffsetNotFoundException
  {
    DbusEventBuffer dbuf  = new DbusEventBuffer(getConfig(10000000,
                DbusEventBuffer.Config.DEFAULT_INDIVIDUAL_BUFFER_SIZE,
                100000, 1000000, AllocationPolicy.HEAP_MEMORY,
                QueuePolicy.OVERWRITE_ON_WRITE,
                AssertLevel.ALL));

    int numEntries = 50000;
    int eventWindowSize = 20;
    HashMap<Long, KeyValue> testDataMap = new HashMap<Long, KeyValue>(20000);
    dbuf.start(0);
    for (long i=1; i < numEntries; i+=20)
    {
      //LOG.info("Iteration:"+i);
      DbusEventKey key = new DbusEventKey(RngUtils.randomLong());
      String value = RngUtils.randomString(20);
      dbuf.startEvents();
      for (int j=0; j < eventWindowSize; ++j)
      {
        assertTrue(dbuf.appendEvent(key, pPartitionId, lPartitionId, timeStamp, srcId, schemaId, value.getBytes(), false));
        testDataMap.put(i, new KeyValue(key, value));
      }
      dbuf.endEvents(i);
    }

    long minDbusEventBufferScn = dbuf.getMinScn();

    //TODO (medium) try out corner cases, more batches, etc.
    int batchFetchSize = 5000;
    Checkpoint cp = new Checkpoint();
    cp.setWindowScn(minDbusEventBufferScn);
    cp.setWindowOffset(0);
    cp.setConsumptionMode(DbusClientMode.ONLINE_CONSUMPTION);
    WritableByteChannel writeChannel = null;
    File directory = new File(".");
    File writeFile = File.createTempFile("test", ".dbus", directory);
    try
    {
      writeChannel = Utils.openChannel(writeFile, true);
      StreamEventsArgs args = new StreamEventsArgs(batchFetchSize);
      dbuf.streamEvents(cp, writeChannel, args);
    }
    catch (ScnNotFoundException e)
    {
    }

    writeChannel.close();
    LOG.debug(writeFile.canRead());

    ReadableByteChannel readChannel = Utils.openChannel(writeFile, false);
    DbusEventBuffer checkDbusEventBuffer =
        new DbusEventBuffer(getConfig(50000, DbusEventBuffer.Config.DEFAULT_INDIVIDUAL_BUFFER_SIZE,
                                      100000, 10000, AllocationPolicy.HEAP_MEMORY,
                                      QueuePolicy.OVERWRITE_ON_WRITE,
                                      AssertLevel.ALL));
    int messageSize = 0;
    long lastWindowScn = 0;
    checkDbusEventBuffer.clear();
    checkDbusEventBuffer.readEvents(readChannel);
    LOG.debug("Reading events");
    DbusEventIterator eventIterator = checkDbusEventBuffer.acquireIterator("check");
    DbusEventInternalWritable e=null;
    while (eventIterator.hasNext())
    {
      e = eventIterator.next();
      //LOG.info(e.scn()+"," + e.windowScn());
      messageSize += e.size();
      lastWindowScn = e.sequence();
    }
    assertTrue(messageSize <= batchFetchSize);
    LOG.debug("Reading events 2");
    // now we regress
    cp.setWindowScn(lastWindowScn-5);
    cp.setWindowOffset(0);
    checkDbusEventBuffer.releaseIterator(eventIterator);
    LOG.debug("Reading events 3");
    writeFile.delete();
    writeFile = File.createTempFile("test", ".dbus", directory);

    try
    {
      writeChannel = Utils.openChannel(writeFile, true);
      StreamEventsArgs args = new StreamEventsArgs(batchFetchSize);
      dbuf.streamEvents(cp, writeChannel, args);
    }
    catch (ScnNotFoundException e1)
    {
      LOG.error("mainDbus threw ScnNotFound exception");
    }
    LOG.debug("mainDbus Read status a = " + dbuf.getReadStatus());
    assertEquals(0, dbuf.getReadStatus());

    LOG.debug("Reading events 4");
    LOG.debug(writeFile.canRead());

    readChannel = Utils.openChannel(writeFile, false);
    checkDbusEventBuffer.clear();
    messageSize = 0;
    lastWindowScn = 0;
    checkDbusEventBuffer.readEvents(readChannel);
    LOG.debug("Reading events 5");

    eventIterator = checkDbusEventBuffer.acquireIterator("eventIterator");
    LOG.debug("Reading events 6");

    while (eventIterator.hasNext())
    {
      e = eventIterator.next();
      //LOG.info(e.scn()+"," + e.windowScn());
      //assertEquals(startScn+messageOffset + messageNum, e.scn());
      messageSize += e.size();
      lastWindowScn = e.sequence();
      //LOG.info("Reading events...");
    }
    assertTrue(messageSize <= batchFetchSize);

    LOG.debug("Reading events 7");
    checkDbusEventBuffer.releaseIterator(eventIterator);
    LOG.debug("mainDbus Read status = " + dbuf.getReadStatus());
    writeFile.delete();
  }

  private boolean runReaderWriterTest(EventBufferTestInput testInput)
  throws Exception
  {
    return runReaderWriterTest(testInput, true);
  }

  // "enableAsserts" is an overstatement; the boolean merely checks the most
  // common assertion failure and, if it's going to fail, bypasses that and
  // all subsequent asserts.  It's still possible for the overall number of
  // (data) events to check out but the number of system events to fail.
  private boolean runReaderWriterTest(EventBufferTestInput testInput, boolean enableAsserts)
  throws Exception
  {
    // Src Event producer
    Vector<DbusEvent> srcTestEvents = new Vector<DbusEvent>();
    // Dest Event consumer
    Vector<DbusEvent> dstTestEvents = new Vector<DbusEvent>();

    // emitter/producer/appender -> DEB -> writer -> pipe -> reader -> DEB -> consumer
    // Test configurations;
    DbusEventsStatisticsCollector emitterStats = new DbusEventsStatisticsCollector(1,"appenderStats",true,true,null);
    DbusEventsStatisticsCollector streamStats = new DbusEventsStatisticsCollector(1,"streamStats",true,true,null);
    DbusEventsStatisticsCollector clientStats = new DbusEventsStatisticsCollector(1,"clientStats",true,true,null);

    srcTestEvents.clear();
    dstTestEvents.clear();
    emitterStats.reset();  // a.k.a. producer, a.k.a. appender
    streamStats.reset();   // a.k.a. writer (== reader from relay buffer)
    clientStats.reset();   // a.k.a. reader (== writer into client buffer)

    assertEquals(0, dstTestEvents.size());

    assertTrue(runConstEventsReaderWriter(srcTestEvents,dstTestEvents,testInput,emitterStats,streamStats,clientStats));

    int numEvents = testInput.getNumEvents();
    // check data!
    assertEquals("numEvents generated", numEvents, srcTestEvents.size());
    if (!enableAsserts && numEvents != dstTestEvents.size())
    {
      LOG.info(testInput.getTestName() + " would fail if asserts were enabled:  numEvents seen by consumer = " +
               dstTestEvents.size() + " < " + numEvents);
      return false;
    }
    assertEquals(testInput.getTestName() + ": numEvents seen by consumer", numEvents, dstTestEvents.size());

    assertEquals(testInput.getTestName() + ": numDataEvents seen by producer",
                 srcTestEvents.size(),
                 emitterStats.getTotalStats().getNumDataEvents());
    assertEquals(testInput.getTestName() + ": numDataEvents seen by writer",
                 dstTestEvents.size(),
                 streamStats.getTotalStats().getNumDataEvents());
    assertEquals(testInput.getTestName() + ": numDataEvents seen by producer",
                 dstTestEvents.size(),
                 clientStats.getTotalStats().getNumDataEvents());
    assertTrue(testInput.getTestName() + ": numSysEvents seen by producer",
               emitterStats.getTotalStats().getNumSysEvents() != 0);
    assertEquals(testInput.getTestName() + ": numSysEvents seen by writer",
                 emitterStats.getTotalStats().getNumSysEvents(),
                 streamStats.getTotalStats().getNumSysEvents());
    assertEquals(testInput.getTestName() + ": numSysEvents seen by reader",
                 streamStats.getTotalStats().getNumSysEvents(),
                 clientStats.getTotalStats().getNumSysEvents());
    // check integrity of each event received
    for (int i=0; i < numEvents; ++i)
    {
      DbusEvent src = srcTestEvents.get(i);
      DbusEvent dst = dstTestEvents.get(i);
      assertEquals(testInput.getTestName() + ": src/dst srcIds of event #" + i, src.getSourceId(), dst.getSourceId());
      assertEquals(testInput.getTestName() + ": src/dst values of event #" + i, src.value(), dst.value());
    }

    return true;
  }

  @Test
  public void testReaderWriterCase1()
  throws Exception
  {
    int numEvents = 5000;
    int payloadSize = 20; //constant
    int[] corruptList = new int[0];
    //trust the defaults! ; sanity
    //tests.add(new EventBufferTestInput());

    //case 1: batchSize smaller than numEvents; windowSize fixed and smaller than batchSize
    //shared buffers are large enough
    //staging buffer is smaller than numEvents;
    //batchSize is fixed:
    EventBufferTestInput testInput =
        new EventBufferTestInput(numEvents, //int numEvents,
                                 numEvents/10,//int windowSize,
                                 payloadSize,//int payloadSize,
                                 numEvents*2,//int sharedBufferSize,		SCALED BY eventSize == 61+20 == 81x
                                 (numEvents*3)/5, //int stagingBufferSize,	SCALED BY eventSize == 61+20 == 81x
                                 numEvents*2,//int producerBufferSize,		SCALED BY eventSize == 61+20 == 81x
                                 numEvents*2,//int individualBufferSize,	SCALED BY eventSize == 61+20 == 81x
                                 numEvents/2,//int batchSize,			SCALED BY eventSize == 61+20 == 81x
                                 numEvents/4,//int indexSize,			SCALED BY eventSize == 61+20 == 81x
                                             // note:  indexSize is quite big:  1/8 as large as DEB, vs. "typical" 1/256
                                 EventCorruptionType.NONE,//EventCorruptionType corruptionType,
                                 corruptList,//int[] corruptIndexList,
                                 QueuePolicy.OVERWRITE_ON_WRITE, //QueuePolicy consQueuePolicy,
                                 QueuePolicy.OVERWRITE_ON_WRITE,//QueuePolicy prodQueuePolicy)
                                 0//int deleteInterval
                                 );
    testInput.setTestName("testReaderWriterCase1");
    runReaderWriterTest(testInput);
  }

  @Test
  public void testReaderWriterCase2()
  throws Exception
  {
    int numEvents = 5000;
    int payloadSize = 20; //constant
    int[] corruptList = new int[0];
    //trust the defaults! ; sanity
    //tests.add(new EventBufferTestInput());

    //case 2: batchSize fixed
    //shared buffers are large enough
    EventBufferTestInput testInput =
        new EventBufferTestInput(numEvents, //int numEvents,
                                 numEvents/5,//int windowSize,
                                 payloadSize,//int payloadSize,
                                 numEvents*2,//int sharedBufferSize,
                                 (numEvents)/10, //int stagingBufferSize,
                                 numEvents*2,//int producerBufferSize,
                                 numEvents*2,//int individualBufferSize,
                                 numEvents/2,//int batchSize,
                                 numEvents/4,//int indexSize,
                                 EventCorruptionType.NONE,//EventCorruptionType corruptionType,
                                 corruptList,//int[] corruptIndexList,
                                 QueuePolicy.OVERWRITE_ON_WRITE, //QueuePolicy consQueuePolicy,
                                 QueuePolicy.OVERWRITE_ON_WRITE,//QueuePolicy prodQueuePolicy),
                                 0//int deleteInterval
                                 );
    testInput.setTestName("testReaderWriterCase2");
    runReaderWriterTest(testInput);
  }

  @Test
  public void testReaderWriterCase3()
  throws Exception
  {
    int numEvents = 5000;
    int payloadSize = 20; //constant
    int[] corruptList = new int[0];
    //trust the defaults! ; sanity
    //tests.add(new EventBufferTestInput());

    //case 3: batchSize fixed
    //shared buffers are large enough
    //staging buffer smaller than window size
    EventBufferTestInput testInput =
        new EventBufferTestInput(numEvents, //int numEvents,
                                 numEvents/5,//int windowSize,
                                 payloadSize,//int payloadSize,
                                 numEvents*2,//int sharedBufferSize,
                                 (numEvents)/10, //int stagingBufferSize,
                                 numEvents*2,//int producerBufferSize,
                                 numEvents*2,//int individualBufferSize,
                                 numEvents/2,//int batchSize,
                                 numEvents/4,//int indexSize,
                                 EventCorruptionType.NONE,//EventCorruptionType corruptionType,
                                 corruptList,//int[] corruptIndexList,
                                 QueuePolicy.OVERWRITE_ON_WRITE, //QueuePolicy consQueuePolicy,
                                 QueuePolicy.OVERWRITE_ON_WRITE,//QueuePolicy prodQueuePolicy)
                                 0
                                 );
    testInput.setTestName("testReaderWriterCase3");
    runReaderWriterTest(testInput);
  }

  @Test
  public void testReaderWriterCase4()
  throws Exception
  {
    int numEvents = 5000;
    int payloadSize = 20; //constant
    int[] corruptList = new int[0];
    //trust the defaults! ; sanity
    //tests.add(new EventBufferTestInput());

    //case 4: batchSize
    //shared buffers are large enough
    //staging buffer large enough
    //batch size < windowSize
    EventBufferTestInput testInput =
        new EventBufferTestInput(numEvents, //int numEvents,
                                 numEvents/5,//int windowSize,
                                 payloadSize,//int payloadSize,
                                 numEvents*2,//int sharedBufferSize,
                                 numEvents*2, //int stagingBufferSize,
                                 numEvents*2,//int producerBufferSize,
                                 numEvents*2,//int individualBufferSize,
                                 numEvents/10,//int batchSize,
                                 numEvents/4,//int indexSize,
                                 EventCorruptionType.NONE,//EventCorruptionType corruptionType,
                                 corruptList,//int[] corruptIndexList,
                                 QueuePolicy.OVERWRITE_ON_WRITE, //QueuePolicy consQueuePolicy,
                                 QueuePolicy.OVERWRITE_ON_WRITE,//QueuePolicy prodQueuePolicy)
                                 0
                                 );
    testInput.setTestName("testReaderWriterCase4");
    runReaderWriterTest(testInput);
  }

  @Test
  public void testReaderWriterCase5()
  throws Exception
  {
    int numEvents = 5000;
    int payloadSize = 20; //constant
    int[] corruptList = new int[0];
    //trust the defaults! ; sanity
    //tests.add(new EventBufferTestInput());

    //case 5:
    //shared buffers are large enough
    //staging buffer < totalNumEvents
    //batch size < windowSize
    EventBufferTestInput testInput =
        new EventBufferTestInput(numEvents, //int numEvents,
                                 numEvents/5,//int windowSize,
                                 payloadSize,//int payloadSize,
                                 numEvents*2,//int sharedBufferSize,
                                 numEvents/2, //int stagingBufferSize,
                                 numEvents*2,//int producerBufferSize,
                                 numEvents*2,//int individualBufferSize,
                                 numEvents/10,//int batchSize,
                                 numEvents/4,//int indexSize,
                                 EventCorruptionType.NONE,//EventCorruptionType corruptionType,
                                 corruptList,//int[] corruptIndexList,
                                 QueuePolicy.OVERWRITE_ON_WRITE, //QueuePolicy consQueuePolicy,
                                 QueuePolicy.OVERWRITE_ON_WRITE,//QueuePolicy prodQueuePolicy)
                                 0);
    testInput.setTestName("testReaderWriterCase5");
    runReaderWriterTest(testInput);
  }

  @Test
  public void testReaderWriterCase6()
  throws Exception
  {
    int numEvents = 5000;
    int payloadSize = 20; //constant
    int[] corruptList = new int[0];
    //trust the defaults! ; sanity
    //tests.add(new EventBufferTestInput());

    //case 6:
    //shared buffers are large enough
    //staging buffer < windowSize
    //batch size < windowSize
    EventBufferTestInput testInput =
        new EventBufferTestInput(numEvents, //int numEvents,
                                 numEvents/5,//int windowSize,
                                 payloadSize,//int payloadSize,
                                 numEvents*2,//int sharedBufferSize,
                                 numEvents/10, //int stagingBufferSize,
                                 numEvents*2,//int producerBufferSize,
                                 numEvents*2,//int individualBufferSize,
                                 numEvents/10,//int batchSize,
                                 numEvents/4,//int indexSize,
                                 EventCorruptionType.NONE,//EventCorruptionType corruptionType,
                                 corruptList,//int[] corruptIndexList,
                                 QueuePolicy.OVERWRITE_ON_WRITE, //QueuePolicy consQueuePolicy,
                                 QueuePolicy.OVERWRITE_ON_WRITE,//QueuePolicy prodQueuePolicy)
                                 0);
    testInput.setTestName("testReaderWriterCase6");
    runReaderWriterTest(testInput);
  }

  @Test
  public void testReaderWriterCase7()
  throws Exception
  {
    int numEvents = 5000;
    int payloadSize = 20; //constant
    int[] corruptList = new int[0];
    //trust the defaults! ; sanity
    //tests.add(new EventBufferTestInput());

    //case 7:
    //batch size > windowSize;
    //window size < stagingBufferSize;
    EventBufferTestInput testInput =
        new EventBufferTestInput(numEvents, //int numEvents,
                                 numEvents/10,//int windowSize,
                                 payloadSize,//int payloadSize,
                                 numEvents*2,//int sharedBufferSize,
                                 numEvents*2, //int stagingBufferSize,
                                 numEvents*2,//int producerBufferSize,
                                 numEvents*2,//int individualBufferSize,
                                 numEvents/5,//int batchSize,
                                 numEvents/4,//int indexSize,
                                 EventCorruptionType.NONE,//EventCorruptionType corruptionType,
                                 corruptList,//int[] corruptIndexList,
                                 QueuePolicy.OVERWRITE_ON_WRITE, //QueuePolicy consQueuePolicy,
                                 QueuePolicy.OVERWRITE_ON_WRITE,//QueuePolicy prodQueuePolicy)
                                 0);
    testInput.setTestName("testReaderWriterCase7");
    runReaderWriterTest(testInput);
  }


  @Test
  public void testReaderWriterCase8()
  throws Exception
  {
    int numEvents = 5000;
    int payloadSize = 20; //constant
    int[] corruptList = new int[0];
    //trust the defaults! ; sanity
    //tests.add(new EventBufferTestInput());

    //case 8:
    //batch size = windowSize;
    //window size < sharedBufferSize;
    EventBufferTestInput testInput =
        new EventBufferTestInput(numEvents, //int numEvents,
                                 numEvents/2,//int windowSize,
                                 payloadSize,//int payloadSize,
                                 numEvents*2,//int sharedBufferSize,
                                 numEvents*2, //int stagingBufferSize,
                                 numEvents*2,//int producerBufferSize,
                                 numEvents*2,//int individualBufferSize,
                                 numEvents/2,//int batchSize,
                                 numEvents/4,//int indexSize,
                                 EventCorruptionType.NONE,//EventCorruptionType corruptionType,
                                 corruptList,//int[] corruptIndexList,
                                 QueuePolicy.OVERWRITE_ON_WRITE, //QueuePolicy consQueuePolicy,
                                 QueuePolicy.OVERWRITE_ON_WRITE,//QueuePolicy prodQueuePolicy)
                                 0);
    testInput.setTestName("testReaderWriterCase8");
    runReaderWriterTest(testInput);
  }

  /**
   * This case tests 60 variations of event-buffer configs (client event-buffer sizes,
   * relay/client ByteBuffer sizes) and transaction-window sizes.
   */
  @Test
  public void testClientIteratorsWithFixedEventSizeAndMultipleConfigs() throws Exception
  {
    final Logger log = Logger.getLogger("ClientIteratorsWithFixedEventSizeAndMultipleConfigs");
    //log.setLevel(Level.INFO);

    final int numEvents = 5000;
    final int payloadSize = 55;  // constant
    final int[] corruptedIndexList = new int[0];

    final int eventSize = 61 + payloadSize; // == 116 (used for logging only; shouldn't hardcode header size!)

    final int[] windowSizeDivisors = {19, 41, 79, 163};
    final int[] clientBufSizeDivisors = {3, 17, 127};  // roughly equal to number of times numEvents will wrap
    final int[] byteBufferCounts = {1, 2, 3, 5, 7};
    // TODO:  could also vary payloadSize, but prefer jitter approach, i.e., do it within runConstEventsReaderWriter()

    final boolean enablePerVariantAsserts = false;  // test all variants and assert only on summary data

    int numVariantsRun = 0;
    int numVariantsFailedButAllowedForNow = 0;  // "soft" failures, i.e., known bug (to be fixed, but can ignore for now)
    int numVariantsFailed = 0;

    // Sole known bad case (DDSDBUS-1814) is when last ByteBuffer of the client DEB is
    // smaller than rest and exactly matches an event size (i.e., either 61 or 116 bytes
    // in this test, but the former can never happen due to the scaling).  IOW, if
    // ((clientBufSizeUnscaled*eventSize) % (byteBufferSizeUnscaled*eventSize)) == eventSize
    // [or, alternatively, if clientBufSizeUnscaled % byteBufferSizeUnscaled == 1], we
    // set allowedKnownBadCase to true.

    for (int windowSizeDivisor : windowSizeDivisors)
    //int windowSizeDivisor = 19;
    {
      int maxWindowSize = numEvents / windowSizeDivisor;
      for (int clientBufSizeDivisor : clientBufSizeDivisors)
      //int clientBufSizeDivisor = 127;
      {
        int clientBufSizeUnscaled = numEvents / clientBufSizeDivisor;
        for (int byteBufferCount : byteBufferCounts)
        //int byteBufferCount = 5;
        {
          int byteBufferSizeUnscaled = clientBufSizeUnscaled / byteBufferCount;
          boolean allowedKnownBadCase = ((clientBufSizeUnscaled % byteBufferSizeUnscaled) == 1);
          log.info("windowSizeDivisor=" + windowSizeDivisor + "; clientBufSizeDivisor=" + clientBufSizeDivisor +
                   "; byteBufferCount=" + byteBufferCount + "; byteBufferSizeUnscaled=" + byteBufferSizeUnscaled +
                   "; allowedKnownBadCase=" + allowedKnownBadCase);
          if (allowedKnownBadCase)
          {
            log.info("skipping known allowed bad case");
            //continue;
          }
          EventBufferTestInput testInput =
              new EventBufferTestInput(numEvents,
                                       maxWindowSize,          // max events/transaction window
                                       payloadSize,
                                       clientBufSizeUnscaled,  // client/consumer DEB size (sharedBufferSize); SCALED x 116
                                       (numEvents*11)/25,      // stagingBufferSize; SCALED x 116
                                       numEvents*2,            // relay DEB size (producerBufferSize); SCALED x 116
                                       byteBufferSizeUnscaled, // individualBufferSize; SCALED x 116x (ideally would specify separately for two DEBs)
                                       numEvents/20,           // size of batch of "source" events? (batchSize); SCALED x 116
                                       numEvents/128,          // indexSize; SCALED x 116 ("typical" = 1/256 of relay DEB size)
                                       EventCorruptionType.NONE,
                                       corruptedIndexList,
                                       QueuePolicy.BLOCK_ON_WRITE,     // consumer DEB policy
                                       QueuePolicy.OVERWRITE_ON_WRITE, // relay DEB policy
                                       5);                     // deleteInterval (insufficient space in client buffer if don't delete as go)
          testInput.setTestName("test_" +
                                windowSizeDivisor + "_" + clientBufSizeDivisor + "_" + byteBufferCount);
          ++numVariantsRun;
          if (runReaderWriterTest(testInput, enablePerVariantAsserts))
          {
            log.info(testInput.getTestName() + " succeeded");
          }
          else if (allowedKnownBadCase)
          {
            ++numVariantsFailedButAllowedForNow;
            log.warn(testInput.getTestName() + " failed to consume all events" +
                     " (known bug when last ByteBuffer == event size; ignoring for now)");
          }
          else
          {
            ++numVariantsFailed;
            log.error(testInput.getTestName() + " failed to consume all events");
          }
          log.info(testInput.getTestName() + ": windowSizeDivisor=" + windowSizeDivisor +
                   ", clientBufSizeDivisor=" + clientBufSizeDivisor + ", byteBufferCount=" + byteBufferCount +
                   ", eventSize=" + eventSize + " =>\n  maxWindowSize=" + maxWindowSize +
                   " events,\n  clientBufSize=" + (clientBufSizeUnscaled*eventSize) +
                   ",\n  byteBufferSize=" + (byteBufferSizeUnscaled*eventSize) +
                   ",\n  indexSize=" + (eventSize*numEvents/128));
        }
      }
    }

    if (numVariantsFailed > 0)
    {
      final String errMsg = "errors: " +
                            numVariantsFailed + " of " + numVariantsRun + " subtests hard-failed (" +
                            numVariantsFailedButAllowedForNow + " soft-failed)\n";
      log.error(errMsg);
      assertEquals(errMsg, 0, numVariantsFailed);
    }
  }

  static protected void checkEvents(Vector<DbusEvent> srcTestEvents, Vector<DbusEvent> dstTestEvents, int numEvents)
  {
    //check data!
    assertEquals("Src Test Events Size", numEvents, srcTestEvents.size());
    assertEquals("Destination Test Events Size", numEvents, dstTestEvents.size());

    //check integrity of each event received
    for (int i=0; i < numEvents; ++i)
    {
      DbusEvent src = srcTestEvents.get(i);
      DbusEvent dst = dstTestEvents.get(i);
      assertEquals(src.getSourceId(), dst.getSourceId());
      assertEquals(src.value(), dst.value());
    }
  }

  @Test
  public void testEventReadBufferInvalidEvents()
  throws Exception
  {
    final Logger log = Logger.getLogger("TestDbusEventBuffer.testEventReadBufferInvalidEvents");
    log.setLevel(Level.INFO);
    log.info("Started testing invalid events\n");
    // Src Event producer
    Vector<DbusEvent> srcTestEvents = new Vector<DbusEvent>();
    // Dest Event consumer
    Vector<DbusEvent> dstTestEvents = new Vector<DbusEvent>();
    // num events
    int numEvents = 100;

    DbusEventsStatisticsCollector emitterStats =
        new DbusEventsStatisticsCollector(1,"appenderStats", true, true, null);
    DbusEventsStatisticsCollector streamStats =
        new DbusEventsStatisticsCollector(1, "streamStats", true, true, null);
    DbusEventsStatisticsCollector clientStats =
        new DbusEventsStatisticsCollector(1, "clientStats", true, true, null);


    int[][] listOfCorruptedIndices = new int[][] { {81}, {12}, {10,65}, {0}, {99} };
    EventCorruptionType[] corruptionType =
        new EventCorruptionType[] { EventCorruptionType.LENGTH,
                                    EventCorruptionType.PAYLOADCRC,
                                    EventCorruptionType.HEADERCRC,
                                    EventCorruptionType.PAYLOAD };
    int totalInvalid=0;
    for (int i=0; i < listOfCorruptedIndices.length; ++i)
    {
      for (int j=0; j < corruptionType.length; ++j)
      {
        log.info("starting test: corruptionType=" + corruptionType[j] + " corruptIndex=" + i);
        EventBufferTestInput invalidEventInput = new EventBufferTestInput();
        invalidEventInput.setNumEvents(numEvents)
                         .setWindowSize(numEvents/5)
                         .setProducerBufferSize(numEvents*2)
                         .setSharedBufferSize(numEvents*2)
                         .setStagingBufferSize(numEvents*2)
                         .setIndexSize(numEvents/10)
                         .setIndividualBufferSize(numEvents*2)
                         .setCorruptionType(corruptionType[j])
                         .setCorruptIndexList(listOfCorruptedIndices[i])
                         .setTestName("testEventReadBufferInvalidEvents");

        srcTestEvents.clear();
        dstTestEvents.clear();
        emitterStats.reset();
        streamStats.reset();
        clientStats.reset();

        boolean result = runConstEventsReaderWriter(srcTestEvents, dstTestEvents,
                                                    invalidEventInput, emitterStats,
                                                    streamStats, clientStats);

        assertTrue(result);
        //check data!
        assertEquals(numEvents, srcTestEvents.size());
        assertEquals(srcTestEvents.size(), emitterStats.getTotalStats().getNumDataEvents());
        totalInvalid += clientStats.getTotalStats().getNumInvalidEvents();
        //expect no data to be received;
        log.info(String.format("Read %d events until an invalid event was discovered with %s corruption \n",
                               dstTestEvents.size(),
                               corruptionType[j]));
        assertTrue(dstTestEvents.size() < numEvents);
      }
      assertTrue(totalInvalid > 0);
    }
  }

  @Test
  public void testStatsMinMaxScn() throws Exception
  {
    //Src Event producer
    Vector<DbusEvent> srcTestEvents = new Vector<DbusEvent>();
    EventBufferTestInput input = new EventBufferTestInput();
    int numEvents = 10000;
    long startScn=1000;
    int numWinScn = 10;

    //set sharedBufferSize to a value much smaller than total size required
    input.setNumEvents(numEvents)
         .setWindowSize(numEvents/numWinScn)
         .setSharedBufferSize(numEvents/5)
         .setStagingBufferSize(numEvents/5)
         .setIndexSize(numEvents/10)
         .setIndividualBufferSize(numEvents/2)
         .setBatchSize(numEvents/5)
         .setProducerBufferSize(numEvents/2)
         .setPayloadSize(100)
         .setDeleteInterval(1)
         .setProdQueuePolicy(QueuePolicy.OVERWRITE_ON_WRITE);
    input.setTestName("testStatsMinMaxScn");
    DbusEventsStatisticsCollector emitterStats = new DbusEventsStatisticsCollector(1,"appenderStats",true,true,null);

    DbusEventGenerator evGen = new DbusEventGenerator(startScn);
    if (evGen.generateEvents(numEvents,input.getWindowSize(),512,input.getPayloadSize(),true,srcTestEvents) <= 0)
    {
      fail();
      return;
    }
    //sleep 10 ms;

    int eventSize = srcTestEvents.get(0).size();
    DbusEventBuffer prodEventBuffer =
        new DbusEventBuffer(getConfig(input.getProducerBufferSize()*eventSize,
                                      input.getIndividualBufferSize()*eventSize,
                                      input.getIndexSize()*eventSize,
                                      input.getStagingBufferSize()*eventSize,
                                      AllocationPolicy.HEAP_MEMORY,
                                      input.getProdQueuePolicy(),
                                      input.getProdBufferAssertLevel()));
    DbusEventAppender eventProducer = new DbusEventAppender(srcTestEvents, prodEventBuffer,emitterStats);
    Thread tEmitter = new Thread(eventProducer);
    tEmitter.start();
    tEmitter.join();

    //sleep 10 ms;
    int msDelay = 10;
    Thread.sleep(msDelay);

    long min = (numWinScn-3)*input.getWindowSize() + startScn;
    long max=numWinScn*input.getWindowSize()+ startScn;
    //note : event generator generates events such that a one second lag exists between the latest event and prev event
    long expectedRange = (max-min)  + input.getWindowSize()-1;
    System.out.printf("Total timespan = %d\n",
        (srcTestEvents.get(numEvents-1).timestampInNanos() - srcTestEvents.get(0).timestampInNanos())/NANOSECONDS);
    System.out.printf("prevScn=%d\n", emitterStats.getTotalStats().getPrevScn());
    System.out.printf("min = %d , max=%d  buf=%d ,%d\n", emitterStats.getTotalStats().getMinScn(),
                      emitterStats.getTotalStats().getMaxScn(), prodEventBuffer.getMinScn(),
                      prodEventBuffer.lastWrittenScn());
    System.out.printf("timespan=%d , timeSinceLastEvent = %d , timeSinceLastAccess %d\n",
                      emitterStats.getTotalStats().getTimeSpan()/MILLISECONDS,
                      emitterStats.getTotalStats().getTimeSinceLastEvent(),
                      emitterStats.getTotalStats().getTimeSinceLastAccess());

    assertEquals(numEvents, srcTestEvents.size());
    assertEquals(numEvents, emitterStats.getTotalStats().getNumDataEvents());
    assertEquals(min, emitterStats.getTotalStats().getMinScn());
    assertEquals(max, emitterStats.getTotalStats().getMaxScn());
    assertEquals(min-input.getWindowSize(), emitterStats.getTotalStats().getPrevScn());
    assertEquals(emitterStats.getTotalStats().getSizeDataEvents()*numEvents, numEvents*eventSize);
    long tsSpanInSec = emitterStats.getTotalStats().getTimeSpan()/MILLISECONDS;
    assertEquals(expectedRange, tsSpanInSec);
    long tsSinceLastEvent = emitterStats.getTotalStats().getTimeSinceLastEvent();
    assertTrue(tsSinceLastEvent >= msDelay);
    assertTrue(emitterStats.getTotalStats().getTimeLag() >= 0);
    assertTrue(emitterStats.getTotalStats().getMinTimeLag() >= 0);
    assertTrue(emitterStats.getTotalStats().getMaxTimeLag() >= 0);

    DbusEventBuffer readEventBuffer =
        new DbusEventBuffer(getConfig(numEvents*2L*eventSize, numEvents*eventSize, (numEvents/10)*eventSize,
                                      numEvents*2*eventSize, AllocationPolicy.HEAP_MEMORY,
                                      input.getProdQueuePolicy(), input.getProdBufferAssertLevel()));

    //check streaming
    Vector<Long> seenScns = new Vector<Long>();
    Checkpoint cp = new Checkpoint();
    cp.setConsumptionMode(DbusClientMode.ONLINE_CONSUMPTION);

    //case: where sinceScn < min , > prevScn ; return the entire buffer
    cp.setWindowScn(min-1);
    cp.setWindowOffset(-1);
    seenScns.clear();
    readEventBuffer.clear();
    streamWriterReader(prodEventBuffer,numEvents*2*eventSize,cp,"scn",readEventBuffer,seenScns);
    int expectedNumWin =(int) (max-min)/input.getWindowSize()+1;
    assertEquals(expectedNumWin, seenScns.size());
    assertEquals(Long.valueOf(min), seenScns.get(0));
    assertEquals(Long.valueOf(max), seenScns.get(expectedNumWin - 1));


    //case : where sinceScn < prevScn ; exception thrown;
    cp.setWindowScn(startScn);
    cp.setWindowOffset(-1);
    seenScns.clear();
    readEventBuffer.clear();
    streamWriterReader(prodEventBuffer,numEvents*2*eventSize,cp,"scn",readEventBuffer,seenScns);
    assertEquals(0, seenScns.size());

    //case: where sinceScn < min , = prevScn ; offset=-1 ,return the entire buffer
    cp.setWindowScn(prodEventBuffer.getPrevScn());
    cp.setWindowOffset(-1);
    seenScns.clear();
    readEventBuffer.clear();
    streamWriterReader(prodEventBuffer,numEvents*2*eventSize,cp,"scn",readEventBuffer,seenScns);
    assertEquals(expectedNumWin, seenScns.size());
    assertEquals(Long.valueOf(min), seenScns.get(0));
    assertEquals(Long.valueOf(max), seenScns.get(expectedNumWin-1));

    //case: where sinceScn < min , = prevScn ; offset=0 ,return nothing
    cp.setWindowScn(prodEventBuffer.getPrevScn());
    cp.setWindowOffset(0);
    seenScns.clear();
    readEventBuffer.clear();
    assertFalse(streamWriterReader(prodEventBuffer,numEvents*2*eventSize,cp,"scn",readEventBuffer,seenScns));
    assertEquals(0, seenScns.size());
  }

  protected boolean streamWriterReader(DbusEventBuffer prodBuffer,int batchSize,Checkpoint cp,String filename,DbusEventBuffer readBuffer,Vector<Long> seenScn)
  {
    return streamWriterReader(prodBuffer,batchSize,cp,filename,readBuffer,seenScn,null);
  }


  protected boolean streamWriterReader(DbusEventBuffer prodBuffer,int batchSize,Checkpoint cp,String filename,DbusEventBuffer readBuffer,Vector<Long> seenScn,DbusEventsStatisticsCollector stats)
  {
    try
    {
      WritableByteChannel writeChannel = null;
      File directory = new File(".");
      File writeFile = File.createTempFile(filename, ".dbus", directory);
      int numStreamedEvents = 0;
      try
      {
        writeChannel = Utils.openChannel(writeFile, true);
        StreamEventsArgs args = new StreamEventsArgs(batchSize);

        numStreamedEvents = prodBuffer.streamEvents(cp, writeChannel, args).getNumEventsStreamed();

      }
      catch (ScnNotFoundException e)
      {
        e.printStackTrace();
        return false;
      }

      writeChannel.close();

      ReadableByteChannel readChannel = Utils.openChannel(writeFile, false);
      int readEvents = readBuffer.readEvents(readChannel,null,stats);

      writeFile.delete();
      //System.out.printf("Wrote %d events, read %d events\n",numStreamedEvents,readEvents);
      for (DbusEvent e: readBuffer)
      {
        if (e.isEndOfPeriodMarker())
        {
          seenScn.add(e.sequence());
        }
      }
      return (readEvents==numStreamedEvents);
    }
    catch (Exception e)
    {
      e.printStackTrace();
      return false;
    }
  }


  @Test
  public void testStreamScn() throws Exception
  {
    //Src Event producer
    Vector<DbusEvent> srcTestEvents = new Vector<DbusEvent>();
    EventBufferTestInput input = new EventBufferTestInput();
    int numEvents = 500;
    int numScns = 10;
    int windowSize = numEvents/numScns;
    input.setNumEvents(numEvents)
         .setWindowSize(windowSize)
         .setSharedBufferSize(numEvents*2)
         .setStagingBufferSize(numEvents*2)
         .setIndexSize(numEvents/10)
         .setIndividualBufferSize(numEvents*2)
         .setBatchSize(numEvents*2)
         .setProducerBufferSize(numEvents*2)
         .setPayloadSize(100)
         .setDeleteInterval(1)
         .setProdQueuePolicy(QueuePolicy.OVERWRITE_ON_WRITE);
    input.setTestName("testStreamScn");
    DbusEventsStatisticsCollector emitterStats = new DbusEventsStatisticsCollector(1,"appenderStats",true,true,null);
    DbusEventsStatisticsCollector clientStats = new DbusEventsStatisticsCollector(1,"clientStats",true,true,null);

    DbusEventGenerator evGen = new DbusEventGenerator();
    assertTrue(evGen.generateEvents(numEvents,input.getWindowSize(),512,input.getPayloadSize(),true,srcTestEvents) > 0);
    int eventSize = srcTestEvents.get(0).size();
    DbusEventBuffer prodEventBuffer =
        new DbusEventBuffer(getConfig(input.getProducerBufferSize()*eventSize,
                                      input.getIndividualBufferSize()*eventSize,
                                      input.getIndexSize()*eventSize,
                                      input.getStagingBufferSize()*eventSize,
                                      AllocationPolicy.HEAP_MEMORY, input.getProdQueuePolicy(),
                                      input.getProdBufferAssertLevel()));
    DbusEventBuffer readEventBuffer =
        new DbusEventBuffer(getConfig(input.getProducerBufferSize()*eventSize,
                                      input.getIndividualBufferSize()*eventSize, input.getIndexSize()*eventSize,
                                      input.getStagingBufferSize()*eventSize,
                                      AllocationPolicy.HEAP_MEMORY, input.getProdQueuePolicy(),
                                      input.getProdBufferAssertLevel()));
    DbusEventAppender eventProducer = new DbusEventAppender(srcTestEvents, prodEventBuffer,emitterStats);

    Vector<Long> seenScns = new Vector<Long>();
    Checkpoint cp = new Checkpoint();
    cp.setConsumptionMode(DbusClientMode.ONLINE_CONSUMPTION);
    boolean origEmptyValue  = prodEventBuffer.empty();

    //empty buffer; prevScn=-1 , minScn=-1 ; so no Scn not found exception
    cp.setWindowScn(2L);
    cp.setWindowOffset(-1);
    seenScns.clear();
    readEventBuffer.clear();
    boolean res = streamWriterReader(prodEventBuffer,input.getBatchSize()*eventSize,cp,"scn",readEventBuffer,seenScns);
    assertEquals(-1L, prodEventBuffer.getPrevScn());
    assertTrue(res);
    assertEquals(0, seenScns.size());

    //partial buffer; with no complete window written;  prevScn > sinceScn , minScn=-1 ; Scn not found exception thrown;
    cp.setWindowScn(2L);
    cp.setWindowOffset(-1);
    prodEventBuffer.setPrevScn(20L);
    prodEventBuffer.setEmpty(false);
    seenScns.clear();
    readEventBuffer.clear();
    assertEquals(-1L, prodEventBuffer.getMinScn());
    res = streamWriterReader(prodEventBuffer,input.getBatchSize()*eventSize,cp,"scn",readEventBuffer,seenScns);
    assertFalse(res);
    assertEquals(0, seenScns.size());
    //restore
    prodEventBuffer.setPrevScn(-1L);
    prodEventBuffer.setEmpty(origEmptyValue);

    //partial buffer; with no complete window written;  sinceScn >= prevScn , minScn=-1 ; no exception;
    cp.setWindowScn(45L);
    cp.setWindowOffset(-1);
    prodEventBuffer.setPrevScn(20L);
    prodEventBuffer.setEmpty(false);
    seenScns.clear();
    readEventBuffer.clear();
    res = streamWriterReader(prodEventBuffer,input.getBatchSize()*eventSize,cp,"scn",readEventBuffer,seenScns);
    assertTrue(res);
    assertEquals(0, seenScns.size());
    //restore
    prodEventBuffer.setPrevScn(-1L);
    prodEventBuffer.setEmpty(origEmptyValue);

    Thread tEmitter = new Thread(eventProducer);
    tEmitter.start();
    tEmitter.join();

    long minScn = emitterStats.getTotalStats().getMinScn();
    long maxScn = emitterStats.getTotalStats().getMaxScn();
    long prevScn = emitterStats.getTotalStats().getPrevScn();

    System.out.printf("minScn=%d,maxScn=%d,prevScn=%d,range=%d\n",minScn,maxScn,prevScn,emitterStats.getTotalStats().getTimeSpan());
    assertEquals(numEvents - 1, emitterStats.getTotalStats().getTimeSpan()/MILLISECONDS);
    assertEquals(prodEventBuffer.getTimestampOfFirstEvent(), emitterStats.getTotalStats().getTimestampMinScnEvent());

    //stream with scn < max; expect last window; not last 2
    cp.setWindowScn(maxScn-1);
    cp.setWindowOffset(-1);
    seenScns.clear();
    readEventBuffer.clear();
    streamWriterReader(prodEventBuffer,input.getBatchSize()*eventSize,cp,"scn",readEventBuffer,seenScns);
    assertEquals(1, seenScns.size());
    assertEquals(Long.valueOf(maxScn), seenScns.get(0));

    //set windowScn to maxScn; get >= behaviour here ; get the last window
    cp.setWindowScn(maxScn);
    cp.setWindowOffset(0);
    seenScns.clear();
    readEventBuffer.clear();
    streamWriterReader(prodEventBuffer,input.getBatchSize()*eventSize,cp,"scn",readEventBuffer,seenScns);
    assertEquals(1, seenScns.size());
    assertEquals(Long.valueOf(maxScn), seenScns.get(0));

    //stream with scn >= max ; get a window higher than max - expect nothing
    cp.setWindowScn(maxScn);
    cp.setWindowOffset(-1);
    readEventBuffer.clear();
    seenScns.clear();
    streamWriterReader(prodEventBuffer,input.getBatchSize()*eventSize,cp,"scn",readEventBuffer,seenScns);
    assertEquals(0, seenScns.size());

    //stream with scn > max
    cp.setWindowScn(maxScn+1);
    cp.setWindowOffset(-1);
    readEventBuffer.clear();
    seenScns.clear();
    streamWriterReader(prodEventBuffer,input.getBatchSize()*eventSize,cp,"scn",readEventBuffer,seenScns);
    assertEquals(0, seenScns.size());

    //stream with scn >= min
    cp.setWindowScn(minScn);
    cp.setWindowOffset(-1);
    readEventBuffer.clear();
    seenScns.clear();
    streamWriterReader(prodEventBuffer,input.getBatchSize()*eventSize,cp,"scn",readEventBuffer,seenScns);
    assertEquals(numScns-1, seenScns.size());
    assertTrue(seenScns.get(0)!=minScn);
    assertEquals(Long.valueOf(maxScn), seenScns.get(numScns-2));

    //stream with scn < min but >= prevScn
    cp.setWindowScn(prevScn);
    cp.setWindowOffset(-1);
    readEventBuffer.clear();
    seenScns.clear();
    streamWriterReader(prodEventBuffer,input.getBatchSize()*eventSize,cp,"scn",readEventBuffer,seenScns,clientStats);
    assertEquals(numScns, seenScns.size());
    assertEquals(Long.valueOf(minScn), seenScns.get(0));
    System.out.printf("Clientstats: minScn=%d , maxScn=%d , timespan=%d timeSinceLastEvent=%d\n",
                      clientStats.getTotalStats().getMinScn(),
                      clientStats.getTotalStats().getMaxScn(),
                      clientStats.getTotalStats().getTimeSpan(),
                      clientStats.getTotalStats().getTimeSinceLastEvent());
    assertEquals(maxScn, clientStats.getTotalStats().getMaxScn());
    assertEquals(numEvents - 1, clientStats.getTotalStats().getTimeSpan()/MILLISECONDS);
    assertEquals(clientStats.getTotalStats().getTimestampMaxScnEvent(),
                 emitterStats.getTotalStats().getTimestampMaxScnEvent());
    assertEquals(minScn, clientStats.getTotalStats().getMinScn());

    //stream with scn < prevScn
    cp.setWindowScn(prevScn-1);
    cp.setWindowOffset(-1);
    readEventBuffer.clear();
    seenScns.clear();
    streamWriterReader(prodEventBuffer,input.getBatchSize()*eventSize,cp,"scn",readEventBuffer,seenScns);
    assertEquals(0, seenScns.size());

    //stream with scn == prevScn but windowOffset=0
    cp.setWindowScn(prevScn);
    cp.setWindowOffset(0);
    readEventBuffer.clear();
    seenScns.clear();
    streamWriterReader(prodEventBuffer,input.getBatchSize()*eventSize,cp,"scn",readEventBuffer,seenScns);
    assertEquals(0, seenScns.size());


    //stream with scn > min
    cp.setWindowScn(minScn+1);
    cp.setWindowOffset(-1);
    readEventBuffer.clear();
    seenScns.clear();
    streamWriterReader(prodEventBuffer,input.getBatchSize()*eventSize,cp,"scn",readEventBuffer,seenScns);
    assertEquals(numScns-1, seenScns.size());
    assertEquals(Long.valueOf(maxScn), seenScns.get(numScns-2));
    assertTrue(seenScns.get(0)!=minScn);
  }

  @Test
  public void testScnIndexInvalidation() throws Exception
  {
    Vector<DbusEvent> srcTestEvents = new Vector<DbusEvent>();
    EventBufferTestInput input = new EventBufferTestInput();
    int numEvents = 1000;
    int numScns = 10;
    int windowSize = numEvents/numScns;
    DbusEventGenerator evGen = new DbusEventGenerator();

    for (int i=1; i<2;++i)
    {
      LOG.info("I="+ i);
      int bufferSize = (windowSize*2);
      input.setNumEvents(numEvents)
      .setWindowSize(windowSize)
      .setSharedBufferSize(bufferSize)
      .setStagingBufferSize(bufferSize)
      .setIndexSize(bufferSize)
      .setIndividualBufferSize(bufferSize)
      .setBatchSize(bufferSize)
      .setProducerBufferSize(bufferSize)
      .setPayloadSize(10)
      .setDeleteInterval(1)
      .setProdQueuePolicy(QueuePolicy.OVERWRITE_ON_WRITE);
      DbusEventsStatisticsCollector emitterStats = new DbusEventsStatisticsCollector(1,"appenderStats",true,true,null);

      if (srcTestEvents.isEmpty())
      {
        assertTrue(evGen.generateEvents(numEvents,input.getWindowSize(),512,input.getPayloadSize(),false,srcTestEvents) > 0);
      }
      int eventSize= 0;
      for (DbusEvent e: srcTestEvents)
      {
        if (!e.isControlMessage())
        {
          eventSize = e.size();
          break;
        }
      }
      LOG.info("event size="+ eventSize);
      int indexSize = (input.getIndexSize()*eventSize/100);
      LOG.info("indexSize=" + indexSize);
      DbusEventBuffer prodEventBuffer =
          new DbusEventBuffer(getConfig(input.getProducerBufferSize()*eventSize+i,
                                        input.getIndividualBufferSize()*eventSize,
                                        indexSize, input.getStagingBufferSize()*eventSize,
                                        AllocationPolicy.HEAP_MEMORY, input.getProdQueuePolicy(),
                                        input.getProdBufferAssertLevel()));
      LOG.info("Allocated buffer size=" + prodEventBuffer.getAllocatedSize());
      DbusEventAppender eventProducer = new DbusEventAppender(srcTestEvents, prodEventBuffer,emitterStats,0.600);
      Thread t = new Thread(eventProducer);
      t.start();
      t.join();
      DbusEventsTotalStats stats = emitterStats.getTotalStats();
      LOG.info("minScn=" + prodEventBuffer.getMinScn() + "totalEvents=" + stats.getNumDataEvents());
      //weak assertion ! just check it's not -1 for now
      //TODO! get this assertion to succeed
      //assertTrue(prodEventBuffer.getMinScn()!=-1);
    }
  }



  /**
   * Produces a specified number of constant sized events; sets up writer/reader eventBuffer over
   * a Pipe; and sends the events generated to the reader thread.
   *
   * @param srcTestEvents: container to house Random events generated
   * @param dstTestEvents: container that receives the events generated in srcTestEvents
   * @param input : Input test parameters
   * @return output : dstTestEvents contains the events received from the src event buffer over the
   *                  Pipe to the dst receiver buffer (reader)
   * @return true if the test runs without issues; false otherwise; note that tests are
   *              responsible for assertions involving srcTestEvents and dstTstEvents
   */
  protected boolean runConstEventsReaderWriter(Vector<DbusEvent> srcTestEvents,
                                               Vector<DbusEvent> dstTestEvents,
                                               EventBufferTestInput input,
                                               DbusEventsStatisticsCollector emitterStats,
                                               DbusEventsStatisticsCollector streamStats,
                                               DbusEventsStatisticsCollector clientStats)
  throws Exception
  {
    return runConstEventsReaderWriter(srcTestEvents, dstTestEvents, input, emitterStats,
                                      streamStats, clientStats, false);
  }

  // data flow:  emitter/producer/appender -> DEB -> writer -> pipe -> reader -> DEB -> consumer
  protected boolean runConstEventsReaderWriter(Vector<DbusEvent> srcTestEvents,
                                               Vector<DbusEvent> dstTestEvents,
                                               EventBufferTestInput input,
                                               DbusEventsStatisticsCollector emitterStats,
                                               DbusEventsStatisticsCollector streamStats,
                                               DbusEventsStatisticsCollector clientStats,
                                               boolean autoStartBuffer)
  throws Exception
  {
    LOG.info("starting runConstEventsReaderWriter for " + input.getTestName());
    int numEvents = input.getNumEvents();
    int maxWindowSize = input.getWindowSize();

    DbusEventGenerator evGen = new DbusEventGenerator();
    if (evGen.generateEvents(numEvents, maxWindowSize, 512, input.getPayloadSize(), srcTestEvents) <= 0)
    {
      return false;
    }

    int eventSize = srcTestEvents.get(0).size();

    long producerBufferSize = input.getProducerBufferSize() * eventSize;
    long sharedBufferSize = input.getSharedBufferSize() * eventSize;
    int stagingBufferSize = input.getStagingBufferSize() * eventSize;
    int individualBufferSize  = input.getIndividualBufferSize() * eventSize;
    int indexSize = input.getIndexSize() * eventSize;

    QueuePolicy prodQueuePolicy = input.getProdQueuePolicy();
    QueuePolicy consQueuePolicy = input.getConsQueuePolicy();

    //create the main event buffers
    DbusEventBuffer prodEventBuffer =
        new DbusEventBuffer(getConfig(producerBufferSize, individualBufferSize, indexSize ,
                                      stagingBufferSize, AllocationPolicy.HEAP_MEMORY,
                                      prodQueuePolicy,
                                      input.getProdBufferAssertLevel()));
    DbusEventBuffer consEventBuffer =
        new DbusEventBuffer(getConfig(sharedBufferSize, individualBufferSize , indexSize,
                                      stagingBufferSize, AllocationPolicy.HEAP_MEMORY,
                                      consQueuePolicy,
                                      input.getConsBufferAssertLevel()));

    //Producer of events, a.k.a. "emitter"
    DbusEventAppender eventProducer = new DbusEventAppender(srcTestEvents,
                                                            prodEventBuffer,
                                                            emitterStats,
                                                            autoStartBuffer);

    //commn channels between reader and writer
    Pipe pipe = Pipe.open();
    Pipe.SinkChannel writerStream = pipe.sink();
    Pipe.SourceChannel readerStream = pipe.source();
    writerStream.configureBlocking(true);
    readerStream.configureBlocking(false);

    //Event writer - Relay in the real world
    int batchSize = input.getBatchSize() * eventSize;
    DbusEventBufferWriter writer = new DbusEventBufferWriter(prodEventBuffer, writerStream,
                                                             batchSize, streamStats);

    //Event readers - Clients in the real world
    DbusEventBufferConsumer consumer = new DbusEventBufferConsumer(consEventBuffer, numEvents,
                                                                   input.getDeleteInterval(),
                                                                   dstTestEvents);
    Vector<EventBufferConsumer> consList = new Vector<EventBufferConsumer>();
    consList.add(consumer);
    //Event readers - Clients in the real world
    DbusEventBufferReader reader = new DbusEventBufferReader(consEventBuffer, readerStream,
                                                             consList, clientStats);

    UncaughtExceptionTrackingThread tEmitter = new UncaughtExceptionTrackingThread(eventProducer,"EventProducer");
    UncaughtExceptionTrackingThread tWriter = new UncaughtExceptionTrackingThread(writer,"Writer");
    UncaughtExceptionTrackingThread tReader = new UncaughtExceptionTrackingThread(reader,"Reader");
    UncaughtExceptionTrackingThread tConsumer = new UncaughtExceptionTrackingThread(consumer,"Consumer");

    long emitterWaitms = 20000;
    long writerWaitms = 10000;
    long readerWaitms = 10000;
    long consumerWaitms = readerWaitms;

    //start emitter;
    tEmitter.start();

    //tarnish events written to buffer;
    int [] corruptIndexList = input.getCorruptIndexList();
    if (corruptIndexList.length > 0)
    {
      tEmitter.join(emitterWaitms);
      EventCorruptionType corruptionType = input.getCorruptionType();
      eventProducer.tarnishEventsInBuffer(corruptIndexList, corruptionType);
    }

    //start  consumer / reader /writer
    tConsumer.start();
    tWriter.start();
    tReader.start();

    //wait until all events have been written;
    dumpEmitterWriterReaderConsumerState(eventProducer, writer, reader, consumer, emitterStats, streamStats, clientStats, dstTestEvents, prodEventBuffer, consEventBuffer);
    LOG.info("runConstEventsReaderWriter(): waiting up to " + (emitterWaitms/1000) + " sec for appender/producer/emitter thread to terminate");
    tEmitter.join(emitterWaitms);
    //try and set a finish for writer
    long eventsEmitted = eventProducer.eventsEmitted();
    writer.setExpectedEvents(eventsEmitted);

    //wait for writer to finish;
    dumpEmitterWriterReaderConsumerState(eventProducer, writer, reader, consumer, emitterStats, streamStats, clientStats, dstTestEvents, prodEventBuffer, consEventBuffer);
    LOG.info("runConstEventsReaderWriter(): waiting up to " + (writerWaitms/1000) + " sec for writer thread to terminate");
    tWriter.join(writerWaitms);
    //close the writer Stream;
    dumpEmitterWriterReaderConsumerState(eventProducer, writer, reader, consumer, emitterStats, streamStats, clientStats, dstTestEvents, prodEventBuffer, consEventBuffer);
    LOG.info("runConstEventsReaderWriter(): signalling writer to stop");
    writer.stop();

    if (!tReader.isAlive())
    {
      LOG.error("runConstEventsReaderWriter(): reader thread died unexpectedly");
    }

    dumpEmitterWriterReaderConsumerState(eventProducer, writer, reader, consumer, emitterStats, streamStats, clientStats, dstTestEvents, prodEventBuffer, consEventBuffer);
    LOG.info("runConstEventsReaderWriter(): waiting up to " + (consumerWaitms/1000) + " sec for consumer thread to terminate");
    tConsumer.join(consumerWaitms);
    //stop the consumer thread; may or may not have got all events;
    dumpEmitterWriterReaderConsumerState(eventProducer, writer, reader, consumer, emitterStats, streamStats, clientStats, dstTestEvents, prodEventBuffer, consEventBuffer);
    LOG.info("runConstEventsReaderWriter(): signalling consumer to stop");
    consumer.stop();
    dumpEmitterWriterReaderConsumerState(eventProducer, writer, reader, consumer, emitterStats, streamStats, clientStats, dstTestEvents, prodEventBuffer, consEventBuffer);
    LOG.info("runConstEventsReaderWriter(): signalling reader to stop");
    reader.stop();
    dumpEmitterWriterReaderConsumerState(eventProducer, writer, reader, consumer, emitterStats, streamStats, clientStats, dstTestEvents, prodEventBuffer, consEventBuffer);
    LOG.info("runConstEventsReaderWriter(): all stop.");

    assertEquals(null, consumer.getExceptionThrown());

    LOG.info("runConstEventsReaderWriter() consumer thread: " + (consumer.hasInvalidEvent()? "DID" : "did NOT") +
             " receive invalid event(s); num events emitted=" + eventsEmitted +
             ", events written=" + writer.eventsWritten() + ", events read=" + reader.eventsRead());

    return true;
  }

  private void dumpEmitterWriterReaderConsumerState(DbusEventAppender eventProducer,
                                                    DbusEventBufferWriter writer,
                                                    DbusEventBufferReader reader,
                                                    DbusEventBufferConsumer consumer,
                                                    DbusEventsStatisticsCollector emitterStats, // producer/appender
                                                    DbusEventsStatisticsCollector streamStats,  // writer
                                                    DbusEventsStatisticsCollector clientStats,  // reader
                                                    Vector<DbusEvent> consumerEventsVector,  // consumer has no stats
                                                    DbusEventBuffer relayEventBuffer,
                                                    DbusEventBuffer clientEventBuffer)
  {
    LOG.info("dumpEmitterWriterReaderConsumerState():" +
             "\n  producer eventsEmitted=" + eventProducer.eventsEmitted() +
                 ", numDataEvents=" + emitterStats.getTotalStats().getNumDataEvents() +
                 ", numSysEvents=" + emitterStats.getTotalStats().getNumSysEvents() +
                 ", maxScn=" + emitterStats.getTotalStats().getMaxScn() +
                 ", maxSeenWinScn=" + emitterStats.getTotalStats().getMaxSeenWinScn() +
                 ", freeSpace=" + emitterStats.getTotalStats().getFreeSpace() +
             "\n  writer expectedEvents=" + writer.expectedEvents() + ", eventsWritten=" + writer.eventsWritten() +
                 ", numDataEvents=" + streamStats.getTotalStats().getNumDataEvents() +
                 ", numSysEvents=" + streamStats.getTotalStats().getNumSysEvents() +
                 ", maxSeenWinScn=" + streamStats.getTotalStats().getMaxSeenWinScn() +
                 ", freeSpace=" + streamStats.getTotalStats().getFreeSpace() +
             "\n  reader eventsRead=" + reader.eventsRead() +
                 ", numDataEvents=" + clientStats.getTotalStats().getNumDataEvents() +
                 ", numSysEvents=" + clientStats.getTotalStats().getNumSysEvents() +
                 ", maxScn=" + clientStats.getTotalStats().getMaxScn() +
                 ", maxSeenWinScn=" + clientStats.getTotalStats().getMaxSeenWinScn() +
                 ", freeSpace=" + clientStats.getTotalStats().getFreeSpace() +
             "\n  consumer events-vector size=" + consumerEventsVector.size() + ", hasStopped=" + consumer.hasStopped() +
                 ", hasInvalidEvent=" + consumer.hasInvalidEvent() +
             //"\n  relay event buffer = " + relayEventBuffer.toString() +
             // NOTE:  with proposed concurrency fix in DbusEventBuffer.toString() (DDSDBUS-1817), the following line
             // can trigger deadlock:  Consumer blocked on Reader (await()) while holding queue lock; Reader blocked
             // on _busyIteratorPool lock; and main test thread blocked on queue lock while holding _busyIteratorPool
             // lock in toString().  However, without it, concurrent updates of _busyIteratorPool trigger NPEs in
             // most of the tests that use runConstEventsReaderWriter().  So commented out until DDSDBUS-1817 is fixed.
             //"\n  client event buffer = " + clientEventBuffer.toString() +
             "");
  }

  public void trySleep(int msec)
  {
    try
    {
      Thread.sleep(msec);
    }
    catch (InterruptedException ie)
    {
      ie.printStackTrace();
    }
  }

  /**
   * This tests makes sure that the event buffer generates only one error if an output channel
   * is closed (see DDSDBUS-835).
   */
  @Test
  public void testStreamEventsError() throws Exception
  {
    //allocate a small buffer
    DbusEventBuffer.Config confBuilder = new DbusEventBuffer.Config();
    confBuilder.setAllocationPolicy(AllocationPolicy.HEAP_MEMORY.toString());
    confBuilder.setMaxSize(1000);
    confBuilder.setScnIndexSize(80);
    confBuilder.setAverageEventSize(80);

    DbusEventBuffer buf = new DbusEventBuffer(confBuilder);

    DbusEventsStatisticsCollector statsCollector = new DbusEventsStatisticsCollector(1, "in", true, false, null);
    //generate some data in the buffer
    buf.start(0);
    final int W = 2;
    final int E = 2;
    for (int w = 1; w <= W; ++w)
    {
      buf.startEvents();
      int seq = w * 100;
      for (int i = 0; i < E; ++i)
      {
        buf.appendEvent(new DbusEventKey(i), (short)1, (short)1, System.nanoTime(), (short)1,
                        new byte[16], new byte[1], false, statsCollector);
      }
      buf.endEvents(seq, statsCollector);
    }

    //sanity check for the data
    Assert.assertEquals(W, statsCollector.getTotalStats().getNumSysEvents());
    Assert.assertEquals(W * E, statsCollector.getTotalStats().getNumDataEvents());

    File tmpFile= File.createTempFile(TestDbusEventBuffer.class.getSimpleName(), ".tmp");
    tmpFile.deleteOnExit();
    OutputStream outStream = new FileOutputStream(tmpFile);
    WritableByteChannel outChannel = Channels.newChannel(outStream);

    //stream events
    DbusEventsStatisticsCollector outStatsCollector = new DbusEventsStatisticsCollector(1, "out", true, false, null);
    Checkpoint cp = new Checkpoint();
    cp.setFlexible();
    cp.setConsumptionMode(DbusClientMode.ONLINE_CONSUMPTION);
    ErrorCountingAppender errorCountAppender = new ErrorCountingAppender();
    errorCountAppender.setName("errorCountAppender");
    DbusEventSerializable.LOG.addAppender(errorCountAppender);
    Level oldLevel = DbusEventSerializable.LOG.getLevel();
    //ensure ERROR log level because gradle likes to change it
    DbusEventSerializable.LOG.setLevel(Level.ERROR);

    int errNum = errorCountAppender.getErrorNum();
    StreamEventsArgs args = new StreamEventsArgs(10000).setStatsCollector(outStatsCollector);
    int written = buf.streamEvents(cp, outChannel, args).getNumEventsStreamed();

    Assert.assertEquals(errorCountAppender, DbusEventSerializable.LOG.getAppender("errorCountAppender"));
    Assert.assertEquals(W * (E + 1), written);
    Assert.assertEquals(errNum, errorCountAppender.getErrorNum());

    //close the channel to force an error for streamEvents
    outStream.close();
    outChannel.close();

    cp = new Checkpoint();
    cp.setFlexible();
    cp.setConsumptionMode(DbusClientMode.ONLINE_CONSUMPTION);
    errNum = errorCountAppender.getErrorNum();

    written = buf.streamEvents(cp, outChannel, args).getNumEventsStreamed();

    DbusEventSerializable.LOG.setLevel(oldLevel);

    Assert.assertEquals(errorCountAppender, DbusEventSerializable.LOG.getAppender("errorCountAppender"));
    Assert.assertEquals(0, written);
    Assert.assertEquals(errNum + 1, errorCountAppender.getErrorNum());
  }

  /**
   * Testcase sends events with the following sequence :
   * valid packet, EOW, valid packet
   */
  @Test
  public void testNoMissingEOWHappyPath()
      throws Exception
  {
    final DbusEventBuffer dbusBuf =
        new DbusEventBuffer(getConfig(1000,1000,100,500,AllocationPolicy.HEAP_MEMORY,
                                      QueuePolicy.BLOCK_ON_WRITE, AssertLevel.NONE));
    dbusBuf.setDropOldEvents(true);
    DbusEventGenerator generator = new DbusEventGenerator();
    Vector<DbusEvent> events = new Vector<DbusEvent>();
    generator.generateEvents(3, 1, 100, 10, events);

    // don't try this at home, kids!
    DbusEventInternalWritable writableEvent = DbusEventCorrupter.makeWritable(events.get(1));
    writableEvent.setSequence(events.get(0).sequence());
    writableEvent.setSrcId((short)-2);
    writableEvent.applyCrc();

    writableEvent = DbusEventCorrupter.makeWritable(events.get(2));
    writableEvent.setSequence(events.get(0).sequence()+100);
    writableEvent.applyCrc();

    // Increment the SCN and reuse
    for (int i=0; i < 3; ++i)
    {
      DbusEvent e = events.get(i);
      assertTrue("invalid event #" + i, e.isValid());
      LOG.info("DbusEvent " + i + " " + e);
    }
    // set up the ReadChannel with 2 events
    ByteArrayOutputStream oStream = new ByteArrayOutputStream();
    WritableByteChannel oChannel = Channels.newChannel(oStream);
    for (int i = 0; i < 3; ++i)
    {
      ((DbusEventInternalReadable)events.get(i)).writeTo(oChannel,Encoding.BINARY);
    }

    byte[] writeBytes = oStream.toByteArray();
    ByteArrayInputStream iStream = new ByteArrayInputStream(writeBytes);
    final ReadableByteChannel rChannel = Channels.newChannel(iStream);

    try
    {
      dbusBuf.readEvents(rChannel);
      // Should NOT throw invalid event exception
    }
    catch (InvalidEventException ie)
    {
      LOG.error("Exception trace is " + ie);
      Assert.fail();
      return;
    }
  }

  /**
   * Testcase send the following sequence:
   * two valid packets, EOW, two valid packets, EOW
   */
  @Test
  public void testNoMissingEOWHappyPathWithMultiEventWindow()
      throws Exception
  {
    final DbusEventBuffer dbusBuf =
        new DbusEventBuffer(getConfig(1000,1000,100,500,AllocationPolicy.HEAP_MEMORY,
                                      QueuePolicy.BLOCK_ON_WRITE, AssertLevel.NONE));
    dbusBuf.setDropOldEvents(true);
    DbusEventGenerator generator = new DbusEventGenerator();
    Vector<DbusEvent> events = new Vector<DbusEvent>();
    generator.generateEvents(6, 1, 100, 10, events);

    // conversion of readable events to writable is for TESTING ONLY:
    DbusEventInternalWritable writableEvent = DbusEventCorrupter.makeWritable(events.get(1));
    writableEvent.setSequence(events.get(0).sequence());
    writableEvent.applyCrc();

    writableEvent = DbusEventCorrupter.makeWritable(events.get(2));
    writableEvent.setSequence(events.get(0).sequence());
    writableEvent.setSrcId((short)-2);
    writableEvent.applyCrc();

    writableEvent = DbusEventCorrupter.makeWritable(events.get(3));
    writableEvent.setSequence(events.get(0).sequence()+100);
    writableEvent.applyCrc();

    writableEvent = DbusEventCorrupter.makeWritable(events.get(4));
    writableEvent.setSequence(events.get(3).sequence());
    writableEvent.applyCrc();

    writableEvent = DbusEventCorrupter.makeWritable(events.get(5));
    writableEvent.setSequence(events.get(0).sequence()+100);
    writableEvent.setSrcId((short)-2);
    writableEvent.applyCrc();

    // Increment the SCN and reuse
    for (int i=0; i < 6; ++i)
    {
      DbusEvent e = events.get(i);
      assertTrue("invalid event #" + i, e.isValid());
    }

    // Set up the ReadChannel with 2 events
    ByteArrayOutputStream oStream = new ByteArrayOutputStream();
    WritableByteChannel oChannel = Channels.newChannel(oStream);
    for (int i = 0; i < 6; ++i)
    {
      ((DbusEventInternalReadable)events.get(i)).writeTo(oChannel,Encoding.BINARY);
    }

    byte[] writeBytes = oStream.toByteArray();
    ByteArrayInputStream iStream = new ByteArrayInputStream(writeBytes);
    final ReadableByteChannel rChannel = Channels.newChannel(iStream);

    try
    {
      dbusBuf.readEvents(rChannel);
      // Should NOT throw invalid event exception
    }
    catch (InvalidEventException ie)
    {
      LOG.error("Exception trace is " + ie.getMessage(), ie);
      Assert.fail();
      return;
    }
  }

  /**
   * Testcase send the following sequence:
   * two valid packets, EOW, EOW, one valid packet, EOW
   * @throws Exception
   */
  @Test
  public void testNoMissingEOWHappyPathWithMultiEventWindowWithMultiEOWs()
      throws Exception
  {
    final DbusEventBuffer dbusBuf =
        new DbusEventBuffer(getConfig(1000,1000,100,500,AllocationPolicy.HEAP_MEMORY,
                                      QueuePolicy.BLOCK_ON_WRITE, AssertLevel.NONE));
    dbusBuf.setDropOldEvents(true);
    DbusEventGenerator generator = new DbusEventGenerator();
    Vector<DbusEvent> events = new Vector<DbusEvent>();
    generator.generateEvents(6, 1, 100, 10, events);

    // conversion of readable events to writable is for TESTING ONLY:
    DbusEventInternalWritable writableEvent = DbusEventCorrupter.makeWritable(events.get(1));
    writableEvent.setSequence(events.get(0).sequence());
    writableEvent.applyCrc();

    writableEvent = DbusEventCorrupter.makeWritable(events.get(2));
    writableEvent.setSequence(events.get(0).sequence());
    writableEvent.setSrcId((short)-2);
    writableEvent.applyCrc();

    writableEvent = DbusEventCorrupter.makeWritable(events.get(3));
    writableEvent.setSequence(events.get(0).sequence()+50);
    writableEvent.setSrcId((short)-2);
    writableEvent.applyCrc();

    writableEvent = DbusEventCorrupter.makeWritable(events.get(4));
    writableEvent.setSequence(events.get(0).sequence()+100);
    writableEvent.applyCrc();

    writableEvent = DbusEventCorrupter.makeWritable(events.get(5));
    writableEvent.setSequence(events.get(0).sequence()+100);
    writableEvent.setSrcId((short)-2);
    writableEvent.applyCrc();

    // Increment the SCN and reuse
    for (int i=0; i < 6; ++i)
    {
      DbusEvent e = events.get(i);
      assertTrue("invalid event #" + i, e.isValid());
    }

    // Set up the ReadChannel with 2 events
    ByteArrayOutputStream oStream = new ByteArrayOutputStream();
    WritableByteChannel oChannel = Channels.newChannel(oStream);
    for (int i = 0; i < 6; ++i)
    {
      ((DbusEventInternalReadable)events.get(i)).writeTo(oChannel,Encoding.BINARY);
    }

    byte[] writeBytes = oStream.toByteArray();
    ByteArrayInputStream iStream = new ByteArrayInputStream(writeBytes);
    final ReadableByteChannel rChannel = Channels.newChannel(iStream);

    try
    {
      dbusBuf.readEvents(rChannel);
      // Should NOT throw invalid event exception
    }
    catch (InvalidEventException ie)
    {
      LOG.error("Exception trace is " + ie.getMessage(), ie);
      Assert.fail();
      return;
    }
  }

  /**
   * Testcase send the following sequence
   * 1 valid packets, EOW
   * @throws Exception
   */
  @Test
  public void testNoMissingEOWHappyPathWithOneEvent()
      throws Exception
  {
    final DbusEventBuffer dbusBuf =
        new DbusEventBuffer(getConfig(1000,1000,100,500,AllocationPolicy.HEAP_MEMORY,
                                      QueuePolicy.BLOCK_ON_WRITE, AssertLevel.NONE));
    dbusBuf.setDropOldEvents(true);
    dbusBuf.start(0);
    DbusEventGenerator generator = new DbusEventGenerator();
    Vector<DbusEvent> events = new Vector<DbusEvent>();
    generator.generateEvents(2, 1, 100, 10, events);

    // conversion of readable events to writable is for TESTING ONLY:
    DbusEventInternalWritable writableEvent = DbusEventCorrupter.makeWritable(events.get(1));
    writableEvent.setSequence(events.get(0).sequence());
    writableEvent.setSrcId((short)-2);
    writableEvent.applyCrc();

    // Increment the SCN and reuse
    for (int i=0; i < 2; ++i)
    {
      DbusEvent e = events.get(i);
      assertTrue("invalid event #" + i, e.isValid());
    }

    // Set up the ReadChannel with 2 events
    ByteArrayOutputStream oStream = new ByteArrayOutputStream();
    WritableByteChannel oChannel = Channels.newChannel(oStream);
    for (int i = 0; i < 2; ++i)
    {
      ((DbusEventInternalReadable)events.get(i)).writeTo(oChannel,Encoding.BINARY);
    }

    byte[] writeBytes = oStream.toByteArray();
    ByteArrayInputStream iStream = new ByteArrayInputStream(writeBytes);
    final ReadableByteChannel rChannel = Channels.newChannel(iStream);

    try
    {
      dbusBuf.readEvents(rChannel);
      // Should NOT throw invalid event exception
    }
    catch (InvalidEventException ie)
    {
      LOG.error("Exception trace is " + ie);
      Assert.fail();
      return;
    }
  }

  /**
   * Testcase sends events with appropriate EOW events in between
   * valid packet, no EOW for prior packet, valid packet with a higher scn.
   * @throws Exception
   */
  @Test
  public void testMissingEOWUnhappyPath()
      throws Exception
  {
    final DbusEventBuffer dbusBuf =
        new DbusEventBuffer(getConfig(1000,1000,100,500,AllocationPolicy.HEAP_MEMORY,
                                      QueuePolicy.BLOCK_ON_WRITE, AssertLevel.NONE));
    dbusBuf.setDropOldEvents(true);
    DbusEventGenerator generator = new DbusEventGenerator();
    Vector<DbusEvent> events = new Vector<DbusEvent>();
    generator.generateEvents(3, 1, 100, 10, events);

    // conversion of readable events to writable is for TESTING ONLY:
    DbusEventInternalWritable writableEvent = DbusEventCorrupter.makeWritable(events.get(1));
    // No EOW for event 0.
    writableEvent.setSequence(events.get(0).sequence()+50);
    writableEvent.applyCrc();

    writableEvent = DbusEventCorrupter.makeWritable(events.get(2));
    // No EOW for event 1.
    writableEvent.setSequence(events.get(0).sequence()+100);
    writableEvent.applyCrc();

    // Increment the SCN and reuse
    for (int i=0; i < 3; ++i)
    {
      DbusEvent e = events.get(i);
      assertTrue("invalid event #" + i, e.isValid());
      LOG.info("DbusEvent " + i + " " + e);
    }

    // Set up the ReadChannel with 2 events
    ByteArrayOutputStream oStream = new ByteArrayOutputStream();
    WritableByteChannel oChannel = Channels.newChannel(oStream);
    for (int i = 0; i < 3; ++i)
    {
      ((DbusEventInternalReadable)events.get(i)).writeTo(oChannel,Encoding.BINARY);
    }

    byte[] writeBytes = oStream.toByteArray();
    ByteArrayInputStream iStream = new ByteArrayInputStream(writeBytes);
    final ReadableByteChannel rChannel = Channels.newChannel(iStream);

    try
    {
      dbusBuf.readEvents(rChannel);
      Assert.fail();
    }
    catch (InvalidEventException ie)
    {
      LOG.info("Exception trace is " + ie);
      return;
    }
  }

  /**
   * Testcase sends events with appropriate EOW events in between:
   * 2 valid packets, no EOW for prior packet, 2 valid packets with a higher scn.
   * @throws Exception
   */
  @Test
  public void testMissingEOWUnhappyPathWithMultiEventWindow()
      throws Exception
  {
    final DbusEventBuffer dbusBuf =
        new DbusEventBuffer(getConfig(1000,1000,100,500,AllocationPolicy.HEAP_MEMORY,
                                      QueuePolicy.BLOCK_ON_WRITE, AssertLevel.NONE));
    dbusBuf.setDropOldEvents(true);
    DbusEventGenerator generator = new DbusEventGenerator();
    Vector<DbusEvent> events = new Vector<DbusEvent>();
    generator.generateEvents(6, 1, 100, 10, events);

    // conversion of readable events to writable is for TESTING ONLY:
    DbusEventInternalWritable writableEvent = DbusEventCorrupter.makeWritable(events.get(1));
    writableEvent.setSequence(events.get(0).sequence());
    writableEvent.applyCrc();

    writableEvent = DbusEventCorrupter.makeWritable(events.get(2));
    writableEvent.setSequence(events.get(0).sequence());
    writableEvent.applyCrc();

    writableEvent = DbusEventCorrupter.makeWritable(events.get(3));
    // NO EOW for event 0 sequence.
    writableEvent.setSequence(events.get(0).sequence()+100);
    writableEvent.applyCrc();

    writableEvent = DbusEventCorrupter.makeWritable(events.get(4));
    writableEvent.setSequence(events.get(3).sequence());
    writableEvent.applyCrc();

    writableEvent = DbusEventCorrupter.makeWritable(events.get(5));
    writableEvent.setSrcId((short)-2);
    writableEvent.applyCrc();

    // Increment the SCN and reuse
    for (int i=0; i < 6; ++i)
    {
      DbusEvent e = events.get(i);
      assertTrue("invalid event #" + i, e.isValid());
    }

    // Set up the ReadChannel with 6 events
    ByteArrayOutputStream oStream = new ByteArrayOutputStream();
    WritableByteChannel oChannel = Channels.newChannel(oStream);
    for (int i = 0; i < 6; ++i)
    {
      ((DbusEventInternalReadable)events.get(i)).writeTo(oChannel,Encoding.BINARY);
    }

    byte[] writeBytes = oStream.toByteArray();
    ByteArrayInputStream iStream = new ByteArrayInputStream(writeBytes);
    final ReadableByteChannel rChannel = Channels.newChannel(iStream);

    try
    {
      dbusBuf.readEvents(rChannel);
      Assert.fail();
    }
    catch (InvalidEventException ie)
    {
      LOG.info("Exception trace is " + ie);
      return;
    }
  }


  /**
   * Verify that Iterator's CP never gets ahead of Iterator's tail, even at
   * the end of the buffer (client, NO-OVERWITE policy).
   */
  @Test
  public void testInternalIteratorSingleBufFull() throws Exception
  {
    final Logger log =
        Logger.getLogger("TestDbusEventBufferIterator.testInternalIteratorSingleBufFull");
    //log.setLevel(Level.DEBUG);
    log.info("starting");

    final DbusEventBuffer dbusBuf =
        new DbusEventBuffer(TestDbusEventBuffer.getConfig(
            845, 100000, 256, 500, AllocationPolicy.HEAP_MEMORY, QueuePolicy.BLOCK_ON_WRITE,
            AssertLevel.NONE));
    dbusBuf.start(0);

    log.info("append a full buffer");
    DbusEventGenerator generator = new DbusEventGenerator(10);
    final Vector<DbusEvent> events = new Vector<DbusEvent>();
    generator.generateEvents(6, 2, 120, 38, events);

    log.debug(dbusBuf.toShortString());
    dbusBuf.assertBuffersLimits();

    DbusEventAppender appender = new DbusEventAppender(events, dbusBuf, null, 1.0, false, -1);
    appender.run();

    log.info("verify new iterator");
    DbusEventIterator iter1 =
        dbusBuf.acquireIterator("testInternalIteratorSingleBufFull");

    log.debug("it1=" + iter1);
    Assert.assertEquals(iter1.getCurrentPosition(), dbusBuf.getHead());
    Assert.assertEquals(iter1._iteratorTail.getPosition(), dbusBuf.getTail());
    Assert.assertEquals(dbusBuf._busyIteratorPool.size(), 1);
    Assert.assertTrue(iter1.hasNext());
    DbusEvent e = iter1.next();
    Assert.assertTrue(e.isEndOfPeriodMarker());
    Assert.assertTrue(iter1.hasNext());
    dbusBuf.assertBuffersLimits();

    log.info("make sure we can read some events");
    readAndCompareIteratorEvents(iter1, events, 0, 6, true, true, true);
    log.debug("after read: " + dbusBuf.toShortString());
    log.debug(iter1);

    log.info("append more windows");
    final Vector<DbusEvent> events2 = new Vector<DbusEvent>();
    generator = new DbusEventGenerator(200);
    generator.generateEvents(2, 1, 120, 39, events2);
    appender = new DbusEventAppender(events2, dbusBuf, null, 1.0, false, -1);
    appender.run();
    log.debug("after 2 more events added: " + dbusBuf.toShortString());
    log.debug(iter1);

    readAndCompareIteratorEvents(iter1, events2, 0, 2, true, false, true);
    log.debug("after 2 more events read: " + dbusBuf.toShortString());
    log.debug(iter1);
    dbusBuf.assertBuffersLimits();

    // create another iterator - make sure it can read too
    DbusEventIterator iter2 = dbusBuf.acquireIterator("testInternalIteratorSingleBufFull2");

    long iCWP = iter2.getCurrentPosition();
    long head = dbusBuf.getBufferPositionParser().sanitize(dbusBuf.getHead(), dbusBuf.getBuffer());
    Assert.assertEquals(iCWP, head);
    Assert.assertEquals(iter2._iteratorTail.getPosition(), dbusBuf.getTail());
    Assert.assertEquals(dbusBuf._busyIteratorPool.size(), 2);
    Assert.assertTrue(iter2.hasNext());

    log.debug("iter2=" + iter2);
    readAndCompareIteratorEvents(iter2, events2, 0, 2, true, false, true); // read same events and don't remove
    dbusBuf.releaseIterator(iter2);
    dbusBuf.assertBuffersLimits();

    log.debug("iter1=" + iter1);
    iter1.remove();

    log.debug("buf (after read)=" + dbusBuf);

    generator = new DbusEventGenerator(300);
    final Vector<DbusEvent> events3 = new Vector<DbusEvent>();
    generator.generateEvents(4, 2, 120, 39, events3);
    appender = new DbusEventAppender(events3, dbusBuf, null, 1.0, false, -1);
    appender.run();
    dbusBuf.assertBuffersLimits();

    log.info("make sure we can read remainder of events");
    readAndCompareIteratorEvents(iter1, events3, 0, 4, false, true, true);

    dbusBuf.assertBuffersLimits();
    Assert.assertTrue(dbusBuf.empty());

    dbusBuf.releaseIterator(iter1);

    log.info("done");
  }

  private static HashSet<String> _idSet = new HashSet<String>();
  private final AtomicInteger thrCounter = new AtomicInteger();

  private synchronized void addId(String sessionId) throws Exception
  {
    Assert.assertFalse("Id " + sessionId + " already present", _idSet.contains(sessionId));
    _idSet.add(sessionId);
  }

  // Assuming that SessionIdGenerator is a static member in DbusEventBuffer, verify that
  // generating IDs via M parallel threads (each generating N ids sequentially) does not
  // result in duplicate session IDs.
  @Test
  public void testSessionIdGeneration() throws Exception
  {
    class IdGeneratorThread implements Runnable
    {
      private final DbusEventBuffer.SessionIdGenerator _idGenerator;
      private final int _nTimes;
      private final String _ids[];
      public IdGeneratorThread(DbusEventBuffer.SessionIdGenerator idGenerator, int nTimes)
      {
        _idGenerator = idGenerator;
        _nTimes = nTimes;
        _ids = new String[_nTimes];
      }

      @Override
      public void run()
      {
        for (int i = 0; i < _nTimes; i++)
        {
          _ids[i] = _idGenerator.generateSessionId();
        }

        for (int i = 0; i < _nTimes; i++)
        {
          try
          {
            addId(_ids[i]);
          }
          catch (Exception e)
          {
            throw new RuntimeException("Could not add Id" + _ids[i]);
          }
        }
        thrCounter.decrementAndGet();
      }
    }

    final int nThreads = 20;
    final int nTimes = 300;
    final Thread threads[] = new Thread[nThreads];
    final DbusEventBuffer.SessionIdGenerator idGenerator = new DbusEventBuffer.SessionIdGenerator();

    for (int i = 0; i < nThreads; i++)
    {
      threads[i] = new Thread(new IdGeneratorThread(idGenerator, nTimes), "idgen_thread_" + i);
    }

    thrCounter.set(nThreads);
    for (int i = 0; i < nThreads; i++)
    {
      threads[i].start();
    }

    for (int i = 0; i < nThreads; i++)
    {
      threads[i].join();
    }
    Assert.assertEquals(0, thrCounter.get());
  }

  @Test
  public void testConfigBuilder() throws InvalidConfigException
  {
    final int K = 1024;

    DbusEventBuffer.Config cfgBuilder = new DbusEventBuffer.Config();
    cfgBuilder.setAllocationPolicy(AllocationPolicy.HEAP_MEMORY.toString());
    DbusEventBuffer.StaticConfig cfg;
    DbusEventBuffer buf;

    //default maxEventSize
    cfgBuilder.setEnableScnIndex(false);
    cfgBuilder.setMaxIndividualBufferSize(10 * K);
    cfgBuilder.setMaxSize(11 * K);
    cfgBuilder.setAverageEventSize(5 * K);
    cfg = cfgBuilder.build();
    Assert.assertEquals(10 * K, cfg.getMaxIndividualBufferSize());
    Assert.assertEquals(11 * K, cfg.getMaxSize());
    Assert.assertEquals(5 * K, cfg.getReadBufferSize());
    Assert.assertEquals(cfgBuilder.maxMaxEventSize(), cfg.getMaxEventSize());

    buf = new DbusEventBuffer(cfg);
    Assert.assertEquals(2, buf.getBuffer().length);
    Assert.assertEquals(cfg.getMaxIndividualBufferSize(), buf.getBuffer()[0].capacity());
    Assert.assertEquals(cfg.getMaxSize() % cfg.getMaxIndividualBufferSize(), buf.getBuffer()[1].capacity());
    Assert.assertEquals(cfg.getMaxEventSize(), buf.getMaxReadBufferCapacity());

    //explicit maxEventSize
    cfgBuilder.setMaxEventSize(4 * K);
    cfg = cfgBuilder.build();
    Assert.assertEquals(4 * K, cfg.getReadBufferSize());
    Assert.assertEquals(4 * K, cfg.getMaxEventSize());

    //maxEventSize and readBufferSize configs out of bounds
    buf = new DbusEventBuffer(cfg);
    Assert.assertEquals(cfg.getMaxEventSize(), buf.getMaxReadBufferCapacity());

    //maxEventSize and readBufferSizes too big
    cfgBuilder.setMaxEventSize(cfgBuilder.maxMaxEventSize() + 1);
    cfgBuilder.setAverageEventSize(cfgBuilder.getMaxEventSize() + 1);

    cfg = cfgBuilder.build();
    Assert.assertEquals(cfgBuilder.maxMaxEventSize(), cfg.getReadBufferSize());
    Assert.assertEquals(cfgBuilder.maxMaxEventSize(), cfg.getMaxEventSize());
  }

  protected void readAndCompareIteratorEvents(DbusEventIterator iter,
                                              List<DbusEvent> expectedEvents,
                                              final int startIdx, final int endIdx,
                                              boolean prefixMatch, boolean removeAfterRead,
                                              boolean refreshIfNeeded)
  {
    int i = startIdx;
    long lastScn = -1;
    boolean waitForMore = true;
    do
    {
      while (iter.hasNext())
      {
        DbusEvent actualEvent = iter.next();
        if (actualEvent.isEndOfPeriodMarker())
        {
          Assert.assertEquals(actualEvent.sequence(), lastScn);
          lastScn = -1;
          continue;
        }
        if (-1 == lastScn)
        {
          lastScn = actualEvent.sequence();
        }
        else
        {
          Assert.assertEquals(actualEvent.sequence(), lastScn);
        }
        if (i >= endIdx)
        {
          if (prefixMatch)
          {
            break; //we are good
          }
          Assert.fail("unexpected event:" + actualEvent);
        }

        DbusEvent expectedEvent = expectedEvents.get(i);
        Assert.assertEquals("event mismatch for index " + i +
                            ";\n expected:" + expectedEvent +
                            ";\n   actual: " + actualEvent,
                            expectedEvent, actualEvent);
        i++;
      }
      waitForMore = refreshIfNeeded && (i < endIdx);
      if (waitForMore)
      {
        waitForMore = iter.await(100, TimeUnit.MILLISECONDS);
      }
    }
    while (waitForMore);
    Assert.assertTrue((endIdx == i) && (prefixMatch || !iter.hasNext()));
    if (removeAfterRead)
    {
      iter.remove();
    }
  }

} // end of class TestDbusEventBuffer


class ErrorCountingAppender extends AppenderSkeleton
{
  int _errorNum = 0;

  public int getErrorNum()
  {
    return _errorNum;
  }

  @Override
  public void close()
  {
    //NOOP
  }

  @Override
  public boolean requiresLayout()
  {
    return false;
  }

  @Override
  protected void append(LoggingEvent event)
  {
    if (event.getLevel().equals(Level.ERROR) || event.getLevel().equals(Level.FATAL))
    {
      ++_errorNum;
    }
  }

  public int resetErrorNum()
  {
    int v = _errorNum;
    _errorNum = 0;
    return v;
  }

}

class ReadEventsTestParams
{
  public String      _testName;
  public long         _startScn;
  public int         _srcBufferSize;
  public int         _numSrcEvents;
  public int         _maxWindowSize;
  public int         _destBufferSize;
  public int         _destIndividualBufferSize;
  public int         _destStgBufferSize;
  public QueuePolicy _destQueuePolicy;
  public Level       _logLevel       = Level.ERROR;
  public int         _numStreamedEvents;
  public boolean     _dataValidation = true;
  public Logger _log;
  public DbusEventBuffer.StaticConfig _srcBufferCfg;
  public DbusEventBuffer _srcBuf;
  public DbusEventsStatisticsCollector _srcBufStats =
      new DbusEventsStatisticsCollector(1, "src", true, false, null);;
  public DbusEventBuffer.StaticConfig _destBufferCfg;
  public DbusEventBuffer _destBuf;
  public DbusEventsStatisticsCollector _destBufStats =
      new DbusEventsStatisticsCollector(1, "dest", true, false, null);;
  public ByteArrayOutputStream _srcByteStr;
  public DbusEventGenerator _evGen;
  public Vector<DbusEvent> _srcEvents;
  public boolean _expectDestReadError = false;
  public boolean _debuggingMode = false; //increase timeouts to let debugging through code
  public int _eventSize = 100;

  public ReadEventsTestParams()
  {
  }

  public ReadEventsTestParams(String testName,
                              int startScn,
                              int srcBufferSize,
                              int numSrcEvents,
                              int maxWindowSize,
                              int destBufferSize,
                              int destIndividualBufferSize,
                              int destStgBufferSize,
                              QueuePolicy destQueuePolicy)
  {
    _testName = testName;
    _startScn = startScn;
    _srcBufferSize = srcBufferSize;
    _numSrcEvents = numSrcEvents;
    _maxWindowSize = maxWindowSize;
    _destBufferSize = destBufferSize;
    _destIndividualBufferSize = destIndividualBufferSize;
    _destStgBufferSize = destStgBufferSize;
    _destQueuePolicy = destQueuePolicy;
  }

  public void createLogger()
  {
    _log = Logger.getLogger("TestDbusEventBuffer." + _testName);
    _log.setLevel(_logLevel);
  }

  public void createSrcBuffer() throws InvalidConfigException
  {
    if (_srcBufferSize <= 0) return;
    _log.info("Creating source buffer");
    _srcBufferCfg = TestDbusEventBuffer.getConfig(_srcBufferSize,
                                                  _srcBufferSize, 512, 512,
                                                  AllocationPolicy.HEAP_MEMORY,
                                                  QueuePolicy.BLOCK_ON_WRITE,
                                                  AssertLevel.ALL);
    _srcBuf = new DbusEventBuffer(_srcBufferCfg);
    _srcBuf.start(0);
  }

  public void createDestBuffer() throws InvalidConfigException
  {
    if (_destBufferSize <= 0) return;
    _destBufferCfg = TestDbusEventBuffer.getConfig(_destBufferSize, _destIndividualBufferSize, 512,
                                                    _destStgBufferSize,
                                                    AllocationPolicy.HEAP_MEMORY,
                                                    _destQueuePolicy, AssertLevel.NONE);
    _destBuf = new DbusEventBuffer(_destBufferCfg);
    _destBuf.getLog().setLevel(_logLevel);
    _destBuf.start(0);
  }

  public Vector<DbusEvent> generateAndStreamEvents()
  throws Exception
 {
    final Vector<DbusEvent> srcEvents = generateAndAppendEvents();
    streamEvents();
    return srcEvents;
 }

  public void streamEvents() throws ScnNotFoundException,
      OffsetNotFoundException
  {
    _srcByteStr = new ByteArrayOutputStream();
    final WritableByteChannel srcChannel = Channels.newChannel(_srcByteStr);
    Checkpoint cp = new Checkpoint();
    cp.setFlexible();
    _srcBufStats.reset();

    StreamEventsArgs args = new StreamEventsArgs(_srcBufferSize).setStatsCollector(_srcBufStats);

    _numStreamedEvents =
        _srcBuf.streamEvents(cp, srcChannel, args).getNumEventsStreamed();
    Assert.assertEquals(_numSrcEvents, _srcBufStats.getTotalStats().getNumDataEvents());
  }

  public Vector<DbusEvent> generateAndAppendEvents()
  throws Exception
  {
    generateEvents();
    //generate binary representation
    appendGeneratedEvents();
    return _srcEvents;
  }

  public void generateEvents()
  {
    _log.info("Generating events");
    _evGen = new DbusEventGenerator(_startScn);
    _srcEvents = new Vector<DbusEvent>(_numSrcEvents);
    Assert.assertTrue(_evGen.generateEvents(_numSrcEvents, _maxWindowSize, _eventSize + 300, _eventSize, _srcEvents) > 0);
  }

  public void appendGeneratedEvents()
  throws Exception
  {
    final DbusEventAppender appender = new DbusEventAppender(_srcEvents, _srcBuf, _srcBufStats);
    appender.run();
    Assert.assertEquals(_srcEvents.size(), _srcBufStats.getTotalStats().getNumDataEvents());
  }

  public void setup() throws InvalidConfigException
  {
    createLogger();
    createSrcBuffer();
    createDestBuffer();
  }

  public void runAndValidateReadEventsCall() throws InterruptedException, IOException
  {
    readDataAtDestination();
    if (_dataValidation)
    {
      validateDestData();
    }
  }

  private void validateDestData()
  {
    _log.info("Verifying the events");
    Vector<DbusEvent> destEvents1 = new Vector<DbusEvent>(_srcEvents.size());
    DbusEventBufferConsumer consumer =
        new DbusEventBufferConsumer(_destBuf, _srcEvents.size(), 0, destEvents1);
    final long timeout = _debuggingMode ? 100000000 : 1000;
    boolean consumerDone = consumer.runWithTimeout(timeout);
    Assert.assertTrue(consumerDone);
    TestDbusEventBuffer.checkEvents(_srcEvents, destEvents1, _srcEvents.size());
    _log.info("Verified events: " + _srcEvents.size());
  }

  public void readDataAtDestination() throws InterruptedException, IOException
  {
    final ReadableByteChannel readChannel1 =
        Channels.newChannel(new ByteArrayInputStream(_srcByteStr.toByteArray()));
    final AtomicInteger eventsRead1 = new AtomicInteger(-1);
    final AtomicBoolean hasError1 = new AtomicBoolean(false);

    _log.info("Reading events on client side");
    //run the read in a separate thread in case it hangs
    Thread readThread1 = new Thread(new Runnable()
    {
      @Override
      public void run()
      {
        try
        {
          eventsRead1.set(_destBuf.readEvents(readChannel1, _destBufStats));
        }
        catch (InvalidEventException e)
        {
          _log.error("readEvents error: " + e.getMessage(), e);
          hasError1.set(true);
        }
      }
    }, "readEvents" );
    readThread1.setDaemon(true);
    readThread1.start();

    final long timeout = _debuggingMode ? 100000000 : 1000;
    readThread1.join(timeout);
    readChannel1.close();

    //smoke tests
    Assert.assertTrue(!readThread1.isAlive());
    Assert.assertEquals(_expectDestReadError, hasError1.get());
    if (!_expectDestReadError) Assert.assertEquals(_numStreamedEvents, eventsRead1.get());
  }

  public void runReadEventsTests()
  throws Exception
  {
    setup();
    generateAndStreamEvents();
    runAndValidateReadEventsCall();
  }

  public ReadEventsTestParams testName(String testName)
  {
    _testName = testName;
    return this;
  }

  public ReadEventsTestParams startScn(long startScn)
  {
    _startScn = startScn;
    return this;
  }

  public ReadEventsTestParams srcBufferSize(int srcBufferSize)
  {
    _srcBufferSize = srcBufferSize;
    return this;
  }

  public ReadEventsTestParams numSrcEvents(int numSrcEvents)
  {
    _numSrcEvents = numSrcEvents;
    return this;
  }

  public ReadEventsTestParams maxWindowSize(int maxWindowSize)
  {
    _maxWindowSize = maxWindowSize;
    return this;
  }

  public ReadEventsTestParams destBufferSize(int destBufferSize)
  {
    _destBufferSize = destBufferSize;
    return this;
  }

  public ReadEventsTestParams destIndividualBufferSize(int destIndividualBufferSize)
  {
    _destIndividualBufferSize = destIndividualBufferSize;
    return this;
  }

  public ReadEventsTestParams destStgBufferSize(int destStgBufferSize)
  {
    _destStgBufferSize = destStgBufferSize;
    return this;
  }

  public ReadEventsTestParams destQueuePolicy(QueuePolicy destQueuePolicy)
  {
    _destQueuePolicy = destQueuePolicy;
    return this;
  }

  public ReadEventsTestParams logLevel(Level logLevel)
  {
    _logLevel = logLevel;
    return this;
  }

  public ReadEventsTestParams dataValidation(boolean enabled)
  {
    _dataValidation = enabled;
    return this;
  }

  public ReadEventsTestParams expectReadError(boolean enabled)
  {
    _expectDestReadError = enabled;
    return this;
  }

  public ReadEventsTestParams debuggingMode(boolean enabled)
  {
    _debuggingMode = enabled;
    return this;
  }

  /**
   * the size of events to generate
   */
  public ReadEventsTestParams eventSize(int eventSize)
  {
    _eventSize = eventSize;
    return this;
  }
}

