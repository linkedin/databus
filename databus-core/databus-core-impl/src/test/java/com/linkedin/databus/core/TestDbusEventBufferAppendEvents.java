/**
 *
 */
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

import java.io.File;
import java.util.Arrays;
import java.util.Vector;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.core.DbusEventBuffer.AllocationPolicy;
import com.linkedin.databus.core.DbusEventBuffer.QueuePolicy;
import com.linkedin.databus.core.test.DbusEventAppender;
import com.linkedin.databus.core.test.DbusEventBufferReflector;
import com.linkedin.databus.core.test.DbusEventGenerator;
import com.linkedin.databus.core.util.BufferPositionParser;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.AssertLevel;
import com.linkedin.databus2.test.TestUtil;

/**
 * Seperate some tests from TestDbusEventBuffer since it has grown a lot.
 */
public class TestDbusEventBufferAppendEvents
{

  @BeforeClass
  public void setupClass()
  {
    TestUtil.setupLogging(true, "TestDbusEventBufferAppendEvents" + System.currentTimeMillis() +
                          ".log", Level.INFO);
  }


  @Test
  /**
   * Test the case where we have a big event that overlaps both the DbusEventBuffer head and the
   * limit of the current buffer.
   */
  public void testAppendBigEventsHeadScnOverlap()
  throws Exception
  {
    final Logger log = Logger.getLogger("TestDbusEventBuffer.testAppendBigEventsHeadScnOverlap");
    log.setLevel(Level.INFO);
    log.info("starting");
    final DbusEventBuffer dbusBuf =
        new DbusEventBuffer(TestDbusEventBuffer.getConfig(
            1200, 500, 100, 500, AllocationPolicy.HEAP_MEMORY, QueuePolicy.OVERWRITE_ON_WRITE,
            AssertLevel.ALL));
    BufferPositionParser parser = dbusBuf.getBufferPositionParser();
    log.info("append initial events");
    DbusEventGenerator generator = new DbusEventGenerator();
    Vector<DbusEvent> events = new Vector<DbusEvent>();
    generator.generateEvents(7, 1, 120, 39, events);

    // Add events to the EventBuffer. Now the buffer is full
    DbusEventAppender appender = new DbusEventAppender(events,dbusBuf,null);
    appender.run(); // running in the same thread
    log.info("Head:" + parser.toString(dbusBuf.getHead()) + ", Tail:" + parser.toString(dbusBuf.getTail()));
    log.info("Num buffers: " + dbusBuf.getBuffer().length);
    log.info("Buffer: " + Arrays.toString(dbusBuf.getBuffer()));

    long headPos = dbusBuf.getHead();
    long tailPos = dbusBuf.getTail();
    long headGenId = parser.bufferGenId(headPos);
    long headIndexId = parser.bufferIndex(headPos);
    long headOffset = parser.bufferOffset(headPos);
    long tailGenId = parser.bufferGenId(tailPos);
    long tailIndexId = parser.bufferIndex(tailPos);
    long tailOffset = parser.bufferOffset(tailPos);

    assertEquals(0, headGenId);
    assertEquals(0, headIndexId);
    assertEquals(222, headOffset);
    assertEquals(1, tailGenId);
    assertEquals(0, tailIndexId);
    assertEquals(61, tailOffset);

    log.info("append windows with one small and one big event");
    generator = new DbusEventGenerator(100);
    events = new Vector<DbusEvent>();
    generator.generateEvents(1, 1, 280, 139, events);
    generator.generateEvents(1, 1, 480, 339, events);

    // Add events to the EventBuffer. Now the buffer is full
    appender = new DbusEventAppender(events,dbusBuf,null);
    appender.run(); // running in the same thread
    log.info("Head:" + parser.toString(dbusBuf.getHead()) + ", Tail:" + parser.toString(dbusBuf.getTail()));
    log.info("Num buffers: " + dbusBuf.getBuffer().length);
    log.info("Buffer: " + Arrays.toString(dbusBuf.getBuffer()));

    headPos = dbusBuf.getHead();
    tailPos = dbusBuf.getTail();
    headGenId = parser.bufferGenId(headPos);
    headIndexId = parser.bufferIndex(headPos);
    headOffset = parser.bufferOffset(headPos);
    tailGenId = parser.bufferGenId(tailPos);
    tailIndexId = parser.bufferIndex(tailPos);
    tailOffset = parser.bufferOffset(tailPos);

    assertEquals(0, headGenId);
    assertEquals(2, headIndexId);
    assertEquals(61, headOffset);
    assertEquals(1, tailIndexId);
    assertEquals(461, tailOffset);
    assertEquals(322, dbusBuf.getBuffer()[0].limit());

    log.info("finished");
   }

  @Test
  /**
   * Test the following case:
   * A buffer (one of 3) has CWP = 222 and head at 383. Limit is 483 (capacity 500)
   * we create one event of size 211 (61 + 150 payload).
   * we call appender.run() which will add one EOP event (61) and the newly created event 211.
   * The end of event offset will be 222 + 211 + 61 = 494. It should update limit to this value.
   * because of the bug (DDSDBUS-1515) it used to fail when end of event goes beyond current limit
   * but less than the capacity
   * Now it should pass
   */
  public void testAppendEventWhenLimitLessThanCapacity()
  throws Exception
  {
    final Logger log = Logger.getLogger("TestDbusEventBuffer.testAppendEventWhenLimitLessThanCapacity");
    log.setLevel(Level.INFO);
    log.info("starting");
    final DbusEventBuffer dbusBuf =
        new DbusEventBuffer(TestDbusEventBuffer.getConfig(
            1200, 500, 100, 500, AllocationPolicy.HEAP_MEMORY, QueuePolicy.OVERWRITE_ON_WRITE,
            AssertLevel.ALL));
    BufferPositionParser parser = dbusBuf.getBufferPositionParser();
    log.info("append initial events");
    DbusEventGenerator generator = new DbusEventGenerator();
    Vector<DbusEvent> events = new Vector<DbusEvent>();
    generator.generateEvents(11, 1, 120, 39, events);

    // Add events to the EventBuffer. Now the buffer is full
    DbusEventAppender appender = new DbusEventAppender(events,dbusBuf,null);
    appender.run(); // running in the same thread
    log.info("Head:" + parser.toString(dbusBuf.getHead()) + ",Tail:" + parser.toString(dbusBuf.getTail()));
    log.info("Num buffers :" + dbusBuf.getBuffer().length);
    log.info("Buffer :" + Arrays.toString(dbusBuf.getBuffer()));

    long headPos = dbusBuf.getHead();
    long tailPos = dbusBuf.getTail();
    long headGenId = parser.bufferGenId(headPos);
    long headIndexId = parser.bufferIndex(headPos);
    long headOffset = parser.bufferOffset(headPos);
    long tailGenId = parser.bufferGenId(tailPos);
    long tailIndexId = parser.bufferIndex(tailPos);
    long tailOffset = parser.bufferOffset(tailPos);

    // current writing position should be 222 (id=1)
    assertEquals(0, headGenId);
    assertEquals(1, headIndexId);
    assertEquals(383, headOffset);
    assertEquals(1, tailGenId);
    assertEquals(1, tailIndexId);
    assertEquals(222, tailOffset);


    log.info("append event to stretch beyond limit but less than capacity");
    generator = new DbusEventGenerator(100);
    events = new Vector<DbusEvent>();
    generator.generateEvents(1, 1, 400, 150, events); // will add two event 61 + 150

    // Add events. this will cause limit increased
    appender = new DbusEventAppender(events,dbusBuf,null);
    appender.run();
    log.info("Head:" + parser.toString(dbusBuf.getHead()) + ",Tail:" + parser.toString(dbusBuf.getTail()));
    log.info("Num buffers :" + dbusBuf.getBuffer().length);
    log.info("Buffer :" + Arrays.toString(dbusBuf.getBuffer()));


    headPos = dbusBuf.getHead();
    tailPos = dbusBuf.getTail();
    headGenId = parser.bufferGenId(headPos);
    headIndexId = parser.bufferIndex(headPos);
    headOffset = parser.bufferOffset(headPos);
    tailGenId = parser.bufferGenId(tailPos);
    tailIndexId = parser.bufferIndex(tailPos);
    tailOffset = parser.bufferOffset(tailPos);

    assertEquals(1, headGenId);
    assertEquals(0, headIndexId);
    assertEquals(61, headOffset);
    assertEquals(2, tailIndexId);
    assertEquals(61, tailOffset);
    assertEquals(494, dbusBuf.getBuffer()[1].limit());

    log.info("finished");
   }

  @Test
  /**
   * verify that changes to the buffer (appendEvent, endEvent, clear) after the buffer is closed
   * are ignored (and rolled back)
   * we do this by creating buffer (or loading from mmap files from the previous run), adding one window,
   * then doing append, close , append/endEvents (which should throw an exception)
   * then loading again and verifying that it has our events
   */
  public void testAppendEventsWithCloseInMiddle()
  throws Exception
  {
    final Logger log = Logger.getLogger("TestDbusEventBuffer.testAppendEventsWithCloseInMiddle");
    log.setLevel(Level.DEBUG);
    log.info("starting");

    // mmap dir
    File mmapDir = initMmapDir("/tmp/tmp_mmapDir");

    DbusEventBuffer.StaticConfig config = getConfig(1200, 500, 100, 500,
              AllocationPolicy.MMAPPED_MEMORY, mmapDir.getAbsolutePath(), true);
    DbusEventBuffer dbusBuf =  new DbusEventBuffer(config);

    DbusEventBufferReflector bufferReflector = new DbusEventBufferReflector(dbusBuf);
   // BufferPositionParser parser = dbusBuf.getBufferPositionParser();
    log.info("append initial events");
    DbusEventGenerator generator = new DbusEventGenerator();
    Vector<DbusEvent> events = new Vector<DbusEvent>();
    generator.generateEvents(12, 2, 120, 39, events);


    // add first window with 2 events
    dbusBuf.start(-1);
    int i=0;

    addOneEvent(events.get(i++), dbusBuf, EventType.START);
    addOneEvent(events.get(i++), dbusBuf, EventType.END);
    checkEventsInBuffer(bufferReflector, 3); // 2 + 1(EOW)

    // add first event of the second window
    addOneEvent(events.get(i++), dbusBuf, EventType.START);
    checkEventsInBuffer(bufferReflector, 3); // 2 + 1(EOW)

    // now close the buffer
    dbusBuf.closeBuffer(true);
    checkEventsInBuffer(bufferReflector, 3); // 2 + 1(EOW)

    // now add the end
    try {
      addOneEvent(events.get(i), dbusBuf, EventType.END);
    } catch (Throwable ex) {
      log.info("Got e: ", ex);
    }
    checkEventsInBuffer(bufferReflector, 3); // 2 + 1(EOW)

    // create new buffer
    dbusBuf =  new DbusEventBuffer(config); // should load from mmap
    bufferReflector = new DbusEventBufferReflector(dbusBuf);
    checkEventsInBuffer(bufferReflector, 3); // 2 + 1(EOW)

    addOneEvent(events.get(i++), dbusBuf, EventType.START);
    addOneEvent(events.get(i++), dbusBuf, EventType.END);
    checkEventsInBuffer(bufferReflector, 6); // 4 + 2(EOW)

    // add two more events but don't do endEvents
    addOneEvent(events.get(i++), dbusBuf, EventType.START);
    addOneEvent(events.get(i++), dbusBuf, EventType.REG); // no endEvents()
    // now close the buffer
    dbusBuf.closeBuffer(true);
    dbusBuf.closeBuffer(true); // should be ok (WARN in the logs)
    checkEventsInBuffer(bufferReflector, 6); // 4 + 2(EOW)

    // call endEvents(on a closed buffer);
    try {
      dbusBuf.endEvents(events.get(i-1).sequence());
    } catch (Throwable ex) {
      log.info("Got e2: ", ex);
    }
    checkEventsInBuffer(bufferReflector, 6); // 4 + 2(EOW)



    // create new buffer
    dbusBuf =  new DbusEventBuffer(config); // should load from mmap
    bufferReflector = new DbusEventBufferReflector(dbusBuf);
    checkEventsInBuffer(bufferReflector, 6); // 4 + 2(EOW)

    addOneEvent(events.get(i++), dbusBuf, EventType.START);
    addOneEvent(events.get(i++), dbusBuf, EventType.END);
    checkEventsInBuffer(bufferReflector, 9); // 6 + 3(EOW)

    // add two more events but don't do endEvents
    addOneEvent(events.get(i++), dbusBuf, EventType.START);
    addOneEvent(events.get(i++), dbusBuf, EventType.REG); // no endEvents()
    // now close the buffer
    dbusBuf.closeBuffer(true);
    checkEventsInBuffer(bufferReflector, 9); // 6 + 3(EOW)

    // call endEvents(on a closed buffer);
    try {
      dbusBuf.clear();  //should fail
    } catch (Throwable ex) {
      log.info("Got e3: ", ex);
    }
    checkEventsInBuffer(bufferReflector, 9); // 6 + 3(EOW)

    // make sure it is still valid
    dbusBuf =  new DbusEventBuffer(config); // should load from mmap
    bufferReflector = new DbusEventBufferReflector(dbusBuf);
    checkEventsInBuffer(bufferReflector, 9); // 6 + 3(EOW)

  }

  DbusEventBuffer.StaticConfig getConfig(long maxEventBufferSize, int maxIndividualBufferSize,
                                         int maxIndexSize, int maxReadBufferSize,
                                         AllocationPolicy allocationPolicy, String mmapDirectory,
                                         boolean restoreMMappedBuffers)
                                             throws InvalidConfigException
  {
    DbusEventBuffer.Config config = new DbusEventBuffer.Config();
    config.setMaxSize(maxEventBufferSize);
    config.setMaxIndividualBufferSize(maxIndividualBufferSize);
    config.setScnIndexSize(maxIndexSize);
    config.setAverageEventSize(maxReadBufferSize);
    config.setAllocationPolicy(allocationPolicy.name());
    config.setRestoreMMappedBuffers(restoreMMappedBuffers);
    config.setMmapDirectory(mmapDirectory);
    //config.setQueuePolicy(policy.toString());
    //config.setAssertLevel(null != assertLevel ? assertLevel.toString(): AssertLevel.NONE.toString());
    config.setAssertLevel(AssertLevel.NONE.toString());
    return config.build();
  }

  enum EventType {
    START,
    END,
    REG
  };

  private void checkEventsInBuffer(DbusEventBufferReflector bufferReflector, int expCount) {

    Assert.assertTrue(bufferReflector.validateBuffer());
    DbusEventBuffer buf = bufferReflector.getDbusEventBuffer();

    DbusEventBuffer.DbusEventIterator iter = buf.acquireIterator("myIter1");
    Assert.assertNotNull(iter);
    int count = 0;
    while(iter.hasNext()) {
      DbusEvent e = iter.next();
      Assert.assertNotNull(e);
      System.out.println("seq=" + e.sequence() + "sys:" + e.isEndOfPeriodMarker());
      count ++;
    }
    iter.close();
    Assert.assertEquals("doesn't match expected number of events in the buffer", expCount, count);
  }

  private void addOneEvent(DbusEvent ev, DbusEventBuffer buf, EventType eventType) {
    if(eventType.equals(EventType.START))
      buf.startEvents();

    byte[] payload = new byte[((DbusEventInternalReadable)ev).payloadLength()];
    ev.value().get(payload);
    buf.appendEvent(new DbusEventKey(ev.key()),
                        ev.physicalPartitionId(),
                        ev.logicalPartitionId(),
                        ev.timestampInNanos(),
                        ev.srcId(),
                        ev.schemaId(),
                        payload,
                        false,
                        null);

    if(eventType.equals(EventType.END))
      buf.endEvents(ev.sequence());
  }

  private File initMmapDir(String path)
      throws Exception
  {
    File d = new File(path);
    if (d.exists())
    {
      FileUtils.cleanDirectory(d);
    } else {
      d.mkdir();
    }
    d.deleteOnExit();
    return d;
  }
}
