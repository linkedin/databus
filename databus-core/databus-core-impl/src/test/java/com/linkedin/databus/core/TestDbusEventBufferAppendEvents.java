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

import java.util.Arrays;
import java.util.Vector;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.core.DbusEventBuffer.AllocationPolicy;
import com.linkedin.databus.core.DbusEventBuffer.QueuePolicy;
import com.linkedin.databus.core.util.BufferPositionParser;
import com.linkedin.databus.core.util.DbusEventAppender;
import com.linkedin.databus.core.util.DbusEventGenerator;
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
   * limit of the currnt buffer.
   */
  public void testAppendBigEventsHeadScnOverlap() throws InvalidConfigException
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
    Vector<DbusEventInternalWritable> events = new Vector<DbusEventInternalWritable>();
    generator.generateEvents(7, 1, 120, 39, events);

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

    assertEquals(0, headGenId);
    assertEquals(0, headIndexId);
    assertEquals(222, headOffset);
    assertEquals(1, tailGenId);
    assertEquals(0, tailIndexId);
    assertEquals(61, tailOffset);


    log.info("append windows with a small and big event");
    generator = new DbusEventGenerator(100);
    events = new Vector<DbusEventInternalWritable>();
    generator.generateEvents(1, 1, 280, 139, events);
    generator.generateEvents(1, 1, 480, 339, events);

    // Add events to the EventBuffer. Now the buffer is full
    appender = new DbusEventAppender(events,dbusBuf,null);
    appender.run(); // running in the same thread
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

    assertEquals(2, headIndexId);
    assertEquals(61, headOffset);
    assertEquals(1, tailIndexId);
    assertEquals(461, tailOffset);
    assertEquals(322, dbusBuf.getBuffer()[0].limit());

    log.info("finished");
   }

}
