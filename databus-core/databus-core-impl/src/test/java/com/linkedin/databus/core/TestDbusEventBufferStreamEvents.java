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
package com.linkedin.databus.core;

import junit.framework.Assert;
import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.core.DbusEventBuffer.AllocationPolicy;
import com.linkedin.databus.core.DbusEventBuffer.QueuePolicy;
import com.linkedin.databus.core.DbusEventBuffer.StreamingMode;
import com.linkedin.databus.core.test.DbusEventAppender;
import com.linkedin.databus.core.test.DbusEventGenerator;
import com.linkedin.databus.core.util.UncaughtExceptionTrackingThread;
import com.linkedin.databus2.core.AssertLevel;
import com.linkedin.databus2.core.filter.AllowAllDbusFilter;
import com.linkedin.databus2.test.TestUtil;

/**
 * Test cases centered around DbusEventBuffer.streamEvents()
 */
public class TestDbusEventBufferStreamEvents
{
  @BeforeClass
  public void setUp()
  {
    TestUtil.setupLoggingWithTimestampedFile(true, "/tmp/TestDbusEventBufferStreamEvents_",
                                             ".log", Level.ERROR);
  }

  @Test
  /** Tests streamEvents with a large scn with concurrent writes  */
  public void testStreamLargeScn() throws Exception
  {
    final Logger log = Logger.getLogger("TestDbusEventBuffer.testStreamLargeScn");
    //log.setLevel(Level.INFO);
    log.info("starting");
    final DbusEventBuffer dbusBuf =
        new DbusEventBuffer(TestDbusEventBuffer.getConfig(
            120000, 500000, 1024, 500, AllocationPolicy.HEAP_MEMORY, QueuePolicy.OVERWRITE_ON_WRITE,
            AssertLevel.ALL));

    final Checkpoint cp = Checkpoint.createOnlineConsumptionCheckpoint(1000);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final WritableByteChannel ochannel = Channels.newChannel(baos);
    final AtomicBoolean hasError = new AtomicBoolean(false);
    final AtomicInteger num = new AtomicInteger(0);
    final AtomicBoolean genComplete = new AtomicBoolean(false);
    UncaughtExceptionTrackingThread streamThread = new UncaughtExceptionTrackingThread(new Runnable()
    {
      @Override
      public void run()
      {
        try
        {
          while (!genComplete.get() && 0 >= num.get())
          {
            StreamEventsArgs args = new StreamEventsArgs(10000);
            num.set(dbusBuf.streamEvents(cp, ochannel, args).getNumEventsStreamed());
          }
        }
        catch (ScnNotFoundException e)
        {
          hasError.set(true);
        }
        catch (OffsetNotFoundException e)
        {
          hasError.set(true);
        }
      }
    }, "testGetLargeScn.streamThread");
    streamThread.setDaemon(true);
    streamThread.start();

    log.info("append initial events");
    DbusEventGenerator generator = new DbusEventGenerator();
    Vector<DbusEvent> events = new Vector<DbusEvent>();
    generator.generateEvents(70000, 1, 120, 39, events);

    // Add events to the EventBuffer. Now the buffer is full
    DbusEventAppender appender = new DbusEventAppender(events, dbusBuf, null);
    appender.run(); // running in the same thread
    genComplete.set(true);

    streamThread.join(10000);

    Assert.assertFalse(streamThread.isAlive());
    Assert.assertNull(streamThread.getLastException());
    Assert.assertFalse(hasError.get());
    Assert.assertTrue(num.get() > 0);
  }

}
