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


import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.io.File;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.databus.core.DbusEventBuffer.AllocationPolicy;
import com.linkedin.databus.core.DbusEventBuffer.DbusEventIterator;
import com.linkedin.databus.core.DbusEventBuffer.QueuePolicy;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus.core.util.RngUtils;

@Test(singleThreaded=true)
public class TestEventLogWriter
{

  public static final String MODULE = TestEventLogWriter.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);


  private final long timeStamp = 3456L;
  private final short partitionId = 30;
  private final short srcId = 15;
  private final byte[] schemaId = "abcdefghijklmnop".getBytes();
  //private DbusEventBuffer dbuf;
  private final File _writeDir = new File("eventLogTest");



  static
  {
    BasicConfigurator.configure();
    Logger.getRootLogger().setLevel(Level.OFF);
    //Logger.getRootLogger().setLevel(Level.INFO);
  }

  @BeforeMethod
  public void setUp() throws Exception
  {

  }

  @AfterMethod
  public void tearDown() throws Exception
  {
    System.gc();

    assertTrue(_writeDir.isDirectory());
    for (File f: _writeDir.listFiles())
    {
      if (f.isDirectory())
      {
        for (File fil: f.listFiles())
        {
          assertTrue(fil.delete());
        }
      }
      assertTrue(f.delete());
    }
    assertTrue(_writeDir.canWrite());
    assertTrue(_writeDir.delete());

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

  @Test
  public void testRunEventLogWriter() throws InterruptedException, InvalidConfigException
  {
    DbusEventBuffer dbuf  = new DbusEventBuffer(getConfig(10000000, DbusEventBuffer.Config.DEFAULT_INDIVIDUAL_BUFFER_SIZE, 100000, 1000000, AllocationPolicy.MMAPPED_MEMORY, QueuePolicy.OVERWRITE_ON_WRITE));

    String sessionDir = _writeDir + File.separator + "testEventLogWriter";
    performWrites(dbuf, _writeDir, sessionDir, Encoding.JSON, 1, 50000);

  }

  private long performWrites(DbusEventBuffer dbuf, File topLevelDir, String sessionDir, Encoding encoding, long startScn, int numEntries) throws InterruptedException
  {
    EventLogWriter logWriter = new EventLogWriter(
                                                  new EventLogWriter.StaticConfig(dbuf, true,
                                                                                  topLevelDir.getAbsolutePath(),
                                                                                  sessionDir,
                                                                                  encoding,
                                                                                  64000,
                                                                                  (encoding == Encoding.BINARY)?false:true,
                                                                                  1000000,
                                                                                  20,
                                                                                  20,
                                                                                  1000));

    Thread logWriterThread = new Thread(logWriter);
    logWriterThread.start();
    int eventWindowSize = 20;
    long lastWindowScn = startScn;
    //HashMap<Long, KeyValue> testDataMap = new HashMap<Long, KeyValue>(20000);
    // prime the buffer with the lowest scn
    dbuf.start(startScn);

    for (long scn=lastWindowScn+1; scn < startScn+1+numEntries; scn+=eventWindowSize) {
      //System.out.println("Iteration:"+i);
      DbusEventKey key = new DbusEventKey(RngUtils.randomLong());
      String value = RngUtils.randomString(20);
      dbuf.startEvents();
      for (int j=0; j < eventWindowSize; ++j) {
        assertTrue(dbuf.appendEvent(key, (short)0, partitionId, timeStamp, srcId, schemaId, value.getBytes(), false));

        //testDataMap.put(i, new KeyValue(key, value));
      }
      lastWindowScn = scn+eventWindowSize;
      dbuf.endEvents(lastWindowScn);
      Thread.sleep(10);
    }

    dbuf.startEvents();
    for (long scn = lastWindowScn+1; scn < lastWindowScn + 1 + 50; scn += 1)
    {
      DbusEventKey key = new DbusEventKey(RngUtils.randomLong());
      String value = RngUtils.randomString(20);
      assertTrue(dbuf.appendEvent(key, (short)0, partitionId, timeStamp, srcId, schemaId, value.getBytes(), false));
    }

    //Wait for fsync interval
    Thread.sleep(1200);

    logWriter.stop();
    logWriterThread.join();

    return lastWindowScn;
  }

  private void checkIterProperSubset(DbusEventIterator superSetIter, DbusEventIterator subSetIter)
  {
    boolean found =false;

    assertTrue(subSetIter.hasNext());
    DbusEvent firstSubSetEvent = subSetIter.next();
    boolean lastEventIsEOP= true;
    while (!found && superSetIter.hasNext())
    {
      DbusEvent event1 = superSetIter.next();
      //LOG.info("Check:" + event1.scn() + ":" + event1.windowScn());
      if (event1.equals(firstSubSetEvent))
      {
        LOG.info(event1 + " equals " + firstSubSetEvent);
        assertTrue("Last event should be eop", lastEventIsEOP);
        found = true;
      }

      lastEventIsEOP = event1.isEndOfPeriodMarker();
    }

    checkIterEquals(superSetIter, subSetIter);

  }

  private void checkIterEquals(DbusEventIterator iter1, DbusEventIterator iter2)
  {
    while (iter1.hasNext())
    {
      assertTrue("iter1 has more elements", iter2.hasNext());
      DbusEvent event1 = iter1.next();
      DbusEvent event2 = iter2.next();
      LOG.debug("Check:event1:"+event1.sequence()+"event2:"+event2.sequence());
      assertTrue(event1 + " not equal to " + event2, event1.equals(event2));
    }
    assertFalse("iter2 has more elements", iter2.hasNext());
  }

  @Test
  public void testEventLogReaderSameBuffer() throws InterruptedException, InvalidConfigException
  {
    DbusEventBuffer dbuf  = new DbusEventBuffer(getConfig(10000000, DbusEventBuffer.Config.DEFAULT_INDIVIDUAL_BUFFER_SIZE, 100000, 1000000, AllocationPolicy.MMAPPED_MEMORY, QueuePolicy.OVERWRITE_ON_WRITE));
    String sessionDir = _writeDir + File.separator + "sameBufferTest";
    //EventLogWriter.LOG.setLevel(Level.DEBUG);
    long lastWindowScn = performWrites(dbuf, _writeDir, sessionDir, Encoding.BINARY, 1, 50000);
    DbusEventBuffer readDbuf  = new DbusEventBuffer(getConfig(10000000, DbusEventBuffer.Config.DEFAULT_INDIVIDUAL_BUFFER_SIZE, 100000, 10000, AllocationPolicy.MMAPPED_MEMORY, QueuePolicy.OVERWRITE_ON_WRITE));
    EventLogReader.Config eventLogReaderConfig = new EventLogReader.Config();
    eventLogReaderConfig.setEventBuffer(readDbuf);
    eventLogReaderConfig.setEnabled(true);
    eventLogReaderConfig.setTopLevelLogDir(_writeDir.getAbsolutePath());
    eventLogReaderConfig.setReadSessionDir(sessionDir);
    EventLogReader logReader = new EventLogReader(eventLogReaderConfig.build());
    //EventLogReader.LOG.setLevel(Level.DEBUG);
    //DbusEventBuffer.LOG.setLevel(Level.DEBUG);
    Checkpoint cp = logReader.read();
    assertTrue(cp.getWindowOffset() == -1);
    assertTrue(cp.getWindowScn() == lastWindowScn);

    // Read and Write Buffers should be the same
    DbusEventIterator writeIter = dbuf.acquireIterator("testIterator");
    DbusEventIterator readIter = readDbuf.acquireIterator("testIterator");
    //LOG.setLevel(Level.DEBUG);
    checkIterEquals(writeIter, readIter);
    dbuf.releaseIterator(writeIter);
    readDbuf.releaseIterator(readIter);
  }

  @Test
  public void testEventLogReaderSameBufferJSON() throws InterruptedException, InvalidConfigException
  {
    DbusEventBuffer dbuf  = new DbusEventBuffer(getConfig(10000000, DbusEventBuffer.Config.DEFAULT_INDIVIDUAL_BUFFER_SIZE, 100000, 1000000, AllocationPolicy.MMAPPED_MEMORY, QueuePolicy.OVERWRITE_ON_WRITE));
    String sessionDir = _writeDir + File.separator + "sameBufferTestJson";
    //EventLogWriter.LOG.setLevel(Level.DEBUG);
    long lastWindowScn = performWrites(dbuf, _writeDir, sessionDir, Encoding.JSON, 1, 50000);
    DbusEventBuffer readDbuf  = new DbusEventBuffer(getConfig(10000000, DbusEventBuffer.Config.DEFAULT_INDIVIDUAL_BUFFER_SIZE, 100000, 10000, AllocationPolicy.MMAPPED_MEMORY, QueuePolicy.OVERWRITE_ON_WRITE));
    EventLogReader.Config eventLogReaderConfig = new EventLogReader.Config();
    eventLogReaderConfig.setEventBuffer(readDbuf);
    eventLogReaderConfig.setEnabled(true);
    eventLogReaderConfig.setTopLevelLogDir(_writeDir.getAbsolutePath());
    eventLogReaderConfig.setReadSessionDir(sessionDir);
    EventLogReader logReader = new EventLogReader(eventLogReaderConfig.build());
    //EventLogReader.LOG.setLevel(Level.DEBUG);
    //DbusEventBuffer.LOG.setLevel(Level.DEBUG);
    Checkpoint cp = logReader.read();
    assertTrue(cp.getWindowOffset() == -1);
    assertTrue(cp.getWindowScn() == lastWindowScn);

    // Read and Write Buffers should be the same
    DbusEventIterator writeIter = dbuf.acquireIterator("testIterator");
    DbusEventIterator readIter = readDbuf.acquireIterator("testIterator");
    //LOG.setLevel(Level.DEBUG);
    checkIterEquals(writeIter, readIter);
    dbuf.releaseIterator(writeIter);
    readDbuf.releaseIterator(readIter);
  }

  @Test
  public void testEventLogReaderSmallerBuffer() throws InterruptedException, InvalidConfigException
  {
    DbusEventBuffer dbuf  = new DbusEventBuffer(getConfig(10000000, DbusEventBuffer.Config.DEFAULT_INDIVIDUAL_BUFFER_SIZE, 100000, 1000000, AllocationPolicy.MMAPPED_MEMORY, QueuePolicy.OVERWRITE_ON_WRITE));

    String sessionDir = _writeDir + File.separator + "smallerBufferTest";
    long lastWindowScn = performWrites(dbuf, _writeDir, sessionDir, Encoding.BINARY, 1, 50000);
    //    ScnIndex.LOG.setLevel(Level.DEBUG);
    DbusEventBuffer readDbuf  = new DbusEventBuffer(getConfig(100000, DbusEventBuffer.Config.DEFAULT_INDIVIDUAL_BUFFER_SIZE, 100000, 10000, AllocationPolicy.MMAPPED_MEMORY, QueuePolicy.OVERWRITE_ON_WRITE));
    EventLogReader.Config eventLogReaderConfig = new EventLogReader.Config();
    eventLogReaderConfig.setEventBuffer(readDbuf);
    eventLogReaderConfig.setEnabled(true);
    eventLogReaderConfig.setTopLevelLogDir(_writeDir.getAbsolutePath());
    eventLogReaderConfig.setReadSessionDir(sessionDir);
    EventLogReader logReader = new EventLogReader(eventLogReaderConfig.build());

    Checkpoint cp = logReader.read();
    assertTrue(cp.getWindowOffset() == -1);
    assertTrue(cp.getWindowScn() == lastWindowScn);

    // Read and Write Buffers should be the same
    DbusEventIterator writeIter = dbuf.acquireIterator("testIterator");
    DbusEventIterator readIter = readDbuf.acquireIterator("testIterator");
    // LOG.info("First event = " + readIter.next());
    // LOG.info("Second event = " + readIter.next());
    // LOG.info("Third event = "  + readIter.next());

    checkIterProperSubset(writeIter, readIter);
    dbuf.releaseIterator(writeIter);
    readDbuf.releaseIterator(readIter);

  }

  @Test
  public void testEventLogReaderSmallerBufferJSON() throws InterruptedException, InvalidConfigException
  {
    DbusEventBuffer dbuf  = new DbusEventBuffer(getConfig(10000000, DbusEventBuffer.Config.DEFAULT_INDIVIDUAL_BUFFER_SIZE, 100000, 1000000, AllocationPolicy.HEAP_MEMORY, QueuePolicy.OVERWRITE_ON_WRITE));
    String sessionDir = _writeDir + File.separator + "smallerBufferTestJSON";
    long lastWindowScn = performWrites(dbuf, _writeDir, sessionDir, Encoding.JSON, 1, 50000);
    DbusEventBuffer readDbuf  = new DbusEventBuffer(getConfig(100000, DbusEventBuffer.Config.DEFAULT_INDIVIDUAL_BUFFER_SIZE, 100000, 10000, AllocationPolicy.HEAP_MEMORY, QueuePolicy.OVERWRITE_ON_WRITE));
    EventLogReader.Config eventLogReaderConfig = new EventLogReader.Config();
    eventLogReaderConfig.setEventBuffer(readDbuf);
    eventLogReaderConfig.setEnabled(true);
    eventLogReaderConfig.setTopLevelLogDir(_writeDir.getAbsolutePath());
    eventLogReaderConfig.setReadSessionDir(sessionDir);
    EventLogReader logReader = new EventLogReader(eventLogReaderConfig.build());

    Checkpoint cp = logReader.read();
    assertTrue(cp.getWindowOffset() == -1);
    assertTrue(cp.getWindowScn() == lastWindowScn);

    // Read and Write Buffers should be the same
    DbusEventIterator writeIter = dbuf.acquireIterator("testIterator");
    DbusEventIterator readIter = readDbuf.acquireIterator("testIterator");
    // LOG.info("First event = " + readIter.next());
    // LOG.info("Second event = " + readIter.next());
    // LOG.info("Third event = "  + readIter.next());

    checkIterProperSubset(writeIter, readIter);
    dbuf.releaseIterator(writeIter);
    readDbuf.releaseIterator(readIter);

  }

  @Test
  public void testEventLogReaderSmallerBufferJSONPlusMoreWrites() throws InterruptedException, InvalidConfigException
  {
    DbusEventBuffer dbuf  = new DbusEventBuffer(getConfig(10000000, DbusEventBuffer.Config.DEFAULT_INDIVIDUAL_BUFFER_SIZE, 100000, 1000000, AllocationPolicy.HEAP_MEMORY, QueuePolicy.OVERWRITE_ON_WRITE));
    String sessionDir = _writeDir + File.separator + "smallerBufferTestJSON";
    long lastWindowScn = performWrites(dbuf, _writeDir, sessionDir, Encoding.JSON, 1, 10000);
    DbusEventBuffer readDbuf  = new DbusEventBuffer(getConfig(100000, DbusEventBuffer.Config.DEFAULT_INDIVIDUAL_BUFFER_SIZE, 100000, 10000, AllocationPolicy.HEAP_MEMORY, QueuePolicy.OVERWRITE_ON_WRITE));
    EventLogReader.Config eventLogReaderConfig = new EventLogReader.Config();
    eventLogReaderConfig.setEventBuffer(readDbuf);
    eventLogReaderConfig.setEnabled(true);
    eventLogReaderConfig.setTopLevelLogDir(_writeDir.getAbsolutePath());
    eventLogReaderConfig.setReadSessionDir(sessionDir);
    EventLogReader logReader = new EventLogReader(eventLogReaderConfig.build());

    Checkpoint cp = logReader.read();
    assertTrue(cp.getWindowOffset() == -1);
    assertTrue(cp.getWindowScn() == lastWindowScn);

    // Read and Write Buffers should be the same
    DbusEventIterator writeIter = dbuf.acquireIterator("testIterator");
    DbusEventIterator readIter = readDbuf.acquireIterator("testIterator");
    // LOG.info("First event = " + readIter.next());
    // LOG.info("Second event = " + readIter.next());
    // LOG.info("Third event = "  + readIter.next());

    checkIterProperSubset(writeIter, readIter);
    dbuf.releaseIterator(writeIter);
    readDbuf.releaseIterator(readIter);
    dbuf.endEvents(lastWindowScn);

    lastWindowScn = performWrites(dbuf, _writeDir, sessionDir, Encoding.JSON, lastWindowScn, 200);

  }

}
