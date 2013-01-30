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



import com.linkedin.databus.core.DbusEvent.EventScanStatus;
import com.linkedin.databus.core.DbusEventBuffer.AllocationPolicy;
import com.linkedin.databus.core.DbusEventBuffer.DbusEventIterator;
import com.linkedin.databus.core.DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException;
import com.linkedin.databus.core.util.DbusEventGenerator;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.AssertLevel;
import com.linkedin.databus2.relay.config.PhysicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;



public class TestDbusEventBufferPersistence
{
  public static final Logger LOG = Logger.getLogger(TestDbusEventBufferPersistence.class);
  static {
    PatternLayout defaultLayout = new PatternLayout("%d{ISO8601} +%r [%t] (%p) {%c} %m%n");
    ConsoleAppender defaultAppender = new ConsoleAppender(defaultLayout);

    Logger.getRootLogger().removeAllAppenders();
    Logger.getRootLogger().addAppender(defaultAppender);

    Logger.getRootLogger().setLevel(Level.OFF);
    //Logger.getRootLogger().setLevel(Level.ERROR);
    //Logger.getRootLogger().setLevel(Level.INFO);
    //Logger.getRootLogger().setLevel(Level.DEBUG);
  }
  String _mmapDirStr;
  File _mmapDir;
  String _mmapBakDirStr;
  File _mmapBakDir;

  DbusEventBuffer.StaticConfig getConfig(long maxEventBufferSize, int maxIndividualBufferSize, int maxIndexSize,
                                         int maxReadBufferSize, AllocationPolicy allocationPolicy,
                                         String mmapDirectory, boolean restoreMMappedBuffers)
                                             throws InvalidConfigException
  {
    DbusEventBuffer.Config config = new DbusEventBuffer.Config();
    config.setMaxSize(maxEventBufferSize);
    config.setMaxIndividualBufferSize(maxIndividualBufferSize);
    config.setScnIndexSize(maxIndexSize);
    config.setReadBufferSize(maxReadBufferSize);
    config.setAllocationPolicy(allocationPolicy.name());
    config.setRestoreMMappedBuffers(restoreMMappedBuffers);
    config.setMmapDirectory(mmapDirectory);
    //config.setQueuePolicy(policy.toString());
    //config.setAssertLevel(null != assertLevel ? assertLevel.toString(): AssertLevel.NONE.toString());
    config.setAssertLevel(AssertLevel.NONE.toString());
    return config.build();
  }

  private File initDir(String path)
  throws Exception
  {
    File d = new File(path);
    if (d.exists())
    {
      FileUtils.deleteDirectory(d);
    }
    d.mkdir();
    d.deleteOnExit();
    return d;
  }

  @BeforeMethod
  public void setup() throws Exception {
    _mmapDirStr = "/tmp/test_mmap";
    _mmapBakDirStr = _mmapDirStr + DbusEventBufferMult.BAK_DIRNAME_SUFFIX;

    _mmapDir = initDir(_mmapDirStr);
    _mmapBakDir = initDir(_mmapBakDirStr);
  }

  @AfterMethod
  public void tearDown() throws IOException {
    if(_mmapDir != null && _mmapDir.exists()) {
      FileUtils.deleteDirectory(_mmapDir);
    }
    if (_mmapBakDir != null && _mmapBakDir.exists())
    {
      FileUtils.deleteDirectory(_mmapBakDir);
    }
  }

  @Test
  public void testMetaFileCreationMult() throws InvalidConfigException, IOException {
    int maxEventBufferSize = 1144;
    int maxIndividualBufferSize = 500;
    int bufNum = maxEventBufferSize/maxIndividualBufferSize;
    if(maxEventBufferSize % maxIndividualBufferSize > 0)
      bufNum++;

    DbusEventBuffer.StaticConfig config = getConfig(maxEventBufferSize,maxIndividualBufferSize,
                                      100,500,AllocationPolicy.MMAPPED_MEMORY, _mmapDirStr, true);


    // create buffer mult
    DbusEventBufferMult bufMult = createBufferMult(config);

    // go over all the buffers
    for(DbusEventBuffer dbusBuf : bufMult.bufIterable()) {
      File metaFile = new File(_mmapDir, dbusBuf.metaFileName());
      // check that we don't have the files
      Assert.assertFalse(metaFile.exists());
    }
    bufMult.saveBufferMetaInfo(false);
    // now the files should be there
    for(DbusEventBuffer dbusBuf : bufMult.bufIterable()) {
      File metaFile = new File(_mmapDir, dbusBuf.metaFileName());
      // check that we don't have the files
      Assert.assertTrue(metaFile.exists());
      validateFiles(metaFile, bufNum);
    }
  }

  @Test
  public void testMetaFileCloseMult() throws Exception
  {
    int maxEventBufferSize = 1144;
    int maxIndividualBufferSize = 500;
    int bufNum = maxEventBufferSize/maxIndividualBufferSize;
    if(maxEventBufferSize % maxIndividualBufferSize > 0)
      bufNum++;

    DbusEventBuffer.StaticConfig config = getConfig(maxEventBufferSize,maxIndividualBufferSize,
                                                    100,500,AllocationPolicy.MMAPPED_MEMORY, _mmapDirStr, true);

    // create buffer mult
    DbusEventBufferMult bufMult = createBufferMult(config);

    // Save all the files and validate the meta files.
    bufMult.close();
    for(DbusEventBuffer dbusBuf : bufMult.bufIterable()) {
      File metaFile = new File(_mmapDir, dbusBuf.metaFileName());
      // check that we don't have the files
      Assert.assertTrue(metaFile.exists());
      validateFiles(metaFile, bufNum);
    }
    File[] entries = _mmapDir.listFiles();

    // When we create a new multi-buffer, we should get renamed files as well as new files.
    bufMult = createBufferMult(config);
    entries = _mmapDir.listFiles(); // Has session dirs and renamed meta files.
    // Create an info file for one buffer.
    DbusEventBuffer buf = bufMult.bufIterable().iterator().next();
    buf.saveBufferMetaInfo(true);
    File infoFile = new File(_mmapDir, buf.metaFileName() + ".info");
    Assert.assertTrue(infoFile.exists());

    // Create a session directory that has one file in it.
    File badSes1 = new File(_mmapDir, DbusEventBuffer.getSessionPrefix() + "m");
    badSes1.mkdir();
    badSes1.deleteOnExit();
    File junkFile = new File(badSes1.getAbsolutePath() + "/junkFile");
    junkFile.createNewFile();
    junkFile.deleteOnExit();
    // Create a directory that is empty
    File badSes2 = new File(_mmapDir, DbusEventBuffer.getSessionPrefix() + "n");
    badSes2.mkdir();
    badSes2.deleteOnExit();

    // Create a good file under mmap directory that we don't want to see removed.
    final String goodFile = "GoodFile";
    File gf = new File(_mmapDir, goodFile);
    gf.createNewFile();

    // Now close the multibuf, and see that the new files are still there.
    // We should have deleted the unused sessions and info files.
    bufMult.close();

    HashSet<String> validEntries = new HashSet<String>(bufNum);
    for(DbusEventBuffer dbusBuf : bufMult.bufIterable()) {
      File metaFile = new File(_mmapDir, dbusBuf.metaFileName());
      // check that we don't have the files
      Assert.assertTrue(metaFile.exists());
      validateFiles(metaFile, bufNum);
      validEntries.add(metaFile.getName());
      DbusEventBufferMetaInfo mi = new DbusEventBufferMetaInfo(metaFile);
      mi.loadMetaInfo();
      validEntries.add(mi.getSessionId());
    }

    validEntries.add(goodFile);

    // Now we should be left with meta files, and session dirs and nothing else.
    entries = _mmapDir.listFiles();
    for (File f : entries)
    {
      Assert.assertTrue(validEntries.contains(f.getName()));
      validEntries.remove(f.getName());
    }
    Assert.assertTrue(validEntries.isEmpty());

    // And everything else should have moved to the .BAK directory
    entries = _mmapBakDir.listFiles();
    HashMap<String, File> fileHashMap = new HashMap<String, File>(entries.length);
    for (File f: entries)
    {
      fileHashMap.put(f.getName(), f);
    }

    Assert.assertTrue(fileHashMap.containsKey(badSes1.getName()));
    Assert.assertTrue(fileHashMap.get(badSes1.getName()).isDirectory());
    Assert.assertEquals(fileHashMap.get(badSes1.getName()).listFiles().length, 1);
    Assert.assertEquals(fileHashMap.get(badSes1.getName()).listFiles()[0].getName(), junkFile.getName());
    fileHashMap.remove(badSes1.getName());

    Assert.assertTrue(fileHashMap.containsKey(badSes2.getName()));
    Assert.assertTrue(fileHashMap.get(badSes2.getName()).isDirectory());
    Assert.assertEquals(fileHashMap.get(badSes2.getName()).listFiles().length, 0);
    fileHashMap.remove(badSes2.getName());

    // We should have the renamed meta files in the hash now.
    for (File f : entries)
    {
      if (f.getName().startsWith(DbusEventBuffer.getMmapMetaInfoFileNamePrefix()))
      {
        Assert.assertTrue(fileHashMap.containsKey(f.getName()));
        Assert.assertTrue(f.isFile());
        fileHashMap.remove(f.getName());
      }
    }

    Assert.assertTrue(fileHashMap.isEmpty());

    // One more test to make sure we create the BAK directory dynamically if it does not exist.
    FileUtils.deleteDirectory(_mmapBakDir);
    bufMult = createBufferMult(config);
    entries = _mmapDir.listFiles();
    // Create an info file for one buffer.
    buf = bufMult.bufIterable().iterator().next();
    buf.saveBufferMetaInfo(true);
    infoFile = new File(_mmapDir, buf.metaFileName() + ".info");
    Assert.assertTrue(infoFile.exists());
    bufMult.close();
    entries = _mmapBakDir.listFiles();
    fileHashMap = new HashMap<String, File>(entries.length);
    for (File f: entries)
    {
      fileHashMap.put(f.getName(), f);
    }
    Assert.assertTrue(fileHashMap.containsKey(infoFile.getName()));
    Assert.assertTrue(fileHashMap.get(infoFile.getName()).isFile());
  }


  @Test
  public void testMetaFileCreation() throws InvalidConfigException, IOException {

    int maxEventBufferSize = 1144;
    int maxIndividualBufferSize = 500;

    DbusEventBuffer dbusBuf =
        new DbusEventBuffer(getConfig(maxEventBufferSize,maxIndividualBufferSize,
                                      100,500,AllocationPolicy.HEAP_MEMORY, _mmapDirStr, true));

    File metaFile = new File(_mmapDir, dbusBuf.metaFileName());
    dbusBuf.saveBufferMetaInfo(false);
    Assert.assertFalse(metaFile.exists()); // because of Allocation policy HEAP

    dbusBuf = new DbusEventBuffer(getConfig(maxEventBufferSize,maxIndividualBufferSize,
                                            100,500,AllocationPolicy.MMAPPED_MEMORY, _mmapDirStr, true));
    // after buffer is created - meta file should be removed
    Assert.assertFalse(metaFile.exists());
    dbusBuf.saveBufferMetaInfo(false);
    // new file should be created
    Assert.assertTrue(metaFile.exists());

    int bufNum = maxEventBufferSize/maxIndividualBufferSize;
    if(maxEventBufferSize % maxIndividualBufferSize > 0)
      bufNum++;

    validateFiles(metaFile, bufNum);

    // now files are there - but if we create a buffer again - the meta file should be moved
    dbusBuf = new DbusEventBuffer(getConfig(maxEventBufferSize,maxIndividualBufferSize,
                                            100,500,AllocationPolicy.MMAPPED_MEMORY, _mmapDirStr, true));
    Assert.assertFalse(metaFile.exists());
    dbusBuf.saveBufferMetaInfo(true);
    // .info file should be create , but not a regular meta file
    Assert.assertFalse(metaFile.exists());
    Assert.assertTrue(new File(metaFile.getAbsolutePath()+".info").exists());

  }

  @Test
  public void testMetaFileTimeStamp() throws InvalidConfigException, IOException {
    int maxEventBufferSize = 1144;
    int maxIndividualBufferSize = 500;

    DbusEventBuffer dbusBuf = new DbusEventBuffer(getConfig(maxEventBufferSize,maxIndividualBufferSize,
                                            100,500,AllocationPolicy.MMAPPED_MEMORY, _mmapDirStr, true));

    File metaFile = new File(_mmapDir, dbusBuf.metaFileName());
    // after buffer is created - meta file should be removed
    Assert.assertFalse(metaFile.exists());
    dbusBuf.saveBufferMetaInfo(false);
    // new file should be created
    Assert.assertTrue(metaFile.exists());
    DbusEventBufferMetaInfo mi = new DbusEventBufferMetaInfo(metaFile);
    mi.loadMetaInfo();
    Assert.assertTrue(mi.isValid());

    // now let's touch one of the mmap files
    File writeBufferFile = new File(new File(metaFile.getParent(), mi.getSessionId()), "writeBuffer_0");
    long modTime = System.currentTimeMillis()+1000; //presission of the mod time is 1 sec

    writeBufferFile.setLastModified(modTime);
    LOG.debug("setting mod time for " + writeBufferFile + " to " +  modTime + " now val = " + writeBufferFile.lastModified());
    mi = new DbusEventBufferMetaInfo(metaFile);
    mi.loadMetaInfo();
    Assert.assertFalse(mi.isValid());
  }

  @Test
  /**
   * test:
   * 1. create buffer, push events , validate events are there
   * 2. save meta info
   * 3. create a new buffer - it should pick up all the events.Validate it
   * 4. create another buffer (without calling save metainfo again) - this one should be empty.
   *
   * @throws InvalidConfigException
   * @throws IOException
   */
  public void testRestoringBufferWithEvents() throws InvalidConfigException, IOException {
    int maxEventBufferSize = 1144;
    int maxIndividualBufferSize = 500;
    int numEvents = 10; // must be even



    DbusEventBuffer.StaticConfig conf = getConfig(maxEventBufferSize,maxIndividualBufferSize,
                      100,500,AllocationPolicy.MMAPPED_MEMORY, _mmapDirStr, true);
    DbusEventBuffer dbusBuf = new DbusEventBuffer(conf);

    pushEventsToBuffer(dbusBuf, numEvents);
    // verify events are in the buffer
    DbusEventIterator it = dbusBuf.acquireIterator("allevents");
    int count=-1; // first event is "prev" event
    while(it.hasNext()) {
      DbusEvent e = it.next();
      Assert.assertTrue(e.isValid());
      count ++;
    }
    dbusBuf.releaseIterator(it);
    Assert.assertEquals(count, numEvents);

    // save meta data
    dbusBuf.saveBufferMetaInfo(false);

    // create another similar buffer and see if has the events
    dbusBuf = new DbusEventBuffer(conf);
    dbusBuf.validateEventsInBuffer();
    it = dbusBuf.acquireIterator("alleventsNew");
    count=-1; // first event is "prev" event
    while(it.hasNext()) {
      DbusEvent e = it.next();
      Assert.assertTrue(e.isValid());
      count ++;
    }
    Assert.assertEquals(count, numEvents);
    dbusBuf.releaseIterator(it);

    // meta file should be gone by now
    File metaFile = new File(_mmapDir, dbusBuf.metaFileName());
    Assert.assertFalse(metaFile.exists());

    // we can start a buffer and would be empty
    dbusBuf = new DbusEventBuffer(conf);
    it = dbusBuf.acquireIterator("alleventsNewEmpty");
    Assert.assertFalse(it.hasNext());
    dbusBuf.releaseIterator(it);
  }


  private void pushEventsToBuffer(DbusEventBuffer dbusBuf, int numEvents) {
    dbusBuf.start(1);
    dbusBuf.startEvents();
    DbusEventGenerator generator = new DbusEventGenerator();
    Vector<DbusEvent> events = new Vector<DbusEvent>();

    generator.generateEvents(numEvents, 1, 100, 10, events);

    // set end of windows
    for (int i=0;i<numEvents-1;i++)
    {
      long scn = events.get(i).sequence();
      i++;

      DbusEvent e = events.get(i);
      e.setSrcId((short)-2);
      e.setSequence(scn);
      e.applyCrc();
      Assert.assertTrue(e.isValid(true));
      Assert.assertTrue( EventScanStatus.OK == e.scanEvent(true));
    }

    //Setup the ReadChannel with 2 events
    ByteArrayOutputStream oStream = new ByteArrayOutputStream();
    WritableByteChannel oChannel = Channels.newChannel(oStream);
    for ( int i = 0; i < numEvents; i++)
      events.get(i).writeTo(oChannel,Encoding.BINARY);

    byte[] writeBytes = oStream.toByteArray();
    ByteArrayInputStream iStream = new ByteArrayInputStream(writeBytes);
    ReadableByteChannel rChannel = Channels.newChannel(iStream);
    try {
      dbusBuf.readEvents(rChannel);
    } catch (InvalidEventException ie) {
      LOG.error("Exception trace is " + ie);
      Assert.fail();
      return;
    }

  }

  private void validateFiles(File metaFile, int bufNumbers) throws DbusEventBufferMetaInfoException {
    DbusEventBufferMetaInfo mi = new DbusEventBufferMetaInfo(metaFile);
    mi.loadMetaInfo();
    Assert.assertTrue(mi.isValid());
    String sessionId = mi.getSessionId(); // figure out what session directory to use for the content of the buffers
    File dir = new File(metaFile.getParentFile(), sessionId);
    Assert.assertTrue(new File(dir, "scnIndexMetaFile").exists());
    for(int i=0; i<bufNumbers; i++) {
      Assert.assertTrue(new File(dir, "writeBuffer_"+i).exists());
    }
    Assert.assertTrue(new File(dir, "scnIndex").exists());
    Assert.assertTrue(new File(dir, "readBuffer").exists());

  }


  private DbusEventBufferMult createBufferMult(DbusEventBuffer.StaticConfig config) throws IOException, InvalidConfigException {
    ObjectMapper mapper = new ObjectMapper();
    InputStreamReader isr = new InputStreamReader(IOUtils.toInputStream(TestDbusEventBufferMult._configSource1));

    PhysicalSourceConfig pConfig1 = mapper.readValue(isr, PhysicalSourceConfig.class);

    isr.close();
    isr = new InputStreamReader(IOUtils.toInputStream(TestDbusEventBufferMult._configSource2));
    PhysicalSourceConfig pConfig2 = mapper.readValue(isr, PhysicalSourceConfig.class);

    PhysicalSourceStaticConfig pStatConf1 = pConfig1.build();
    PhysicalSourceStaticConfig pStatConf2 = pConfig2.build();

    PhysicalSourceStaticConfig[] _physConfigs =  new PhysicalSourceStaticConfig [] {pStatConf1, pStatConf2};

    return new DbusEventBufferMult(_physConfigs, config);
  }
}
