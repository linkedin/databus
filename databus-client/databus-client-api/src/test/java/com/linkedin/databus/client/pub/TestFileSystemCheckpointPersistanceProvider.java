package com.linkedin.databus.client.pub;
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
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.annotations.Test;

import com.linkedin.databus.client.pub.FileSystemCheckpointPersistenceProvider.CacheEntry;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DbusClientMode;
import com.linkedin.databus.core.data_model.DatabusSubscription;

public class TestFileSystemCheckpointPersistanceProvider
{
  public static final Logger LOG = Logger.getLogger(TestFileSystemCheckpointPersistanceProvider.class);

  static
  {
    BasicConfigurator.configure();
    Logger.getRootLogger().setLevel(Level.OFF);
    //Logger.getRootLogger().setLevel(Level.ERROR);
    //Logger.getRootLogger().setLevel(Level.INFO);
    //Logger.getRootLogger().setLevel(Level.DEBUG);
  }

  @Test
  public void testCacheEntryloadCurrentCheckpoint_new() throws Exception
  {
    File checkpointDir = new File("/tmp/databus2-checkpoints-test");

    FileSystemCheckpointPersistenceProvider.Config config =
      new FileSystemCheckpointPersistenceProvider.Config();

    config.setRootDirectory(checkpointDir.getAbsolutePath());
    config.getRuntime().setHistoryEnabled(false);

    FileSystemCheckpointPersistenceProvider checkpointProvider =
      new FileSystemCheckpointPersistenceProvider(config, 2);

    List<String> sourceNames = Arrays.asList("source1", "source2");
    List<DatabusSubscription> subs = DatabusSubscription.createSubscriptionList(sourceNames);
    List<String> subsList = checkpointProvider.convertSubsToListOfStrings(subs);

    String streamId = FileSystemCheckpointPersistenceProvider.calcStreamId(subsList);
    assertEquals("cp_source1-source2", streamId);

    File streamFile = new File(checkpointDir, streamId + ".current");
    if (streamFile.exists())
    {
      assertTrue(streamFile.delete());
    }

    CacheEntry cacheEntry = checkpointProvider.new CacheEntry(streamId, null);
    Checkpoint checkpoint = cacheEntry.getCheckpoint();
    assertNull(checkpoint);
  }

  @Test
  public void testCacheEntry_store_load_noroll() throws Exception
  {
    File checkpointDir = new File("/tmp/databus2-checkpoints-test");

    FileSystemCheckpointPersistenceProvider.Config config =
      new FileSystemCheckpointPersistenceProvider.Config();

    config.setRootDirectory(checkpointDir.getAbsolutePath());
    config.getRuntime().setHistoryEnabled(false);

    FileSystemCheckpointPersistenceProvider checkpointProvider =
      new FileSystemCheckpointPersistenceProvider(config, 2);

    List<String> sourceNames = Arrays.asList("source1", "source2");
    List<DatabusSubscription> subs = DatabusSubscription.createSubscriptionList(sourceNames);
    List<String> subsList = checkpointProvider.convertSubsToListOfStrings(subs);

    String streamId = FileSystemCheckpointPersistenceProvider.calcStreamId(subsList);
    assertEquals("cp_source1-source2", streamId);

    File checkpointFile = new File(checkpointDir, streamId + ".current");
    if (checkpointFile.exists())
    {
      assertTrue(checkpointFile.delete());
    }

    //simple checkpoint
    Checkpoint cp1 = new Checkpoint();
    cp1.setFlexible();

    CacheEntry cacheEntry = checkpointProvider.new CacheEntry(streamId, null);
    assertTrue(cacheEntry.setCheckpoint(cp1));
    assertTrue(checkpointFile.exists());

    CacheEntry cacheEntry2 = checkpointProvider.new CacheEntry(streamId, null);
    Checkpoint cp2 = cacheEntry2.getCheckpoint();
    assertNotNull(cp2);
    //TODO need to use a Checkpoint.equals() method
    assertEquals(DbusClientMode.ONLINE_CONSUMPTION, cp2.getConsumptionMode());
    assertTrue(cp2.getFlexible());

    //more complex checkpoint plus overwriting current state
    Checkpoint cp3 = new Checkpoint();
    cp3.setWindowScn(1234L);
    cp3.setWindowOffset(9876);
    cp3.setConsumptionMode(DbusClientMode.ONLINE_CONSUMPTION);

    assertTrue(cacheEntry.setCheckpoint(cp3));
    assertTrue(checkpointFile.exists());

    File cpBackupFile = new File(checkpointDir, streamId + ".oldcurrent");
    assertTrue(cpBackupFile.exists());

    cacheEntry2 = checkpointProvider.new CacheEntry(streamId, null);
    cp2 = cacheEntry2.getCheckpoint();
    assertNotNull(cp2);
    //TODO need to use a Checkpoint.equals() method
    assertEquals(cp3.getConsumptionMode(), cp2.getConsumptionMode());
    assertEquals(cp3.getWindowScn(), cp2.getWindowScn());
    assertEquals(cp3.getPrevScn(), cp2.getPrevScn());
    assertEquals(cp3.getWindowOffset(), cp2.getWindowOffset());

    //make sure the backup still works
    assertTrue(checkpointFile.delete());
    assertTrue(!checkpointFile.exists());

    if (!cpBackupFile.renameTo(checkpointFile))
        LOG.error("file rename failed: " + cpBackupFile.getAbsolutePath() + " --> " +
                  checkpointFile.getAbsolutePath());
    assertTrue(checkpointFile.exists());

    cacheEntry2 = checkpointProvider.new CacheEntry(streamId, null);
    cp2 = cacheEntry2.getCheckpoint();
    assertNotNull(cp2);
    //TODO need to use a Checkpoint.equals() method
    assertEquals(DbusClientMode.ONLINE_CONSUMPTION, cp2.getConsumptionMode());
    assertTrue(cp2.getFlexible());

    //try to keep some stuff around and see if things go south
    Checkpoint cp4 = new Checkpoint();
    cp4.setWindowScn(1111L);
    cp4.setWindowOffset(2222);
    cp4.setConsumptionMode(DbusClientMode.BOOTSTRAP_CATCHUP);

    File newCpFile = new File(checkpointDir, streamId + ".newcheckpoint");
    PrintWriter tmpWriter = new PrintWriter(newCpFile);
    try{
      tmpWriter.println("Dummy");

      assertTrue(cacheEntry.setCheckpoint(cp4));
      assertTrue(checkpointFile.exists());

      cacheEntry2 = checkpointProvider.new CacheEntry(streamId, null);
      cp2 = cacheEntry2.getCheckpoint();
      assertNotNull(cp2);
      //TODO need to use a Checkpoint.equals() method
      assertEquals(cp4.getConsumptionMode(), cp2.getConsumptionMode());
      assertEquals(cp4.getWindowScn(), cp2.getWindowScn());
      assertEquals(cp4.getPrevScn(), cp2.getPrevScn());
      assertEquals(cp4.getWindowOffset(), cp2.getWindowOffset());
    }
    finally
    {
      tmpWriter.close();
    }
  }

  @Test
  public void testCacheEntry_store_load_withroll() throws Exception
  {
    File checkpointDir = new File("/tmp/databus2-checkpoints-test");

    FileSystemCheckpointPersistenceProvider.Config config =
      new FileSystemCheckpointPersistenceProvider.Config();

    config.setRootDirectory(checkpointDir.getAbsolutePath());
    config.getRuntime().setHistoryEnabled(true);
    config.getRuntime().setHistorySize(10);

    FileSystemCheckpointPersistenceProvider checkpointProvider =
      new FileSystemCheckpointPersistenceProvider(config, 2);

    List<String> sourceNames = Arrays.asList("source1", "source2", "source3");
    List<DatabusSubscription> subs = DatabusSubscription.createSubscriptionList(sourceNames);
    List<String> subsList = checkpointProvider.convertSubsToListOfStrings(subs);

    String streamId = FileSystemCheckpointPersistenceProvider.calcStreamId(subsList);
    assertEquals("cp_source1-source2-source3", streamId);

    //clean up checkpoint files
    File checkpointFile = new File(checkpointDir, streamId + ".current");
    if (checkpointFile.exists())
    {
      assertTrue(checkpointFile.delete());
    }

    for (int i = 0; i < 15; ++i)
    {
      File f = FileSystemCheckpointPersistenceProvider.StaticConfig.generateCheckpointFile(
          checkpointDir, streamId + ".", i);
      if (f.exists())
      {
        assertTrue(f.delete());
      }
    }

    Checkpoint[] cp = new Checkpoint[15];

    //store checkpoints
    CacheEntry cacheEntry = checkpointProvider.new CacheEntry(streamId, null);
    for (int i = 0; i < 15; ++i)
    {
      cp[i] = new Checkpoint();
      cp[i].setWindowScn((long)i * i * i);
      cp[i].setWindowOffset(i * i);
      cp[i].setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);

      assertTrue(cacheEntry.setCheckpoint(cp[i]));
      assertTrue(checkpointFile.exists());

      CacheEntry cacheEntry2 = checkpointProvider.new CacheEntry(streamId, null);
      Checkpoint cp2 = cacheEntry2.getCheckpoint();
      assertNotNull(cp2);
      //TODO need to use a Checkpoint.equals() method
      assertEquals(cp[i].getConsumptionMode(), cp2.getConsumptionMode());
      assertEquals(cp[i].getWindowScn(), cp2.getWindowScn());
      assertEquals(cp[i].getPrevScn(), cp2.getPrevScn());
      assertEquals(cp[i].getWindowOffset(), cp2.getWindowOffset());
    }

    //make sure we don't go over history size
    for (int i = 10; i < 15; ++i)
    {
      File f = FileSystemCheckpointPersistenceProvider.StaticConfig.generateCheckpointFile(
          checkpointDir, streamId, i);
      assertTrue(!f.exists());
    }

    //check correctness of history
    for (int i = 0; i < 10; ++i)
    {
      assertTrue(checkpointFile.delete());
      File f = FileSystemCheckpointPersistenceProvider.StaticConfig.generateCheckpointFile(
          checkpointDir, streamId + ".", i);
      assertTrue(f.renameTo(checkpointFile));

      CacheEntry cacheEntry2 = checkpointProvider.new CacheEntry(streamId, null);
      Checkpoint cp2 = cacheEntry2.getCheckpoint();
      assertNotNull(cp2);
      final int j = 13 -i;
      //TODO need to use a Checkpoint.equals() method
      assertEquals(cp[j].getConsumptionMode(), cp2.getConsumptionMode());
      assertEquals(cp[j].getWindowScn(), cp2.getWindowScn());
      assertEquals(cp[j].getPrevScn(), cp2.getPrevScn());
      assertEquals(cp[j].getWindowOffset(), cp2.getWindowOffset());
    }
  }

  @Test
  public void testCheckpointPersistance_singleSource() throws Exception
  {
    File checkpointDir = new File("/tmp/databus2-checkpoints-test");

    FileSystemCheckpointPersistenceProvider.Config config =
      new FileSystemCheckpointPersistenceProvider.Config();

    config.setRootDirectory(checkpointDir.getAbsolutePath());
    config.getRuntime().setHistoryEnabled(true);
    config.getRuntime().setHistorySize(10);

    FileSystemCheckpointPersistenceProvider checkpointProvider =
      new FileSystemCheckpointPersistenceProvider(config, 2);

    List<String> sourceNames = Arrays.asList("source1", "source_2", "source-3");
    List<DatabusSubscription> subs = DatabusSubscription.createSubscriptionList(sourceNames);
    List<String> subsList = checkpointProvider.convertSubsToListOfStrings(subs);

    String streamId = FileSystemCheckpointPersistenceProvider.calcStreamId(subsList);
    //assertEquals("correct streamid", "cp_source1-source_2-source_3", streamId);
    assertEquals("cp_source1-source_2-source_3", streamId);

    //clean up checkpoint files
    File checkpointFile = new File(checkpointDir, streamId + ".current");
    if (checkpointFile.exists())
    {
      assertTrue(checkpointFile.delete());
    }

    for (int i = 0; i < 15; ++i)
    {
      File f = FileSystemCheckpointPersistenceProvider.StaticConfig.generateCheckpointFile(
          checkpointDir, streamId + ".", i);
      if (f.exists())
      {
        assertTrue(f.delete());
      }
    }

    Checkpoint[] cp = new Checkpoint[15];

    Random rng = new Random();

    //store checkpoints
    for (int i = 0; i < 15; ++i)
    {
      cp[i] = new Checkpoint();
      cp[i].setWindowScn(rng.nextLong());
      cp[i].setWindowOffset(rng.nextInt());
      cp[i].setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);

      checkpointProvider.storeCheckpoint(sourceNames, cp[i]);
      assertTrue(checkpointFile.exists());

      Checkpoint cp2 = checkpointProvider.loadCheckpoint(sourceNames);
      assertNotNull(cp2);
      //TODO need to use a Checkpoint.equals() method
      assertEquals(cp[i].getConsumptionMode(), cp2.getConsumptionMode());
      assertEquals(cp[i].getWindowScn(), cp2.getWindowScn());
      assertEquals(cp[i].getPrevScn(), cp2.getPrevScn());
      assertEquals(cp[i].getWindowOffset(), cp2.getWindowOffset());
    }

    //create a new persister and read the last state
    FileSystemCheckpointPersistenceProvider checkpointProvider2 =
      new FileSystemCheckpointPersistenceProvider(config, 2);

    Checkpoint cp2 = checkpointProvider2.loadCheckpoint(sourceNames);
    assertNotNull(cp2);
    //TODO need to use a Checkpoint.equals() method
    assertEquals(cp[14].getConsumptionMode(), cp2.getConsumptionMode());
    assertEquals(cp[14].getWindowScn(), cp2.getWindowScn());
    assertEquals(cp[14].getPrevScn(), cp2.getPrevScn());
    assertEquals(cp[14].getWindowOffset(), cp2.getWindowOffset());
  }

  @Test
  public void testCheckpointPersistance_multiSource() throws Exception
  {
    File checkpointDir = new File("/tmp/databus2-checkpoints-test");

    FileSystemCheckpointPersistenceProvider.Config config =
      new FileSystemCheckpointPersistenceProvider.Config();

    config.setRootDirectory(checkpointDir.getAbsolutePath());
    config.getRuntime().setHistoryEnabled(true);
    config.getRuntime().setHistorySize(20);

    FileSystemCheckpointPersistenceProvider checkpointProvider =
      new FileSystemCheckpointPersistenceProvider(config, 2);

    ArrayList<List<String>> sourceNames = new ArrayList<List<String>>();
    sourceNames.add(Arrays.asList("source1"));
    sourceNames.add(Arrays.asList("source2", "source1"));
    sourceNames.add(Arrays.asList("source3", "source2", "source1"));
    sourceNames.add(Arrays.asList("source4", "source3", "source2", "source1"));
    sourceNames.add(Arrays.asList("source5", "source4", "source3", "source2", "source1"));

    Checkpoint[] cp = new Checkpoint[15];

    Random rng = new Random();

    //store checkpoints
    for (int i = 0; i < 15; ++i)
    {
      cp[i] = new Checkpoint();
      cp[i].setWindowScn(rng.nextLong());
      cp[i].setWindowOffset(rng.nextInt());
      cp[i].setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
    }

    for (int i = 0; i < 15; ++i)
    {
      for (int j = 0; j < 5; ++j)
      {
        final int cpN = (i + j) % 15;
        //List<DatabusSubscription> subs = DatabusSubscription.getSubscriptionList(sourceNames.get(j));
        checkpointProvider.storeCheckpoint(sourceNames.get(j), cp[cpN]);
        //checkpointProvider.storeCheckpointV3(subs, cp[cpN]);

        Checkpoint cp2 = checkpointProvider.loadCheckpoint(sourceNames.get(j));
        //Checkpoint cp2 = checkpointProvider.loadCheckpointV3(subs);
        assertNotNull(cp2);
        //TODO need to use a Checkpoint.equals() method
        assertEquals(cp[cpN].getConsumptionMode(), cp2.getConsumptionMode());
        assertEquals(cp[cpN].getWindowScn(), cp2.getWindowScn());
        assertEquals(cp[cpN].getPrevScn(), cp2.getPrevScn());
        assertEquals(cp[cpN].getWindowOffset(), cp2.getWindowOffset());
      }

      FileSystemCheckpointPersistenceProvider checkpointProvider2 =
        new FileSystemCheckpointPersistenceProvider(config, 2);

      for (int j = 0; j < 5; ++j)
      {
        final int cpN = (i + j) % 15;
        //List<DatabusSubscription> subs = DatabusSubscription.getSubscriptionList(sourceNames.get(j));
        Checkpoint cp2 = checkpointProvider2.loadCheckpoint(sourceNames.get(j));
        //Checkpoint cp2 = checkpointProvider2.loadCheckpointV3(subs);
        assertNotNull(cp2);
        //TODO need to use a Checkpoint.equals() method
        assertEquals(cp[cpN].getConsumptionMode(), cp2.getConsumptionMode());
        assertEquals(cp[cpN].getWindowScn(), cp2.getWindowScn());
        assertEquals(cp[cpN].getPrevScn(), cp2.getPrevScn());
        assertEquals(cp[cpN].getWindowOffset(), cp2.getWindowOffset());
      }
    }

  }

  @Test
  public void testCheckpointPersistance_manySources() throws Exception
  {
    File checkpointDir = new File("/tmp/databus2-checkpoints-test");

    FileSystemCheckpointPersistenceProvider.Config config =
      new FileSystemCheckpointPersistenceProvider.Config();

    config.setRootDirectory(checkpointDir.getAbsolutePath());
    config.getRuntime().setHistoryEnabled(true);
    config.getRuntime().setHistorySize(20);

    FileSystemCheckpointPersistenceProvider checkpointProvider =
      new FileSystemCheckpointPersistenceProvider(config, 2);

    //Test with com.linkedin.events. prefix
    List<String> sourceNames = new ArrayList<String>();
    int numberOfSources = 50;
    for (int i = 0; i < numberOfSources; ++i)
    {
      sourceNames.add("com.linkedin.events.VeryLongSource" + i);
    }

    String streamId = FileSystemCheckpointPersistenceProvider.calcStreamId(sourceNames);
    assertTrue(streamId.indexOf("com.linkedin.events") < 0);

    Random rng = new Random();

    //store checkpoint
    Checkpoint cp = new Checkpoint();
    cp.setWindowScn(rng.nextLong());
    cp.setWindowOffset(rng.nextInt());
    cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);

    checkpointProvider.storeCheckpoint(sourceNames, cp);
    Checkpoint cp2 = checkpointProvider.loadCheckpoint(sourceNames);
    assertNotNull(cp2);
    //TODO need to use a Checkpoint.equals() method
    assertEquals(cp.getConsumptionMode(), cp2.getConsumptionMode());
    assertEquals(cp.getWindowScn(), cp2.getWindowScn());
    assertEquals(cp.getPrevScn(), cp2.getPrevScn());
    assertEquals(cp.getWindowOffset(), cp2.getWindowOffset());

    //Test without com.linkedin.events. prefix
    sourceNames.clear();
    for (int i = 0; i < numberOfSources; ++i)
    {
      sourceNames.add("com.linkedin.Some.Other.VeryLongSource" + i);
    }
    cp = new Checkpoint();
    cp.setWindowScn(rng.nextLong());
    cp.setWindowOffset(rng.nextInt());
    cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);

    checkpointProvider.storeCheckpoint(sourceNames, cp);
    cp2 = checkpointProvider.loadCheckpoint(sourceNames);
    assertNotNull(cp2);
    //TODO need to use a Checkpoint.equals() method
    assertEquals(cp.getConsumptionMode(), cp2.getConsumptionMode());
    assertEquals(cp.getWindowScn(), cp2.getWindowScn());
    assertEquals(cp.getPrevScn(), cp2.getPrevScn());
    assertEquals(cp.getWindowOffset(), cp2.getWindowOffset());
  }

}
