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


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.I0Itec.zkclient.ZkServer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.client.pub.ClusterCheckpointPersistenceProvider.ClusterCheckpointException;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DbusClientMode;
import com.linkedin.databus.core.util.FileUtils;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus.core.util.RngUtils;
import com.linkedin.databus.core.util.Utils;
import com.linkedin.databus2.test.TestUtil;

public class TestClusterCheckpointPersistenceProvider
{
  protected static final Logger LOG = Logger.getLogger(TestClusterCheckpointPersistenceProvider.class);

  static int localZkPort = Utils.getAvailablePort(2193);
  static final String zkAddr = "localhost:" + localZkPort;
  static final String clusterName = "test-databus-cluster-cp";
  static List<ZkServer> _localZkServers = null;

  @BeforeClass
  public void startZookeeper() throws IOException
  {
    TestUtil.setupLoggingWithTimestampedFile(true, "/tmp/TestClusterCheckpointPersistenceProvider_",
        ".log", Level.WARN);
    File zkroot = FileUtils.createTempDir("TestClusterCheckpointPersistenceProvider_zkroot");
    LOG.info("starting ZK on port " + localZkPort + " and datadir " + zkroot.getAbsolutePath());

    ZkServer zkServer = TestUtil.startZkServer(zkroot.getAbsolutePath(), 0,
        localZkPort , 2000);
    if (zkServer != null)
    {
      _localZkServers  = new Vector<ZkServer>(1);
      _localZkServers.add(zkServer);
    }
  }

  @AfterClass
  public void stopZookeeper()
  {
    if (_localZkServers != null)
    {
      TestUtil.stopLocalZookeeper(_localZkServers);
    }
  }


  @Test
  public void testClusterCheckpointPersistence()
  {
    Checkpoint cp = new Checkpoint();
    cp.setWindowScn(50532L);
    cp.setWindowOffset(-1);
    cp.setConsumptionMode(DbusClientMode.ONLINE_CONSUMPTION);

    String id = "4";
    String clusterName = "test-cluster-persistence";
    ClusterCheckpointPersistenceProvider.createCluster(zkAddr,clusterName);
    ClusterCheckpointPersistenceProvider.Config conf = new ClusterCheckpointPersistenceProvider.Config();
    conf.setClusterName(clusterName);
    conf.setZkAddr(zkAddr);

    ArrayList<String> sources= new ArrayList<String>(3);
    sources.add("source1");
    sources.add("source2");
    sources.add("source3");

    try
    {
      ClusterCheckpointPersistenceProvider ccp = new ClusterCheckpointPersistenceProvider(id,conf);
      ccp.storeCheckpoint(sources, cp);

      Checkpoint newCp = ccp.loadCheckpoint(sources);
      Assert.assertTrue(newCp != null);
      Assert.assertTrue(newCp.getWindowOffset()==cp.getWindowOffset());
      Assert.assertTrue(newCp.getWindowScn()==cp.getWindowScn());
      Assert.assertTrue(newCp.getConsumptionMode()==cp.getConsumptionMode());

    }
    catch (InvalidConfigException e)
    {
      System.err.println("Invalid config: " + e);
      Assert.assertTrue(false);
    }
    catch (IOException e)
    {
      System.err.println("Error storing checkpoint: " + e);
      Assert.assertTrue(false);
    }
    catch (ClusterCheckpointException e)
    {
      Assert.assertTrue(false);
    }
    finally
    {
      ClusterCheckpointPersistenceProvider.close(clusterName);
    }

  }

  @Test
  public void testFrequencyOfCheckpoints() throws Exception
  {
    Checkpoint cp = new Checkpoint();
    long startWindowScn = 50532L;
    cp.setWindowScn(startWindowScn);
    cp.setWindowOffset(-1);
    cp.setConsumptionMode(DbusClientMode.ONLINE_CONSUMPTION);

    final int checkPointIntervalMs = 75;
    final long delayMs = 31;
    final int numAttemptedWrites = 7;
    // We should write at 0, 31, 62, 93, 123, 155, 186, but only at at 0, 93, 155
    // Persistent provider clock: 0              75           150            225
    // checkpoint store clock     0    31   62       93  123      155  186
    final int expectedActualStores = 3;

    String id = "5";
    String clusterName = "test-cluster-freq";
    ClusterCheckpointPersistenceProvider.createCluster(zkAddr, clusterName);
    ClusterCheckpointPersistenceProvider.Config conf = new ClusterCheckpointPersistenceProvider.Config();
    conf.setClusterName(clusterName);
    conf.setZkAddr(zkAddr);
    conf.setCheckpointIntervalMs(checkPointIntervalMs);

    ArrayList<String> sources = new ArrayList<String>(3);
    sources.add("source1");
    sources.add("source2");
    sources.add("source3");

    try
    {
      TestFrequencyCPP ccp = new TestFrequencyCPP(id, conf);
      for (int i = 0; i < numAttemptedWrites; ++i)
      {
        cp.setWindowScn(startWindowScn + i);
        ccp.storeCheckpoint(sources, cp);
        Checkpoint newCp = ccp.getStoredCheckpoint();
        // cp integrity checks
        Assert.assertTrue(newCp != null);
        Assert.assertTrue(newCp.getWindowOffset() == cp
            .getWindowOffset());
        Assert.assertTrue(newCp.getConsumptionMode() == cp
            .getConsumptionMode());
        // skipped store test;
        Thread.sleep(delayMs);
      }
      Assert.assertEquals(ccp.getnStores(), expectedActualStores);
    }
    finally
    {
      ClusterCheckpointPersistenceProvider.close(clusterName);
    }
  }

  @Test
  public void testMultipleClusterCheckpointPersistence()
  {
    try
    {
      String[] partitionIds = { "1", "2", "3", "4", "5", "6" };
      String[] clusters = { "tcluster1", "tcluster2", "tcluster3" };
      ArrayList<CheckpointRW> cpRws = new ArrayList<TestClusterCheckpointPersistenceProvider.CheckpointRW>();
      for (String c : clusters)
      {
        // create clusters;
        ClusterCheckpointPersistenceProvider.createCluster(zkAddr, c);
        for (String p : partitionIds)
        {
          cpRws.add(new CheckpointRW(c, p, RngUtils
              .randomPositiveLong()));
        }
      }
      for (CheckpointRW cpRW : cpRws)
      {
        cpRW.start();
      }
      for (CheckpointRW cpRW : cpRws)
      {
        cpRW.join(10000);
        Assert.assertFalse(cpRW.hasError());
      }
    }
    catch (Exception e)
    {
      Assert.assertTrue(false);
    }
  }

  /**
   * thread that writes Checkpoints to clusters
   *
   */
  public class CheckpointRW extends Thread
  {
    private final String _clusterName;
    private final String _partitionId;
    private final long _startScn;
    private final long _durationMs = 5000;
    private final long _delayMs = 200;
    private boolean _hasError = false;

    public CheckpointRW(String cluster, String partitionId, long startScn)
    {
      _clusterName = cluster;
      _partitionId = partitionId;
      _startScn = startScn;
    }

    public boolean hasError()
    {
      return _hasError;
    }

    public String getClusterName()
    {
      return _clusterName;
    }
    finally
    {
      ClusterCheckpointPersistenceProvider.close(clusterName);
    }
  }

    @Override
    public void run()
    {
      try
      {
        ArrayList<String> sources = new ArrayList<String>(3);
        sources.add("src1");
        sources.add("src2");
        sources.add("src3");
        long endTimeMs = System.currentTimeMillis() + _durationMs;
        while (System.currentTimeMillis() < endTimeMs)
        {
          ClusterCheckpointPersistenceProvider.Config conf = new ClusterCheckpointPersistenceProvider.Config();
          conf.setClusterName(_clusterName);
          conf.setZkAddr(zkAddr);
          conf.setCheckpointIntervalMs(_delayMs - 10);

          Checkpoint cp = new Checkpoint();
          cp.setWindowScn(_startScn);
          cp.setWindowOffset(-1);
          cp.setConsumptionMode(DbusClientMode.ONLINE_CONSUMPTION);

          // cluster creation code
          ClusterCheckpointPersistenceProvider ccp = new ClusterCheckpointPersistenceProvider(
              _partitionId, conf);
          ccp.storeCheckpoint(sources, cp);

          Checkpoint newCp = ccp.loadCheckpoint(sources);

          Assert.assertTrue(newCp != null);
          Assert.assertTrue(newCp.getWindowOffset() == cp
              .getWindowOffset());
          Assert.assertTrue(newCp.getWindowScn() == cp.getWindowScn());
          Assert.assertTrue(newCp.getConsumptionMode() == cp
              .getConsumptionMode());

          Thread.sleep(_delayMs);
        }
      }
      catch (Exception e)
      {
        LOG.error("Exception caught " + e, e);
        _hasError = true;
      }
      finally
      {
        ClusterCheckpointPersistenceProvider.close(_clusterName);
      }
    }
  }
  public static class TestFrequencyCPP extends ClusterCheckpointPersistenceProvider
  {
    private int nStores = 0;

    private Checkpoint storedCheckpoint = null;

    public TestFrequencyCPP(String id, Config config)
        throws InvalidConfigException, ClusterCheckpointException
    {
      super(id, config);
    }

    @Override
    protected void storeZkRecord(List<String> sourceNames, Checkpoint checkpoint)
    {
      storedCheckpoint = checkpoint;
      nStores++;
    }

    public int getnStores()
    {
      return nStores;
    }

    public Checkpoint getStoredCheckpoint()
    {
      return storedCheckpoint;
    }
  }
}
