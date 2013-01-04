package com.linkedin.databus.client.pub;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import junit.framework.Assert;

import org.I0Itec.zkclient.ZkServer;
import org.apache.log4j.Level;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeSuite;

import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DbusClientMode;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.test.TestUtil;

public class TestClusterCheckpointPersistenceProvider 
{
    
    
    
	static
	{
		TestUtil.setupLogging(true, null, Level.WARN); 
	}
    
  static final int localZkPort = 2193;
  static final String zkAddr = "localhost:" + localZkPort;
  static final String clusterName = "test-databus-cluster-cp";
  static List<ZkServer> _localZkServers = null;
    
  @BeforeSuite
  public void startZookeeper() throws IOException
  {
      ZkServer zkServer = TestUtil.startZkServer(".", 0, localZkPort , 2000);
      if (zkServer != null)
      {
          _localZkServers  = new Vector<ZkServer>(1);
          _localZkServers.add(zkServer);
      }
  }
  
  @AfterSuite
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
      
      String id = "5"; 
      String clusterName = "test-cluster";
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
      
  }
  
  
  
}
