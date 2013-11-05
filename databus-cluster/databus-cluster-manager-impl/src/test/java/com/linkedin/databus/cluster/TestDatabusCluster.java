package com.linkedin.databus.cluster;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import junit.framework.Assert;

import org.I0Itec.zkclient.ZkServer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.client.registration.ClusterRegistrationStaticConfig;
import com.linkedin.databus.cluster.DatabusCluster.DatabusClusterMember;
import com.linkedin.databus.core.util.FileUtils;
import com.linkedin.databus.core.util.Utils;
import com.linkedin.databus2.test.TestUtil;

public class TestDatabusCluster
{
    protected static final Logger LOG = Logger.getLogger(TestDatabusCluster.class);

	static final int localZkPort = Utils.getAvailablePort(2192);
	static final String zkAddr = "localhost:" + localZkPort;
	static final String clusterName = "test-databus-cluster";
	static List<ZkServer> _localZkServers = null;

	@BeforeClass
	public void startZookeeper() throws IOException
	{
      TestUtil.setupLogging(true, null, Level.WARN);
	  File zkroot = FileUtils.createTempDir("TestDatabusCluster_zkroot");
	  LOG.info("starting ZK on port " + localZkPort + " and datadir " + zkroot.getAbsolutePath());

	  ZkServer zkServer = TestUtil.startZkServer(zkroot.getAbsolutePath(), 0, localZkPort, 2000);
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

	public void runBasicDatabusClusterExpt(boolean createClusterConcurrently)
	{
	    try
        {
            String cluster = clusterName;
            int startId = 100;
            int numClients = 4;
            int numPartitions = 10;
            int quorum=1;

            Vector<TestDatabusClusterNode> testClients = new Vector<TestDatabusClusterNode>(10);
            for (int i=0;i < numClients; i++)
            {
                Integer id = startId + i;
                TestDatabusClusterNode node = new TestDatabusClusterNode(id.toString(), cluster, numPartitions, quorum);
                if (!createClusterConcurrently)
                {
                    node.createCluster();
                }
                testClients.add(node);
            }

            //start the clients;
            for (TestDatabusClusterNode node: testClients)
            {
                node.start();
            }

            Thread.sleep(6*1000);

            LOG.info("Shutting down one of the clients ");
            testClients.get(numClients-1).shutdown();
            testClients.get(numClients-1).join();

            Thread.sleep(2*1000);

            //Start another client;
            Integer id = startId + numClients;
            testClients.add(new TestDatabusClusterNode(id.toString(), cluster, numPartitions, quorum));
            testClients.get(numClients).start();

            Thread.sleep(10*1000);

            //stop the clients;
            int assignedPartitions = 0;
            for (TestDatabusClusterNode node: testClients)
            {
                node.shutdown();
                node.join(2000);
                Assert.assertTrue(!node.isAlive());
                Vector<Integer> p = node.getPartitions();
                assignedPartitions += p.size();
                LOG.info("Node: " + node.getIdentifier() + " received " + p.size() + " partitions ");
            }
            Assert.assertTrue(assignedPartitions>=numPartitions);
        }
        catch(InterruptedException e)
        {
            Assert.assertTrue(false);
        }
        catch (Exception e)
        {
            LOG.error("run failed! " + e.getMessage());
            Assert.assertTrue(false);
        }

	}

	@Test
	public void testDatabusCluster()
	{
		runBasicDatabusClusterExpt(false);
	}

	@Test
	public void testLeaderFollower()
	{
	    try
        {
            String cluster = "leader-follower";
            int startId = 100;
            int numClients = 4;
            int numPartitions = 1;
            int quorum=1;

            Vector<TestDatabusClusterNode> testClients = new Vector<TestDatabusClusterNode>(10);
            for (int i=0;i < numClients; i++)
            {
                Integer id = startId + i;
                TestDatabusClusterNode node = new TestDatabusClusterNode(id.toString(), cluster, numPartitions, quorum);
                node.createCluster();
                testClients.add(node);
            }

            //start the clients;
            for (TestDatabusClusterNode node: testClients)
            {
                node.start();
            }

            Thread.sleep(6*1000);

            LOG.info("Shutting down one of the clients ");
            testClients.get(numClients-1).shutdown();
            testClients.get(numClients-1).join();

            Thread.sleep(2*1000);
            //Start another client;
            Integer id = startId + numClients;
            testClients.add(new TestDatabusClusterNode(id.toString(), cluster, numPartitions, quorum));
            testClients.get(numClients).start();

            Thread.sleep(10*1000);

            //stop the clients;
            int assignedPartitions = 0;
            for (TestDatabusClusterNode node: testClients)
            {
                node.shutdown();
                node.join(2000);
                Assert.assertTrue(!node.isAlive());
                Vector<Integer> p = node.getPartitions();
                assignedPartitions += p.size();
                LOG.info("Node: " + node.getIdentifier() + " received " + p.size() + " partitions ");
            }
            Assert.assertTrue(assignedPartitions>=numPartitions);

        }
        catch(InterruptedException e)
        {
            Assert.assertTrue(false);
        }
	    catch (Exception e)
	    {
	        Assert.assertTrue(false);
	    }
	}

	@Test
	public void testConcurrentClusterCreation()
	{
	    //create TestNode concurrently;
	    runBasicDatabusClusterExpt(true);
	}

	/**
	 *
	 * @author snagaraj
	 *	client nodes;
	 */
	public class TestDatabusClusterNode extends Thread
	{

		private DatabusCluster _cluster = null;
		private DatabusClusterMember _member = null;
		private TestDatabusClusterNotifier _notifier = null;
		private final String _id;
		private volatile boolean _shutdown = false;
		private final int _quorum;
		private final int _numPartitions;
		private final String _clusterName;
		private boolean _error = false;

		public TestDatabusClusterNode(String id,String clusterName, int numPartitions, int quorum)
		{
		    super("testClusterNode_"+id);
		    _id = id;
            _clusterName = clusterName;
		    _numPartitions = numPartitions;
		    _quorum = quorum;
		}

		public  void createCluster() throws Exception
		{
		    ClusterRegistrationStaticConfig c = 
		              new ClusterRegistrationStaticConfig(_clusterName, zkAddr, _numPartitions, _quorum, 0,5*60*1000, 30*1000, 60*100);
		    _cluster = new DatabusCluster(c);
		    LOG.warn("Created cluster object! " + _clusterName + " id = " + _id);
            _notifier = new TestDatabusClusterNotifier(_id);
            //get global and local allocation notifications
            _cluster.addDataNotifier(_notifier);
            _member = _cluster.addMember(_id,_notifier);
		}

		public String getIdentifier()
		{
		    return _id;
		}

		public boolean isError()
		{
		    return _error;
		}

		@Override
		public void run()
		{
		    try
		    {
		        LOG.warn("Started TestDatabusClusterNode for id= " + _id);
		        if (_cluster==null)
		        {
		            createCluster();
		        }
	             LOG.warn("Created TestDatabusClusterNode for id= " + _id);
		        _cluster.start();
		        if (_member==null)
		        {
		            LOG.error("No member handle for  " + _id);
		            _error=true;
		            return;
		        }
		        boolean t = _member.join();
		        if (t)
		        {
		            synchronized(this)
		            {
		                while (!_shutdown)
		                {
		                    try
		                    {
		                        wait();
		                    }
		                    catch (InterruptedException e)
		                    {

		                    }
		                }
		            }
		        }
		        else
		        {
		            LOG.error("Join failed for client node:" + _id);
  	               _error=true;
		        }
		    }
		    catch (Exception e)
		    {
		        LOG.error("Exception in thread: " + _id + " = " + e.getMessage());
		        _error=true;
		    }
		}



		public void shutdown()
		{
			if (!_shutdown)
			{

				if (_member==null)
				{
					LOG.error("Shutting down failed for member " + _id);
					return;
				}
				_member.leave();
				_cluster.removeDataNotifier(_notifier);
				_cluster.shutdown();
				synchronized(this)
				{
					_shutdown = true;
					notifyAll();
				}
			}
		}

		public Vector<Integer> getPartitions()
		{
			return _notifier.getPartitions();
		}

	}

	public class TestDatabusClusterNotifier implements DatabusClusterNotifier,  DatabusClusterDataNotifier
	{

		final private String _id ;
		final private HashSet<Integer> _partitions;
		private String _leader;

		public TestDatabusClusterNotifier(String id)
		{
			_id = id;
			_partitions = new HashSet<Integer>(10);
		}

		@Override
		public void onGainedPartitionOwnership(int partition)
		{
			synchronized (this)
			{
			    if (!_partitions.contains(partition))
			    {
			        _partitions.add(partition);
			    }
			    else
			    {
			        LOG.warn("Node " + _id + " Adding partition before removing. p=" + partition);
			    }
			}
		}

		@Override
		public void onLostPartitionOwnership(int partition)
		{
			 synchronized (this)
			 {
                 LOG.warn("Node " + _id + " Removing partition  p=" + partition);
				 _partitions.remove(partition);
			 }
		}

		@Override
		public void onError(int partition)
		{
			LOG.warn("Node " + _id + " removed error " + partition );
		}

		@Override
		public void onReset(int partition)
		{
			LOG.warn("Node " + _id + " received reset " + partition );
		}

		public synchronized Vector<Integer> getPartitions()
		{
			Vector<Integer> p = new Vector<Integer>(_partitions.size());
			Iterator<Integer> it = _partitions.iterator();
			while (it.hasNext())
			{
				p.add(it.next());
			}
			return p;
		}

        @Override
        public void onInstanceChange(List<String> activeNodes)
        {
            //no op

        }

        @Override
        public void onPartitionMappingChange(
                Map<Integer, String> activePartitionMap)
        {
            synchronized (this)
            {
                String leader = activePartitionMap.get(0);
                if ((leader != null) && (_leader==null || !_leader.equals(leader)))
                {
                    _leader=leader;
                    LOG.warn("Id: " + _id + " New Leader found! " + _leader );
                }
            }
        }

        synchronized String getLeader()
        {
            return _leader;
        }

	}


}
