package com.linkedin.databus.cluster;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import junit.framework.Assert;

import org.I0Itec.zkclient.ZkServer;
import org.apache.log4j.Level;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import com.linkedin.databus.cluster.DatabusCluster.DatabusClusterMember;
import com.linkedin.databus2.test.TestUtil;

public class TestDatabusCluster
{
	

	static
	{
		TestUtil.setupLogging(true, null, Level.ERROR); 
	}
	
	static final int localZkPort = 2191;
	static final String zkAddr = "localhost:" + localZkPort;
	static final String clusterName = "test-databus-cluster";
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
	public void testDatabusCluster()
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
				testClients.add(new TestDatabusClusterNode(id.toString(), cluster, numPartitions, quorum));
			}

			//start the clients;
			for (TestDatabusClusterNode node: testClients)
			{
				node.start();
			}
			
			Thread.sleep(6*1000);
			
			System.out.println("Shutting down one of the clients ");
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
				System.out.println("Node: " + node.getIdentifier() + " received " + p.size() + " partitions ");
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
                testClients.add(new TestDatabusClusterNode(id.toString(), cluster, numPartitions, quorum));
            }

            //start the clients;
            for (TestDatabusClusterNode node: testClients)
            {
                node.start();
            }
            
            Thread.sleep(6*1000);
            
            System.out.println("Shutting down one of the clients ");
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
                System.out.println("Node: " + node.getIdentifier() + " received " + p.size() + " partitions ");
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
	
	/**
	 * 
	 * @author snagaraj
	 *	client nodes;
	 */
	public class TestDatabusClusterNode extends Thread
	{
		
		private final DatabusCluster _cluster;
		private final DatabusClusterMember _member;
		private final TestDatabusClusterNotifier _notifier;
		private final String _id;
		boolean _shutdown = false;
		
		public TestDatabusClusterNode(String id,String clusterName,int numPartitions, int quorum) throws Exception
		{
		    super("testClusterNode_"+id);
			_id =id;
			_cluster = new DatabusCluster(zkAddr, clusterName, "default-resource", numPartitions, quorum);
			_notifier = new TestDatabusClusterNotifier(id);
			//get global and local allocation notifications
			_cluster.addDataNotifier(_notifier);
			_member = _cluster.addMember(id,_notifier);
		}
		
		public String getIdentifier()
		{
		    return _id;
		}
		
		@Override
		public void run()
		{
			System.out.println("Started TestDatabusClusterNode for id= " + _id);
			_cluster.start();
			if (_member==null)
			{
			    System.err.println("No member handle for  " + _id);
			    return;
			}
			boolean t = _member.join();
			if (t)
			{
				System.out.println("TestDatabusClusterNode: Awaiting shutdown for " + _id);
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
				System.err.println("Join failed for client node:" + _id);
			}
			System.out.println("Exiting client node: " + _id);
		}
		
		public void shutdown()
		{
		    if (_member==null)
		    {
		        System.err.println("Shutting down failed for member " + _id);
		        return;
		    }
			_member.leave();
			System.out.println("Shutting down ClusterNode id = " + _id);
			_cluster.removeDataNotifier(_notifier);
			synchronized(this)
			{
				_shutdown = true;
				notifyAll();
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
			        System.err.println("Node " + _id + " Adding partition before removing. p=" + partition);
			    }
			}
		}

		@Override
		public void onLostPartitionOwnership(int partition)
		{
			 synchronized (this)
			 {
                 System.err.println("Node " + _id + " Removing partition  p=" + partition);
				 _partitions.remove(partition);
			 }
		}

		@Override
		public void onError(int partition)
		{
			System.out.println("Node " + _id + " removed error " + partition );
		}

		@Override
		public void onReset(int partition)
		{
			System.out.println("Node " + _id + " received reset " + partition );
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
                    System.out.println("Id: " + _id + " New Leader found! " + _leader );
                }
            }
        }
        
        synchronized String getLeader()
        {
            return _leader;
        }
		
	}
	
	
}
