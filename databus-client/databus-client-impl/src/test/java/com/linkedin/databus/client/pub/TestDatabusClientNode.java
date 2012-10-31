package com.linkedin.databus.client.pub;


import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.Vector;

import org.I0Itec.zkclient.ZkServer;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.linkedin.databus.client.DatabusClientDSCUpdater;
import com.linkedin.databus.client.LastWriteTimeTrackerImpl;
import com.linkedin.databus.client.pub.TestDatabusClientNode.DummyClient.DummyClientException;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DbusClientMode;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus.groupleader.impl.zkclient.LeaderElectUtils;

public class TestDatabusClientNode
{

  // static private final String CLUSTER_SERVER_LIST = "localhost:2181,localhost:2182,localhost:2183";
  static private final String CLUSTER_SERVER_LIST = "localhost:8100";


  protected static final int _zkServerTickTime = 2000;

  public List<ZkServer> startZk(int port) throws IOException
  {
    String zkTestDataParentDir = ".";
    String zkServerList =  "localhost:" + port;

    String zkTestDataRootDir = zkTestDataParentDir + "/zkroot-client";

     List<Integer> localPortsList = LeaderElectUtils.parseLocalPorts(zkServerList);
     List<ZkServer> localZkServers = LeaderElectUtils.startLocalZookeeper(localPortsList, zkTestDataRootDir, _zkServerTickTime);
     return localZkServers;
  }

  public void stopZk(List<ZkServer> localZkServers)
  {
    LeaderElectUtils.stopLocalZookeeper(localZkServers);
  }


  @Test
  public void testMasterSlave() throws InterruptedException, DummyClientException, IOException
  {
    List<ZkServer> localZkServers = startZk(8100);
    try
    {
      Integer totalCountExpected  = 100;
      int numNodes = 10;
      int delayMs = 10;
      int numIter = 1;
      Integer expectedWorkLoad = totalCountExpected/numNodes;
      long updateTimestamp  = System.currentTimeMillis();
      for (int iter = 0; iter < numIter; ++iter)
      {
        DummyClient nodes[] = new DummyClient[numNodes];
        Thread threads[] = new Thread[numNodes];
        for (int i=0; i < numNodes; ++i)
        {
          nodes[i] =  new DummyClient(expectedWorkLoad,"counters","worker_" +
                                      (int) (Math.random()*1000),
                                      totalCountExpected,
                                      delayMs,
                                      updateTimestamp);
         // nodes[i] =  new DummyClient(expectedWorkLoad,"counters","worker_"  ,totalCountExpected,delayMs);

          threads[i] = new Thread(nodes[i]);
        }



        for (int i=0; i < threads.length; ++i)
        {
          threads[i].start();
          Thread.sleep(i*(iter*2+20));
        }

        for (int i=0; i < threads.length;++i)
        {
          //wait for 1000ms  - protect against hung processes
          threads[i].join(1000);
        }
        boolean exhausted=false;
        boolean tsExhausted=false;
        for (DummyClient w1: nodes)
        {
          System.err.printf("Worker =%s\n",w1.toString());
          AssertJUnit.assertTrue(w1.getActualWorkCount()==expectedWorkLoad);
          if (w1.getSharedWorkCounter() == totalCountExpected) {
            exhausted = true;
          }
          long diff = w1.getDSCTimestamp() - updateTimestamp;
          if (diff == totalCountExpected) {
            tsExhausted = true;
          }
          System.err.println("Timestamp Diff=" + diff);
        }
        AssertJUnit.assertTrue(exhausted);
        AssertJUnit.assertTrue(tsExhausted);
      }
    }
    finally
    {
      stopZk(localZkServers);
    }
  }


  @Test
  public void testSharedCheckPointPersistence() throws InvalidConfigException, IOException
  {
    List<ZkServer> localZkServers = startZk(8100);
    DatabusClientNode clusterNode = null;
    try
    {
       clusterNode = new DatabusClientNode (new DatabusClientNode.StaticConfig(true,CLUSTER_SERVER_LIST,
                                                                                                2000,
                                                                                                5000,
                                                                                                "/DummyClient",
                                                                                                "dbus-client",
                                                                                                "one",true,null));

      DatabusClientGroupMember groupMember = clusterNode.getMember("/DummyClient","dbus-client","one");
      AssertJUnit.assertTrue(groupMember != null);
      SharedCheckpointPersistenceProvider sharedCp =
                            new SharedCheckpointPersistenceProvider(groupMember,
                                                                    new SharedCheckpointPersistenceProvider.StaticConfig(
                                                                                                                         0));

      //join the group;
      AssertJUnit.assertTrue(groupMember.join());

      AssertJUnit.assertTrue(groupMember.waitForLeaderShip(5000));

      Vector<String> sources1 = new Vector<String> ();
      sources1.add("bizfollow_biz"); sources1.add("bizfollow_rev");

      Vector<String> sources2 = new Vector<String> ();
      sources2.add("bizfollow_biz");

      Vector<String> sources3 = new Vector<String> ();
      sources3.add("bizfollow_biz") ; sources3.add("bizfollow_test") ; sources3.add("bizfollow_intl");

      checkSharedCps(sharedCp,sources1);
      checkSharedCps(sharedCp,sources2);
      checkSharedCps(sharedCp,sources3);

      //delete the node;
      groupMember.removeSharedData();

    }
    finally {
      if (clusterNode != null)
      clusterNode.close();
      stopZk(localZkServers);
    }

  }

  @Test
  public void testZkTempIsolation() throws DummyClientException, InterruptedException, IOException
  {
    //zk is up;
    Integer totalCountExpected  = 5000;
    int numNodes = 2;
    int delayMs = 10;
    int interruptCount = 1000;
    Integer expectedWorkLoad = totalCountExpected/numNodes;
    long updateTimestamp=System.currentTimeMillis();
    List<ZkServer> localZkServers = startZk(8100);

    try
    {
      DummyClient nodes[] = new DummyClient[numNodes];
      Thread threads[] = new Thread[numNodes];
      for (int i=0; i < numNodes; ++i)
      {
        nodes[i] =  new DummyClient(expectedWorkLoad,"counters","worker_" + (int) (Math.random()*1000),totalCountExpected,delayMs,updateTimestamp);
        threads[i] = new Thread(nodes[i]);
      }

      for (int i=0; i < threads.length; ++i)
      {
        threads[i].start();
        Thread.sleep(10);
      }

      Thread.sleep(2000);
      DummyClient origLeader = findLeader(nodes);
      AssertJUnit.assertTrue(origLeader != null);

      System.out.printf("Current leader=%s\n",origLeader.getName());

      //Threads have started; and are writing to shared storage; now kill zookeeper
      Thread.sleep(interruptCount*delayMs);
      origLeader.leaveGroup();
      Thread.sleep(1000);
      origLeader.joinGroup();

      DummyClient newLeader = findLeader(nodes);
      AssertJUnit.assertTrue(newLeader != null);
      System.out.printf("New leader=%s\n",newLeader.getName());

      for (int i=0; i < threads.length;++i)
      {
        //wait for 1000ms  - protect against hung processes
        threads[i].join(expectedWorkLoad*delayMs*10);
      }

      AssertJUnit.assertFalse(origLeader.getName().equals(newLeader.getName()));
      AssertJUnit.assertTrue(origLeader.getSharedWorkCounter() < expectedWorkLoad);
      AssertJUnit.assertTrue(newLeader.getSharedWorkCounter() < totalCountExpected);
      AssertJUnit.assertTrue(newLeader.getSharedWorkCounter().equals(expectedWorkLoad));
    }
    finally
    {
      stopZk(localZkServers);
    }

  }


  private DummyClient findLeader(DummyClient[] nodes)
  {
      for (DummyClient n: nodes) {
        if (n.isLeader())
        {
            return n;
        }
      }
      return null;
  }

  protected void checkSharedCps(SharedCheckpointPersistenceProvider sharedCp, Vector<String> sources)
  {

    Checkpoint cp = new Checkpoint();
    cp.setConsumptionMode(DbusClientMode.ONLINE_CONSUMPTION);
    Random r = new Random(System.currentTimeMillis());
    long scn = r.nextLong();

    cp.setWindowScn(scn);
    cp.setWindowOffset(-1);
    try
    {
      sharedCp.storeCheckpoint(sources,cp);
    }
    catch (IOException e)
    {
      e.printStackTrace();
      AssertJUnit.assertTrue(false);
    }
    Checkpoint newCp = sharedCp.loadCheckpoint(sources);

    AssertJUnit.assertTrue(newCp != null);
    AssertJUnit.assertTrue(newCp.getWindowScn() == cp.getWindowScn());
    AssertJUnit.assertTrue(newCp.getWindowOffset().equals(cp.getWindowOffset()));
    AssertJUnit.assertTrue(newCp.getConsumptionMode() == cp.getConsumptionMode());

    System.out.printf("Stored scn = %d\n",newCp.getWindowScn());


  }
  /**
   *
   * @author snagaraj
   * Dummy client that uses clientNode to test master/slave relationship;
   * Initially, just use the blocking API;
   * Task is to read a counter from shared state; and increment with a delay -countWorkLoad- number of times and then die;
   */
  public static class DummyClient implements Runnable {

    //represents the logical node/entity participating on the physical connection;
    private final DatabusClientNode _clusterNode;
    //represents the 'physical' node connection to group;
    private final Integer _countWorkload;
    private Integer _actualWorkCount;
    private Integer _sharedWorkCounter;
    private final Integer _totalWorkLoadCount;


    private final static String NAMESPACE = "/DummyClientTest";
    private final  int _delayMs;
    private final static int sessionTimeOutMillis = 10000;
    private final static int connectionTimeOutMillis = 5000;
    private final static String KEY= "dummy-key";

    private final String _group;
    private final String _name;

    private final DatabusClientDSCUpdater _dscUpdater;
    private final long _baseUpdateTimestamp;

    public DummyClient(Integer countWorkload,String group, String name,Integer totalWorkLoadCount,int delayMs,long updateTimeInMs) throws DummyClientException {
       try
      {
         _group = group;
         _name = name;
        _clusterNode = new DatabusClientNode(new DatabusClientNode.StaticConfig(true,CLUSTER_SERVER_LIST,
                                                                                       sessionTimeOutMillis,
                                                                                       connectionTimeOutMillis,
                                                                                       NAMESPACE,
                                                                                       _group,
                                                                                       _name,true,"test-shared"));
      }
      catch (InvalidConfigException e)
      {
        e.printStackTrace();
        throw new DummyClientException();
      }
      _countWorkload = countWorkload;
      _actualWorkCount = 0;
      _sharedWorkCounter=0;
      _totalWorkLoadCount = totalWorkLoadCount;
      _delayMs = delayMs;
      if (!_clusterNode.isConnected()) throw new DummyClientException();
      DatabusClientGroupMember member = _clusterNode.getMember(NAMESPACE, _group, _name);
      _dscUpdater = new DatabusClientDSCUpdater(member,new LastWriteTimeTrackerImpl(),2);
      _baseUpdateTimestamp  = updateTimeInMs;
    }

    public String getName()
    {
      return _name;
    }

    public  boolean isLeader()
    {
      DatabusClientGroupMember member = _clusterNode.getMember(NAMESPACE, _group, _name);
      if (member != null)
      {
        return member.isLeader();
      }
      return false;
    }

    public Integer getActualWorkCount() {
      return _actualWorkCount;
    }

    public Integer getSharedWorkCounter() {
      return _sharedWorkCounter;
    }

    public long getDSCTimestamp() {
      return _dscUpdater.getLocalTimestamp();
    }

    @Override
    public void run()
    {
      Integer start = null;
      long baseTs = 0 ;
      synchronized (this)
      {
        DatabusClientGroupMember member = _clusterNode.getMember(NAMESPACE, _group, _name);
        //hello group;
        if (member.join())
        {
          //sit around till leadership is conferred;
          //start dsc reader thread;
          Thread t = new Thread(_dscUpdater);
          t.start();

          member.waitForLeaderShip();
          System.out.printf("Acquired leadership: %s\n",member.getName());

          //read shared data
          start = (Integer) member.readSharedData(KEY);

          //leader; reads local value: this should contain the last updated time value;
          baseTs = _dscUpdater.getLocalTimestamp();
          if (baseTs==0) {
              baseTs = _baseUpdateTimestamp;
          }
          //stop the dsc thread for master;
          _dscUpdater.stop();
        }
        else
        {
           System.err.printf("%s could not join group!\n",member.getName());
           _clusterNode.close();
           return;
        }
      }


      if (start==null)
      {
        //bootstrap;
        start=0;
        System.err.printf("Start is  null! Start at: %d\n",(int) start);
      }
      System.err.printf("Start at: %d\n",(int) start);
      Integer i=0;
      for (;i < _countWorkload; ++i) {
        start++;
        _actualWorkCount++;
        //shared write of dsc timestamp;
        _dscUpdater.writeTimestamp(baseTs + _actualWorkCount);
        try
        {
          Thread.sleep(_delayMs);
        }
        catch (InterruptedException e)
        {
          e.printStackTrace();
          System.err.printf("start=%d",start);
        }

      }
      boolean wroteSharedData = false;
      synchronized(this)
      {
        DatabusClientGroupMember member = _clusterNode.getMember(NAMESPACE, _group, _name);
        wroteSharedData = member.writeSharedData(KEY,start);
        if (wroteSharedData)
        {
          _sharedWorkCounter = start;
          if (_sharedWorkCounter.equals(_totalWorkLoadCount)) {
            System.err.printf("Cleanup: %s is cleaning up:\n",member.toString());
            member.removeSharedData();
          }
        }
        else
        {
          DatabusClientGroupMember m = _clusterNode.getMember(NAMESPACE, _group, _name);
          System.err.printf("%s write of shared state=%d didn't succeed: Current leader is : %s\n",m.getName(),(int) i,m.getLeader());
        }
        _clusterNode.close();

      }
    }
    //use only in unit-tests
    synchronized void  leaveGroup()
    {
		DatabusClientGroupMember m = _clusterNode.getMember(NAMESPACE, _group, _name);
    	m.leave();
       _clusterNode.close();
    }

    //use only in unit-tests
    synchronized void  joinGroup()
    {
      _clusterNode.open();
      DatabusClientGroupMember member = _clusterNode.getMember(NAMESPACE, _group, _name);
      member.join();
    }

    @Override
    public String toString()
    {
      return "name=  " + _name + " countWorkLoad=" + _countWorkload + "; actualWorkCount=" + _actualWorkCount + "; sharedWorkCounter=" + _sharedWorkCounter + " totalWorkLoadCount=" + _totalWorkLoadCount ;
    }

    @SuppressWarnings("serial")
    public static class DummyClientException extends Exception {
      public DummyClientException() {
        super();
      }
    }

  }

}
