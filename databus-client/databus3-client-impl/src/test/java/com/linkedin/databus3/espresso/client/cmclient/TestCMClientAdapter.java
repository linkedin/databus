package com.linkedin.databus3.espresso.client.cmclient;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.helix.ConfigAccessor;
import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.ClusterMessagingService;
import com.linkedin.helix.ConfigChangeListener;
import com.linkedin.helix.ControllerChangeListener;
import com.linkedin.helix.CurrentStateChangeListener;
import com.linkedin.helix.ExternalViewChangeListener;
import com.linkedin.helix.HealthStateChangeListener;
import com.linkedin.helix.HelixAdmin;
import com.linkedin.helix.HelixDataAccessor;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.IdealStateChangeListener;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.LiveInstanceChangeListener;
import com.linkedin.helix.MessageListener;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.NotificationContext.Type;
import com.linkedin.helix.PreConnectCallback;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.healthcheck.ParticipantHealthReportCollector;
import com.linkedin.helix.model.ExternalView;
import com.linkedin.helix.participant.StateMachineEngine;
import com.linkedin.helix.store.PropertyStore;
import com.linkedin.helix.store.zk.ZkHelixPropertyStore;
import com.linkedin.databus.client.pub.DatabusServerCoordinates;
import com.linkedin.databus.core.cmclient.ResourceKey;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.test.TestUtil;

public class TestCMClientAdapter 
{ 
  public static final Logger LOG = Logger.getLogger("TestCMClientAdapter");
  
  @BeforeClass
  public void setupClass() throws InvalidConfigException
  {
	  TestUtil.setupLogging(true, null, Level.DEBUG);
  }

  @Test
  public void testDetectNoChangeOnRelayLeaderStandby()
  throws Exception
  {
	  TestClusterManager cm = new TestClusterManager();	  
	  ClientAdapter ca = new ClientAdapter("127.0.0.1:8999", "db", cm, true);	
	  TestExternalViewChangeObserver observer = new TestExternalViewChangeObserver();
	  
	  String db = new String("relayLeaderStandby");
	  Map<String, Map<String, String>> mp = new HashMap<String, Map<String, String>>();
	  ZNRecord oldZnRecord = getZRecords(db, mp);
	  List<ExternalView> evs = new ArrayList<ExternalView>();
	  evs.add(new ExternalView(oldZnRecord));
	  ca.addExternalViewChangeObservers(observer);
	  NotificationContext nc = new NotificationContext(cm);
	  nc.setType(Type.INIT);
	  ca.onExternalViewChange(evs, nc);

	  Map<ResourceKey, List<DatabusServerCoordinates>> actualResourceToServerCoordinatesMap = observer.getNewResourceToServerCoordinatesMap();
	  Map<DatabusServerCoordinates, List<ResourceKey>> actualServerToResouceMap = observer.getNewServerCoordinatesToResourceMap();
	  
	  Assert.assertEquals(observer.getOldResourceToServerCoordinatesMap() == null, true, "Old ResourceToServerMap is Null !");
	  Assert.assertEquals(observer.getOldServerCoordinatesToResourceMap() == null, true, "Old ServerCoordinatesToResourceMap is Null !");
	  Assert.assertEquals(actualResourceToServerCoordinatesMap == null, true, "New ResourceToServerMap is not Null !");
	  Assert.assertEquals(actualServerToResouceMap == null, true, "New ServerCoordinatesToResourceMap is not Null !");	  
  }
  
  @Test
  public void testExternalViewChange() 
    throws Exception
  {	  
	  TestClusterManager cm = new TestClusterManager();	  
	  ClientAdapter ca = new ClientAdapter("127.0.0.1:8999", "db", cm, true);
	  TestExternalViewChangeObserver observer = new TestExternalViewChangeObserver();
	  
	  Map<String, Map<String, String>> mp = new HashMap<String, Map<String, String>>();
	  Map<ResourceKey, List<DatabusServerCoordinates>> expMap = new HashMap<ResourceKey, List<DatabusServerCoordinates>>();
	  Set<ResourceKey> rKeySet = new HashSet<ResourceKey>();
	  Map<DatabusServerCoordinates, List<ResourceKey>> expRMap = new HashMap<DatabusServerCoordinates, List<ResourceKey>>();
	  Set<DatabusServerCoordinates> serverSet = new HashSet<DatabusServerCoordinates>();
      
	  populateCase1(mp,expMap,rKeySet, expRMap, serverSet);
	  
	  String db = new String("db");
	  ZNRecord oldZnRecord = getZRecords(db, mp);
	  List<ExternalView> evs = new ArrayList<ExternalView>();
	  evs.add(new ExternalView(oldZnRecord));
	  ca.addExternalViewChangeObservers(observer);
	  NotificationContext nc = new NotificationContext(cm);
	  nc.setType(Type.INIT);
	  ca.onExternalViewChange(evs, nc);
	  Map<ResourceKey, List<DatabusServerCoordinates>> actualResourceToServerCoordinatesMap = observer.getNewResourceToServerCoordinatesMap();
	  Map<DatabusServerCoordinates, List<ResourceKey>> actualServerToResouceMap = observer.getNewServerCoordinatesToResourceMap();
	  LOG.info(actualServerToResouceMap);
	  
	  Assert.assertEquals(observer.getOldResourceToServerCoordinatesMap() == null, true, "Old ResourceToServerMap is Null !");
	  Assert.assertEquals(observer.getOldServerCoordinatesToResourceMap() == null, true, "Old ServerCoordinatesToResourceMap is Null !");
	  Assert.assertEquals(actualResourceToServerCoordinatesMap == null, false, "New ResourceToServerMap is not Null !");
	  Assert.assertEquals(actualServerToResouceMap == null, false, "New ServerCoordinatesToResourceMap is not Null !");
	  
	  Assert.assertEquals(actualResourceToServerCoordinatesMap.size(), expMap.size());
	  
	  for (ResourceKey rk : rKeySet)
	  {
		LOG.info("Comparing ServerCoordinates !!");
		List<DatabusServerCoordinates> exp = expMap.get(rk);
		List<DatabusServerCoordinates> act = actualResourceToServerCoordinatesMap.get(rk);
		Collections.sort(exp);
		Collections.sort(act);
				
	    Assert.assertEquals(act,exp);
	  }	  
	  
	  Assert.assertEquals(actualServerToResouceMap.size(), expRMap.size());
	  
	  for (DatabusServerCoordinates s : serverSet)
	  {
		LOG.info("Comparing ResourceKeys !!");
		List<ResourceKey> exp = expRMap.get(s);
		List<ResourceKey> act = actualServerToResouceMap.get(s);
		Collections.sort(exp);
		Collections.sort(act);
	    Assert.assertEquals(act,exp);
	  }	
	  
	  Map<String, Map<String, String>> mp2 = new HashMap<String, Map<String, String>>();
	  Map<ResourceKey, List<DatabusServerCoordinates>> expMap2 = new HashMap<ResourceKey, List<DatabusServerCoordinates>>();
	  Set<ResourceKey> rKeySet2 = new HashSet<ResourceKey>();
      Map<DatabusServerCoordinates, List<ResourceKey>> expRMap2 = new HashMap<DatabusServerCoordinates, List<ResourceKey>>();
      Set<DatabusServerCoordinates> serverSet2 = new HashSet<DatabusServerCoordinates>();
	  populateCase2(mp2,expMap2,rKeySet2,expRMap2, serverSet2);
	  ZNRecord newZnRecord = getZRecords(db, mp2);
	  evs = new ArrayList<ExternalView>();
	  evs.add(new ExternalView(newZnRecord));
	  ca.onExternalViewChange(evs, nc);
	  
	  actualResourceToServerCoordinatesMap = observer.getOldResourceToServerCoordinatesMap();
	  Map<ResourceKey, List<DatabusServerCoordinates>> actualResourceToServerCoordinatesMap2 = observer.getNewResourceToServerCoordinatesMap();
	  actualServerToResouceMap = observer.getOldServerCoordinatesToResourceMap();
	  Map<DatabusServerCoordinates, List<ResourceKey>> actualServerToResouceMap2 = observer.getNewServerCoordinatesToResourceMap();

	  Assert.assertEquals(actualResourceToServerCoordinatesMap.size(), expMap.size());
	  
	  for (ResourceKey rk : rKeySet)
	  {
		LOG.info("Comparing Old ServerCoordinates !!");
		List<DatabusServerCoordinates> exp = expMap.get(rk);
		List<DatabusServerCoordinates> act = actualResourceToServerCoordinatesMap.get(rk);
		Collections.sort(exp);
		Collections.sort(act);
	    Assert.assertEquals(exp,act);
	  }	  
	  	  
	  for (DatabusServerCoordinates s : serverSet)
	  {
		LOG.info("Comparing ResourceKeys !!");
		List<ResourceKey> exp = expRMap.get(s);
		List<ResourceKey> act = actualServerToResouceMap.get(s);
		Collections.sort(exp);
		Collections.sort(act);
	    Assert.assertEquals(act,exp);
	  }	
	  
	  Assert.assertEquals(actualResourceToServerCoordinatesMap2.size(), expMap2.size());
	  Assert.assertEquals(actualServerToResouceMap2.size(), expRMap2.size());

	  for (ResourceKey rk : rKeySet2)
	  {
		LOG.info("Comparing New ServerCoordinates !!");
		List<DatabusServerCoordinates> exp = expMap2.get(rk);
		List<DatabusServerCoordinates> act = actualResourceToServerCoordinatesMap2.get(rk);
		Collections.sort(exp);
		Collections.sort(act);
	    Assert.assertEquals(exp,act);
	  }	  	 
	  
	  
  }

  @Test
  public void testEmptyExternalView() 
    throws Exception
  {	  
	  TestClusterManager cm = new TestClusterManager();
	  String dbName = "db";
	  ClientAdapter ca = new ClientAdapter("127.0.0.1:8099", "db", cm, true);
	  Map<ResourceKey, List<DatabusServerCoordinates>> ev1 = ca.getExternalView(true, dbName);
	  Assert.assertEquals(0, ev1.size());
	  Map<ResourceKey, List<DatabusServerCoordinates>> ev2 = ca.getExternalView(false, dbName);
	  Assert.assertEquals(0, ev2.size());
	  Map<ResourceKey, List<DatabusServerCoordinates>> ev3 = ClientAdapter.getExternalView(null);
	  Assert.assertEquals(0, ev3.size());

	  Map<DatabusServerCoordinates, List<ResourceKey>> iev1 = ca.getInverseExternalView(true, dbName);
	  Assert.assertEquals(0, iev1.size());
	  Map<DatabusServerCoordinates, List<ResourceKey>> iev2 = ca.getInverseExternalView(false, dbName);
	  Assert.assertEquals(0, iev2.size());
	  Map<DatabusServerCoordinates, List<ResourceKey>> iev3 = ClientAdapter.getInverseExternalView(null);
	  Assert.assertEquals(0, iev3.size());
  }
  
  private void populateCase1(Map<String, Map<String, String>> mp,  
		                     Map<ResourceKey, List<DatabusServerCoordinates>> expMap,
		                     Set<ResourceKey> rKeySet,
		                     Map<DatabusServerCoordinates, List<ResourceKey>> expRMap,
		                     Set<DatabusServerCoordinates> serverSet)
	 throws Exception
  {
	  String[] oldResourceKeys = 
		  { 
			  "ela4-db1-espresso.prod.linkedin.com_1521,bizProfile,p1_1,MASTER",
			  "ela4-db11-espresso.prod.linkedin.com_1521,bizProfile,p1_1,SLAVE",
			  "ela4-db2-espresso.prod.linkedin.com_1522,bizProfile,p2_1,MASTER",
			  "ela4-db12-espresso.prod.linkedin.com_1522,bizProfile,p2_1,SLAVE",
		  };

	  String[] oldRelayHosts = {
			  "ela4-rly1.corp.linkedin.com_1111",
			  "ela4-rly2.corp.linkedin.com_2222",
			  "ela4-rly3.corp.linkedin.com_3333"
	  };

	  String oldOfflineHost = "ela4-rly4.corp.linkedin.com_2333";
	  populateCase(mp, expMap, rKeySet, expRMap, serverSet, oldResourceKeys, oldRelayHosts, oldOfflineHost);
  }
  
  private void populateCase2(Map<String, Map<String, String>> mp,  
		                     Map<ResourceKey, List<DatabusServerCoordinates>> expMap,
		                     Set<ResourceKey> rKeySet,
		                     Map<DatabusServerCoordinates, List<ResourceKey>> expRMap,
		                     Set<DatabusServerCoordinates> serverSet)
		  	throws Exception
  {
	  String[] oldResourceKeys = 
		  { 
			  "ela4-db1-espresso.prod.linkedin.com_1521,bizProfile,p1_1,MASTER",
			  "ela4-db11-espresso.prod.linkedin.com_1521,bizProfile,p1_1,SLAVE",
			  "ela4-dbNew2-espresso.prod.linkedin.com_1522,bizProfile,p2_1,MASTER",
			  "ela4-dbNew12-espresso.prod.linkedin.com_1522,bizProfile,p2_1,SLAVE",
		  };

	  String[] oldRelayHosts = {
			  "ela4-rly21.corp.linkedin.com_1111",
			  "ela4-rly22.corp.linkedin.com_2222",
			  "ela4-rly23.corp.linkedin.com_3333"
	  };

	  String oldOfflineHost = "ela4-rlyNew4.corp.linkedin.com_2333";
	  populateCase(mp, expMap, rKeySet, expRMap, serverSet, oldResourceKeys, oldRelayHosts, oldOfflineHost);
  }
  
  private void populateCase(Map<String, Map<String, String>> mp,  Map<ResourceKey, 
		                    List<DatabusServerCoordinates>> expMap,
		                    Set<ResourceKey> rKeySet,
		                    Map<DatabusServerCoordinates, List<ResourceKey>> expRMap,
		                    Set<DatabusServerCoordinates> serverSet,
		                    String[] oldResourceKeys,
		                    String[] oldRelayHosts,
		                    String oldOfflineHost)
  	throws Exception
  {

	  for ( int i = 0 ; i < oldResourceKeys.length; i++)
	  {
		  Map<String, String> mp2 = new HashMap<String,String>();
		  List<DatabusServerCoordinates> c = new ArrayList<DatabusServerCoordinates>();		
		  ResourceKey r1 = new ResourceKey(oldResourceKeys[i]);
		  
		  for ( int j = 0; j < oldRelayHosts.length; j++)
		  {
			  mp2.put(oldRelayHosts[j], "ONLINE");

			  String []ip = oldRelayHosts[j].split("_");
			  InetSocketAddress ia = new InetSocketAddress(ip[0], Integer.parseInt(ip[1]) );
			  DatabusServerCoordinates rc = new DatabusServerCoordinates(ia,"ONLINE");
			  serverSet.add(rc);
			  c.add(rc);
			  
			  List<ResourceKey> rkeys = expRMap.get(rc);
			  if (rkeys == null)
			  {
				  rkeys = new ArrayList<ResourceKey>();
				  expRMap.put(rc, rkeys);
			  }
			  rkeys.add(r1);
		  }
		  
		  mp2.put(oldOfflineHost, "OFFLINE");
		  String []ip = oldOfflineHost.split("_");
		  InetSocketAddress ia = new InetSocketAddress(ip[0], Integer.parseInt(ip[1]) );
		  DatabusServerCoordinates rc = new DatabusServerCoordinates(ia,"OFFLINE");
		  c.add(rc);
		  serverSet.add(rc);
		  mp.put(oldResourceKeys[i], mp2);
		  rKeySet.add(r1);
		  
		  List<ResourceKey> rkeys = expRMap.get(rc);
		  if (rkeys == null)
		  {
			  rkeys = new ArrayList<ResourceKey>();
			  expRMap.put(rc, rkeys);
		  }
		  rkeys.add(r1);
		  
		  expMap.put(r1,c);
	  }
  }
  
  
  private ZNRecord getZRecords(String id, Map<String, Map<String, String>> mp)
  {
	  ZNRecord znRecord = new ZNRecord(id);
	  znRecord.setMapFields(mp);
	  return znRecord;
  }
  
  public static class TestExternalViewChangeObserver
  	implements DatabusExternalViewChangeObserver
  {
	private Map<ResourceKey, List<DatabusServerCoordinates>> oldResourceToServerCoordinatesMap;
	private Map<DatabusServerCoordinates, List<ResourceKey>> oldServerCoordinatesToResourceMap;
	private Map<ResourceKey, List<DatabusServerCoordinates>> newResourceToServerCoordinatesMap;
	private Map<DatabusServerCoordinates, List<ResourceKey>> newServerCoordinatesToResourceMap;
	
	@Override
	public void onExternalViewChange(
			String dbName,
			Map<ResourceKey, List<DatabusServerCoordinates>> oldResourceToServerCoordinatesMap,
			Map<DatabusServerCoordinates, List<ResourceKey>> oldServerCoordinatesToResourceMap,
			Map<ResourceKey, List<DatabusServerCoordinates>> newResourceToServerCoordinatesMap,
			Map<DatabusServerCoordinates, List<ResourceKey>> newServerCoordinatesToResourceMap) 
	{
		this.newResourceToServerCoordinatesMap = newResourceToServerCoordinatesMap;
		this.oldResourceToServerCoordinatesMap = oldResourceToServerCoordinatesMap;
		this.newServerCoordinatesToResourceMap = newServerCoordinatesToResourceMap;
		this.oldServerCoordinatesToResourceMap = oldServerCoordinatesToResourceMap;
	}

	public Map<ResourceKey, List<DatabusServerCoordinates>> getOldResourceToServerCoordinatesMap() {
		return oldResourceToServerCoordinatesMap;
	}

	public Map<DatabusServerCoordinates, List<ResourceKey>> getOldServerCoordinatesToResourceMap() {
		return oldServerCoordinatesToResourceMap;
	}

	public Map<ResourceKey, List<DatabusServerCoordinates>> getNewResourceToServerCoordinatesMap() {
		return newResourceToServerCoordinatesMap;
	}

	public Map<DatabusServerCoordinates, List<ResourceKey>> getNewServerCoordinatesToResourceMap() {
		return newServerCoordinatesToResourceMap;
	}
	
  }
  
  public static class TestClusterManager implements HelixManager
  {

	@Override
	public void addConfigChangeListener(ConfigChangeListener arg0)
			throws Exception 
	{
		
	}

	@Override
	public void addControllerListener(ControllerChangeListener arg0) 
	{
		
	}

	@Override
	public void addCurrentStateChangeListener(CurrentStateChangeListener arg0,
			String arg1, String arg2) throws Exception 
	{
		
	}

	@Override
	public void addExternalViewChangeListener(ExternalViewChangeListener arg0)
			throws Exception 
	{		
	}

	@Override
	public void addIdealStateChangeListener(IdealStateChangeListener arg0)
			throws Exception 
	{		
	}

	@Override
	public void addLiveInstanceChangeListener(LiveInstanceChangeListener arg0)
			throws Exception 
	{		
	}

	@Override
	public void addMessageListener(MessageListener arg0, String arg1)
			throws Exception 
	{	
	}

	@Override
	public void connect() throws Exception 
	{		
	}

	@Override
	public void disconnect() 
	{		
	}

	@Override
	public HelixAdmin getClusterManagmentTool() 
	{
		return null;
	}

	@Override
	public String getClusterName() 
	{
		return null;
	}

	@Override
	public DataAccessor getDataAccessor() 
	{
		return null;
	}

	@Override
	public ParticipantHealthReportCollector getHealthReportCollector() 
	{
		return null;
	}

	@Override
	public String getInstanceName() 
	{
		return null;
	}

	@Override
	public InstanceType getInstanceType() 
	{
		return null;
	}

	@Override
	public long getLastNotificationTime() 
	{
		return 0;
	}

	@Override
	public ClusterMessagingService getMessagingService() 
	{
		return null;
	}

	@Override
	public PropertyStore<ZNRecord> getPropertyStore() 
	{
		return null;
	}

	@Override
	public String getSessionId() 
	{
		return null;
	}

	@Override
	public boolean isConnected() 
	{
		return false;
	}

	@Override
	public boolean removeListener(Object arg0) 
	{
		return false;
	}

	@Override
	public void addHealthStateChangeListener(
			HealthStateChangeListener listener, String instanceName)
			throws Exception {
		
	}

	@Override
	public ConfigAccessor getConfigAccessor() {
		return null;
	}

	@Override
	public String getVersion() {
		return null;
	}

	@Override
	public StateMachineEngine getStateMachineEngine() {
		return null;
	}

	@Override
	public boolean isLeader() {
		return false;
	}

	@Override
	public void startTimerTasks() {
		
	}

	@Override
	public void stopTimerTasks() {
		
	}

	@Override
	public HelixDataAccessor getHelixDataAccessor() {
		return null;
	}

	@Override
	public void addPreConnectCallback(PreConnectCallback callback) {		
	}

	@Override
	public ZkHelixPropertyStore<ZNRecord> getHelixPropertyStore() {
	    return null;
	}

  }
	
}

