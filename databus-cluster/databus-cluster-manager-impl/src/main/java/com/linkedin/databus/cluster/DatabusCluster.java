package com.linkedin.databus.cluster;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Vector;

import org.apache.log4j.Logger;

import com.linkedin.helix.ExternalViewChangeListener;
import com.linkedin.helix.HelixException;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.HelixManagerFactory;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.LiveInstanceChangeListener;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.controller.HelixControllerMain;
import com.linkedin.helix.manager.zk.ZKHelixAdmin;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.model.ExternalView;
import com.linkedin.helix.model.IdealState.IdealStateModeProperty;
import com.linkedin.helix.model.InstanceConfig;
import com.linkedin.helix.model.LiveInstance;
import com.linkedin.helix.model.StateModelDefinition;
import com.linkedin.helix.participant.StateMachineEngine;
import com.linkedin.helix.tools.StateModelConfigGenerator;

public class DatabusCluster
{
	public static final String MODULE = DatabusCluster.class.getName();
	public static final Logger LOG = Logger.getLogger(MODULE);
	
	public static final String DEFAULT_STATE_MODEL = "OnlineOffline";
	protected final ZkClient _zkClient;
	protected final ZKHelixAdmin _admin;
	protected final String _clusterName;
	protected final String _zkAddr;
	protected final String _resourceName;
	protected final int _quorum;
	protected final int _numPartitions;
	protected final HashSet<DatabusClusterDataNotifier> _dataNotifiers;
	
	//populated by external watcher;
	protected final DatabusHelixWatcher _watcher;

	
	public DatabusCluster(String zkAddr, String clusterName,String resourceName,int numPartitions,int quorum) throws Exception
	{
		_zkAddr = zkAddr;
		_clusterName = clusterName;
		_resourceName = resourceName;
		_quorum = quorum;
		_numPartitions = numPartitions;
		
		_zkClient = new ZkClient(_zkAddr, ZkClient.DEFAULT_SESSION_TIMEOUT,
				ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());
		_admin = new ZKHelixAdmin(_zkClient);
		_watcher = new DatabusHelixWatcher();
		_dataNotifiers = new HashSet<DatabusClusterDataNotifier> (5);
		
		boolean clusterAdded = true;
		// add cluster
		try 
		{
		    _admin.addCluster(_clusterName, false);
		}
		catch (HelixException e)
		{
		    LOG.warn("Warn! Cluster might already exist! " + _clusterName);
		    clusterAdded = false;
		}
		
		if (clusterAdded)
		{
		    // add state model definition
		    StateModelConfigGenerator generator = new StateModelConfigGenerator();
		    _admin.addStateModelDef(
		            _clusterName,
		            DEFAULT_STATE_MODEL,
		            new StateModelDefinition(generator
		                    .generateConfigForOnlineOffline()));

		    _admin.addResource(_clusterName, _resourceName, _numPartitions,
		            DEFAULT_STATE_MODEL,
		            IdealStateModeProperty.AUTO_REBALANCE.toString());

		    _admin.rebalance(_clusterName, _resourceName, 1);
		}
	}
	
	
	public void start()
	{
	    _watcher.start();
	}
	
	public void shutdown()
	{
	    _watcher.stop();
	    _dataNotifiers.clear();
	}

	public DatabusClusterMember addMember(String id)
	{
		return addMember(id,null);
	}
	
	public DatabusClusterMember addMember(String id,DatabusClusterNotifier notifier)
	{ 
		try
		{
			if (_admin != null)
			{
			    InstanceConfig config = null;
			    try
			    {
			        config = _admin.getInstanceConfig(_clusterName,id);
			    }
			    catch (HelixException e)
			    {
			        //the instance doesn't exist , so adding a new config
			    }
			    if (config != null)
			    {
			        LOG.warn("Member id already exists! Overwriting instance for id=" + id);
			        _admin.dropInstance(_clusterName, config);
			        config=null;
			    }
			    config = new InstanceConfig(id);
			    config.setHostName(InetAddress.getLocalHost().getCanonicalHostName());
			    config.setInstanceEnabled(true);
			    _admin.addInstance(_clusterName, config);
			    return new DatabusClusterMember(id,notifier);
			}
		}
		catch (Exception e)
		{
			LOG.error("Error creating databus cluster member " + id  +  " exception:" + e);
		}
		return null;
	}
	
	/** Add a cluster data notifier to the cluster for notifications on cluster metadata */
	synchronized public void addDataNotifier(DatabusClusterDataNotifier notifier)
	{
	    if (notifier != null)
	    {
	        _dataNotifiers.add(notifier);
	    }
	    else
	    {
	        LOG.warn("Add failed. Attempting to add null DatabusClusterDataNotifier!");
	    }
	}
	
	synchronized public void removeDataNotifier(DatabusClusterDataNotifier notifier)
    {
        if (notifier != null)
        {
            _dataNotifiers.remove(notifier);
        }
    }
	
	public int getNumPartitions()
	{
	    return _numPartitions;
	}
	
	public int getNumActiveMembers()
	{
	    return _watcher.getNumActiveInstances();
	}
	
	public int getNumActivePartitions()
	{
	    return _watcher.getNumActivePartitions();
	}
	
	public HashMap<Integer,String> getActivePartitions()
	{
	    return _watcher.getPartitions();
	}
	
	public String getPartitionOwner(int partition)
	{
	    return _watcher.getOwnerId(partition);
	}
	
	public String getClusterName()
	{
	    return _clusterName;
	}
	
	public String getResourceName()
	{
	    return _resourceName;
	}
	
	public int getQuorum()
	{
		return _quorum;
	}
	
	
	/** A 
	 * cluster member who has the ability to join and leave the cluster **/
	public class DatabusClusterMember
	{
		
		private final HelixManager _manager;
		private  final String _id ;
		private DatabusClusterNotifier _notifier=null;
		
		DatabusClusterMember(String id,DatabusClusterNotifier notifier) throws Exception
		{
			_id = id;
			_manager = HelixManagerFactory.getZKHelixManager(_clusterName,
					_id,
					InstanceType.PARTICIPANT,
					_zkAddr);
			_notifier = notifier;
			registerNotifier();
		}
		
		void registerNotifier()
		{
			StateMachineEngine stateMach = _manager.getStateMachineEngine();
		    DatabusClusterNotifierFactory modelFactory =
		          new DatabusClusterNotifierFactory(_notifier);
		    stateMach.registerStateModelFactory(DEFAULT_STATE_MODEL, modelFactory);
		}
		
		public boolean join()
		{
			if (_manager != null)
			{
				if (!_manager.isConnected())
				{
					try
					{
						_manager.connect();
						return true;
					}
					catch (Exception e)
					{
						LOG.error("Member " + _id + " could not connect! " + e);
					}
				}
				else
				{
					LOG.warn("Member " + _id + " cannot join. Already joined! ");
					return true;
				}
			}
			return false;
		}
		
		public boolean leave()
		{
			if (_manager != null)
			{
				try
				{
					_manager.disconnect();
				}
				catch (Exception e)
				{
					LOG.error("Member " + _id + " could not disconnect! " + e);
				}
			}
			//ensure that whitelisted instance configs go away
			if (_admin != null)
			{
			    try
			    {
			        _admin.dropInstance(_clusterName, _admin.getInstanceConfig(_clusterName, _id));
			    }
			    catch (HelixException e)
			    {
			        LOG.warn("Drop instance failed for id= " + _id  + " exception" + e);
			    }
			}
			return true;
		}
		
		/** Used for unit-testing **/
		protected DatabusClusterMember()
		{
			_manager = null;
			_id = null;
		}
		
	}

	
	
	
	private class DatabusHelixWatcher implements LiveInstanceChangeListener, ExternalViewChangeListener
	{
	    
	    final private HelixManager _manager;
	    private int _numActiveInstances =-1; 
	    final private Random _random = new Random(System.currentTimeMillis()); 
	    final private HashMap<Integer,String> _partitionMap;

	    //State to help control helix assignment - enable/disable cluster
	    private HelixManager _helixManager = null;
	    //has Cluster been paused?
	    private boolean _paused = false;
	    private final int _id;
	    
	    public DatabusHelixWatcher() throws Exception
        { 
            
	        _id = _random.nextInt();
	        _manager = HelixManagerFactory.getZKHelixManager(_clusterName,
	                "watcher_" + _clusterName + "_" + _id ,
	                InstanceType.SPECTATOR,
	                _zkAddr);
	        _partitionMap = new HashMap<Integer,String>(_numPartitions);
            
        }

	    public void start()
	    {
	        if (_manager != null)
	        {
	            try
                {
                    _manager.connect();
                    _manager.addLiveInstanceChangeListener(this);
                    _manager.addExternalViewChangeListener(this);
                }
	            catch (Exception e)
                {
                   LOG.error("Cannot start HelixWatcher! " + e) ;
                }
	        }
	    }
	    
	    public void stop()
	    {
	        if (_manager != null)
	        {
	            _manager.disconnect();
	        }
	        stopHelixController();
	    }
	    
        @Override
        public synchronized void onLiveInstanceChange(List<LiveInstance> liveInstances,
                NotificationContext changeContext)
        {
            _numActiveInstances = liveInstances.size();
            boolean quorumReached = (_numActiveInstances >= _quorum);
            if (quorumReached)
            {
                if (_helixManager == null)
                {
                    LOG.warn("Quorum Reached! numNodes=" + _numActiveInstances + " quorum=" + _quorum);
                    //controller needs to be started
                    startHelixController();
                }
                else if (_paused)
                {
                    resumeHelixController();
                    _paused=false;
                }
            } 
            else
            {
                LOG.warn("Number of nodes inadequate=" + _numActiveInstances + " Need at least:" + _quorum);
                if (_helixManager != null && !_paused)
                {
                    //controller has started; but pauseCluster
                    pauseHelixController();
                    _paused=true;
                }
            }
            
            //perform user-specified callback
            if (!_dataNotifiers.isEmpty())
            {
                Vector<String> nodeList = new Vector<String>(liveInstances.size());
                for (LiveInstance i : liveInstances)
                {   
                    nodeList.add(i.getInstanceName());
                }
                for (DatabusClusterDataNotifier notifier: _dataNotifiers)
                {
                    notifier.onInstanceChange(nodeList);
                }
            }
        }
        

        private Integer getPartition(String partition)
        {
            String[] ps = partition.split("_");
            if (ps.length >= 2)
            {
                return Integer.parseInt(ps[ps.length-1]);
            }
            return -1;
        }
        
        @Override
        public synchronized void onExternalViewChange(List<ExternalView> externalViewList,
                NotificationContext changeContext)
        {
            _partitionMap.clear();
            for (ExternalView v: externalViewList)
            {
                if (v.getResourceName().equals(_resourceName)) 
                {
                    for (String k : v.getPartitionSet())
                    {
                        Map<String,String> map = v.getStateMap(k);
                        if (map != null)
                        {
                            for (String mk: map.keySet()) 
                            {
                                String value = map.get(mk);
                                if (value != null)
                                {
                                    Integer partition = getPartition(k);
                                    if (value.equals("ONLINE"))
                                    {
                                        _partitionMap.put(partition, mk);
                                    }
                                }
                            }
                        }
                    }
                }
            } 
            
            //external call
            if (!_dataNotifiers.isEmpty())
            {
                HashMap<Integer,String> pmap = getPartitions();
                for (DatabusClusterDataNotifier notifier: _dataNotifiers)
                {
                    notifier.onPartitionMappingChange(pmap);
                }
            }
        } 
        
        
        synchronized public int getNumActiveInstances()
        {
            return _numActiveInstances;
        }
	    
        synchronized public int getNumActivePartitions()
        {
            return _partitionMap.size();
        }
        
        synchronized public String getOwnerId(int numPartition)
        {
            return _partitionMap.get(numPartition);
        }
        
        synchronized public HashMap<Integer,String> getPartitions()
        {
            HashMap<Integer,String> map = new HashMap<Integer,String>(_partitionMap.size());
            map.putAll(_partitionMap);
            return map;
        }
        
        /** methods to control helix's assignment of partitions **/ 
        
        void stopHelixController()
        {   
            if (_helixManager != null)
            {
                LOG.warn("Shutting down cluster : " + _helixManager.getClusterName() + " instance:"+ _helixManager.getInstanceName());
                _helixManager.disconnect();
            }
        }
        
        void pauseHelixController()
        {
            if (_admin != null)
            {
                LOG.warn("Pausing  cluster : " + _clusterName);
                _admin.enableCluster(_clusterName, false);
            }
        }
        
        void resumeHelixController()
        {
            if (_admin != null)
            {
                LOG.warn("Resuming  cluster : " + _clusterName);
                _admin.enableCluster(_clusterName, true);
            }
        }
        
        void startHelixController()
        {
            try
            {
                String controllerId = "controller_" + _id;
                LOG.info("Starting cluster controller for cluster=" + _clusterName +  " with id = " + controllerId);
                _helixManager = HelixControllerMain
                        .startHelixController(_zkAddr, _clusterName, controllerId,
                                HelixControllerMain.STANDALONE);
                if (_admin != null)
                {
                    _admin.enableCluster(_helixManager.getClusterName(), true);
                }
            }
            catch (Exception e)
            {
                LOG.error("Cannot start cluster controller for cluster=" +  _clusterName + e);
            }
        }
        
	}

	/**
	 * Used only for unit-testing
	 */
	protected DatabusCluster()
	{
		_admin = null;
		_zkAddr = null;
		_watcher = null;
		_zkClient = null;
		_clusterName = null;
		_resourceName = null;
		_quorum = 0;
		_numPartitions = 0;
		_dataNotifiers = null;
	}


	@Override
	public String toString() {
		return "DatabusCluster [_clusterName=" + _clusterName + ", _zkAddr="
				+ _zkAddr + ", _resourceName=" + _resourceName + ", _quorum="
				+ _quorum + ", _numPartitions=" + _numPartitions + "]";
	}
	
	
}
