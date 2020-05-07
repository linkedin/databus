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

import java.net.InetAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Vector;

import org.apache.helix.ExternalViewChangeListener;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.LiveInstanceChangeListener;
import org.apache.helix.NotificationContext;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.IdealStateModeProperty;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.tools.StateModelConfigGenerator;
import org.apache.log4j.Logger;

import com.linkedin.databus.client.registration.ClusterRegistrationStaticConfig;

public class DatabusCluster
{
    public static final String MODULE = DatabusCluster.class.getName();
    public static final Logger LOG = Logger.getLogger(MODULE);

    public static final String DEFAULT_STATE_MODEL = "OnlineOffline";
    public static final String DEFAULT_RESOURCE_NAME = "default-resource";
    //default time in ms that we wait for cluster to be setup
    public static final int DEFAULT_CLUSTER_CREATE_WAIT_MS = 1000;

    private static final String HELIX_MANAGER_ZK_SESSION_TIMEOUT_KEY = "zk.session.timeout";

    protected final ZkClient _zkClient;
    protected final ZKHelixAdmin _admin;
    protected final String _clusterName;
    protected final String _zkAddr;
    protected final int _quorum;
    protected final int _numPartitions;
    protected final HashSet<DatabusClusterDataNotifier> _dataNotifiers;

    // populated by external watcher;
    protected final DatabusHelixWatcher _watcher;

    private final int _zkConnectionTimeoutMs;
    private final int _zkSessionTimeoutMs;

    public DatabusCluster(ClusterRegistrationStaticConfig config) throws Exception
    {
        _zkAddr = config.getZkAddr();
        _clusterName = config.getClusterName();
        _quorum = (int)(config.getQuorum());
        _numPartitions = (int)(config.getNumPartitions());

        _zkSessionTimeoutMs = config.getZkSessionTimeoutMs();
        _zkConnectionTimeoutMs = config.getZkConnectionTimeoutMs();

        updateHelixManagerZkSessionTimeout(_zkSessionTimeoutMs);

        _zkClient = new ZkClient(_zkAddr, _zkSessionTimeoutMs, _zkConnectionTimeoutMs, new ZNRecordSerializer());
        _admin = new ZKHelixAdmin(_zkClient);
        _dataNotifiers = new HashSet<DatabusClusterDataNotifier>(5);

        //attempt to create a cluster
        int part = create(_admin, _zkClient, _clusterName, _numPartitions);

        //at this stage a cluster and resources should have been created, either by this instance or someone else
        if (part >= 0)
        {
            if (_numPartitions != part)
            {
                String msg = "Cannot create DatabusCluster!  Cluster exists with num partitions="
                        + part
                        + ". Tried to join with "
                        + _numPartitions
                        + " partitions";
                throw new DatabusClusterException(msg);
            }
        }
        else
        {
            throw new DatabusClusterException("Cluster " + _clusterName + " could not be accessed. Num partitions returned -1");
        }

        // initialize watcher after creating a cluster
        _watcher = new DatabusHelixWatcher();
    }

    /**
     * Updates the zk.session.timeout system property for ZK connections made by the Helix manager
     */
    private static void updateHelixManagerZkSessionTimeout(int timeoutMs)
    {
      String timeoutStr = System.getProperty(HELIX_MANAGER_ZK_SESSION_TIMEOUT_KEY);
      if (null != timeoutStr)
      {
        try
        {
          int envTimeoutMs = Integer.parseInt(timeoutStr);
          if (envTimeoutMs >= timeoutMs)
          {
            //the existing timeout is larger than ours, so keep as it is
            return;
          }
        }
        catch (NumberFormatException e)
        {
          LOG.warn("invalid existing value for " + HELIX_MANAGER_ZK_SESSION_TIMEOUT_KEY + ": " + timeoutStr);
        }
      }

      System.setProperty(HELIX_MANAGER_ZK_SESSION_TIMEOUT_KEY, Integer.toString(timeoutMs));
    }

    /**
     *
     * @return number of partitions in this resource; called mainly to do sanity
     *         check - if requested number of partitions is same as existing
     *         partition If this can't be determined;return -1 To resize, use a
     *         new cluster name
     */
    static protected int getNumPartitionsInResource(ZKHelixAdmin admin, String clusterName,String resourceName)
    {
        if (admin != null)
        {
            try
            {
                IdealState idealState = admin.getResourceIdealState(clusterName,
                        resourceName);
                if (idealState != null)
                {
                    return idealState.getNumPartitions();
                }
                else
                {
                    return 0;
                }
            }
            catch (Exception e)
            {
                LOG.warn("Resource " + resourceName + " not found in " + clusterName);
                return 0;
            }
        }
        return -1;
    }

    /**
     * create a cluster with a partitioned resource .
     * If successful, a non-zero number indicating the number of partitions created in the cluster is returned ;
     * This is meant to be atomic. If more than one thread/instance attempts to create the same cluster with diff number of partitions
     * only one of them will win . If a cluster exists, the number returned however will be the same in both those instances.
     * If the cluster could not be reached 0 is returned (retry possible) , and if there are other errors -1 is returned (retry not possible)
     */
    static public int create(ZKHelixAdmin admin, ZkClient zkClient, String clusterName,
            int numPartitions)
    {
        boolean clusterAdded = true;
        // add cluster
        try
        {
          /**
           *  TODO : HACK !! : Copying this logic from OLD Helix library to mimic similar old "behavior".
           *
           *  Please see DDSDBUS-2579/HELIX-137
           *  The helix addCluster() ( in 0.6.2.3) API  has a new problem  where callers could not differentiate
           *  between the case when new cluster is created and the case where it was created by some other client. This was needed
           *  so that the follow-up steps of adding state-model (non-idempotent operation) can be done only by the client creating the cluster.
           *  Both old (0.6.1.3) and new Helix (0.6.2.3 ) library has the following issue:
           *   (a)  "No Atomicity in the face of the ZK client disconnects which results in cluster only partly
           *       initialized and unusable. This is noticed in PCL/PCS environment"
           *
           *  In order to workaround the backwards incompatibility issue between the 2 helix versions, we are reproducing part
           *  of the old addCluster() implementation below to get the same behavior as that of using 0.6.1.3 . The problem referred
           *  as (a) still exists.
           *
           */
          if (zkClient.exists("/" + clusterName))
          {
            throw new Exception("Cluster already exists !!");
          }

          clusterAdded = admin.addCluster(clusterName, false);

          if ( ! clusterAdded )
          {
            LOG.error("Problem creating cluster (" + clusterName + ")");
          }
        }
        catch (Exception e)
        {
            LOG.warn("Warn! Cluster might already exist! " + clusterName + " Exception="  + e.getMessage());
            clusterAdded = false;
        }

        if (clusterAdded)
        {
             //LOG.warn("Added new cluster " + clusterName
             //      + " . Creating resource " + DEFAULT_RESOURCE_NAME);
            // add state model definition
            try
            {
                admin.addStateModelDef(
                        clusterName,
                        DEFAULT_STATE_MODEL,
                        new StateModelDefinition(StateModelConfigGenerator
                                .generateConfigForOnlineOffline()));

                admin.addResource(clusterName, DEFAULT_RESOURCE_NAME,
                        numPartitions, DEFAULT_STATE_MODEL,
                        IdealStateModeProperty.AUTO_REBALANCE.toString());

                admin.rebalance(clusterName, DEFAULT_RESOURCE_NAME, 1);
            }
            catch (Exception e)
            {
                LOG.warn("Resource addition incomplete. May have been completed by another instance: " + e.getMessage());
                clusterAdded = false;
            }
        }

        //Ensure that cluster is setup fully
        int part = getNumPartitionsInResource(admin, clusterName, DEFAULT_RESOURCE_NAME);
        if (part == 0)
        {
            long startTimeMs = System.currentTimeMillis();
            try
            {
                do
                {
                    Thread.sleep(100);
                    part = getNumPartitionsInResource(admin, clusterName, DEFAULT_RESOURCE_NAME);
                }
                while (part==0 && ((System.currentTimeMillis()-startTimeMs) < DEFAULT_CLUSTER_CREATE_WAIT_MS) );
            }
            catch (InterruptedException e)
            {
                LOG.warn("Cluster create wait interrupted for cluster=" + clusterName + " exception= " + e.getMessage());
            }
        }
        return part;
    }


    public void start()
    {

        if (_watcher != null)
        {
            _watcher.start();
        }
    }

    public void shutdown()
    {
        if (_watcher != null)
        {
            _watcher.stop();
        }
        if (_dataNotifiers != null)
        {
            _dataNotifiers.clear();
        }
        if (_zkClient != null)
        {
            _zkClient.close();
        }
    }

    public DatabusClusterMember addMember(String id)
    {
        return addMember(id, null);
    }

    public DatabusClusterMember addMember(String id,
            DatabusClusterNotifier notifier)
    {
        try
        {
            if (_admin != null)
            {
                InstanceConfig config = null;
                try
                {
                    config = _admin.getInstanceConfig(_clusterName, id);
                }
                catch (HelixException e)
                {
                    // the instance doesn't exist , so adding a new config
                }
                if (config != null)
                {
                    LOG.warn("Member id already exists! Overwriting instance for id="
                            + id);
                    _admin.dropInstance(_clusterName, config);
                    config = null;
                }
                config = new InstanceConfig(id);
                config.setHostName(InetAddress.getLocalHost()
                        .getCanonicalHostName());
                config.setInstanceEnabled(true);
                _admin.addInstance(_clusterName, config);
                return new DatabusClusterMember(id, notifier);
            }
        }
        catch (Exception e)
        {
            LOG.error("Error creating databus cluster member " + id
                    + " exception:" + e);
        }
        return null;
    }

    /**
     * Add a cluster data notifier to the cluster for notifications on cluster
     * metadata
     */
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

    synchronized public void removeDataNotifier(
            DatabusClusterDataNotifier notifier)
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

    public HashMap<Integer, String> getActivePartitions()
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

    public int getQuorum()
    {
        return _quorum;
    }

    /**
     * A cluster member who has the ability to join and leave the cluster
     **/
    public class DatabusClusterMember
    {

        private final HelixManager _manager;
        private final String _id;
        private DatabusClusterNotifier _notifier = null;

        DatabusClusterMember(String id, DatabusClusterNotifier notifier)
                throws Exception
        {
            _id = id;
            _manager = HelixManagerFactory.getZKHelixManager(_clusterName, _id,
                    InstanceType.PARTICIPANT, _zkAddr);
            _notifier = notifier;
            registerNotifier();
        }

        void registerNotifier()
        {
            StateMachineEngine stateMach = _manager.getStateMachineEngine();
            DatabusClusterNotifierFactory modelFactory = new DatabusClusterNotifierFactory(
                    _notifier);
            stateMach.registerStateModelFactory(DEFAULT_STATE_MODEL,
                    modelFactory);
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
                while (true)
                {
                    try
                    {
                        _manager.disconnect();
                        break;
                    } catch (Exception e)
                    {
                        LOG.error("Member " + _id + " could not disconnect now! Retry in 10s. " + e);
                        try
                        {
                            Thread.sleep(10000);
                        }
                        catch (Exception ex)
                        {
                            LOG.warn(ex);
                        }
                    }
                }
            }
            // ensure that whitelisted instance configs go away
            if (_admin != null)
            {
                while (true)
                {
                    try
                    {
                        _admin.dropInstance(_clusterName, _admin.getInstanceConfig(_clusterName, _id));
                        break;
                    } catch (HelixException e)
                    {
                        LOG.warn("Drop instance failed for id= " + _id + "now. Retry in 10s. Exception" + e);
                        try
                        {
                            Thread.sleep(10000);
                        }
                        catch (Exception ex)
                        {
                            LOG.warn(ex);
                        }
                    }
                }
            }

            LOG.info("Member " + _id + " has left successfully.");
            return true;
        }

        /** Used for unit-testing **/
        protected DatabusClusterMember()
        {
            _manager = null;
            _id = null;
        }

    }

    @SuppressWarnings("serial")
    static public class DatabusClusterException extends Exception
    {
        public DatabusClusterException(String msg)
        {
            super(msg);
        }

    }

    private class DatabusHelixWatcher implements LiveInstanceChangeListener,
            ExternalViewChangeListener
    {

        final private HelixManager _manager;
        private int _numActiveInstances = -1;
        final private Random _random = new Random(System.currentTimeMillis());
        final private HashMap<Integer, String> _partitionMap;

        // State to help control helix assignment - enable/disable cluster
        private HelixManager _helixManager = null;
        // has Cluster been paused?
        private boolean _paused = false;
        private final int _id;

        public DatabusHelixWatcher() throws Exception
        {

            _id = _random.nextInt();
            _manager = HelixManagerFactory.getZKHelixManager(_clusterName,
                    "watcher_" + _clusterName + "_" + _id,
                    InstanceType.SPECTATOR, _zkAddr);
            _partitionMap = new HashMap<Integer, String>(_numPartitions);

        }

        public void start()
        {
            if (_manager != null)
            {
                try
                {
                    if (!_manager.isConnected())
                    {
                        _manager.connect();
                        _manager.addLiveInstanceChangeListener(this);
                        _manager.addExternalViewChangeListener(this);
                    }
                }
                catch (Exception e)
                {
                    LOG.error("Cannot start HelixWatcher! " + e);
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
        public synchronized void onLiveInstanceChange(
                List<LiveInstance> liveInstances,
                NotificationContext changeContext)
        {
            _numActiveInstances = liveInstances.size();
            boolean quorumReached = (_numActiveInstances >= _quorum);
            if (quorumReached)
            {
                if (_helixManager == null)
                {
                    LOG.warn("Quorum Reached! numNodes=" + _numActiveInstances
                            + " quorum=" + _quorum);
                    // controller needs to be started
                    startHelixController();
                }
                else if (_paused)
                {
                    resumeHelixController();
                    _paused = false;
                }
            }
            else
            {
                LOG.warn("Number of nodes inadequate=" + _numActiveInstances
                        + " Need at least:" + _quorum);
                if (_helixManager != null && !_paused)
                {
                    // controller has started; but pauseCluster
                    pauseHelixController();
                    _paused = true;
                }
            }

            // perform user-specified callback
            if (!_dataNotifiers.isEmpty())
            {
                Vector<String> nodeList = new Vector<String>(
                        liveInstances.size());
                for (LiveInstance i : liveInstances)
                {
                    nodeList.add(i.getInstanceName());
                }
                for (DatabusClusterDataNotifier notifier : _dataNotifiers)
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
                return Integer.parseInt(ps[ps.length - 1]);
            }
            return -1;
        }

        @Override
        public synchronized void onExternalViewChange(
                List<ExternalView> externalViewList,
                NotificationContext changeContext)
        {
            _partitionMap.clear();
            for (ExternalView v : externalViewList)
            {
                if (v.getResourceName().equals(DEFAULT_RESOURCE_NAME))
                {
                    for (String k : v.getPartitionSet())
                    {
                        Map<String, String> map = v.getStateMap(k);
                        if (map != null)
                        {
                            for (Map.Entry<String, String> mkPair : map
                                    .entrySet())
                            {
                                String value = mkPair.getValue();
                                if (value != null)
                                {
                                    Integer partition = getPartition(k);
                                    if (value.equals("ONLINE"))
                                    {
                                        _partitionMap.put(partition,
                                                mkPair.getKey());
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // external call
            if (!_dataNotifiers.isEmpty())
            {
                HashMap<Integer, String> pmap = getPartitions();
                for (DatabusClusterDataNotifier notifier : _dataNotifiers)
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

        synchronized public HashMap<Integer, String> getPartitions()
        {
            HashMap<Integer, String> map = new HashMap<Integer, String>(
                    _partitionMap.size());
            map.putAll(_partitionMap);
            return map;
        }

        /** methods to control helix's assignment of partitions **/

        void stopHelixController()
        {
            if (_helixManager != null)
            {
                LOG.warn("Shutting down cluster : "
                        + _helixManager.getClusterName() + " instance:"
                        + _helixManager.getInstanceName());
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
                LOG.info("Starting cluster controller for cluster="
                        + _clusterName + " with id = " + controllerId);
                _helixManager = HelixControllerMain.startHelixController(
                        _zkAddr, _clusterName, controllerId,
                        HelixControllerMain.STANDALONE);
                if (_admin != null)
                {
                    _admin.enableCluster(_helixManager.getClusterName(), true);
                }
            }
            catch (Exception e)
            {
                LOG.error("Cannot start cluster controller for cluster="
                        + _clusterName + e);
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
        _quorum = 0;
        _numPartitions = 0;
        _dataNotifiers = null;
        _zkConnectionTimeoutMs = ZkClient.DEFAULT_CONNECTION_TIMEOUT;
        _zkSessionTimeoutMs = ZkClient.DEFAULT_SESSION_TIMEOUT;
    }

	@Override
	public String toString() {
		return "DatabusCluster [_zkClient=" + _zkClient + ", _admin=" + _admin
				+ ", _clusterName=" + _clusterName + ", _zkAddr=" + _zkAddr
				+ ", _quorum=" + _quorum + ", _numPartitions=" + _numPartitions
				+ ", _dataNotifiers=" + _dataNotifiers + ", _watcher="
				+ _watcher + ", _zkConnectionTimeoutMs="
				+ _zkConnectionTimeoutMs + ", _zkSessionTimeoutMs="
				+ _zkSessionTimeoutMs + "]";
	}
}
