package com.linkedin.databus.client.registration;
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
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.management.MBeanServer;

import org.apache.log4j.Logger;

import com.linkedin.databus.client.DatabusHttpClientImpl;
import com.linkedin.databus.client.DbusPartitionInfoImpl;
import com.linkedin.databus.client.pub.CheckpointPersistenceProvider;
import com.linkedin.databus.client.pub.ClusterCheckpointPersistenceProvider;
import com.linkedin.databus.client.pub.ClusterCheckpointPersistenceProvider.ClusterCheckpointException;
import com.linkedin.databus.client.pub.DatabusClientException;
import com.linkedin.databus.client.pub.DatabusCombinedConsumer;
import com.linkedin.databus.client.pub.DatabusRegistration;
import com.linkedin.databus.client.pub.DbusClusterConsumerFactory;
import com.linkedin.databus.client.pub.DbusClusterInfo;
import com.linkedin.databus.client.pub.DbusPartitionInfo;
import com.linkedin.databus.client.pub.DbusPartitionListener;
import com.linkedin.databus.client.pub.DbusServerSideFilterFactory;
import com.linkedin.databus.client.pub.FetchMaxSCNRequest;
import com.linkedin.databus.client.pub.FlushRequest;
import com.linkedin.databus.client.pub.RegistrationId;
import com.linkedin.databus.client.pub.RelayFindMaxSCNResult;
import com.linkedin.databus.client.pub.RelayFlushMaxSCNResult;
import com.linkedin.databus.client.pub.mbean.ConsumerCallbackStats;
import com.linkedin.databus.client.pub.mbean.ConsumerCallbackStatsMBean;
import com.linkedin.databus.client.pub.mbean.UnifiedClientStats;
import com.linkedin.databus.client.pub.mbean.UnifiedClientStatsMBean;
import com.linkedin.databus.client.pub.monitoring.events.ConsumerCallbackStatsEvent;
import com.linkedin.databus.client.pub.monitoring.events.UnifiedClientStatsEvent;
import com.linkedin.databus.cluster.DatabusCluster;
import com.linkedin.databus.cluster.DatabusCluster.DatabusClusterMember;
import com.linkedin.databus.cluster.DatabusClusterDataNotifier;
import com.linkedin.databus.cluster.DatabusClusterNotifier;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DatabusComponentStatus;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.monitoring.mbean.AggregatedDbusEventsStatisticsCollector;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollectorMBean;
import com.linkedin.databus.core.monitoring.mbean.StatsCollectors;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.filter.DbusKeyCompositeFilterConfig;

public class DatabusV2ClusterRegistrationImpl implements
    DatabusMultiPartitionRegistration, DatabusClusterNotifier, DatabusClusterDataNotifier
{
  public class Status extends DatabusComponentStatus
  {
    public Status()
    {
      super(getStatusName());
    }

    @Override
    public void start()
    {
      try {
        DatabusV2ClusterRegistrationImpl.this.start();
      } catch (Exception e) {
        _log.error("Got exception while trying to start the registration", e);
        throw new RuntimeException(e);
      }
      super.start();
    }

    @Override
    public void shutdown()
    {
      DatabusV2ClusterRegistrationImpl.this.shutdown();
      super.shutdown();
    }

    @Override
    public void pause()
    {
      DatabusV2ClusterRegistrationImpl.this.pause();
      super.pause();
    }

    @Override
    public void resume()
    {
      DatabusV2ClusterRegistrationImpl.this.resume();
      super.resume();
    }

    @Override
    public void suspendOnError(Throwable error)
    {
      DatabusV2ClusterRegistrationImpl.this.suspendOnError(error);
      super.suspendOnError(error);
    }
  }

    /** State of the registration **/
  private RegistrationState _state;

  /** Client-instance wide unique id to locate the registration **/
  private RegistrationId _id;

  /** Client instance **/
  private final DatabusHttpClientImpl _client;

  /** Databus Componenent Status for this registration **/
  private Status _status = new Status();

  /** Child Registrations container **/
  public final Map<DbusPartitionInfo, DatabusRegistration> regMap =
      new HashMap<DbusPartitionInfo, DatabusRegistration>();

  /** Checkpoint Persisitence Provider Config **/
  private final ClusterCheckpointPersistenceProvider.StaticConfig _ckptPersistenceProviderConfig;

  /** Aggregator for relay callback statistics across all partitions */
  private StatsCollectors<ConsumerCallbackStats> _relayCallbackStatsMerger;

  /** Aggregator for bootstrap callback statistics across all partitions */
  private StatsCollectors<ConsumerCallbackStats> _bootstrapCallbackStatsMerger;

  /** Aggregator for unified (and simplified) relay/bootstrap client statistics across all partitions */
  private StatsCollectors<UnifiedClientStats> _unifiedClientStatsMerger;

  /** Aggregator for relay event statistics across all partitions */
  private StatsCollectors<DbusEventsStatisticsCollector> _relayEventStatsMerger;

  /** Aggregator for bootstrap event statistics across all partitions */
  private StatsCollectors<DbusEventsStatisticsCollector> _bootstrapEventStatsMerger;

  /** Registration specific Logger */
  private Logger _log;

  /** Server Side Filter Factory **/
  private final DbusServerSideFilterFactory _serverSideFilterFactory;

  /** Consumer Factory **/
  private final DbusClusterConsumerFactory _consumerFactory;

  /** Partition Listener **/
  private final DbusPartitionListener _partitionListener;

  /** Set of Partitions **/
  private final Set<DbusPartitionInfo> _partitionSet;

  /** Databus Cluster Config **/
  private final DbusClusterInfo _clusterInfo;

  /** Sources list **/
  private final List<String> _sources;

  /** Current Active Nodes **/
  private List<String> _currentActiveNodes;

  /** Partition to node map **/
  private Map<Integer, String> _activePartitionMap;

  private final ClusterRegistrationStaticConfig _clientClusterConfig;

  /** Databus Cluster **/
  private DatabusCluster _cluster;
  private DatabusClusterMember _clusterMember;

  public DatabusV2ClusterRegistrationImpl(RegistrationId id,
                      DatabusHttpClientImpl client,
                      ClusterCheckpointPersistenceProvider.StaticConfig ckptPersistenceProviderConfig,
                      DbusClusterInfo clusterInfo,
                      DbusClusterConsumerFactory consumerFactory,
                      DbusServerSideFilterFactory filterFactory,
                      DbusPartitionListener partitionListener,
                      String[] sources)
  {
    _client = client;
    _id = id;
    _ckptPersistenceProviderConfig = ckptPersistenceProviderConfig;
    _state = RegistrationState.INIT;
    _log = Logger.getLogger(getClass().getName() + (null  == _id ? "" : "." + _id.getId()));
    _consumerFactory = consumerFactory;
    _serverSideFilterFactory = filterFactory;
    _partitionListener = partitionListener;
    _partitionSet = new HashSet<DbusPartitionInfo>();
    _sources = new ArrayList<String>();
    _clusterInfo = clusterInfo;
    _clientClusterConfig = client.getClientStaticConfig().getClientCluster(clusterInfo.getName());

    if ( null != sources)
      _sources.addAll(Arrays.asList(sources));
  }

  /**
   * Set Logger for this registration
   * @param log
   */
  public synchronized void setLogger(Logger log)
  {
    _log = log;
  }

  @Override
  public synchronized boolean start()
      throws IllegalStateException, DatabusClientException
  {
    if (_state == RegistrationState.INIT || _state == RegistrationState.DEREGISTERED)
    {
      String errMsg = "Registration (" + _id + ") cannot be started from its current state, which is " + _state +
                      " .It may only be started from REGISTERED or SHUTDOWN state";
      _log.error(errMsg);
      throw new IllegalStateException(errMsg);
    }

    if (_state.isRunning())
    {
      _log.info("Registration (" + _id + ") already started !!");
      return false;
    }

    String host = null;

    try {
      host = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      _log.error("Unable to fetch the local hostname !! Trying to fetch IP Address !!", e);
      try
      {
        host = InetAddress.getLocalHost().getHostAddress();
      } catch (UnknownHostException e1) {
        _log.error("Unable to fetch the local IP Address too !! Giving up", e1);
        host = "UNKNOWN_HOST";
      }
    }

    /**
     *  The id below has to be unique within a given cluster. HttpPort is used to get a unique name for service colocated in a single box.
     *  The Container id is not necessarily a sufficient differentiating factor.
     */
    String id = host + "-" + _client.getContainerStaticConfig().getHttpPort() + "-" +  _client.getContainerStaticConfig().getId();

    try {
      _cluster = createCluster();
      _cluster.start();
    } catch (Exception e) {
      _log.fatal("Got exception while trying to create the cluster with id (" + id + ")", e);
      throw new DatabusClientException(e);
    }

    initializeStatsCollectors();

    _log.info("Dabatus cluster object created : " + _cluster + " with id :" + id);

    _clusterMember = _cluster.addMember(id,this);

    boolean joined = _clusterMember.join();

    if ( ! joined )
    {
      _log.fatal("Unable to join the cluster "  + _clusterInfo);
      throw new DatabusClientException("Unable to join the cluster :" + _clusterInfo);
    }
    _state =RegistrationState.STARTED;
    activatePartitions();
    return true;
  }

  @Override
  public synchronized void shutdown() throws IllegalStateException
  {
    boolean left = true;

    if (! _state.isRunning())
    {
      _log.warn("Registration (" + _id + ") is not in running state to be shutdown. Current state :" + _state);
      return;
    }

    for (Entry<DbusPartitionInfo, DatabusRegistration> e : regMap.entrySet())
    {
      _log.info("Shutting registration for Partition :" + e.getKey());
      try
      {
        if ( e.getValue().getState().isRunning())
          e.getValue().shutdown();
      } catch (Exception ex) {
        _log.error("Unable to shutdown registration for partition :" + e.getKey(), ex);
      }
    }

    if ( null != _clusterMember)
      left = _clusterMember.leave();

    if ( !left )
      _log.error("Unable to leave the cluster cleanly !!" + _clusterInfo);
    if (null != _cluster)
        {
            _cluster.shutdown();
      ClusterCheckpointPersistenceProvider.close(_cluster.getClusterName());
        }
    _state = RegistrationState.SHUTDOWN;
  }

  @Override
  public synchronized void pause()
      throws IllegalStateException
  {
    if ( _state == RegistrationState.PAUSED)
      return;

    if ( (_state != RegistrationState.STARTED) && ( _state != RegistrationState.RESUMED))
      throw new IllegalStateException(
          "Registration (" + _id + ") is not in correct state to be paused. Current state :" + _state);

    for (Entry<DbusPartitionInfo, DatabusRegistration> e : regMap.entrySet())
    {
      _log.info("Shutting registration for Partition :" + e.getKey());
      try
      {
        e.getValue().pause();
      } catch (Exception ex) {
        _log.error("Unable to pause registration for partition :" + e.getKey(), ex);
      }
    }
    _state = RegistrationState.PAUSED;
  }

  @Override
  public synchronized void suspendOnError(Throwable ex)
      throws IllegalStateException
  {
    if ( _state == RegistrationState.SUSPENDED_ON_ERROR)
      return;

    if ( !_state.isRunning())
      throw new IllegalStateException(
          "Registration (" + _id + ") is not in correct state to be suspended. Current state :" + _state);

    for (Entry<DbusPartitionInfo, DatabusRegistration> e : regMap.entrySet())
    {
      _log.info("Shutting registration for Partition :" + e.getKey());
      try
      {
        e.getValue().suspendOnError(ex);
      } catch (Exception e1) {
        _log.error("Unable to suspend registration for partition :" + e.getKey(), e1);
      }
    }
    _state = RegistrationState.SUSPENDED_ON_ERROR;
  }

  @Override
  public synchronized void resume() throws IllegalStateException
  {
    if ( _state == RegistrationState.RESUMED)
      return;

    if ( (_state != RegistrationState.PAUSED) && (_state != RegistrationState.SUSPENDED_ON_ERROR))
      throw new IllegalStateException(
          "Registration (" + _id + ") is not in correct state to be resumed. Current state :" + _state);

    for (Entry<DbusPartitionInfo, DatabusRegistration> e : regMap.entrySet())
    {
      _log.info("Shutting registration for Partition :" + e.getKey());
      try
      {
        e.getValue().resume();
      } catch (Exception e1) {
        _log.error("Unable to resume registration for partition :" + e.getKey(), e1);
      }
    }
    _state = RegistrationState.RESUMED;
  }

  @Override
  public RegistrationState getState()
  {
    return _state;
  }

  @Override
  public synchronized boolean deregister() throws IllegalStateException
  {
    if ((_state == RegistrationState.DEREGISTERED) || (_state == RegistrationState.INIT))
      return false;

    if ( _state.isRunning())
      shutdown();

    _client.deregister(this);

    // Clear Child registrations
    regMap.clear();

    _state = RegistrationState.DEREGISTERED;
    return true;
  }

  @Override
  public Collection<DatabusSubscription> getSubscriptions()
  {
    Set<DatabusSubscription> subscriptions = new HashSet<DatabusSubscription>();

    for (Entry<DbusPartitionInfo, DatabusRegistration> e : regMap.entrySet())
    {
      subscriptions.addAll(e.getValue().getSubscriptions());
    }

    return subscriptions;
  }

  @Override
  public DatabusComponentStatus getStatus()
  {
    return _status;
  }

  @Override
  public synchronized Logger getLogger() {
    return _log;
  }

  @Override
  public DatabusRegistration getParent()
  {
    return null;
  }

  @Override
  public synchronized DatabusRegistration withRegId(RegistrationId regId)
      throws DatabusClientException, IllegalStateException
  {
    if ( (_id != null) && (_id.equals(regId)))
      return this;

    if (! RegistrationIdGenerator.isIdValid(regId))
      throw new DatabusClientException("Another registration with the same regId (" + regId + ") already present !!");

    if (_state.isRunning())
      throw new IllegalStateException("Cannot update regId when registration is in running state. RegId :" + _id + ", State :" + _state);

    _id = regId;
    _status = new Status(); // Component Status should use the correct component name

    return this;
  }

  @Override
  public synchronized DatabusRegistration withServerSideFilter(
      DbusKeyCompositeFilterConfig filterConfig)
      throws IllegalStateException
    {
    // Nothing to do
    return this;
  }

  @Override
  public Collection<DbusPartitionInfo> getPartitions()
  {
    Set<DbusPartitionInfo> partitions = new HashSet<DbusPartitionInfo>();

    for ( DbusPartitionInfo p : regMap.keySet())
    {
      partitions.add(p);
    }

    return partitions;
  }

  @Override
  public Checkpoint getLastPersistedCheckpoint()
  {
    throw new RuntimeException("Operation Not supported !!");
  }

  @Override
  public boolean storeCheckpoint(Checkpoint ckpt)
      throws IllegalStateException
  {
    throw new RuntimeException("Operation Not supported !!");
  }

  @Override
  public DbusEventsStatisticsCollectorMBean getRelayEventStats()
  {
    return _relayEventStatsMerger.getStatsCollector();
  }

  @Override
  public DbusEventsStatisticsCollectorMBean getBootstrapEventStats()
  {
    return _bootstrapEventStatsMerger.getStatsCollector();
  }

  @Override
  public ConsumerCallbackStatsMBean getRelayCallbackStats()
  {
    return _relayCallbackStatsMerger.getStatsCollector();
  }

  @Override
  public ConsumerCallbackStatsMBean getBootstrapCallbackStats()
  {
    return _bootstrapCallbackStatsMerger.getStatsCollector();
  }

  @Override
  public UnifiedClientStatsMBean getUnifiedClientStats()
  {
    return _unifiedClientStatsMerger.getStatsCollector();
  }

  @Override
  public RelayFindMaxSCNResult fetchMaxSCN(FetchMaxSCNRequest request)
      throws InterruptedException
  {
    throw new RuntimeException("Operation Not supported !!");
  }

  @Override
  public RelayFlushMaxSCNResult flush(RelayFindMaxSCNResult fetchSCNResult,
      FlushRequest flushRequest) throws InterruptedException
  {
    throw new RuntimeException("Operation Not supported !!");
  }

  @Override
  public RelayFlushMaxSCNResult flush(FetchMaxSCNRequest maxScnRequest,
      FlushRequest flushRequest) throws InterruptedException
  {
    throw new RuntimeException("Operation Not supported !!");
  }

  @Override
  public RegistrationId getRegistrationId()
  {
    return _id;
  }

  @Override
  public Map<DbusPartitionInfo, DatabusRegistration> getPartitionRegs()
  {
    return regMap;
  }

  protected String getStatusName()
  {
    return "Status" + ((_id != null ) ? "_" + _id.getId() : "");
  }

  protected synchronized void initializeStatsCollectors()
  {
    //some safety against null pointers coming from unit tests
    MBeanServer mbeanServer = null;
    int ownerId = -1;
    long pullerThreadDeadnessThresholdMs = UnifiedClientStats.DEFAULT_DEADNESS_THRESHOLD_MS;

    if (null != _client)
    {
      mbeanServer = _client.getMbeanServer();
      ownerId = _client.getContainerStaticConfig().getId();
      pullerThreadDeadnessThresholdMs = _client.getClientStaticConfig().getPullerThreadDeadnessThresholdMs();
    }

    String regId = null != _id ? _id.getId() : "unknownReg";

    ConsumerCallbackStats relayConsumerStats =
        new ConsumerCallbackStats(ownerId, regId + ".callback.relay",
                                  regId, true, false, new ConsumerCallbackStatsEvent());
    ConsumerCallbackStats bootstrapConsumerStats =
        new ConsumerCallbackStats(ownerId, regId + ".callback.bootstrap",
                                  regId, true, false, new ConsumerCallbackStatsEvent());
    UnifiedClientStats unifiedClientStats =
        new UnifiedClientStats(ownerId, regId + ".callback.unified",
                               regId, true, false,
                               pullerThreadDeadnessThresholdMs,
                               new UnifiedClientStatsEvent());
    _relayCallbackStatsMerger = new StatsCollectors<ConsumerCallbackStats>(relayConsumerStats);
    _bootstrapCallbackStatsMerger = new StatsCollectors<ConsumerCallbackStats>(bootstrapConsumerStats);
    _unifiedClientStatsMerger = new StatsCollectors<UnifiedClientStats>(unifiedClientStats);
    _relayEventStatsMerger = new StatsCollectors<DbusEventsStatisticsCollector>(
        new AggregatedDbusEventsStatisticsCollector(ownerId, regId + ".inbound", true, false, mbeanServer));
    _bootstrapEventStatsMerger = new StatsCollectors<DbusEventsStatisticsCollector>(
        new AggregatedDbusEventsStatisticsCollector(ownerId, regId + ".inbound.bs", true, false, mbeanServer));

    if (null != _client)
    {
      _client.getBootstrapEventsStats().addStatsCollector(
          regId, _bootstrapEventStatsMerger.getStatsCollector());
      _client.getInBoundStatsCollectors().addStatsCollector(
          regId, _relayEventStatsMerger.getStatsCollector());
      _client.getRelayConsumerStatsCollectors().addStatsCollector(
          regId, _relayCallbackStatsMerger.getStatsCollector());
      _client.getBootstrapConsumerStatsCollectors().addStatsCollector(
          regId, _bootstrapCallbackStatsMerger.getStatsCollector());
      _client.getUnifiedClientStatsCollectors().addStatsCollector(
          regId, _unifiedClientStatsMerger.getStatsCollector() /* a.k.a. getUnifiedClientStats() */);
      _client.getGlobalStatsMerger().registerStatsCollector(_relayEventStatsMerger);
      _client.getGlobalStatsMerger().registerStatsCollector(_bootstrapEventStatsMerger);
      _client.getGlobalStatsMerger().registerStatsCollector(_relayCallbackStatsMerger);
      _client.getGlobalStatsMerger().registerStatsCollector(_bootstrapCallbackStatsMerger);
      _client.getGlobalStatsMerger().registerStatsCollector(_unifiedClientStatsMerger);
    }
  }

  /**
   * Callback for ClusterManage when a partition getting added
   * @param partition
   * @throws IllegalStateException
   */
  private synchronized void addPartition(DbusPartitionInfo partition)
    throws IllegalStateException, DatabusClientException
  {
    if(_state == RegistrationState.REGISTERED)
    {
      _partitionSet.add(partition);
    } else if ( _state.isRunning()) {
      if ( ! _partitionSet.contains(partition))
      {
        _partitionSet.add(partition);
        activateOnePartition(partition);
      } else {
        _log.info("Partition (" + partition + ") already added !!");
      }
    } else {
      throw new IllegalStateException("Registration is not in correct state to add a partition !! State :" + _state);
    }
  }

  /**
   * Callback to activate partitions when quorum is reached.
   *
   * @throws DatabusException
   */
  private synchronized void activatePartitions()
    throws DatabusClientException
  {
    _state = RegistrationState.STARTED;

    for (DbusPartitionInfo p : _partitionSet)
      activateOnePartition(p);
  }

  /**
   * Callback to activate one partition that was added
   * @param partition
   * @throws DatabusException
   */
  private synchronized void activateOnePartition(DbusPartitionInfo partition)
    throws DatabusClientException
  {
    _log.info("Trying to activate partition :" + partition);

    try
    {
      if ( regMap.containsKey(partition))
      {
        _log.info("Partition (" + partition
            + ") is already added and is currently in state : "
            + regMap.get(partition).getState() + " skipping !!");
        return;
      }

      // Call factories to get consumer callbacks and server-side filter config.
      Collection<DatabusCombinedConsumer> consumers = _consumerFactory.createPartitionedConsumers(_clusterInfo, partition);
      DbusKeyCompositeFilterConfig filterConfig = null;

      if (_serverSideFilterFactory != null)
        filterConfig = _serverSideFilterFactory.createServerSideFilter(_clusterInfo, partition);


      if ( (null == consumers) || (consumers.isEmpty()))
      {
        _log.error("ConsumerFactory for cluster (" + _clusterInfo + ") returned null or empty consumers ");
        throw new DatabusClientException("ConsumerFactory for cluster (" + _clusterInfo + ") returned null or empty consumers");
      }

      // Create Registration
      RegistrationId id = new RegistrationId(_id + "-" + partition.getPartitionId());
      CheckpointPersistenceProvider ckptProvider = createCheckpointPersistenceProvider(partition);

      DatabusV2RegistrationImpl reg = createChildRegistration(id, _client, ckptProvider);
      reg.addDatabusConsumers(consumers);

      String[] srcs = new String[_sources.size()];
      srcs = _sources.toArray(srcs);
      reg.addSubscriptions(srcs);

      regMap.put(partition,reg);
      reg.onRegister();

      // Add Server-Side Filter
      if ( null != filterConfig)
        reg.withServerSideFilter(filterConfig);

      // Notify Listener
      if (null != _partitionListener)
        _partitionListener.onAddPartition(partition, reg);

      // Start the registration
      try {
        reg.start();
      } catch (DatabusClientException e) {
        _log.error("Got exception while starting the registration for partition (" + partition + ")", e);
        throw e;
      }

      //Add partition Specific metrics to cluster-merge
      _relayEventStatsMerger.addStatsCollector(id.getId(), (DbusEventsStatisticsCollector)reg.getRelayEventStats());
      _bootstrapEventStatsMerger.addStatsCollector(id.getId(), (DbusEventsStatisticsCollector)reg.getBootstrapEventStats());
      _relayCallbackStatsMerger.addStatsCollector(id.getId(), (ConsumerCallbackStats)reg.getRelayCallbackStats());
      _bootstrapCallbackStatsMerger.addStatsCollector(id.getId(), (ConsumerCallbackStats)reg.getBootstrapCallbackStats());

      _log.info("Partition (" + partition + ") started !!");
    } catch (DatabusException e) {
      _log.error("Got exception while activating partition(" + partition + ")",e);
      throw new DatabusClientException(e);
    } catch (ClusterCheckpointException e) {
      _log.error("Got exception while activating partition(" + partition + ")",e);
      throw new DatabusClientException(e);
    }
  }

  private DatabusV2RegistrationImpl createChildRegistration(RegistrationId id, DatabusHttpClientImpl client, CheckpointPersistenceProvider ckptProvider)
  {
    return new DatabusClusterChildRegistrationImpl(id, client, this, ckptProvider);
  }

  /**
   * Callback to drop one partition
   *
   * @param partition
   * @throws DatabusException
   */
  private synchronized void dropOnePartition(DbusPartitionInfo partition)
    throws DatabusException
  {
    if(_state == RegistrationState.REGISTERED)
    {
      _partitionSet.remove(partition);
    } else if ( _state.isRunning()) {
      if (! regMap.containsKey(partition))
      {
        _log.warn("Partition (" + partition
            + ") not available to be dropped. skipping !! Active Partitions :" + regMap.keySet());
        return;
      }

      DatabusRegistration reg = regMap.get(partition);

      // Deregister the regMap which will shutdown the partition
      reg.deregister();

      regMap.remove(partition);
      _partitionSet.remove(partition);

      // remove dropped partition specific metrics
      _relayEventStatsMerger.removeStatsCollector(reg.getRegistrationId().getId());
      _bootstrapEventStatsMerger.removeStatsCollector(reg.getRegistrationId().getId());
      _relayCallbackStatsMerger.removeStatsCollector(reg.getRegistrationId().getId());
      _bootstrapCallbackStatsMerger.removeStatsCollector(reg.getRegistrationId().getId());

      // Notify Listener
      _partitionListener.onDropPartition(partition, reg);
    } else {
      throw new IllegalStateException("Registration is not in correct state to drop a partition !! State :" + _state);
    }

  }


  private static class DatabusClusterChildRegistrationImpl
     extends DatabusV2RegistrationImpl
  {

    public DatabusClusterChildRegistrationImpl(RegistrationId id,
        DatabusHttpClientImpl client,
        DatabusRegistration parentReg,
        CheckpointPersistenceProvider ckptProvider)
    {
      super(id, client, ckptProvider);
      setParent(parentReg);
    }

    @Override
    protected void deregisterFromClient()
    {
      //do nothing
    }

    /**
     * Enable per-partition stats only if required by config.
     */
    @Override
    protected synchronized void initializeStatsCollectors()
    {
      MBeanServer mbeanServer =  null;

      if ( null != _client )
      {
        mbeanServer = _client.getClientStaticConfig().isEnablePerConnectionStats() ?
            _client.getMbeanServer() : null;
      }

      int ownerId = null == _client ? -1 : _client.getContainerStaticConfig().getId();
      String regId = null != _id ? _id.getId() : "unknownReg";

      initializeStatsCollectors(regId, ownerId, mbeanServer);

      if (null != _client && _client.getClientStaticConfig().isEnablePerConnectionStats())
      {
        _client.getBootstrapEventsStats().addStatsCollector(regId, _bootstrapEventsStatsCollector );
        _client.getInBoundStatsCollectors().addStatsCollector(regId, _inboundEventsStatsCollector);
        _client.getRelayConsumerStatsCollectors().addStatsCollector(regId, _relayConsumerStats);
        _client.getBootstrapConsumerStatsCollectors().addStatsCollector(regId, _bootstrapConsumerStats);
        _client.getUnifiedClientStatsCollectors().addStatsCollector(regId, _unifiedClientStats);
      }
    }
  }

  @Override
  public synchronized void onGainedPartitionOwnership(int partition)
  {
    _log.info("Partition (" + partition + ") getting added !!");

    DbusPartitionInfo partitionInfo = new DbusPartitionInfoImpl(partition);
    try {
      addPartition(partitionInfo);
    } catch (DatabusClientException e) {
      _log.error("Unable to add partition. Shutting down the cluster !!", e);
      deregister();
    }

  }

  @Override
  public synchronized void onLostPartitionOwnership(int partition)
  {
    _log.info("Partition (" + partition + ") getting removed !!");

    DbusPartitionInfo partitionInfo = new DbusPartitionInfoImpl(partition);
    try {
      dropOnePartition(partitionInfo);
    } catch (DatabusException e) {
      _log.error("Unable to drop partition. Shutting down the cluster !!", e);
      deregister();
    }
  }

  @Override
  public synchronized void onError(int partition)
  {
    _log.error("Error notification received for partition");
    onLostPartitionOwnership(partition);
  }

  @Override
  public synchronized void onReset(int partition)
  {
    _log.error("Reset notification received for partition");
    onLostPartitionOwnership(partition);
  }

  /**
   * Add sources to a given registration object
   * Adding an already existent subscription, will be a no-op.
   *
   * This does not create any new the DatabusRegistration object ( only modifies the current one ).
   * Hence the id of the registration remains the same
   *
   * @throws IllegalStateException if this registration has already been started.
   */
  public synchronized void addSubscriptions(String ... sources)
          throws IllegalStateException
  {
    if ( ! _state.isPreStartState())
      throw new IllegalStateException("Cannot add sources when state is running/shutdown. Current State :" + _state);

    for (String s : sources)
      if (! _sources.contains(s))
        _sources.add(s);
  }

  /**
   * Remove subscriptions from a given registration object
   * Removing a non-existent subscription, will be a no-op.
   *
   * @throws IllegalStateException if this registration has already been started
   */
  public synchronized void removeSubscriptions(String ... sources)
          throws IllegalStateException
  {
    if ( ! _state.isRunning())
      throw new IllegalStateException("Cannot remove sources when state is running. Current State :" + _state);

    for (String s : sources)
      _sources.remove(s);
  }

  /**
   * Callback when registration is added to client Registration Set.
   * @param state
   */
  public synchronized void onRegister()
  {
    _state = RegistrationState.REGISTERED;
  }


  @Override
  public synchronized void onInstanceChange(List<String> activeNodes)
  {
    if ( _currentActiveNodes == null)
      _currentActiveNodes = new ArrayList<String>();

    List<String> newOfflineNodes = new ArrayList<String>();
    List<String> newOnlineNodes = new ArrayList<String>();

    findDiff(_currentActiveNodes,activeNodes,newOfflineNodes);
    findDiff(activeNodes,_currentActiveNodes,newOnlineNodes);

    if ( ! newOfflineNodes.isEmpty())
      _log.info("The following nodes went offline :" + newOfflineNodes);

    if ( ! newOnlineNodes.isEmpty())
      _log.info("The following nodes came online :" + newOnlineNodes);

    _currentActiveNodes.clear();
    _currentActiveNodes.addAll(activeNodes);

  }


  private static void findDiff(Collection<String> lhs, Collection<String> rhs, Collection<String> result)
  {
    for (String c : lhs)
    {
      if (! rhs.contains(c))
        result.add(c);
    }
  }


  @Override
  public synchronized void onPartitionMappingChange(Map<Integer, String> activePartitionMap)
  {
    for (int i = 0 ; i < _clusterInfo.getNumTotalPartitions(); i++)
    {
      if (! activePartitionMap.containsKey(i))
        _log.error("No Client listening to partition : " + i);
    }

    if ( null == _activePartitionMap)
    {
      _activePartitionMap = new HashMap<Integer,String>();
    }

    _activePartitionMap.clear();
    _activePartitionMap.putAll(activePartitionMap);

    _log.info("Current Partition to Client mapping :" + activePartitionMap);
  }

  protected DatabusCluster createCluster() throws Exception
  {
    return new DatabusCluster(_clientClusterConfig);
  }

  protected CheckpointPersistenceProvider createCheckpointPersistenceProvider(DbusPartitionInfo partition)
      throws InvalidConfigException,ClusterCheckpointException
  {
    return new ClusterCheckpointPersistenceProvider(partition.getPartitionId(),_ckptPersistenceProviderConfig);
  }

  public List<String> getCurrentActiveNodes()
  {
    return _currentActiveNodes;
  }

  public Map<Integer, String> getActivePartitionMap()
  {
    return _activePartitionMap;
  }

  public DbusClusterInfo getClusterInfo()
  {
    return _clusterInfo;
  }

  @Override
  public DbusKeyCompositeFilterConfig getFilterConfig()
  {
    return null;
  }
}
