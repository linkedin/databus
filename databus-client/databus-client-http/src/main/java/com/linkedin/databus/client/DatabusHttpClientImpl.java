package com.linkedin.databus.client;
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


import com.linkedin.databus.client.DatabusHttpClientImpl.CheckpointPersistenceStaticConfig.ProviderType;
import com.linkedin.databus.client.consumer.DatabusV2ConsumerRegistration;
import com.linkedin.databus.client.consumer.LoggingConsumer;
import com.linkedin.databus.client.consumer.SelectingDatabusCombinedConsumer;
import com.linkedin.databus.client.consumer.SelectingDatabusCombinedConsumerFactory;
import com.linkedin.databus.client.netty.NettyHttpConnectionFactory;
import com.linkedin.databus.client.pub.CheckpointPersistenceProvider;
import com.linkedin.databus.client.pub.ClusterCheckpointPersistenceProvider;
import com.linkedin.databus.client.pub.DatabusBootstrapConsumer;
import com.linkedin.databus.client.pub.DatabusClient;
import com.linkedin.databus.client.pub.DatabusClientException;
import com.linkedin.databus.client.pub.DatabusClientGroupMember;
import com.linkedin.databus.client.pub.DatabusClientNode;
import com.linkedin.databus.client.pub.DatabusCombinedConsumer;
import com.linkedin.databus.client.pub.DatabusRegistration;
import com.linkedin.databus.client.pub.DatabusRegistration.RegistrationState;
import com.linkedin.databus.client.pub.DatabusStreamConsumer;
import com.linkedin.databus.client.pub.DatabusV3Registration;
import com.linkedin.databus.client.pub.DbusClusterConsumerFactory;
import com.linkedin.databus.client.pub.DbusClusterInfo;
import com.linkedin.databus.client.pub.DbusPartitionListener;
import com.linkedin.databus.client.pub.DbusServerSideFilterFactory;
import com.linkedin.databus.client.pub.FileSystemCheckpointPersistenceProvider;
import com.linkedin.databus.client.pub.RegistrationId;
import com.linkedin.databus.client.pub.ServerInfo;
import com.linkedin.databus.client.pub.ServerInfo.ServerInfoBuilder;
import com.linkedin.databus.client.pub.ServerInfo.ServerInfoSetBuilder;
import com.linkedin.databus.client.pub.SharedCheckpointPersistenceProvider;
import com.linkedin.databus.client.pub.mbean.ConsumerCallbackStats;
import com.linkedin.databus.client.registration.ClusterRegistrationConfig;
import com.linkedin.databus.client.registration.ClusterRegistrationStaticConfig;
import com.linkedin.databus.client.registration.DatabusMultiPartitionRegistration;
import com.linkedin.databus.client.registration.DatabusV2ClusterRegistrationImpl;
import com.linkedin.databus.client.registration.DatabusV2RegistrationImpl;
import com.linkedin.databus.client.registration.RegistrationIdGenerator;
import com.linkedin.databus.client.request.ClientStateRequestProcessor;
import com.linkedin.databus.client.request.ClientStatsRequestProcessor;
import com.linkedin.databus.core.DatabusComponentStatus;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.monitoring.mbean.AggregatedDbusEventsStatisticsCollector;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.monitoring.mbean.StatsCollectors;
import com.linkedin.databus.core.util.ConfigApplier;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus.core.util.ConfigManager;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.monitoring.mbean.DatabusComponentAdmin;
import com.linkedin.databus2.core.container.monitoring.mbean.HttpStatisticsCollector;
import com.linkedin.databus2.core.container.netty.ServerContainer;
import com.linkedin.databus2.core.filter.DbusKeyCompositeFilterConfig;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.log4j.Logger;


/**
 */
public class DatabusHttpClientImpl extends ServerContainer implements DatabusClient
{
  /**
   * Public instance variables
   */

  public static final String MODULE = DatabusHttpClientImpl.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);
  public static final int GLOBAL_STATS_MERGE_INTERVAL_MS=500;

  /**
   * Protected instance variables
   */

  /** Map from source list to relays that can provide the list */
  protected Map<List<DatabusSubscription>, Set<ServerInfo>> _relayGroups;

  /** Map from source list to relays that can provide the list */
  protected Map<List<DatabusSubscription>, Set<ServerInfo>> _bootstrapGroups;

  /** All relay connections */
  protected final List<DatabusSourcesConnection> _relayConnections;

  /** Static configuration */
  protected final StaticConfig _clientStaticConfig;
  /** Runtime config manager */
  protected final ConfigManager<RuntimeConfig> _configManager;
  /** Checkpoint persistence provider */
  protected final CheckpointPersistenceProvider _checkpointPersistenceProvider;
  /** Statistics collector about databus events */
  protected final HttpStatisticsCollector _httpStatsCollector;
  protected final LoggingConsumer _loggingListener;
  protected final DatabusRelayConnectionFactory _relayConnFactory;
  protected final DatabusBootstrapConnectionFactory _bootstrapConnFactory;

  protected final DbusEventsStatisticsCollector _bootstrapEventsStatsCollector;
  protected final StatsCollectors<DbusEventsStatisticsCollector> _bootstrapEventsStats;

  protected final StatsCollectors<ConsumerCallbackStats> _consumerStatsCollectors;
  protected final StatsCollectors<ConsumerCallbackStats> _bsConsumerStatsCollectors;

  protected DatabusHttpClientStatus _clientStatus;
  /** node that will join a cluster */
  protected final DatabusClientNode _clientNode;
  /** convenience handle to cluster /group */
  protected final DatabusClientGroupMember _groupMember;

  /**last write time tracker for DSC */
  protected LastWriteTimeTrackerImpl _writeTimeTracker = null;
  protected DatabusClientDSCUpdater _dscUpdater = null;

  /**
   * Private instance variables
   */

  /** Map from source list to stream consumers that will use those */
  private final Map<List<DatabusSubscription>, List<DatabusV2ConsumerRegistration>>
          _relayGroupStreamConsumers;

  /** Map from source list to bootstrap consumers that will use those */
  private final Map<List<DatabusSubscription>, List<DatabusV2ConsumerRegistration>>
          _relayGroupBootstrapConsumers;

  /**
   * A flag to indicate if the client is cluster manager enabled
   */
  protected boolean _cmEnabled = false;

  /**
   * List of registrations for this client instance  
   */
  private List<DatabusRegistration> _regList = new CopyOnWriteArrayList<DatabusRegistration>();

  /**
   *  Set of clusters that have been registered.
   */
  private Set<String> _activeClusters = new HashSet<String>();
  
  /**
   * Creates a client with a configuration based on specified properties.
   * @param  propertyPrefix   the prefix for all property names for this client
   * @param  props            the configuration properties
   */
  public DatabusHttpClientImpl(String propertyPrefix, Properties props)
        throws InvalidConfigException, IOException, DatabusException
  {
    this(createConfigBuilder(propertyPrefix, props).build());
  }

  public boolean isCMEnabled()
  {
    return _cmEnabled;
  }

  public Map<String, String> printClientInfo()
  throws InvalidConfigException
  {
    throw new UnsupportedOperationException("Not supported in this version of client");
  }

  /**
   * Reset all the relay connections, and pick new relays to connect to.
   */
  public void resetRelayConnections()
  {
    for (DatabusSourcesConnection conn : _relayConnections)
    {
      conn.getRelayPullThread().killConnection();
    }
  }

  public DatabusHttpClientImpl() throws InvalidConfigException, IOException, DatabusException
  {
    this(new Config());
  }

  public DatabusHttpClientImpl(Config config) throws InvalidConfigException, IOException,
                                                     DatabusException
  {
    this(config.build());
  }

  public DatabusHttpClientImpl(StaticConfig config) throws IOException, InvalidConfigException,
                                                           DatabusException
  {
    super(config.getContainer());


    _clientStaticConfig = config;

    _bootstrapEventsStatsCollector = new AggregatedDbusEventsStatisticsCollector(getContainerStaticConfig().getId(),
                                                         "eventsBootstrap",
                                                         true,
                                                         true,
                                                         getMbeanServer());
    //create a meta collector across physical sources ; pass in existing collector for backward compat
    _bootstrapEventsStats = new StatsCollectors<DbusEventsStatisticsCollector>(_bootstrapEventsStatsCollector);
    _consumerStatsCollectors = new StatsCollectors<ConsumerCallbackStats>();
    _bsConsumerStatsCollectors = new StatsCollectors<ConsumerCallbackStats>();

    _relayGroups = new HashMap<List<DatabusSubscription>, Set<ServerInfo>>(100);
    _bootstrapGroups = new HashMap<List<DatabusSubscription>, Set<ServerInfo>>(100);
    _relayGroupStreamConsumers =
        new HashMap<List<DatabusSubscription>,
                    List<DatabusV2ConsumerRegistration>>(100);
    _relayGroupBootstrapConsumers =
        new HashMap<List<DatabusSubscription>,
                    List<DatabusV2ConsumerRegistration>>(100);
    _relayConnections = new ArrayList<DatabusSourcesConnection>(20);



    _clientNode = new DatabusClientNode(config.getCluster());
    _groupMember = _clientNode.isEnabled() ?  _clientNode.getMember(_clientStaticConfig.getCluster().getDomain(),
                                                            _clientStaticConfig.getCluster().getGroup(),
                                                            _clientStaticConfig.getCluster().getName()) : null;


    _checkpointPersistenceProvider = _clientStaticConfig.getCheckpointPersistence()
                                     .getOrCreateCheckpointPersistenceProvider(_groupMember);

    HttpStatisticsCollector httpStatsColl = _clientStaticConfig.getHttpStatsCollector()
                                            .getExistingStatsCollector();
    if (null == httpStatsColl)
    {
      httpStatsColl = new HttpStatisticsCollector(getContainerStaticConfig().getId(),
                                                  "httpOutbound",
                                                  _clientStaticConfig.getRuntime().getHttpStatsCollector().isEnabled(),
                                                  true,
                                                  getMbeanServer());
    }
    _httpStatsCollector = httpStatsColl;

    _loggingListener = new LoggingConsumer(_clientStaticConfig.getLoggingListener());

    _clientStaticConfig.getRuntime().setManagedInstance(this);
    _configManager = new ConfigManager<RuntimeConfig>(_clientStaticConfig.getRuntimeConfigPrefix(),
                                                      _clientStaticConfig.getRuntime());

    NettyHttpConnectionFactory defaultConnFacory =
        new NettyHttpConnectionFactory(getBossExecutorService(), getIoExecutorService() ,
                                       getContainerStatsCollector(),
                                       getNetworkTimeoutTimer(),
                                       _clientStaticConfig.getContainer().getWriteTimeoutMs(),
                                       _clientStaticConfig.getContainer().getReadTimeoutMs(),
                                       _clientStaticConfig.getProtocolVersion(),
                                       getHttpChannelGroup());
    _relayConnFactory = defaultConnFacory;
    _bootstrapConnFactory = defaultConnFacory;

    initializeClientCommandProcessors();
    //_inBoundStatsCollectors is merged in ServerContainer
    getGlobalStatsMerger().registerStatsCollector(_bootstrapEventsStats);
    //_consumerStatsCollectors and _bsConsumerStatsCollectors are not meaningful at a global level
  }

  /**
   * Creates a client configuration builder from a properties collection
   * @param  propertyPrefix   the prefix for all property names for this client
   * @param  props            the configuration properties
   */
  public static Config createConfigBuilder(String propertyPrefix, Properties props)
          throws InvalidConfigException
  {
    Config configBuilder = new Config();
    ConfigLoader<StaticConfig> confLoader =
        new ConfigLoader<StaticConfig>(propertyPrefix, configBuilder);
    confLoader.loadConfig(props);

    return configBuilder;
  }

  public StaticConfig getClientStaticConfig()
  {
    return _clientStaticConfig;
  }

  /**
   * Registers a consumer for streaming events for the given sources
   * @param listener        the consumer callback object
   * @param filterConfig    configuration for filtering events (can be null)
   * @param sources         the list of sources to listen to (order is significant)
   */
  @Override
  public void registerDatabusStreamListener(DatabusStreamConsumer listener, DbusKeyCompositeFilterConfig filterConfig, String ... sources)
                                    throws DatabusClientException
  {
    registerDatabusStreamListener(listener, Arrays.asList(sources), filterConfig);
  }

  @Override
  public void registerDatabusStreamListener(DatabusStreamConsumer[] listeners, DbusKeyCompositeFilterConfig filterConfig, String ... sources)
                                    throws DatabusClientException
  {
    registerDatabusStreamListener(listeners, Arrays.asList(sources), filterConfig);
  }

  @Override
  public void registerDatabusBootstrapListener(DatabusBootstrapConsumer listener, DbusKeyCompositeFilterConfig filterConfig, String ... sources)
                                               throws DatabusClientException
  {
    registerDatabusBootstrapListener(listener, Arrays.asList(sources), filterConfig);
  }

  @Override
  public void registerDatabusBootstrapListener(DatabusBootstrapConsumer[] listeners, DbusKeyCompositeFilterConfig filterConfig, String ... sources)
  throws DatabusClientException
  {
    registerDatabusBootstrapListener(listeners, Arrays.asList(sources), filterConfig);
  }

  /**
   *
   * @param listener
   * @param sources
   * @param filterConfig
   * @throws DatabusClientException
   */
  public void registerDatabusStreamListener(DatabusStreamConsumer listener,
                                            List<String> sources,
                                            DbusKeyCompositeFilterConfig filterConfig)
                                            throws DatabusClientException
  {
	  DatabusStreamConsumer[] listeners = { listener };
	  registerDatabusStreamListener(listeners, sources, filterConfig);
  }

  /**
   *
   * @param listeners
   * @param sources
   * @param filterConfig
   * @throws DatabusClientException
   */
  public synchronized void registerDatabusStreamListener(
		  					DatabusStreamConsumer[] listeners,
                            List<String> sources,
                            DbusKeyCompositeFilterConfig filterConfig)
          throws DatabusClientException
  {
	  List<DatabusStreamConsumer> listenersList = Arrays.asList(listeners);
	  List<SelectingDatabusCombinedConsumer> sdccListenersList =
			  SelectingDatabusCombinedConsumerFactory.convertListOfStreamConsumers(listenersList);
	  List<DatabusCombinedConsumer> dccListenersList = new ArrayList<DatabusCombinedConsumer>();
	  for(SelectingDatabusCombinedConsumer sdcc: sdccListenersList)
	  {
		  dccListenersList.add(sdcc);
	  }

	  DatabusV2ConsumerRegistration consumerReg =
			  new DatabusV2ConsumerRegistration(dccListenersList, sources, filterConfig);

	  List<DatabusV2ConsumerRegistration> consumers =
			  registerDatabusListener(consumerReg, _relayGroups, getRelayGroupStreamConsumers(), DatabusSubscription.createSubscriptionList(sources));
	  if (listeners[0] != getLoggingListener() && 1 == consumers.size())
	  {
		  DatabusStreamConsumer logConsumer = getLoggingListener();
		  SelectingDatabusCombinedConsumer sdccLogConsumer = new SelectingDatabusCombinedConsumer(logConsumer);

		  DatabusV2ConsumerRegistration loggingReg =
				  new DatabusV2ConsumerRegistration(sdccLogConsumer,	sources,filterConfig);
		  consumers.add(loggingReg);
	  }
  }

  /**
   *
   * @param listener
   * @param sources
   * @param filterConfig
   * @throws DatabusClientException
   */
  public void registerDatabusBootstrapListener(DatabusBootstrapConsumer listener,
                                               List<String> sources,
                                               DbusKeyCompositeFilterConfig filterConfig)
                                              throws DatabusClientException
  {
	  DatabusBootstrapConsumer[] listeners = { listener };
	  registerDatabusBootstrapListener(listeners, sources, filterConfig);
  }

  /**
   *
   * @param listeners
   * @param sources
   * @param filter
   * @throws DatabusClientException
   */
  public synchronized void registerDatabusBootstrapListener(DatabusBootstrapConsumer[] listeners,
                                               List<String> sources,
                                               DbusKeyCompositeFilterConfig filter)
                                              throws DatabusClientException
  {
		List<DatabusBootstrapConsumer> listenersList = Arrays.asList(listeners);
		List<SelectingDatabusCombinedConsumer> sdccListenersList =
				SelectingDatabusCombinedConsumerFactory.convertListOfBootstrapConsumers(listenersList);
		List<DatabusCombinedConsumer> dccListenersList = new ArrayList<DatabusCombinedConsumer>();
		for(SelectingDatabusCombinedConsumer sdcc: sdccListenersList)
		{
			dccListenersList.add(sdcc);
		}

		DatabusV2ConsumerRegistration consumerReg =
				new DatabusV2ConsumerRegistration(dccListenersList, sources, filter);

		List<DatabusV2ConsumerRegistration> consumers =
				registerDatabusListener(consumerReg, _relayGroups, getRelayGroupBootstrapConsumers(),
						DatabusSubscription.createSubscriptionList(sources));
		if (listeners[0] != getLoggingListener() && 1 == consumers.size())
		{
			DatabusBootstrapConsumer logConsumer = getLoggingListener();
			SelectingDatabusCombinedConsumer sdccLogConsumer = new SelectingDatabusCombinedConsumer(logConsumer);
			DatabusV2ConsumerRegistration loggerReg =
					new DatabusV2ConsumerRegistration(sdccLogConsumer, sources, filter);
			consumers.add(loggerReg);
		}
  }

  /**
   *
   * @param listener
   * @throws DatabusClientException
   */
  public synchronized void unregisterDatabusStreamListener(DatabusStreamConsumer listener)
                                      throws DatabusClientException
  {
      for (List<DatabusSubscription> relayGroup: getRelayGroupStreamConsumers().keySet())
      {
        while (getRelayGroupStreamConsumers().get(relayGroup).remove(listener));
      }
  }

  /**
   *
   * @param listener
   * @throws DatabusClientException
   */
  public synchronized void unregisterDatabusBootstrapListener(DatabusBootstrapConsumer listener)
                                                 throws DatabusClientException
  {
      for (List<DatabusSubscription> relayGroup: getRelayGroupBootstrapConsumers().keySet())
      {
        while (getRelayGroupBootstrapConsumers().get(relayGroup).remove(listener));
      }
  }

  public synchronized void doRegisterBootstrapService(ServerInfo serverInfo)
  {
      LOG.info("Registering bootstrap: " + serverInfo.toString());
      List<DatabusSubscription> subList = DatabusSubscription.createSubscriptionList(serverInfo.getSources());
      Set<ServerInfo> sourceBootstrapServers = _bootstrapGroups.get(subList);
      if (null == sourceBootstrapServers)
      {
        sourceBootstrapServers = new HashSet<ServerInfo>();
        _bootstrapGroups.put(subList, sourceBootstrapServers);
      }
      sourceBootstrapServers.add(serverInfo);
  }

  public synchronized void doUnregisterBootstrapService(ServerInfo serverInfo)
  {
      Set<ServerInfo> sourceBootstrapServers = _bootstrapGroups.get(DatabusSubscription.createSubscriptionList(serverInfo.getSources()));
      if (null != sourceBootstrapServers)
      {
        LOG.info("Unregistering bootstrap: " + serverInfo.toString());
        sourceBootstrapServers.remove(serverInfo);
      }
  }

  /**
   *
   * @return
   */
  public Map<List<DatabusSubscription>, Set<ServerInfo>> getRelayGroups() {
    return _relayGroups;
  }

  /**
   *
   * @return
   */
  public List<ServerInfo> getRelays() {
    RuntimeConfig runtimeConfig = getClientConfigManager().getReadOnlyConfig();
    return runtimeConfig.getRelays();
  }

  /**
   *
   * @return
   */
  public synchronized List<DatabusSourcesConnection> getRelayConnections() {
	return _relayConnections;
  }

  /**
   *
   * @return
   */
  public synchronized List<ServerInfo> getBootstrapServices() {
    RuntimeConfig runtimeConfig = getClientConfigManager().getReadOnlyConfig();
    return runtimeConfig.getBootstrap().getServices();
  }

  /**
   *
   * @return
   */
  public synchronized Map<List<DatabusSubscription>, List<DatabusV2ConsumerRegistration>>
  getRelayGroupStreamConsumers()
  {
    return _relayGroupStreamConsumers;
  }

  /**
   *
   * @return
   */
  public synchronized Map<List<DatabusSubscription>, List<DatabusV2ConsumerRegistration>>
  getRelayGroupBootstrapConsumers()
  {
    return _relayGroupBootstrapConsumers;
  }

  /**
   *
   * @return
   */
  public ConfigManager<RuntimeConfig> getClientConfigManager()
  {
    return _configManager;
  }

  /**
   *
   * @return
   */
  public CheckpointPersistenceProvider getCheckpointPersistenceProvider()
  {
    return _checkpointPersistenceProvider;
  }

  /**
   *
   * @return
   */
  public HttpStatisticsCollector getHttpStatsCollector()
  {
    return _httpStatsCollector;
  }

  /**
   *
   * @return
   */
  public LoggingConsumer getLoggingListener()
  {
    return _loggingListener;
  }

  /**
   *
   * @return
   */
  public DatabusRelayConnectionFactory getRelayConnFactory()
  {
    return _relayConnFactory;
  }

  /**
   *
   * @return
   */
  public DatabusBootstrapConnectionFactory getBootstrapConnFactory()
  {
    return _bootstrapConnFactory;
  }

  /**
   * return set of bootstrap events collectors
   */
  public StatsCollectors<DbusEventsStatisticsCollector> getBootstrapEventsStats()
  {
	  return _bootstrapEventsStats;
  }

  /**
   * return set of relay consumer stats collectors
   */
  public StatsCollectors<ConsumerCallbackStats> getRelayConsumerStatsCollectors()
  {
	  return _consumerStatsCollectors;
  }

  /**
   * return set of bootstrap consumer stats collectors
   */
  public StatsCollectors<ConsumerCallbackStats> getBootstrapConsumerStatsCollectors()
  {
	  return _bsConsumerStatsCollectors;
  }

  /**
   *
   * @return
   */
  public DbusEventsStatisticsCollector getBootstrapEventsStatsCollector()
  {
    return _bootstrapEventsStats.getStatsCollector();
  }



  /**
   *
   * @param writeTimeTracker
   */
  public void setWriteTimeTracker(LastWriteTimeTrackerImpl writeTimeTracker)
  {
    _writeTimeTracker = writeTimeTracker;
  }

  /**
   *
   * @return
   */
  public LastWriteTimeTrackerImpl getWriteTimeTracker()
  {
    return _writeTimeTracker;
  }

  /**
   *
   * @return
   */
  public DatabusClientDSCUpdater getDSCUpdater()
  {
    return _dscUpdater ;
  }
  @Override
  public DatabusRegistration register(DatabusCombinedConsumer consumer,
                                      String... sources)
               throws DatabusClientException
  {
         if ((null == consumer))
               throw new DatabusClientException("Consumer Callback is null !!");

         if ((null == sources) || (sources.length == 0))
               throw new DatabusClientException("Sources is empty !!");

         RegistrationId regId =
                 RegistrationIdGenerator.generateNewId(consumer.getClass().getSimpleName(), 
                		                               DatabusSubscription.createSubscriptionList(Arrays.asList(sources)));
         DatabusV2RegistrationImpl reg = new DatabusV2RegistrationImpl(regId, this, getCheckpointPersistenceProvider());
         List<DatabusCombinedConsumer> consumers = new ArrayList<DatabusCombinedConsumer>();
         consumers.add(consumer);
         reg.addDatabusConsumers(consumers);
         reg.addSubscriptions(sources);
         _regList.add(reg);
         reg.onRegister();
         return reg;
  }

  @Override
  public DatabusRegistration register(
               Collection<DatabusCombinedConsumer> consumers,
               String... sources)
        throws DatabusClientException
  {
               if ((null == consumers)  || (consumers.isEmpty()))
                       throw new DatabusClientException("Consumer Callback list is empty !!");

               if ((null == sources) || (sources.length == 0))
                       throw new DatabusClientException("Sources is empty !!");

               RegistrationId regId =
                       RegistrationIdGenerator.generateNewId(consumers.iterator().next().getClass().getSimpleName(),
                    		                                DatabusSubscription.createSubscriptionList(Arrays.asList(sources)));
               DatabusV2RegistrationImpl reg = new DatabusV2RegistrationImpl(regId, this, getCheckpointPersistenceProvider());
               reg.addDatabusConsumers(consumers);
               reg.addSubscriptions(sources);
               _regList.add(reg);
               reg.onRegister();
               return reg;
  }

  @Override
  public DatabusRegistration registerCluster(String cluster,
               DbusClusterConsumerFactory consumerFactory,
               DbusServerSideFilterFactory filterFactory,
               DbusPartitionListener partitionListener, String... sources)  
                                throws DatabusClientException
  
  {
         if ((null == sources) || (sources.length == 0))
               throw new DatabusClientException("Sources is empty !!");
          
         if ( _activeClusters.contains(cluster))
        	 throw new DatabusClientException("Cluster :" + cluster + " has already been registed to this client instance." +
                                              " Only one registration per cluster is allowed for a databus client instance !!");
         
         ClusterRegistrationStaticConfig c = _clientStaticConfig.getClientCluster(cluster);
          
         if ( null == c )
                 throw new DatabusClientException("Cluster Configuration for cluster (" + cluster + ") not provided !!");

         if ( null == consumerFactory)
        	 throw new DatabusClientException("Consumer Factory is null !!");
         
         ClusterCheckpointPersistenceProvider.StaticConfig ckptPersistenceProviderConfig = new ClusterCheckpointPersistenceProvider.StaticConfig(c.getZkAddr(),c.getClusterName(),c.getMaxCkptWritesSkipped());

         DbusClusterInfo clusterInfo = new DbusClusterInfo(c.getClusterName(), c.getNumPartitions(), c.getQuorum());

         RegistrationId regId =
                 RegistrationIdGenerator.generateNewId(c.getClusterName());
         
         DatabusV2ClusterRegistrationImpl reg = new DatabusV2ClusterRegistrationImpl(regId, this, ckptPersistenceProviderConfig, clusterInfo, consumerFactory, filterFactory, partitionListener, sources);
         _regList.add(reg);
         reg.onRegister();
         _activeClusters.add(cluster);
         return reg;
  }

  public boolean deregister(DatabusRegistration reg)
  {
         return _regList.remove(reg);
  }


  public Collection<DatabusMultiPartitionRegistration> getAllClientClusterRegistrations()
  {
         List<DatabusMultiPartitionRegistration> regs = new ArrayList<DatabusMultiPartitionRegistration>();
         for (DatabusRegistration reg : _regList)
         {
                 if ( reg instanceof DatabusV2ClusterRegistrationImpl)
                         regs.add((DatabusV2ClusterRegistrationImpl)reg);
         }
         return regs;
  }

  public Collection<DatabusRegistration> getAllRegistrations()
  {
         return Collections.unmodifiableCollection(_regList);
         
  }

  /**
   *
   * @return
   */
  public boolean isClusterEnabled()
  {
	  return (_clientNode != null) && (_clientNode.isEnabled());
  }


  @Override
  public void pause()
  {
    _clientStatus.pause();
  }

  @Override
  public void resume()
  {
    _clientStatus.resume();
  }

  @Override
  public void suspendOnError(Throwable cause)
  {
    _clientStatus.suspendOnError(cause);
  }

  /**
   *
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception
  {
    Config staticConfigBuilder = new Config();
    DatabusHttpClientImpl client = createFromCli(args, staticConfigBuilder);
    client.startAndBlock();
  }

  /**
   * Starts the databus client library. Meant to be used from the command line.
   * @param   args                   the command line arguments
   * @param   defaultConfigBuilder   default values for the Databus client library configuration; can
   *                                 be null
   **/
  public static DatabusHttpClientImpl createFromCli(String[] args,
                                                    Config defaultConfigBuilder) throws Exception
  {
    Properties startupProps =  ServerContainer.processCommandLineArgs(args);
    if (null == defaultConfigBuilder) defaultConfigBuilder = new Config();

    ConfigLoader<StaticConfig> staticConfigLoader =
        new ConfigLoader<StaticConfig>("databus.client.", defaultConfigBuilder);

    StaticConfig staticConfig = staticConfigLoader.loadConfig(startupProps);

    DatabusHttpClientImpl dbusHttpClient = new DatabusHttpClientImpl(staticConfig);

    return dbusHttpClient;
  }

  @Override
  public void start()
  {
    super.start();
  }

  @Override
  protected void doStart()
  {
    super.doStart();

    LOG.info("relay consumers: " + getRelayGroupStreamConsumers().size());
    LOG.info("bootstrap consumers: " + getRelayGroupBootstrapConsumers().size());

    // Disabling dscUpdater on purpose: Reason - no users
    //_dscUpdater = new DatabusClientDSCUpdater(_groupMember,_writeTimeTracker,_clientStaticConfig.getDSCUpdateIntervalMs());
    if (_clientNode.isEnabled())
    {
      if (!waitForLeadership())
      {
        LOG.error("Error acquiring leadership! Not starting client");
		return;
      }
    }

    _httpStatsCollector.registerMastershipStatus(1);
    if (getClientStaticConfig().getProtocolVersion() == 2)
    {
        initializeRelayConnections();
        for (DatabusRegistration r : _regList)
        {
        	if (! r.getState().isRunning())
        	{
        		try {

        			LOG.info("Registration (" + r.getRegistrationId() + ") has not been started yet !! Starting !!");
        			r.start();
        		} catch (DatabusClientException e) {
        			LOG.error("Got exception while starting registration !!", e);
        			throw new RuntimeException(e);
        		}
        	}
        }
    }
    else
    {
        // The connections are made during registerDatabusListener call and not when the client is started
    	LOG.info("Client version 3: Connections to relays during a register call");
    }
    if ((_dscUpdater != null) && _dscUpdater.isRunning())
    {
      _dscUpdater.stop();
    }
  }

  @Override
  protected void doShutdown()
  {
    LOG.info(getClass().getSimpleName() +": shutting down...");
    unregisterMbeans();
    for (DatabusSourcesConnection relayConn: _relayConnections)
    {
      relayConn.stop();
    }
    
    for(DatabusRegistration reg : _regList)
    {
    	LOG.info("Shutting down registration :" + reg.getRegistrationId());
    	try
    	{
    		if (reg.getState() != RegistrationState.SHUTDOWN)
    			reg.shutdown();
    	} catch (Exception ex) {
    		LOG.error("Unable to shutdown registration :" + reg.getRegistrationId(),ex);
    	}
    }

    //shutdown dsc updater thread if running; it might write to zk; so leave zk after this
    if ((_dscUpdater != null) && _dscUpdater.isRunning())
    {
      _dscUpdater.stop();
      _dscUpdater.awaitShutdown();
    }

  //leave group;
    if (_groupMember != null)
    {
    	_groupMember.leave();
    }
    //close zk connection
    _clientNode.close();
    _httpStatsCollector.registerMastershipStatus(0);

    super.doShutdown();
    LOG.info(getClass().getSimpleName() +": shutdown.");
  }

  /**
   *
   * @throws DatabusException
   */
  protected void initializeClientCommandProcessors() throws DatabusException
  {
    _processorRegistry.register(ClientStatsRequestProcessor.COMMAND_NAME,
                               new ClientStatsRequestProcessor(null, this));
    _processorRegistry.register(ClientStateRequestProcessor.COMMAND_NAME, 
    		                    new ClientStateRequestProcessor(null, this));
  }

  /**
   *
   * @return
   */
  protected boolean waitForLeadership()
  {
    DatabusClientGroupMember member = _groupMember;
    if (member != null)
    {
      if (member.join())
      {
        LOG.info("Waiting for leadership: " + member.toString());
      //lauch dscUpdater thread
        if (_dscUpdater != null)
        {
        	Thread t = new Thread(_dscUpdater, "DscUpdater");
        	t.setDaemon(true);
        	t.start();
        }
        boolean acquiredLeadership = member.waitForLeaderShip();
        LOG.info("Acquired leadership=  " + acquiredLeadership + " member=" +  member.toString());
        return acquiredLeadership;
      }
    }
    LOG.info("Started Failed! Non availability of group/cluster handler! http relay connection for sources:" + getRelayGroupStreamConsumers());
    return false;
  }

  /**
   *
   */
  protected void unregisterMbeans()
  {
      getHttpStatsCollector().unregisterMBeans();
      getBootstrapEventsStatsCollector().unregisterMBeans();
      for (DbusEventsStatisticsCollector b: _bootstrapEventsStats.getStatsCollectors())
      {
    	  b.unregisterMBeans();
      }
      for (DatabusSourcesConnection conn: _relayConnections)
      {
    	  conn.unregisterMbeans();
      }
  }

  /**
   *
   * @param groupsServers
   * @param sources
   * @return
   * @throws DatabusClientException
   */
  protected ServerInfo getRandomRelay(Map<List<DatabusSubscription>, Set<ServerInfo>> groupsServers,
		  							List<DatabusSubscription> sources)
  throws DatabusClientException
  {
	    List<ServerInfo> candidateServers = findServers(groupsServers, sources);
	    if (0 == candidateServers.size())
	    {

	      throw new DatabusClientException("Unable to find servers to support sources: " + sources);
	    }
	    Random rng = new Random();
	    ServerInfo randomRelay = candidateServers.get(rng.nextInt(candidateServers.size()));
	    return randomRelay;
  }

  protected List<DatabusV2ConsumerRegistration> registerDatabusListener(
      DatabusV2ConsumerRegistration listener,
      Map<List<DatabusSubscription>, Set<ServerInfo>> groupsServers,
      Map<List<DatabusSubscription>, List<DatabusV2ConsumerRegistration>> groupsListeners,
      List<DatabusSubscription> sources
      )
      throws DatabusClientException
  {
	List<DatabusSubscription> subsSources = null;
    ServerInfo randomRelay = getRandomRelay(groupsServers, sources);
    if ( null == randomRelay )
    {
    	// Possible only in V3 case, where a registration succeeds even if there is no relay
    	// available to serve it immediately
    	assert (getClientStaticConfig().getProtocolVersion() == 3);
    	subsSources = sources;
    }
    else
    {
        try
        {
          subsSources = DatabusSubscription.createFromUriList(randomRelay.getSources());
        }
        catch (DatabusException e)
        {
          throw new DatabusClientException("source list decode error:" + e.getMessage(), e);
        }
        catch (URISyntaxException e)
        {
          throw new DatabusClientException("source list decode error:" + e.getMessage(), e);
        }
    }
    List<DatabusV2ConsumerRegistration> consumers = getListOfConsumerRegsFromSubList(groupsListeners, subsSources);
    if (null == consumers)
    {
      consumers = new CopyOnWriteArrayList<DatabusV2ConsumerRegistration>();
      groupsListeners.put(subsSources, consumers);
    }
    consumers.add(listener);
    return consumers;
  }

  /*
   * Given a mapping between a List of subscriptions to List of registrations, and a list of interested subscriptions,
   * returns a corresponding list of registrations.
   *
   * The semantics of this vary for V2 and V3 clients currently. This method is overridden in V3 with its semantics
   */
  protected static List<DatabusV2ConsumerRegistration> getListOfConsumerRegsFromSubList(
		  											   Map<List<DatabusSubscription>, List<DatabusV2ConsumerRegistration>> groupsListeners,
		  											   List<DatabusSubscription> subsSources)
  {
	  return groupsListeners.get(subsSources);
  }

  @Override
  protected DatabusComponentAdmin createComponentAdmin()
  {
    return new DatabusComponentAdmin(this,
                                     getMbeanServer(),
                                     DatabusHttpClientImpl.class.getSimpleName());
  }

  @Override
  protected DatabusComponentStatus createComponentStatus()
  {
    _clientStatus = new DatabusHttpClientStatus();
    return _clientStatus;
  }

  protected synchronized void doRegisterRelay(ServerInfo serverInfo)
  {
	  LOG.info("Registering relay: " + serverInfo.toString());

	  List<DatabusSubscription> subList = DatabusSubscription.createSubscriptionList(serverInfo.getSources());
	  Set<ServerInfo> sourceRelays = _relayGroups.get(subList);
	  if (null == sourceRelays)
	  {
		  sourceRelays = new HashSet<ServerInfo>(5);
		  _relayGroups.put(subList, sourceRelays);
	  }
	  sourceRelays.add(serverInfo);

  }


  /**
   * Should be only used by unittest
   * */
  public void setCMEnabled(boolean enabled)
  {
    _cmEnabled = enabled;
  }

  protected synchronized void doUnregisterRelay(ServerInfo serverInfo)
  {
      Set<ServerInfo> sourceRelays = _relayGroups.get(DatabusSubscription.createSubscriptionList(serverInfo.getSources()));
      if (null != sourceRelays)
      {
        LOG.info("Unregistering relay: " + serverInfo.toString());
        sourceRelays.remove(serverInfo);
      }
  }

  /** generate a stats object name for a sources connection object */
  String generateSubsStatsName(List<String> sourcesStrList)
  {
	  int subsListSize = sourcesStrList.size();
	  String lastSubs = (subsListSize != 0) ? sourcesStrList.get(subsListSize-1) : "null" ;
	  String[] cmpt =  lastSubs.split("\\.");
	  String name =  (cmpt.length >= 4) ? cmpt[3] : lastSubs;
	  LOG.info("sourcename= " + name);
	  return name;
  }

  private synchronized void initializeRelayConnections()
  {
	  for(List<DatabusSubscription> subsList: _relayGroups.keySet())
	  {
		  List<String> sourcesStrList = DatabusSubscription.getStrList(subsList);
		  List<DatabusV2ConsumerRegistration> relayConsumers =
		      getRelayGroupStreamConsumers().get(subsList);
		  //nothing to do
		  if (null == relayConsumers || 0 == relayConsumers.size()) continue;
		  try
		  {
			  DatabusSourcesConnection.StaticConfig connConfig =
					  getClientStaticConfig().getConnection(sourcesStrList);
			  if (null == connConfig)
			  {
				  connConfig = getClientStaticConfig().getConnectionDefaults();
			  }

			  // Disabling SCN index works only with the BLOCK_ON_WRITE policy. If Scn index is disabled,
			  // make sure we have the right policy.
			  if (!connConfig.getEventBuffer().isEnableScnIndex() &&
				  connConfig.getEventBuffer().getQueuePolicy() != DbusEventBuffer.QueuePolicy.BLOCK_ON_WRITE)
			  {
				  throw new InvalidConfigException("If SCN index is disabled, queue policy must be BLOCK_ON_WRITE");
			  }
			  CheckpointPersistenceProvider cpPersistenceProvder = getCheckpointPersistenceProvider();
			  if (null != cpPersistenceProvder && getClientStaticConfig().getCheckpointPersistence().isClearBeforeUse())
			  {
				  cpPersistenceProvder.removeCheckpoint(sourcesStrList);
			  }

			  ServerInfo server0 = _relayGroups.get(subsList).iterator().next();

			  ArrayList<DatabusV2ConsumerRegistration> bstConsumersRegs =
					  new ArrayList<DatabusV2ConsumerRegistration>();
			  for (List<DatabusSubscription> bstSubSourcesList: getRelayGroupBootstrapConsumers().keySet())
			  {
				  List<DatabusV2ConsumerRegistration> bstRegsistrations
				  = getRelayGroupBootstrapConsumers().get(bstSubSourcesList);
				  for (DatabusV2ConsumerRegistration bstConsumerReg: bstRegsistrations)
				  {
					  if (server0.supportsSources(bstConsumerReg.getSources()))
					  {
						  bstConsumersRegs.add(bstConsumerReg);
					  }
				  }
			  }

			  DbusEventBuffer eventBuffer = connConfig.getEventBuffer().getOrCreateEventBuffer();
			  eventBuffer.setDropOldEvents(true);
			  eventBuffer.start(0);

			  DbusEventBuffer bootstrapBuffer = new DbusEventBuffer(connConfig.getEventBuffer());
			  bootstrapBuffer.setDropOldEvents(false);
			  bootstrapBuffer.start(0);

			  LOG.info("The sourcesList is " + sourcesStrList);
			  LOG.info("The relayGroupStreamConsumers is " + getRelayGroupStreamConsumers().get(subsList));

			  Set<ServerInfo> relays = _relayGroups.get(subsList);
			  Set<ServerInfo> bootstrapServices = _bootstrapGroups.get(subsList);

			  String statsCollectorName = generateSubsStatsName(sourcesStrList);
              int ownerId = getContainerStaticConfig().getId();

			  _bootstrapEventsStats.addStatsCollector(
			      statsCollectorName,
			      new DbusEventsStatisticsCollector(ownerId,
			                                        statsCollectorName + ".inbound.bs",
			                                        true,
			                                        false,
			                                        getMbeanServer()));

			  _inBoundStatsCollectors.addStatsCollector(
			      statsCollectorName,
			      new DbusEventsStatisticsCollector(ownerId,
			                                        statsCollectorName + ".inbound",
			                                        true,
			                                        false,
			                                        getMbeanServer()));

			  _outBoundStatsCollectors.addStatsCollector(
			      statsCollectorName,
			      new DbusEventsStatisticsCollector(ownerId,
			                                        statsCollectorName + ".outbound",
			                                        true,
			                                        false,
			                                        getMbeanServer()));

			    ConsumerCallbackStats relayConsumerStats =
			        new ConsumerCallbackStats(ownerId,
			                                  statsCollectorName + ".inbound.cons",
			                                  statsCollectorName + ".inbound.cons",
			                                  true,
			                                  false,
			                                  null,
			                                  getMbeanServer());
			    _consumerStatsCollectors.addStatsCollector(statsCollectorName, relayConsumerStats);

			    ConsumerCallbackStats bootstrapConsumerStats =
			        new ConsumerCallbackStats(ownerId,
			                                  statsCollectorName + ".inbound.bs.cons" ,
			                                  statsCollectorName + ".inbound.bs.cons",
			                                  true,
			                                  false,
			                                  null,
			                                  getMbeanServer());
			    _bsConsumerStatsCollectors.addStatsCollector(statsCollectorName, bootstrapConsumerStats);

			  DatabusSourcesConnection newConn =
					  new DatabusSourcesConnection(
							  connConfig,
							  subsList,
							  relays,
							  bootstrapServices,
							  relayConsumers,
							  //_relayGroupBootstrapConsumers.get(sourcesList),
							  bstConsumersRegs,
							  eventBuffer,
							  bootstrapBuffer,
							  getDefaultExecutorService(),
							  getContainerStatsCollector(),
							  _inBoundStatsCollectors.getStatsCollector(statsCollectorName),
							  _bootstrapEventsStats.getStatsCollector(statsCollectorName),
							  _consumerStatsCollectors.getStatsCollector(statsCollectorName),
							  _bsConsumerStatsCollectors.getStatsCollector(statsCollectorName),
							  getCheckpointPersistenceProvider(),
							  getRelayConnFactory(),
							  getBootstrapConnFactory(),
							  getHttpStatsCollector(),
							  null,
							  this);

			  _consumerStatsCollectors.addStatsCollector(statsCollectorName,newConn.getRelayConsumerStats());
			  _bsConsumerStatsCollectors.addStatsCollector(statsCollectorName,newConn.getBootstrapConsumerStats());

			  newConn.start();
			  _relayConnections.add(newConn);
		  }
		  catch (Exception e)
		  {
			  LOG.error("connection initialization issue for source(s):" + subsList +
					  "; please check your configuration", e);
		  }
	  }

	  if (0 == _relayConnections.size())
	  {
		  LOG.warn("No connections specified");
	  }
  }

  protected static List<ServerInfo> findServers(Map<List<DatabusSubscription>, Set<ServerInfo>> groups,
                                                 List<DatabusSubscription> subs)
  {
    boolean debugEnabled = LOG.isDebugEnabled();
    ArrayList<ServerInfo> result = new ArrayList<ServerInfo>(10);
    for(Map.Entry<List<DatabusSubscription>, Set<ServerInfo>> entry : groups.entrySet())
    {
      List<DatabusSubscription> serverSubs = entry.getKey();
      if (ServerInfo.checkSubsequenceSubsV3(subs, serverSubs))
      {
        result.addAll(entry.getValue());
      }
      if (debugEnabled) LOG.debug("Log an individual entry in group " + entry);
    }

    return result;
  }

  private static List<ServerInfo> parseServerInfosMap(Map<String, ServerInfoBuilder> map)
                          throws InvalidConfigException
  {
    ArrayList<ServerInfo> infos = new ArrayList<ServerInfo>((int)(map.size() * 1.3));
    for (Map.Entry<String, ServerInfoBuilder> entry : map.entrySet())
    {
      LOG.info("parseServerInfo: " + entry.toString());
      ServerInfoBuilder builder = entry.getValue();
      boolean added = infos.add(builder.build());
      LOG.info("added=" + added);
    }
    LOG.info("info size=" + infos.size());
    return infos;
  }

  /**
   * Not applicable in V2
   * @return
   */
  public Map<RegistrationId, DatabusV3Registration> getRegistrationIdMap()
  {
	  return null;
  }

  /**
   * Return DatabusSourcesConnection corresponding to this regId
   */
  public DatabusSourcesConnection getDatabusSourcesConnection(String regIdStr)
  {
	  return null;
  }

  /**
   * Runtime configuration for bootstrapping of the consumers
   *
   * @author pganti
   *
   */
  public static class BootstrapClientRuntimeConfig implements ConfigApplier<BootstrapClientRuntimeConfig>
  {
    private final boolean _enabled;
    private final List<ServerInfo> _services;

    public BootstrapClientRuntimeConfig(boolean enabled, List<ServerInfo> bootstrapServices)
    {
      super();
      _enabled = enabled;
      _services = bootstrapServices;
    }

    /** A flag that indicates if bootstrapping is enabled. */
    public boolean isEnabled()
    {
      return _enabled;
    }

    /** Server configuration for the bootstrap servers */
    public List<ServerInfo> getServices()
    {
      return _services;
    }

    public Set<ServerInfo> getServicesSet(){
      return new HashSet<ServerInfo>(_services);
    }

    @Override
    public void applyNewConfig(BootstrapClientRuntimeConfig oldConfig)
    {
      //Nothing to do
    }

    @Override
    public boolean equals(Object otherConfig)
    {
      if (null == otherConfig || ! (otherConfig instanceof BootstrapClientRuntimeConfig)) return false;
      return equalsConfig((BootstrapClientRuntimeConfig)otherConfig);
    }

    @Override
    public boolean equalsConfig(BootstrapClientRuntimeConfig otherConfig)
    {
      if (null == otherConfig) return false;
      return isEnabled() == otherConfig.isEnabled() &&
             getServices().equals(otherConfig.getServices());
    }

    @Override
    public int hashCode()
    {
      return (_enabled ? 0xFFFFFFFF : 0) ^ _services.hashCode();
    }
  }

  /**
   *
   * @author pganti
   *
   */
  public static class BootstrapClientRuntimeConfigBuilder
                      implements ConfigBuilder<BootstrapClientRuntimeConfig>
  {
    private boolean _enabled;
    private final Map<String, ServerInfoBuilder> _services;
    private DatabusHttpClientImpl _managedInstance;
    private String _servicesList = "";

    public BootstrapClientRuntimeConfigBuilder()
    {
      _enabled = false;
      _services = new HashMap<String, ServerInfoBuilder>();
    }

    public boolean isEnabled()
    {
      return _enabled;
    }

    public void setEnabled(boolean enabled)
    {
      _enabled = enabled;
    }

    public void setService(String id, ServerInfoBuilder serverInfo)
    {
      _services.put(id, serverInfo);
    }

    public ServerInfoBuilder getService(String id)
    {
      ServerInfoBuilder bootstrapService = _services.get(id);
      if (null == bootstrapService)
      {
        bootstrapService = new ServerInfoBuilder();
        _services.put(id, bootstrapService);
      }

      return bootstrapService;
    }

    @Override
    public BootstrapClientRuntimeConfig build() throws InvalidConfigException
    {
      if (null == _managedInstance)
      {
        throw new InvalidConfigException("No managed databus client");
      }

      List<ServerInfo> bootstrapServices = parseServerInfosMap(_services);
      if (null != _servicesList && _servicesList.length() > 0) bootstrapServices = RuntimeConfigBuilder.parseServerInfoList(_servicesList, bootstrapServices);

      return new BootstrapClientRuntimeConfig(isEnabled(), bootstrapServices);
    }

    public DatabusHttpClientImpl getManagedInstance()
    {
      return _managedInstance;
    }

    public void setManagedInstance(DatabusHttpClientImpl managedInstance)
    {
      _managedInstance = managedInstance;
    }

    public String getServicesList()
    {
      return _servicesList;
    }

    public void setServicesList(String servicesList)
    {
      _servicesList = servicesList;
    }

  }

  /**
   *
   * @author pganti
   *
   */
  public static class CheckpointPersistenceRuntimeConfig
               implements ConfigApplier<CheckpointPersistenceRuntimeConfig>
  {
    private final FileSystemCheckpointPersistenceProvider.RuntimeConfig _fileSystem;
    private final SharedCheckpointPersistenceProvider.RuntimeConfig _shared;

    public CheckpointPersistenceRuntimeConfig(
                                              FileSystemCheckpointPersistenceProvider.RuntimeConfig fileSystem)
    {
      super();
      _fileSystem = fileSystem;
      _shared= null;
    }

    public CheckpointPersistenceRuntimeConfig(
                                              SharedCheckpointPersistenceProvider.RuntimeConfig shared)
    {
      super();
      _shared = shared;
      _fileSystem = null;
    }


    /** Runtime configuration for the file-system-based checkpoint persistence provider */
    public FileSystemCheckpointPersistenceProvider.RuntimeConfig getFileSystem()
    {
      return _fileSystem;
    }

    public SharedCheckpointPersistenceProvider.RuntimeConfig getShared()
    {
      return _shared;
    }

    @Override
    public void applyNewConfig(CheckpointPersistenceRuntimeConfig oldConfig)
    {
      if (_fileSystem != null)
      {
        if (null == oldConfig || !getFileSystem().equals(oldConfig.getFileSystem()))
        {
          getFileSystem().applyNewConfig(null != oldConfig ? oldConfig.getFileSystem() : null);
        }
      }
      else if (_shared != null)
      {
        if (null == oldConfig || !getShared().equals(oldConfig.getShared()))
        {
          getShared().applyNewConfig(null != oldConfig ? oldConfig.getShared() : null);
        }
      }
    }

    @Override
    public boolean equals(Object otherConfig)
    {
      if (null == otherConfig || !(otherConfig instanceof CheckpointPersistenceRuntimeConfig)) return false;
      return equalsConfig((CheckpointPersistenceRuntimeConfig)otherConfig);
    }

    @Override
    public boolean equalsConfig(CheckpointPersistenceRuntimeConfig otherConfig)
    {
      if (null == otherConfig) return false;
      if (_fileSystem!=null)
      {
        return getFileSystem().equals(otherConfig.getFileSystem());
      }
      else if (_shared != null)
      {
        return getShared().equals(otherConfig.getShared());
      }
      return false;
    }

    @Override
    public int hashCode()
    {
      return (_fileSystem != null) ? _fileSystem.hashCode() : _shared.hashCode();
    }


  }

  /**
   *
   * @author pganti
   *
   */
  public static class CheckpointPersistenceRuntimeConfigBuilder
                      implements ConfigBuilder<CheckpointPersistenceRuntimeConfig>
  {
    private FileSystemCheckpointPersistenceProvider.RuntimeConfigBuilder _fileSystem;
    private SharedCheckpointPersistenceProvider.RuntimeConfigBuilder _shared;
    private DatabusHttpClientImpl _managedInstance = null;

    public CheckpointPersistenceRuntimeConfigBuilder()
    {
      _fileSystem = new FileSystemCheckpointPersistenceProvider.RuntimeConfigBuilder();
      _shared =  new SharedCheckpointPersistenceProvider.RuntimeConfigBuilder();
    }

    public FileSystemCheckpointPersistenceProvider.RuntimeConfigBuilder getFileSystem()
    {
      return _fileSystem;
    }


    public void setFileSystem(FileSystemCheckpointPersistenceProvider.RuntimeConfigBuilder fileSystem)
    {
      _fileSystem = fileSystem;
    }

    public DatabusHttpClientImpl getManagedInstance()
    {
      return _managedInstance;
    }

    public void setManagedInstance(DatabusHttpClientImpl managedInstance)
    {
      _managedInstance = managedInstance;
      if (null != _managedInstance)
      {
        CheckpointPersistenceProvider persistenceProvider =
            _managedInstance.getCheckpointPersistenceProvider();
        if (persistenceProvider instanceof FileSystemCheckpointPersistenceProvider)
        {
          _fileSystem.setManagedInstance((FileSystemCheckpointPersistenceProvider)persistenceProvider);
        }
        else if (persistenceProvider instanceof SharedCheckpointPersistenceProvider)
        {
          _shared.setManagedInstance((SharedCheckpointPersistenceProvider)persistenceProvider);
        }
      }
    }

    @Override
    public CheckpointPersistenceRuntimeConfig build() throws InvalidConfigException
    {
      if (null == _managedInstance)
      {
        throw new InvalidConfigException("No associated client for runtime config");
      }
      if (_fileSystem.getManagedInstance()!= null) {
        return new CheckpointPersistenceRuntimeConfig(_fileSystem.build());
      } else if (_shared.getManagedInstance()!=null) {
        return new CheckpointPersistenceRuntimeConfig(_shared.build());
      }
      return null;
    }

    public SharedCheckpointPersistenceProvider.RuntimeConfigBuilder getShared()
    {
      return _shared;
    }

    public void setShared(SharedCheckpointPersistenceProvider.RuntimeConfigBuilder sharedState)
    {
      _shared = sharedState;
    }
  }

  /**
   *
   * @author pganti
   *
   */
  public static class CheckpointPersistenceStaticConfig
  {
    public static enum ProviderType
    {
      FILE_SYSTEM,
      EXISTING,
      NONE,
      SHARED
    }

    private final ProviderType _type;
    private final FileSystemCheckpointPersistenceProvider.StaticConfig _fileSystem;
    private final SharedCheckpointPersistenceProvider.StaticConfig _sharedState;
    private final CheckpointPersistenceProvider _existing;
    private final CheckpointPersistenceRuntimeConfigBuilder _runtime;
    private final String _runtimeConfigPrefix;
    private final boolean _clearBeforeUse;
    private final int _version; // default is 2


    @Override
	public String toString() {
		return "CheckpointPersistenceStaticConfig [_type=" + _type
				+ ", _fileSystem=" + _fileSystem + ", _sharedState="
				+ _sharedState + ", _existing=" + _existing + ", _runtime="
				+ _runtime + ", _runtimeConfigPrefix=" + _runtimeConfigPrefix
				+ ", _clearBeforeUse=" + _clearBeforeUse + "]";
	}

	public CheckpointPersistenceStaticConfig(ProviderType type,
                                             FileSystemCheckpointPersistenceProvider.StaticConfig fileSystem,
                                             SharedCheckpointPersistenceProvider.StaticConfig sharedState,
                                             CheckpointPersistenceProvider existing,
                                             CheckpointPersistenceRuntimeConfigBuilder runtime,
                                             String runtimeConfigPrefix,
                                             boolean clearBeforeUse,
                                             int version)
    {
      super();
      _type = type;
      _fileSystem = fileSystem;
      _sharedState = sharedState;
      _existing = existing;
      _runtime = runtime;
      _runtimeConfigPrefix = runtimeConfigPrefix;
      _clearBeforeUse = clearBeforeUse;
      _version = version;
    }

    /** The type of the checkpoint persistence provider.
     *
     * <ul>
     *  <li> FILE_SYSTEM specifies to store the checkpoints in a directory on a local file system</li>
     *  <li> EXISTING specifies to use a Spring-wired checkpoint persisten provider </li>
     *  <li> SHARED specifies to store the checkpoints in a shared location as distributed state </li>
     *  <li> NONE specifies not to persist checkpoints (*Use at your own risk!*) </li>
     * </ul>
     **/
    public ProviderType getType()
    {
      return _type;
    }

    /** relay-communication protocol version
     * <ul>
     * <li> 2 - v2 clients
     * <li> 3 - espresso clients
     * </ul>
     */
    public int getVersion() {
      return _version;
    }

    /**
     * The static configuration for the file-system-based checkpoint persistence provider.
     * {@link #getType()} needs to be {@link ProviderType}.FILE_SYSTEM for this property to have an
     * effect. */
    public FileSystemCheckpointPersistenceProvider.StaticConfig getFileSystem()
    {
      return _fileSystem;
    }

    /**
     * The static configuration for the shared-state based checkpoint persistence provider.
     * {@link #getType()} needs to be {@link ProviderType} SHARED for this property to have an
     * effect. */
    public SharedCheckpointPersistenceProvider.StaticConfig getSharedState()
    {
      return _sharedState;
    }

    /**
     * Obtain the existing checkpoint persistence provider if one has been specified.
     *
     * NOTE: We don't use the standard getXXX() naming convention to avoid attempts
     * to serialize it.
     **/
    public CheckpointPersistenceProvider existing()
    {
      return ProviderType.EXISTING == _type ? _existing : null;
    }

    public String getRuntimeConfigPrefix()
    {
      return _runtimeConfigPrefix;
    }

    /** The runtime configuration for the checkpoint persistence provider */
    public CheckpointPersistenceRuntimeConfigBuilder getRuntime()
    {
      return _runtime;
    }

    /**
     * A flag that indicates whether to clear any existing persisted checkpoints on first use.
     *
     * NOTE: This should be used mostly for testing where we want to make sure that no state from
     * previous tests is used.
     * */
    public boolean isClearBeforeUse()
    {
      return _clearBeforeUse;
    }

    public CheckpointPersistenceProvider getOrCreateCheckpointPersistenceProvider()
    throws InvalidConfigException
    {
      return getOrCreateCheckpointPersistenceProvider(null);
    }

    public CheckpointPersistenceProvider getOrCreateCheckpointPersistenceProvider(DatabusClientGroupMember groupMember)
           throws InvalidConfigException
    {
      CheckpointPersistenceProvider cpPersistenceProvider = null;
      switch (getType())
      {
        case FILE_SYSTEM:
        LOG.info("Creating file-system checkpoint persistence provider");
        cpPersistenceProvider =
            new FileSystemCheckpointPersistenceProvider(getFileSystem(), getVersion());
        break;
        case SHARED:
          LOG.info("Creating shared zookeeper based checkpoint persistence provider");
        cpPersistenceProvider  = new SharedCheckpointPersistenceProvider(groupMember,getSharedState());
        break;

        case EXISTING: cpPersistenceProvider = _existing; break;
        case NONE: cpPersistenceProvider = null; break;
        default: throw new InvalidConfigException("Unable to create persistence provider of type: " +
                                       getType());
      }

      return cpPersistenceProvider;
    }
  }

  /**
   *
   * @author pganti
   *
   */
  public static class CheckpointPersistenceStaticConfigBuilder
                      implements ConfigBuilder<CheckpointPersistenceStaticConfig>
  {
    private String _type                       = ProviderType.FILE_SYSTEM.toString();
    private FileSystemCheckpointPersistenceProvider.Config _fileSystem;
    private SharedCheckpointPersistenceProvider.Config _sharedState;
    private CheckpointPersistenceProvider _existing  = null;
    private CheckpointPersistenceRuntimeConfigBuilder _runtime;
    private String _runtimeConfigPrefix;
    private boolean _clearBeforeUse;
    private int _version;

    public CheckpointPersistenceStaticConfigBuilder()
    {
      _fileSystem = new FileSystemCheckpointPersistenceProvider.Config();
      _runtime = new CheckpointPersistenceRuntimeConfigBuilder();
      _runtime.setFileSystem(_fileSystem.getRuntime());
      setRuntimeConfigPrefix("databus.checkpointPersistence.");
      _sharedState = new SharedCheckpointPersistenceProvider.Config();
      _clearBeforeUse = false;
      _version = 2; // default is 2
    }
    public int getVersion() {
      return _version;
    }
    public void setVersion(int version) {
      _version = version;
    }

    public String getType()
    {
      return _type;
    }

    public void setType(String type)
    {
      _type = type;
    }

    public FileSystemCheckpointPersistenceProvider.Config getFileSystem()
    {
      return _fileSystem;
    }

    public void setFileSystem(FileSystemCheckpointPersistenceProvider.Config fileSystem)
    {
      _fileSystem = fileSystem;
    }

    public CheckpointPersistenceProvider getExisting()
    {
      return _existing;
    }

    /** Sets a spring-wired existing checkpoint persistence provider. {@link #getType()} needs to be
     * {@link ProviderType}.EXISTING for this property to have an effect. */
    public void setExisting(CheckpointPersistenceProvider existing)
    {
      _existing = existing;
    }

    public String getRuntimeConfigPrefix()
    {
      return _runtimeConfigPrefix;
    }

    public void setRuntimeConfigPrefix(String runtimeConfigPrefix)
    {
      _runtimeConfigPrefix = runtimeConfigPrefix;
      _fileSystem.setRuntimeConfigPrefix(_runtimeConfigPrefix + ".fileSystem");
    }

    public CheckpointPersistenceRuntimeConfigBuilder getRuntime()
    {
      return _runtime;
    }

    public void setRuntime(CheckpointPersistenceRuntimeConfigBuilder runtime)
    {
      _runtime = runtime;
    }

    @Override
    public CheckpointPersistenceStaticConfig build() throws InvalidConfigException
    {
      CheckpointPersistenceStaticConfig.ProviderType providerType = null;

      try
      {
        providerType = CheckpointPersistenceStaticConfig.ProviderType.valueOf(_type.toUpperCase());
      }
      catch (Exception e)
      {
        throw new InvalidConfigException("invalid cp3 type:" + _type, e);
      }

      if (CheckpointPersistenceStaticConfig.ProviderType.EXISTING == providerType &&
          null == getExisting())
      {
        throw new InvalidConfigException("no existing checkpoint persistence provider specified");
      }

      LOG.info("checkpoint persistence type: " + _type);
      LOG.info("clear before use: " + _clearBeforeUse);
      LOG.info("version: " + _version);

      return new CheckpointPersistenceStaticConfig(providerType, getFileSystem().build(),getSharedState().build(),
                                                   getExisting(), getRuntime(),
                                                   getRuntimeConfigPrefix(),
                                                   isClearBeforeUse(),
                                                   getVersion());
    }

    public boolean isClearBeforeUse()
    {
      return _clearBeforeUse;
    }

    public void setClearBeforeUse(boolean clearBeforeUse)
    {
      _clearBeforeUse = clearBeforeUse;
    }

    public SharedCheckpointPersistenceProvider.Config getSharedState()
    {
      return _sharedState;
    }

    public void setSharedState(SharedCheckpointPersistenceProvider.Config sharedState)
    {
      _sharedState = sharedState;
    }
  }

  /**
   *
   * @author pganti
   *
   */
  public class RuntimeConfig implements ConfigApplier<RuntimeConfig>
  {
    private final ServerContainer.RuntimeConfig _container;
    private final CheckpointPersistenceRuntimeConfig _checkpointPersistence;
    private final List<ServerInfo> _relays;
    private final HttpStatisticsCollector.RuntimeConfig _httpStatsCollector;
    private final LoggingConsumer.RuntimeConfig _loggingListener;
    private final BootstrapClientRuntimeConfig _bootstrap;

    public RuntimeConfig(ServerContainer.RuntimeConfig container,
                         CheckpointPersistenceRuntimeConfig checkpoint,
                         List<ServerInfo> relays,
                         HttpStatisticsCollector.RuntimeConfig httpStatsCollector,
                         LoggingConsumer.RuntimeConfig loggingListener,
                         BootstrapClientRuntimeConfig bootstrap)
    {
      super();
      _container = container;
      _checkpointPersistence = checkpoint;
      _relays = relays;
      _httpStatsCollector = httpStatsCollector;
      _loggingListener = loggingListener;
      _bootstrap = bootstrap;
    }

    /** Runtime configuration for the Netty serving container */
    public ServerContainer.RuntimeConfig getContainer()
    {
      return _container;
    }

    /** Runtime configuration for the checkpoint persistence provider */
    public CheckpointPersistenceRuntimeConfig getCheckpointPersistence()
    {
      return _checkpointPersistence;
    }

    /** Server configuration for the available relays */
    public List<ServerInfo> getRelays()
    {
      return _relays;
    }

    public Set<ServerInfo> getRelaysSet(){
      return new HashSet<ServerInfo>(_relays);
    }

    /** Add a relay to set of relays */
    public void addRelay(ServerInfo si)
    {
    	if (! _cmEnabled) {
    		LOG.error("Supported only when Helix Integration is enabled (i.e., V3 client + CM enabled");
    	}
    	LOG.info("Adding relay with name " + si.getName() + " " + si.getAddress().getHostName() + " " + si.getAddress().getPort());
    	_relays.add(si);
    	return;
    }

    /** Remove a relay to set of relays */
    public void removeRelay(ServerInfo si)
    {
    	if (! _cmEnabled) {
    		LOG.error("Supported only when Helix Integration is enabled (i.e., V3 client + CM enabled");
    	}
    	LOG.info("Removing relay with name " + si.getName() + " " + si.getAddress().getHostName() + " " + si.getAddress().getPort());
    	_relays.remove(si);
    	return;
    }

    /** Remove a relay to set of relays */
    public void updateRelaySet(Set<ServerInfo> ssi)
    {
    	if (! _cmEnabled) {
    		LOG.error("Supported only when Helix Integration is enabled (i.e., V3 client + CM enabled");
    	}
    	LOG.info("Updating relay set ");
    	for (ServerInfo si: ssi)
    	{
    		LOG.info(si.getName() + " " + si.getAddress().getHostName() + " " + si.getAddress().getPort());
    	}
		_relays.addAll(ssi);
 		return;
    }

    /** Runtime configuration for bootstrapping */
    public BootstrapClientRuntimeConfig getBootstrap()
    {
      return _bootstrap;
    }

    /** Runtime configuration for statistics collector for the HTTP calls to the relays */
    public HttpStatisticsCollector.RuntimeConfig getHttpStatsCollector()
    {
      return _httpStatsCollector;
    }

    /** Runtime configuration for the logging consumer */
    public LoggingConsumer.RuntimeConfig getLoggingListener()
    {
      return _loggingListener;
    }

    @Override
    public void applyNewConfig(RuntimeConfig oldConfig)
    {
      LOG.info("Applying runtime client config");
      if (null == oldConfig || ! getContainer().equals(oldConfig.getContainer()))
      {
        getContainer().applyNewConfig(null != oldConfig ? oldConfig.getContainer() : null);
      }
      if (getCheckpointPersistence() != null) {
      	if (null == oldConfig || !getCheckpointPersistence().equals(oldConfig.getCheckpointPersistence()))
      	{
        	getCheckpointPersistence().applyNewConfig(
	            null != oldConfig ? oldConfig.getCheckpointPersistence() : null);
      	}
      }
      if (null == oldConfig || !getRelays().equals(oldConfig.getRelays()))
      {
        if (null != oldConfig)
        {
          for (ServerInfo serverInfo: oldConfig.getRelays())
          {
            if (! getRelays().contains(serverInfo))
            {
              doUnregisterRelay(serverInfo);
            }
          }
        }

        for (ServerInfo serverInfo: getRelays())
        {
          if (null == oldConfig || ! oldConfig.getRelays().contains(serverInfo))
          {
            doRegisterRelay(serverInfo);
          }
        }

      }

      if (null == oldConfig || !getBootstrapServices().equals(oldConfig.getBootstrap().getServices()))
      {
        if (null != oldConfig)
        {
          for (ServerInfo serverInfo : oldConfig.getBootstrap().getServices())
          {
            if (! getBootstrapServices().contains(serverInfo))
            {
              doUnregisterBootstrapService(serverInfo);
            }
          }
        }

        for (ServerInfo serverInfo: getBootstrap().getServices())
        {
          if (null == oldConfig || ! oldConfig.getBootstrap().getServices().contains(serverInfo))
          {
            doRegisterBootstrapService(serverInfo);
          }
        }
      }
    }

    @Override
    public boolean equals(Object otherConfig)
    {
      if (null == otherConfig || !(otherConfig instanceof RuntimeConfig)) return false;
      return equalsConfig((RuntimeConfig)otherConfig);
    }

    @Override
    public boolean equalsConfig(RuntimeConfig otherConfig)
    {
      if (null == otherConfig) return false;
      return getContainer().equals(otherConfig.getContainer()) &&
             getCheckpointPersistence().equals(otherConfig.getCheckpointPersistence());
    }

    @Override
    public int hashCode()
    {
      return _container.hashCode() ^ _checkpointPersistence.hashCode();
    }

  }

  /**
   *
   * @author pganti
   *
   */
  public static class RuntimeConfigBuilder implements ConfigBuilder<RuntimeConfig>
  {
    private ServerContainer.RuntimeConfigBuilder _container;
    private CheckpointPersistenceRuntimeConfigBuilder _checkpointPersistence;
    private DatabusHttpClientImpl _managedInstance = null;
    private final Map<String, ServerInfoBuilder> _relays;
    private HttpStatisticsCollector.RuntimeConfigBuilder _httpStatsCollector;
    private LoggingConsumer.RuntimeConfigBuilder _loggingListener;
    private BootstrapClientRuntimeConfigBuilder _bootstrap;
    private String _relaysList = "";

    public RuntimeConfigBuilder()
    {
      _container = new ServerContainer.RuntimeConfigBuilder();
      _checkpointPersistence = new CheckpointPersistenceRuntimeConfigBuilder();
      _relays = new HashMap<String, ServerInfoBuilder>();
      _httpStatsCollector = new HttpStatisticsCollector.RuntimeConfigBuilder();
      _loggingListener = new LoggingConsumer.RuntimeConfigBuilder();
      _bootstrap = new BootstrapClientRuntimeConfigBuilder();
    }

    public ServerContainer.RuntimeConfigBuilder getContainer()
    {
      return _container;
    }

    public void setContainer(ServerContainer.RuntimeConfigBuilder container)
    {
      _container = container;
    }

    public CheckpointPersistenceRuntimeConfigBuilder getCheckpointPersistence()
    {
      return _checkpointPersistence;
    }

    public void setCheckpointPersistence(CheckpointPersistenceRuntimeConfigBuilder checkpointPersistence)
    {
      _checkpointPersistence = checkpointPersistence;
    }

    public DatabusHttpClientImpl getManagedInstance()
    {
      return _managedInstance;
    }

    public void setManagedInstance(DatabusHttpClientImpl managedInstance)
    {
      _managedInstance = managedInstance;
      if (null != _managedInstance)
      {
        _container.setManagedInstance(_managedInstance);
        _checkpointPersistence.setManagedInstance(_managedInstance);
        _httpStatsCollector.setManagedInstance(_managedInstance.getHttpStatsCollector());
        _loggingListener.managedInstance(_managedInstance.getLoggingListener());
        _bootstrap.setManagedInstance(_managedInstance);
      }
    }

    public ServerInfoBuilder getRelay(String id)
    {
      ServerInfoBuilder relay = getRelays().get(id);
      if (null == relay)
      {
        relay = new ServerInfoBuilder();
        setRelay(id, relay);
      }

      return relay;
    }

    public void setRelay(String id, ServerInfoBuilder serverInfo)
    {
    	_relays.put(id, serverInfo);
    }

    public Map<String, ServerInfoBuilder> getRelays()
    {
    	return _relays;
    }

    @Override
    public RuntimeConfig build() throws InvalidConfigException
    {
      if (null == _managedInstance)
      {
        throw new InvalidConfigException("No associated http client for runtime config");
      }

      List<ServerInfo> relays = parseServerInfosMap(getRelays());
      LOG.info("Relays size=" + relays.size());
      if (null != _relaysList && _relaysList.length() > 0) relays = parseServerInfoList(_relaysList, relays);
      LOG.info("Relays size=" + relays.size());

      return _managedInstance.new RuntimeConfig(getContainer().build(),
                                                getCheckpointPersistence().build(),
                                                relays,
                                                getHttpStatsCollector().build(),
                                                getLoggingListener().build(),
                                                getBootstrap().build());
    }

    static List<ServerInfo> parseServerInfoList(String serversList, List<ServerInfo> servers)
            throws InvalidConfigException
    {
      if (null == servers) servers = new ArrayList<ServerInfo>(10);
      ServerInfoSetBuilder builder = new ServerInfoSetBuilder();
      builder.setServers(serversList);
      servers.addAll(builder.build());

      return servers;
    }

    public HttpStatisticsCollector.RuntimeConfigBuilder getHttpStatsCollector()
    {
      return _httpStatsCollector;
    }

    public void setHttpStatsCollector(HttpStatisticsCollector.RuntimeConfigBuilder httpStatsCollector)
    {
      _httpStatsCollector = httpStatsCollector;
    }

    public LoggingConsumer.RuntimeConfigBuilder getLoggingListener()
    {
      return _loggingListener;
    }

    public void setLoggingListener(LoggingConsumer.RuntimeConfigBuilder loggingListener)
    {
      _loggingListener = loggingListener;
    }

    public BootstrapClientRuntimeConfigBuilder getBootstrap()
    {
      return _bootstrap;
    }

    public void setBootstrap(BootstrapClientRuntimeConfigBuilder bootstrap)
    {
      _bootstrap = bootstrap;
    }

    public String getRelaysList()
    {
      return _relaysList;
    }

    public void setRelaysList(String relaysList)
    {
      _relaysList = relaysList;
    }

  }

  /**
   *
   * @author pganti
   *
   */
  public static class StaticConfig
  {
    private final CheckpointPersistenceStaticConfig _checkpointPersistence;
    private final ServerContainer.StaticConfig _container;
    private final RuntimeConfigBuilder _runtime;
    private final String _runtimeConfigPrefix;
    private final DatabusSourcesConnection.StaticConfig _connectionDefaults;
    private final Map<List<String>, DatabusSourcesConnection.StaticConfig> _connections;
    private final HttpStatisticsCollector.StaticConfig _httpStatsCollector;
    private final LoggingConsumer.StaticConfig _loggingListener;
    private final DatabusClientNode.StaticConfig _cluster;
    private final long _dscUpdateIntervalMs;
    private final int _pullerBufferUtilizationPct;
    private final boolean _enableReadLatestOnRelayFallOff;
    private final int _protocolVersion;
    private final boolean _enablePerConnectionStats;
    private final Map<String, ClusterRegistrationStaticConfig> _clientClusters;
    
    public StaticConfig(CheckpointPersistenceStaticConfig checkpointPersistence,
                        ServerContainer.StaticConfig container,
                        RuntimeConfigBuilder runtime,
                        String runtimeConfigPrefix,
                        DatabusSourcesConnection.StaticConfig connectionDefaults,
                        Map<List<String>, DatabusSourcesConnection.StaticConfig> connections,
                        HttpStatisticsCollector.StaticConfig httpStatsCollector,
                        LoggingConsumer.StaticConfig loggingListener,
                        DatabusClientNode.StaticConfig cluster,
                        long dscUpdateIntervalMs,
                        int pullerBufferUtilizationPct,
                        boolean enableReadLatestOnRelayFallOff,
                        int protocolVersion,
                        boolean enablePerConnectionStats,
                        Map<String, ClusterRegistrationStaticConfig> clientClusters)
    {
      super();
      _checkpointPersistence = checkpointPersistence;
      _container = container;
      _runtime = runtime;
      _runtimeConfigPrefix = runtimeConfigPrefix;
      _connections = connections;
      _connectionDefaults = connectionDefaults;
      _httpStatsCollector = httpStatsCollector;
      _loggingListener = loggingListener;
      _cluster = cluster;
      _dscUpdateIntervalMs = dscUpdateIntervalMs;
      _pullerBufferUtilizationPct = pullerBufferUtilizationPct;
      _enableReadLatestOnRelayFallOff = enableReadLatestOnRelayFallOff;
      _protocolVersion = protocolVersion;
      _enablePerConnectionStats = enablePerConnectionStats;
      _clientClusters = clientClusters;
    }

    public long getDSCUpdateIntervalMs()
    {
      return _dscUpdateIntervalMs;
    }

    /** The checkpoint persistent provider static configuration */
    public CheckpointPersistenceStaticConfig getCheckpointPersistence()
    {
      return _checkpointPersistence;
    }

    /** The netty container static configuration */
    public ServerContainer.StaticConfig getContainer()
    {
      return _container;
    }

    /** The client runtime configuration */
    public RuntimeConfigBuilder getRuntime()
    {
      return _runtime;
    }

    public DatabusClientNode.StaticConfig getCluster()
    {
      return _cluster;
    }

    public String getRuntimeConfigPrefix()
    {
      return _runtimeConfigPrefix;
    }

    /** All Databus source connections static configurations */
    public Map<List<String>, DatabusSourcesConnection.StaticConfig> getConnections()
    {
      return _connections;
    }

    /** The Databus source connections static configuration */
    public DatabusSourcesConnection.StaticConfig getConnection(List<String> id)
    {
      return _connections.get(id);
    }

    /** The client cluster configuration */
    public ClusterRegistrationStaticConfig getClientCluster(String clusterName) {
      return _clientClusters.get(clusterName);
    }

    /** The default static configuration for all Databus source connections */
    public DatabusSourcesConnection.StaticConfig getConnectionDefaults()
    {
      return _connectionDefaults;
    }

    /** The static configuration for statistics collector for all HTTP calls to the relays */
    public HttpStatisticsCollector.StaticConfig getHttpStatsCollector()
    {
      return _httpStatsCollector;
    }

    /** Static configuration for the logging consumer */
    public LoggingConsumer.StaticConfig getLoggingListener()
    {
      return _loggingListener;
    }

    public int getPullerBufferUtilizationPct()
    {
      return _pullerBufferUtilizationPct;
    }

	public boolean isReadLatestScnOnErrorEnabled()
    {
	  return _enableReadLatestOnRelayFallOff;
    }

	public int getProtocolVersion() {
	  return _protocolVersion;
	}


	@Override
	public String toString() {
		return "StaticConfig [_checkpointPersistence=" + _checkpointPersistence
				+ ", _container=" + _container + ", _runtime=" + _runtime
				+ ", _runtimeConfigPrefix=" + _runtimeConfigPrefix
				+ ", _connectionDefaults=" + _connectionDefaults
				+ ", _connections=" + _connections + ", _httpStatsCollector="
				+ _httpStatsCollector + ", _loggingListener="
				+ _loggingListener + ", _cluster=" + _cluster
				+ ", _dscUpdateIntervalMs=" + _dscUpdateIntervalMs
				+ ", _pullerBufferUtilizationPct="
				+ _pullerBufferUtilizationPct
				+ ", _enableReadLatestOnRelayFallOff="
				+ _enableReadLatestOnRelayFallOff
				+ ",protocolVersion = "
				+ _protocolVersion + "]";
	}

  public boolean isEnablePerConnectionStats()
  {
    return _enablePerConnectionStats;
  }
  }

  /**
   *
   * @author pganti
   *
   */
  public static class StaticConfigBuilderBase
  {
	  protected CheckpointPersistenceStaticConfigBuilder _checkpointPersistence;
	  protected ServerContainer.Config _container;
	  protected RuntimeConfigBuilder _runtime;
	  protected String _runtimeConfigPrefix = "databus.client.";
	  protected DatabusSourcesConnection.Config _connectionDefaults;
	  protected Map<String, DatabusSourcesConnection.Config> _connections;
	  protected HttpStatisticsCollector.Config _httpStatsCollector;
	  protected LoggingConsumer.Config _loggingListener;
	  protected DatabusClientNode.Config _cluster;
	  protected long _dscUpdateIntervalMs;
	  protected int _pullerBufferUtilizationPct;
	  protected int _protocolVersion;
	  protected boolean _enablePerConnectionStats = true;
	  protected Map<String, ClusterRegistrationConfig> _clientClusters;
	  
	  // Flag to do streamFromLastScn on SCNNotFoundException
	  protected boolean _enableReadLatestOnRelayFallOff;

	  protected HashMap<List<String>, DatabusSourcesConnection.StaticConfig> _connConfigs;

	  public StaticConfigBuilderBase() {
		  _dscUpdateIntervalMs = 5*1000;
		  _pullerBufferUtilizationPct = 85;
		  // Flag to do streamFromLastScn on SCNNotFoundException
		  _enableReadLatestOnRelayFallOff = false;
		  _runtime = new RuntimeConfigBuilder();
		  setCheckpointPersistence(new CheckpointPersistenceStaticConfigBuilder());

		  //make sure the default client settings do not conflict with relay's settings
		  ServerContainer.Config containerCfg = new ServerContainer.Config();
		  containerCfg.setHttpPort(containerCfg.getHttpPort() + 1);
		  containerCfg.getJmx().setJmxServicePort(containerCfg.getJmx().getJmxServicePort() + 1);

		  setContainer(containerCfg);
		  _connectionDefaults = new DatabusSourcesConnection.Config();
		  _connections = new HashMap<String, DatabusSourcesConnection.Config>(5);
		  _clientClusters = new HashMap<String, ClusterRegistrationConfig>();
		  _httpStatsCollector = new HttpStatisticsCollector.Config();
		  _loggingListener = new LoggingConsumer.Config();
		  _cluster= new DatabusClientNode.Config();
		  _protocolVersion = 2;
	  }

	  protected void verifyConfig() throws InvalidConfigException
	  {
		  if (_pullerBufferUtilizationPct <= 0 || _pullerBufferUtilizationPct > 100)
		  {
			  throw new InvalidConfigException("invalid puller buffer utilization percentage:" +
					  _pullerBufferUtilizationPct);
		  }
	  }

	  public CheckpointPersistenceStaticConfigBuilder getCheckpointPersistence()
	  {
		  return _checkpointPersistence;
	  }

	  public void setCheckpointPersistence(CheckpointPersistenceStaticConfigBuilder checkpointPersistence)
	  {
		  _checkpointPersistence = checkpointPersistence;
		  _checkpointPersistence.setRuntimeConfigPrefix(_runtimeConfigPrefix + "checkpoint.");
		  _runtime.setCheckpointPersistence(checkpointPersistence.getRuntime());
	  }

	  public ServerContainer.Config getContainer()
	  {
		  return _container;
	  }

	  public void setContainer(ServerContainer.Config container)
	  {
		  _container = container;
		  _container.setRuntimeConfigPropertyPrefix(_runtimeConfigPrefix + "runtime.");
		  _runtime.setContainer(container.getRuntime());
	  }

	  public RuntimeConfigBuilder getRuntime()
	  {
		  return _runtime;
	  }

	  public void setRuntime(RuntimeConfigBuilder runtime)
	  {
		  _runtime = runtime;
	  }

	  public String getRuntimeConfigPrefix()
	  {
		  return _runtimeConfigPrefix;
	  }

	  public void setRuntimeConfigPrefix(String runtimeConfigPrefix)
	  {
		  _runtimeConfigPrefix = runtimeConfigPrefix;
		  _container.setRuntimeConfigPropertyPrefix(_runtimeConfigPrefix + "runtime.");
		  _checkpointPersistence.setRuntimeConfigPrefix(_runtimeConfigPrefix + "checkpoint.");
	  }

	  public int getPullerBufferUtilizationPct()
	  {
		  return _pullerBufferUtilizationPct;
	  }

	  public void setPullerBufferUtilizationPct(int pullerBufferUtilizationPct)
	  {
		  _pullerBufferUtilizationPct = pullerBufferUtilizationPct;
	  }

	  public boolean isEnableReadLatestOnRelayFallOff()
	  {
		  return _enableReadLatestOnRelayFallOff;
	  }


	  public void setEnableReadLatestOnRelayFallOff(boolean enableReadLatestOnRelayFallOff)
	  {
		  this._enableReadLatestOnRelayFallOff = enableReadLatestOnRelayFallOff;
	  }

	  public DatabusSourcesConnection.Config getConnection(String id)
	  {
		  DatabusSourcesConnection.Config conn = _connections.get(id);
		  if (null == conn)
		  {
			  conn = new DatabusSourcesConnection.Config(_connectionDefaults);
			  _connections.put(id, conn);
		  }

		  return conn;
	  }

	  public void setConnection(String id, DatabusSourcesConnection.Config conn)
	  {
		  _connections.put(id, conn);
	  }

	  public ClusterRegistrationConfig getClientCluster(String id)
	  {
		  ClusterRegistrationConfig cluster = _clientClusters.get(id);
		  if (null == cluster)
		  {
			  cluster = new ClusterRegistrationConfig();
			  _clientClusters.put(id, cluster);
		  }

		  return cluster;
	  }
	  
	  public void setClientCluster(String id, ClusterRegistrationConfig cluster)
	  {
		  _clientClusters.put(id, cluster);
	  }
	  
	  public HttpStatisticsCollector.Config getHttpStatsCollector()
	  {
		  return _httpStatsCollector;
	  }

	  public void setHttpStatsCollector(HttpStatisticsCollector.Config httpStatsCollector)
	  {
		  _httpStatsCollector = httpStatsCollector;
	  }

	  public LoggingConsumer.Config getLoggingListener()
	  {
		  return _loggingListener;
	  }

	  public void setLoggingListener(LoggingConsumer.Config loggingListener)
	  {
		  _loggingListener = loggingListener;
	  }

	  public long getDscUpdateIntervalMs()
	  {
		  return _dscUpdateIntervalMs;
	  }

	  public void setDscUpdateIntervalMs(long dscUpdateIntervaMs)
	  {
		  _dscUpdateIntervalMs = dscUpdateIntervaMs;
	  }


	  public DatabusClientNode.Config getCluster()
	  {
		  return _cluster;
	  }

	  public void setCluster(DatabusClientNode.Config cluster)
	  {
		  _cluster = cluster;
	  }
	  public DatabusSourcesConnection.Config getConnectionDefaults()
	  {
		  return _connectionDefaults;
	  }

	  public void setConnectionDefaults(DatabusSourcesConnection.Config connectionDefaults)
	  {
		  _connectionDefaults = connectionDefaults;
	  }

    public boolean isEnablePerConnectionStats()
    {
      return _enablePerConnectionStats;
    }

    public void setEnablePerConnectionStats(boolean enablePerConnectionStats)
    {
      _enablePerConnectionStats = enablePerConnectionStats;
    }

  }

  /**
   *
   * @author pganti
   *
   */
  public static class Config extends StaticConfigBuilderBase
  implements ConfigBuilder<StaticConfig>
  {



	public Config()
    {
    	super();
    }

    @Override
    public StaticConfig build() throws InvalidConfigException
    {

      verifyConfig();
      //the default connection configs inherit from clientConfigs; primarily for backward compatibility
      getConnectionDefaults().setConsumeCurrent(!getRuntime().getBootstrap().isEnabled());
      getConnectionDefaults().setPullerBufferUtilizationPct(_pullerBufferUtilizationPct);
      getConnectionDefaults().setReadLatestScnOnError(_enableReadLatestOnRelayFallOff);
      ServerContainer.StaticConfig sconfig = getContainer().build();
      getConnectionDefaults().setId(sconfig.getId());

      //per connection configs: retain explicit settings or defaults of SourcesConnection
      _connConfigs =  new HashMap<List<String>, DatabusSourcesConnection.StaticConfig>((int)(_connections.size() * 1.3));
	  for (String connKey: _connections.keySet() )
	  {
		  String[] keySources = connKey.split("[,]");
		  for (int i = 0; i < keySources.length; ++i) keySources[i] = keySources[i].trim();
		  DatabusSourcesConnection.Config confBuilder = _connections.get(connKey);
		  confBuilder.setId(sconfig.getId());
		  _connConfigs.put(Arrays.asList(keySources), confBuilder.build());
	  }
	  
	  Map<String, ClusterRegistrationStaticConfig> clientClusterStaticConfigs = new HashMap<String, ClusterRegistrationStaticConfig>();
	           
	  for (Entry<String, ClusterRegistrationConfig> e : _clientClusters.entrySet())
	  {
		  String clusterName = e.getValue().getClusterName();

		  if (clientClusterStaticConfigs.containsKey(clusterName))
			  throw new InvalidConfigException("Duplicate configuration for client cluster :" + clusterName);

		  ClusterRegistrationStaticConfig c = e.getValue().build();

		  clientClusterStaticConfigs.put(clusterName, c);
	  }

      return new StaticConfig(getCheckpointPersistence().build(), sconfig,
                              getRuntime(), getRuntimeConfigPrefix(),
                              getConnectionDefaults().build(),
                              _connConfigs,
                              getHttpStatsCollector().build(),
                              getLoggingListener().build(),
                              getCluster().build(),
                              _dscUpdateIntervalMs,
                              _pullerBufferUtilizationPct,
                              _enableReadLatestOnRelayFallOff,
                              _protocolVersion,
                              _enablePerConnectionStats,
                              clientClusterStaticConfigs);
    }

  }

  /**
   * TODO: (DDSDBUS-93) the design needs to be re-visit - when there are multiple connections,
   * what status shall we return on behaver of the client? For now, we just return
   * the first one.
   *
   * @author pganti
   *
   */
  protected class DatabusHttpClientStatus extends DatabusComponentStatus
  {
    public DatabusHttpClientStatus()
    {
      super("DatabusHttpClientImpl");
    }

    @Override
    public Status getStatus()
    {
       if ( !_relayConnections.isEmpty())       
    	   return _relayConnections.get(0).getConnectionStatus().getStatus();
       else if ( ! _regList.isEmpty())
    	   return _regList.get(0).getStatus().getStatus();
    
       return null;
    }

    @Override
    public String getMessage()
    {
    	if ( !_relayConnections.isEmpty())       
    		return _relayConnections.get(0).getConnectionStatus().getMessage();
    	else if ( ! _regList.isEmpty())
    		return _regList.get(0).getStatus().getMessage();
    	
    	return null;
    }

    @Override
    public void pause()
    {
      super.pause();

      for (DatabusSourcesConnection relayConn: _relayConnections)
      {
        relayConn.getConnectionStatus().pause();
      }
      
      for(DatabusRegistration reg : _regList)
      {
    	  LOG.info("Pausing registration :" + reg.getRegistrationId());
    	  try
    	  {
    		  if (reg.getState().isRunning())
    			  reg.pause();
    	  } catch (Exception ex) {
    		  LOG.error("Unable to pause registration :" + reg.getRegistrationId(),ex);
    	  }
      }
    }

    @Override
    public void resume()
    {
      for (DatabusSourcesConnection relayConn: _relayConnections)
      {
        relayConn.getConnectionStatus().resume();
      }
      
      for(DatabusRegistration reg : _regList)
      {
    	  LOG.info("Resuming registration :" + reg.getRegistrationId());
    	  try
    	  {
    		  if (reg.getState() == RegistrationState.PAUSED)
    			  reg.resume();
    	  } catch (Exception ex) {
    		  LOG.error("Unable to resume registration :" + reg.getRegistrationId(),ex);
    	  }
      }

      super.resume();
    }

    @Override
    public void suspendOnError(Throwable cause)
    {
      super.suspendOnError(cause);

      for (DatabusSourcesConnection relayConn: _relayConnections)
      {
        relayConn.getConnectionStatus().suspendOnError(cause);
      }
      
      for(DatabusRegistration reg : _regList)
      {
    	  LOG.info("Suspending registration :" + reg.getRegistrationId());
    	  try
    	  {
    		  if (reg.getState().isRunning())
    			  reg.suspendOnError(cause);
    	  } catch (Exception ex) {
    		  LOG.error("Unable to suspend registration :" + reg.getRegistrationId(),ex);
    	  }
      }
    }

    @Override
    public void shutdown()
    {
      for (DatabusSourcesConnection relayConn: _relayConnections)
      {
        relayConn.getConnectionStatus().shutdown();
      }

      for(DatabusRegistration reg : _regList)
      {
    	  LOG.info("Shutting down registration :" + reg.getRegistrationId());
    	  try
    	  {
    		  if (reg.getState() != RegistrationState.SHUTDOWN)
    			  reg.shutdown();
    	  } catch (Exception ex) {
    		  LOG.error("Unable to shutdown registration :" + reg.getRegistrationId(),ex);
    	  }
      }

      super.shutdown();
    }

    public Map<List<DatabusSubscription>,  Set<ServerInfo>> getRelayGroups()
    {
    	return _relayGroups;
    }
  }

  public DatabusV3Registration getRegistration(RegistrationId rid)
  {
    throw new UnsupportedOperationException();
  }
}

