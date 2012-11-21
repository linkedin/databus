package com.linkedin.databus3.espresso.client;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.linkedin.databus.client.DatabusHttpClientImpl;
import com.linkedin.databus.client.DatabusSourcesConnection;
import com.linkedin.databus.client.GenericDispatcher;
import com.linkedin.databus.client.RelayPullThread;
import com.linkedin.databus.client.SCNUtils;
import com.linkedin.databus.client.ServerSetChangeMessage;
import com.linkedin.databus.client.SingleSourceSCN;
import com.linkedin.databus.client.consumer.ConsumerRegistration;
import com.linkedin.databus.client.consumer.DatabusV2ConsumerRegistration;
import com.linkedin.databus.client.consumer.LoggingConsumer;
import com.linkedin.databus.client.pub.CheckpointPersistenceProvider;
import com.linkedin.databus.client.pub.DatabusClientException;
import com.linkedin.databus.client.pub.DatabusClientNode;
import com.linkedin.databus.client.pub.DatabusCombinedConsumer;
import com.linkedin.databus.client.pub.DatabusServerCoordinates;
import com.linkedin.databus.client.pub.DatabusV3Client;
import com.linkedin.databus.client.pub.DatabusV3Consumer;
import com.linkedin.databus.client.pub.DatabusV3Registration;
import com.linkedin.databus.client.pub.DeregisterResult;
import com.linkedin.databus.client.pub.FetchMaxSCNRequest;
import com.linkedin.databus.client.pub.RegistrationId;
import com.linkedin.databus.client.pub.RelayFindMaxSCNResult;
import com.linkedin.databus.client.pub.RelayFlushMaxSCNResult;
import com.linkedin.databus.client.pub.RelayFlushMaxSCNResult.SummaryCode;
import com.linkedin.databus.client.pub.SCN;
import com.linkedin.databus.client.pub.ServerInfo;
import com.linkedin.databus.client.pub.mbean.ConsumerCallbackStats;
import com.linkedin.databus.client.request.ClientRequestProcessor;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DatabusComponentStatus;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.cmclient.ResourceKey;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus.core.data_model.Role;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.monitoring.mbean.HttpStatisticsCollector;
import com.linkedin.databus2.core.container.netty.ServerContainer;
import com.linkedin.databus2.core.container.request.BinaryProtocol;
import com.linkedin.databus2.core.filter.DbusKeyCompositeFilterConfig;
import com.linkedin.databus3.espresso.client.cmclient.DatabusExternalViewChangeObserver;
import com.linkedin.databus3.espresso.client.cmclient.RelayClusterInfoProvider;
import com.linkedin.databus3.espresso.client.consumer.PartitionMultiplexingConsumer;
import com.linkedin.databus3.espresso.client.consumer.PartitionMultiplexingConsumerRegistration;
import com.linkedin.databus3.espresso.client.data_model.EspressoSubscriptionUriCodec;
import com.linkedin.databus3.espresso.client.request.RegistrationsRequestProcessor;
import com.linkedin.databus3.espresso.cmclient.RelayClusterManagerStaticConfig;
import com.linkedin.databus3.espresso.cmclient.RelayClusterManagerStaticConfigBuilder;

/**
 *
 * @author pganti
 *
 */
public class DatabusHttpV3ClientImpl
    extends DatabusHttpClientImpl
    implements DatabusV3Client, DatabusExternalViewChangeObserver
{

  static
  {
    //make sure the espresso URI codec is loaded
    EspressoSubscriptionUriCodec.getInstance();
  }

  private final Map<RegistrationId, DatabusV3Registration> _registrationIdMap = new ConcurrentHashMap<RegistrationId, DatabusV3Registration>();

  private final Map<String, List<RegistrationId>> _dbToRegistrationsMap = new ConcurrentHashMap<String, List<RegistrationId>>();

  private final Map<RegistrationId, DatabusSourcesConnection> _registrationToSourcesConnectionMap
                                           = new ConcurrentHashMap<RegistrationId, DatabusSourcesConnection>();

  private final AtomicInteger flushRegIdGen = new AtomicInteger();

  private List<String> _dbList;

  // Thread-safe as newSetFromMap preserves concurrency from the backing map.
  private final Set<RegistrationId> loggingRegIdSet = Collections.newSetFromMap(new ConcurrentHashMap<RegistrationId,Boolean>());

  private RelayClusterInfoProvider _relayClusterInfoProvider;

  private static final int PROTOCOL_VERSION = 3;

  public static class StaticConfig extends DatabusHttpClientImpl.StaticConfig
  {
      private final RelayClusterManagerStaticConfig _clusterManagerStaticConfig;

      private final String _dbNames;

      private final String _subscriptions;

      private final boolean _enableRegUpdateRESTApi;
      private final boolean _ignoreSCNHolesCheck;
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
              RelayClusterManagerStaticConfig clusterManagerStaticConfig,
              String dbNames,
              String subscriptions,
              int protocolVersion,
              boolean enableRegUpdateRESTApi,
              boolean enablePerConnectionStats,
              boolean ignoreSCNHolesCheck)
      {
          super(checkpointPersistence, container, runtime, runtimeConfigPrefix, connectionDefaults,
                connections, httpStatsCollector,
                loggingListener, cluster, dscUpdateIntervalMs, pullerBufferUtilizationPct,
                enableReadLatestOnRelayFallOff, protocolVersion,
                enablePerConnectionStats);

          _clusterManagerStaticConfig = clusterManagerStaticConfig;
          _dbNames = dbNames;
          _subscriptions = subscriptions;
          _enableRegUpdateRESTApi = enableRegUpdateRESTApi;
          _ignoreSCNHolesCheck = ignoreSCNHolesCheck;
      }

      /**
       * The configuration for the espresso schema registry
       *
       * @return
       */
      public RelayClusterManagerStaticConfig getClusterManager()
      {
          return _clusterManagerStaticConfig;
      }
      public String getDbNames()
      {
        return _dbNames;
      }
      public String getSubscriptions()
      {
        return _subscriptions;
      }

      public boolean getIgnoreSCNHolesCheck() {
		return _ignoreSCNHolesCheck;
	}

	/**
       * The configuration for the RegistrationUpdate REST service
       *
       * @return
       */
      public boolean isRegUpdateRESTApiEnabled()
      {
        return _enableRegUpdateRESTApi;
      }
  }

  public static class BaseConfigBuilder extends DatabusHttpClientImpl.Config
  {
    protected RelayClusterManagerStaticConfigBuilder _clusterManagerStaticConfigBuilder;
    protected String _dbNames;
    protected String _subscriptions;
    protected boolean _enableRegUpdateRESTApi = false; // enable Registration Update REST API
    protected  boolean _ignoreSCNHolesCheck = false;

    public BaseConfigBuilder()
    {
      super();
      _clusterManagerStaticConfigBuilder = new RelayClusterManagerStaticConfigBuilder();
      _dbNames = "EspressoDB";
      //disable per-connection stats for Espresso because it may generate too many of them
      _enablePerConnectionStats = false;
    }

    public RelayClusterManagerStaticConfigBuilder getClusterManager()
    {
      return _clusterManagerStaticConfigBuilder;
    }
    public void setClusterManager(RelayClusterManagerStaticConfigBuilder rcmsc)
    {
        _clusterManagerStaticConfigBuilder = rcmsc;
    }

    public boolean getIgnoreSCNHolesCheck() {
		return _ignoreSCNHolesCheck;
	}

	public void setIgnoreSCNHolesCheck(boolean _ignoreSCNHolesCheck) {
		this._ignoreSCNHolesCheck = _ignoreSCNHolesCheck;
	}

	public String getDbNames()
    {
      return _dbNames;
    }
    public void setDbNames(String dbNames)
    {
      _dbNames = dbNames;
    }

    public String getSubscriptions()
    {
      return _subscriptions;
    }
    public void setSubscriptions(String subscriptions)
    {
      _subscriptions = subscriptions;
    }

    public boolean getEnableRegUpdateRESTApi()
    {
      return _enableRegUpdateRESTApi;
    }

    public void setEnableRegUpdateRESTApi(boolean _enableRegUpdateRESTApi)
    {
      this._enableRegUpdateRESTApi = _enableRegUpdateRESTApi;
    }
  }

  @Deprecated
  public static class Config extends BaseConfigBuilder
  {
    @Override
    public StaticConfig build() throws InvalidConfigException
    {
      verifyConfig();

      /**
       * Build a version 3 checkpoint persistence builder
       */
      CheckpointPersistenceStaticConfigBuilder cpbb = getCheckpointPersistence();
      cpbb.setVersion(PROTOCOL_VERSION);

      getConnectionDefaults().setConsumeCurrent(!getRuntime().getBootstrap().isEnabled());
      getConnectionDefaults().setPullerBufferUtilizationPct(_pullerBufferUtilizationPct);
      getConnectionDefaults().setReadLatestScnOnError(_enableReadLatestOnRelayFallOff);
      ServerContainer.StaticConfig sconfig = getContainer().build();
      getConnectionDefaults().setId(sconfig.getId());
      _connConfigs =  new HashMap<List<String>, DatabusSourcesConnection.StaticConfig>((int)(_connections.size() * 1.3));

      return new StaticConfig(cpbb.build(),sconfig,
          getRuntime(), getRuntimeConfigPrefix(),
          getConnectionDefaults().build(),
          _connConfigs,
          getHttpStatsCollector().build(),
          getLoggingListener().build(),
          getCluster().build(),
          _dscUpdateIntervalMs,
          _pullerBufferUtilizationPct,
          _enableReadLatestOnRelayFallOff,
          _clusterManagerStaticConfigBuilder.build(),
          getDbNames(),
          getSubscriptions(),
          PROTOCOL_VERSION,
          _enableRegUpdateRESTApi,
          _enablePerConnectionStats,
          _ignoreSCNHolesCheck
      );
    }
  }

  public static class StaticConfigBuilder extends StaticConfigBuilderBase implements
  ConfigBuilder<StaticConfig>
  {
      private final RelayClusterManagerStaticConfigBuilder _clusterManagerStaticConfigBuilder;

      private String _dbNames;

      private String _subscriptions;
      protected  boolean _ignoreSCNHolesCheck = false;
      private boolean _enableRegUpdateRESTApi = false; // enable Registration Update REST API


      public StaticConfigBuilder() throws IOException
      {
        super();
        _clusterManagerStaticConfigBuilder = new RelayClusterManagerStaticConfigBuilder();
	_enablePerConnectionStats = false; //disable per-connection stats for client by default
      }

      public RelayClusterManagerStaticConfigBuilder getClusterManager()
      {
        return _clusterManagerStaticConfigBuilder;
      }

      public String getDbNames()
      {
        return _dbNames;
      }

      public void setDbNames(String dbNames)
      {
        _dbNames = dbNames;
      }
      public boolean getIgnoreSCNHolesCheck() {
  		return _ignoreSCNHolesCheck;
  	}

  	public void setIgnoreSCNHolesCheck(boolean _ignoreSCNHolesCheck) {
  		this._ignoreSCNHolesCheck = _ignoreSCNHolesCheck;
  	}

      public String getSubscriptions()
      {
        return _subscriptions;
      }

      public void setSubscriptions(String subscriptions)
      {
        _subscriptions = subscriptions;
      }

      public boolean getEnableRegUpdateRESTApi() {
		return _enableRegUpdateRESTApi;
      }

      public void setEnableRegUpdateRESTApi(boolean _enableRegUpdateRESTApi) {
		this._enableRegUpdateRESTApi = _enableRegUpdateRESTApi;
      }

      @Override
      public StaticConfig build() throws InvalidConfigException
      {
          verifyConfig();
          CheckpointPersistenceStaticConfigBuilder staticConfigBld = getCheckpointPersistence();
          staticConfigBld.setVersion(PROTOCOL_VERSION);
          getConnectionDefaults().setConsumeCurrent(!getRuntime().getBootstrap().isEnabled());
          getConnectionDefaults().setPullerBufferUtilizationPct(_pullerBufferUtilizationPct);
          getConnectionDefaults().setReadLatestScnOnError(_enableReadLatestOnRelayFallOff);
          ServerContainer.StaticConfig sconfig = getContainer().build();
          getConnectionDefaults().setId(sconfig.getId());
          _connConfigs =  new HashMap<List<String>, DatabusSourcesConnection.StaticConfig>((int)(_connections.size() * 1.3));

          return new StaticConfig(staticConfigBld.build(), getContainer().build(),
                                  getRuntime(), getRuntimeConfigPrefix(),
                                  getConnectionDefaults().build(),
                                  _connConfigs,
                                  getHttpStatsCollector().build(),
                                  getLoggingListener().build(),
                                  getCluster().build(),
                                  _dscUpdateIntervalMs,
                                  _pullerBufferUtilizationPct,
                                  _enableReadLatestOnRelayFallOff,
                                  _clusterManagerStaticConfigBuilder.build(),
                                  _dbNames,
                                  _subscriptions,
                                  PROTOCOL_VERSION,
                                  _enableRegUpdateRESTApi,
                                  _enablePerConnectionStats,
                                  _ignoreSCNHolesCheck
                                  );
      }
  }

  public DatabusHttpV3ClientImpl() throws InvalidConfigException,
      IOException,
      DatabusException
  {
    this(new StaticConfigBuilder());
  }

  public DatabusHttpV3ClientImpl(StaticConfigBuilder config) throws InvalidConfigException,
      IOException,
      DatabusException
  {
    this(config.build());
  }

  public DatabusHttpV3ClientImpl(Config config) throws InvalidConfigException,
      IOException,
      DatabusException
  {
    this(config.build());
  }

  public DatabusHttpV3ClientImpl(StaticConfig config) throws IOException,
      InvalidConfigException,
      DatabusException
  {
    super(config);
    DbusEvent.byteOrder = BinaryProtocol.BYTE_ORDER;

    if ( config.getClusterManager().getEnabled())
    {
      _cmEnabled = true;

      LOG.info("Constructing RelayClusterInfoProvider ");
      _relayClusterInfoProvider = new RelayClusterInfoProvider(config.getClusterManager());
      _relayClusterInfoProvider.registerExternalViewChangeObservers(this);
    } else {
      _dbList =  Collections.synchronizedList(new ArrayList<String>());
    }
    initializeV3ClientCommandProcessors(config);
  }

  /** For testing only */
  DatabusHttpV3ClientImpl(StaticConfig config, RelayClusterInfoProvider rlyClusterInfoProvider)
         throws InvalidConfigException, IOException, DatabusException
  {
    super(config);
    if ( config.getClusterManager().getEnabled())
    {
      assert null != rlyClusterInfoProvider;

      _cmEnabled = true;

      LOG.info("using relayClusterProvider " + rlyClusterInfoProvider);
      _relayClusterInfoProvider = rlyClusterInfoProvider;
      _relayClusterInfoProvider.registerExternalViewChangeObservers(this);
    } else {
      _dbList =  Collections.synchronizedList(new ArrayList<String>());
    }
    initializeV3ClientCommandProcessors(config);
  }

  @Override
  @Deprecated
  public void start()
  {
    throw new UnsupportedOperationException("Start method not supported in V3 client");
  }

  protected void startV3()
  {
    super.start();
  }

  @Override
  public Map<String, String> printClientInfo()
  throws InvalidConfigException
  {
    Map<String, String> infoMap = new HashMap<String, String>();
    if (!(_clientStaticConfig instanceof StaticConfig))
    {
      throw new InvalidConfigException("Invalid configuration");
    }
    StaticConfig clientV3StaticConfig = (StaticConfig)_clientStaticConfig;
    if (clientV3StaticConfig.getClusterManager().getEnabled())
    {
      infoMap.put("clusterManager.relayZkConnectString", clientV3StaticConfig.getClusterManager().getRelayZkConnectString());
      infoMap.put("clusterManager.instanceName", clientV3StaticConfig.getClusterManager().getInstanceName());
    }
    else
    {
      infoMap.put("clusterManager.relayZkConnectString", null);
      infoMap.put("clusterManager.instanceName", null);
    }

    return infoMap;
  }

  /**
  *
  * @throws DatabusException
  */
  protected void initializeV3ClientCommandProcessors(StaticConfig config) throws DatabusException
  {
	  _processorRegistry.register(RegistrationsRequestProcessor.COMMAND_NAME,
                              new RegistrationsRequestProcessor(null, this, config.isRegUpdateRESTApiEnabled()));
    _processorRegistry.register(ClientRequestProcessor.COMMAND_NAME, new ClientRequestProcessor(null, this));
  }

  /**
   * Subscribes a group of consumers to the on-line update stream for the specified subscriptions. The onEvent
   * callbacks will be spread and run in parallel across these consumers. Typically the second (rid) and third
   * (filterConfig) parameters are null.
   * @param listeners
   */
  @Override
  public  DatabusV3Registration registerDatabusListener(DatabusV3Consumer[] listeners,
                            RegistrationId rid,
                            DbusKeyCompositeFilterConfig filterConfig,
                            DatabusSubscription ... subs)
  throws DatabusClientException
  {
    /*
     * TODO: DDSDBIS-515 TODO: Client library has to support subscriptions belonging to the same registration for different physical partitions.
     */

    if (listeners == null || listeners.length == 0 || subs == null)
    {
      throw new DatabusClientException("Client library expects atleast one consumer and atleast one subscription for a successful registration");
    }
    List<DatabusSubscription> subsSources = Arrays.asList(subs);
    RegistrationId registrationId = validateRegistration( rid, listeners[0].getClass().getSimpleName(), subsSources);
    DatabusV3Registration consumerReg = registerV3DatabusListener(listeners, registrationId, subsSources, filterConfig,
                                    _checkpointPersistenceProvider, _clientStatus);
    return consumerReg;
  }

  /**
   * Subscribes a single listener (consumer) to the stream for the specified subscriptions. It is suggested to create a consumer by inheriting from AbstractDatabusV3Consumer.
   * Appropriate callbacks may be overridden as necessary
   * If a registrationId is specified, it will be used to identify this registration. If not, a registrationId will be created.
   * The third parameter should currently be specified as null. In future, this parameter will be used to provide server side filtering feature
   * The subscription is specified as a URI, for e.g., DatabusSubscription.createFromUri("espresso://[MASTER|SLAVE|ANY]/EspressoDBName/PartitionNumber/TableName")
   */
  @Override
  public DatabusV3Registration registerDatabusListener(DatabusV3Consumer listener,
                                  RegistrationId rid,
                                  DbusKeyCompositeFilterConfig filterConfig,
                                  DatabusSubscription ... subs)
  throws DatabusClientException
  {
    if (listener == null)
    {
      throw new DatabusClientException("Client library expects atleast one consumer and atleast one subscription for a successful registration");
    }
    DatabusV3Consumer[] listeners = new DatabusV3Consumer[1];
    listeners[0] = listener;

    return registerDatabusListener(listeners, rid, filterConfig, subs);
  }

  /**
   * Returns a registration object given a RegistrationId
   */
  @Override
  public DatabusV3Registration getRegistration(RegistrationId rid)
  {
    if (_registrationIdMap.containsKey(rid))
    {
      return _registrationIdMap.get(rid);
    }
    else
    {
      return null;
    }
  }

  protected synchronized DatabusV3Registration registerV3DatabusListener(DatabusV3Consumer[] listeners,
                                  RegistrationId rid,
                                List<DatabusSubscription> subsSources,
                                DbusKeyCompositeFilterConfig filterConfig,
                                CheckpointPersistenceProvider checkpointPersistenceProvider,
                                DatabusComponentStatus clientStatus)
            throws DatabusClientException
  {
    DatabusV3ConsumerRegistration consumerReg = null;

    subsSources = expandPPWildcards(subsSources);

    Set<PhysicalPartition> pps = getPhysicalPartitionGivenSubscriptionList(subsSources);
    if (0 == pps.size()) throw new DatabusClientException("At least one subscription expected");
    String dbName = pps.iterator().next().getName();

    boolean isMultiPartitionConsumer = 1 < pps.size();
    if (!isMultiPartitionConsumer)
    {
      consumerReg =
          new DatabusV3ConsumerRegistration(this, dbName, listeners, rid, subsSources, filterConfig,
                                            checkpointPersistenceProvider, clientStatus);
    }
    else if (listeners.length > 1)
    {
      throw new RuntimeException("group registrations for multi-partition subscriptions are not supported yet.");
    }
    else {
      PartitionMultiplexingConsumer multiCons =
          new PartitionMultiplexingConsumer(rid, listeners[0], null,
                                            null,
                                            _clientStaticConfig.getConnectionDefaults().getConsumerTimeBudgetMs() / 2,
                                            false, false, subsSources);
      consumerReg = new PartitionMultiplexingConsumerRegistration(this, dbName,
                                                                  filterConfig, multiCons,
                                                                  rid, multiCons.getLog());
    }


    /**
     * Update _relayGroups based on external view of relay cluster
     */
    if (_cmEnabled && !isMultiPartitionConsumer)
    {
      updateRelayGroups(consumerReg, _relayClusterInfoProvider);
    }

    /**
     * DDSDBUS-636 : Create a seperate thread-safe list for DatabusSourcesConnection which will contain only the registrations included
     * as part of this register call
     */

    List<DatabusV2ConsumerRegistration> consumers = new CopyOnWriteArrayList<DatabusV2ConsumerRegistration>();
    List<DatabusV2ConsumerRegistration>  relayGroupStreamConsumersList =
    		             registerDatabusListener(consumerReg, _relayGroups, getRelayGroupStreamConsumers(), subsSources);

    consumers.add(consumerReg);


    // Only add logging listeners if it is not available
    boolean loggingListenerFound = false;
    for (  DatabusV3Consumer l : listeners)
    {
    	if ( l instanceof LoggingConsumer)
    	{
    		loggingListenerFound = true;
    		break;
    	}
    }

	if (!loggingListenerFound)
	{
		try
		{
			RegistrationId loggingRid = RegistrationIdGenerator.generateNewId("logging_", subsSources);
			DatabusV3ConsumerRegistration loggingReg =
				  new DatabusV3ConsumerRegistration(this, dbName,  new LoggingConsumer(_clientStaticConfig.getLoggingListener(), rid.getId()), loggingRid,
						  subsSources,filterConfig,
						  checkpointPersistenceProvider, clientStatus);
			consumers.add(loggingReg);
			relayGroupStreamConsumersList.add(loggingReg);
			loggingRegIdSet.add(loggingReg.getId());
		} catch (InvalidConfigException ice) {
			LOG.error("Unable to add logging listener for registration (" + consumerReg + ")", ice);
		}
	}

   updateRegistrationMap(rid, consumerReg);
   consumerReg.addDatabusConsumerRegInfo(consumers);
   return consumerReg;
  }

  private List<DatabusSubscription> expandPPWildcards(List<DatabusSubscription> subsSources)
      throws DatabusClientException
  {
    List<DatabusSubscription> result = new ArrayList<DatabusSubscription>(subsSources.size());
    for (DatabusSubscription s: subsSources)
    {
      if (s.getPhysicalPartition().isAnyPartitionWildcard())
      {
        String dbName = s.getPhysicalPartition().getName();
        int partNum = getNumPartitions(dbName);
        LOG.info("expanding sbuscription " + s + " to " + partNum + " partitions.");
        if (0 >= partNum)
          throw new DatabusClientException("unable to determine the number of partitions for database " +
                                           dbName);
        for (int i = 0; i < partNum; ++i)
        {
          DatabusSubscription newSub =
              new DatabusSubscription(s.getPhysicalSource(),
                                      new PhysicalPartition(i, dbName),
                                      s.getLogicalPartition());
          result.add(newSub);
        }
      }
      else
      {
        result.add(s);
      }
    }
    return result;
  }

  /**
   *
   * @param rid
   * @param consumerReg
   * @throws DatabusClientException
   */
  private void updateRegistrationMap(RegistrationId rid,
                                     DatabusV3Registration consumerReg)
       throws DatabusClientException
  {

    if (_registrationIdMap.containsKey(rid))
    {
      throw new DatabusClientException("Registering with the same RegistrationId is not allowed");
    }
    else
    {
      _registrationIdMap.put(rid, consumerReg);
    }

    // update _dbToRegistrationsMap
    String dbName = consumerReg.getDBName();
    List<RegistrationId> registrationIds = _dbToRegistrationsMap.get(dbName);

    if (null == registrationIds)
    {
      registrationIds = new ArrayList<RegistrationId>();
      _dbToRegistrationsMap.put(dbName, registrationIds);
    }

    registrationIds.add(rid);
  }

  /**
  *
  * @param rid
  * @param consumerReg
  * @throws DatabusClientException
  */
 public void updateRegistrationConnectionMap(RegistrationId rid,
                                             DatabusSourcesConnection newConn)
      throws DatabusClientException
 {

   if (_registrationToSourcesConnectionMap.containsKey(rid))
   {
     throw new DatabusClientException("Registering with the same RegistrationId is not allowed");
   }
   else
   {
	   // update _registrationToSourcesConnectionMap
	   _registrationToSourcesConnectionMap.put(rid, newConn);
   }
   return;
 }

  /**
   * A helper function to create a RegistrationId, in case the passed in registrationId is null ( consumer wants client library to
   * generate one for them )
   * Used by both bootstrap and stream registerXXX calls
   *
   * @param rid
   * @param prefix
   * @param subsSources
   * @return
   * @throws DatabusClientException
   */
  private RegistrationId validateRegistration(RegistrationId rid, String prefix, List<DatabusSubscription> subsSources)
  throws DatabusClientException
  {
    RegistrationId registrationId = rid;
    if (rid == null)
    {
      registrationId = RegistrationIdGenerator.generateNewId(prefix, subsSources);
      LOG.info("Generated ID = " + registrationId.getId());
    }
    else if ( rid.getId().isEmpty())
    {
      String error = "If RegistrationId Object is not null, the id cannot be null";
      LOG.error(error);
      throw new DatabusClientException(error);
    }
    else if (! RegistrationIdGenerator.isIdValid(registrationId))
    {
      throw new DatabusClientException("RegistrationId already exists in this session. Cannot reregister with the same ID");

    }
    else
    {
      LOG.info("Using ID given by the consumer " + rid.getId());
    }

    return registrationId;
  }

  protected List<DatabusServerCoordinates> getCandidateServingListForRegistration(
                                        DatabusV3Registration  consumerReg,
                                        boolean doRoleCheck,
                                        boolean doOnlineCheck)
  {
    List<DatabusServerCoordinates> resultList = getCandidateServingListForRegistration(consumerReg, _relayClusterInfoProvider.getExternalView(consumerReg.getDBName()), doRoleCheck, doOnlineCheck);

    return resultList;
  }


  protected List<DatabusServerCoordinates> getCandidateServingListForRegistration(
                            DatabusV3Registration                    consumerReg,
                            Map<ResourceKey, List<DatabusServerCoordinates>> ev,
                            boolean doRoleCheck,
                            boolean doOnlineCheck)
  {
    boolean debugEnabled = LOG.isDebugEnabled();

    if (debugEnabled)
    {
      LOG.debug("Finding Candidate Servers for consumer registration :" + consumerReg);
      LOG.debug("Server External View is :" + ev);
    }

    List<DatabusSubscription> subsSources = consumerReg.getSubscriptions();

    return getCandidateServingListForSubscriptionList(subsSources, ev, doRoleCheck, doOnlineCheck);
  }

  /**
   *
   * Responsible for selection of candidate relays for the subscriptions for a given external view
   * @param subsSources : Subscription list
   * @param ev : Relay External View
   * @param doRoleCheck : Enable filtering of resouce keys based on subscriptions requirement. Callers who need all
   *                      the relays for both master and slave partitions will set the flag to false.
   * @param doOnlineCheck : Enable filtering of online relays. Callers who need all the relays (online/offline) set
   *                         this flag to false. (E.g : FetchMaxSCN sets this flag to false to distinguish
   *                         between empty external view and all offline relay cases).
   * @return  candidate Serving list
   */
  protected List<DatabusServerCoordinates> getCandidateServingListForSubscriptionList(
                            List<DatabusSubscription> subsSources,
                            Map<ResourceKey, List<DatabusServerCoordinates>> ev,
                            boolean doRoleCheck,
                            boolean doOnlineCheck)
  {
    List<DatabusServerCoordinates> servingList = new ArrayList<DatabusServerCoordinates>();

    if ( null == ev || ev.size() == 0)
    {
      return servingList;
    }

    Role requestedPhysicalSourceRole = Role.createUnknown();
    boolean firstValidSubsctiption = true;

    for (DatabusSubscription ds: subsSources)
    {
      LOG.info("Processing subscription ds = " + ds);
      if (ds.getPhysicalPartition().isWildcard())
      {
        LOG.error("Wildcards on physical partitions not supported. Should have gotten rejected at API level");
        continue;
      }

      int partitionNum = -1;

      if (! ds.getPhysicalPartition().isWildcard())
        partitionNum = ds.getPhysicalPartition().getId();

      if (partitionNum < 0)
      {
        LOG.error("Supports only non-negative partition numbers for now " + partitionNum + " is invalid" );
        continue;
      }

      LOG.debug("Logical Partition number is :" + partitionNum  + " for subscription " + ds);

      if (firstValidSubsctiption)
      {
        //All roles for subscription expected to be same (for now). Hence, setting it only once
        //TODO: As part of implementing DDSDBUS-527, we need to revisit this logic
        requestedPhysicalSourceRole = ds.getPhysicalSource().getRole();
        firstValidSubsctiption = false;
      }

      LOG.info("Consumer asking for databus Servers for slave node in role :" + requestedPhysicalSourceRole);

      /**
       * At this point, subscriptions are validated to be of form EDB.*:1
       * Find the subset of relays that will support all the subscriptions
       */
      List<DatabusServerCoordinates> localServingList = new ArrayList<DatabusServerCoordinates>();

      for (ResourceKey rk : ev.keySet())
      {
    	if ( doRoleCheck)
    	{
    		// if requested and current resource Key roles, dont match, skip it
    		//TODO: As part of implementing DDSDBUS-527, we need to revisit this logic
    		if (! requestedPhysicalSourceRole.equals(rk.getRole()))
    			continue;
    	}

        if (rk.getLogicalPartitionNumber() == partitionNum)
        {
          List<DatabusServerCoordinates> rc = ev.get(rk);
          localServingList.addAll(rc);
        }
      }

      LOG.info("The set of relays that serve the current subscription " + localServingList);

      if ( doOnlineCheck)
      {
    	  // Remove offline nodes from the serving list
    	  LOG.info("Candidate Serving list for this subscription before offline trimming : " + localServingList);
    	  if ( null != localServingList)
    	  {
    		  Iterator<DatabusServerCoordinates> itr = localServingList.iterator();
    		  while (itr.hasNext())
    		  {
    			  DatabusServerCoordinates server = itr.next();

    			  if (! server.isOnlineState())
    			  {
    				  LOG.info("Removing Server :" + server + " as it is not ONLINE !!");
    				  itr.remove();
    			  }
    		  }
    	  }
          LOG.info("Candidate Serving list for this subscription after offline trimming : " + localServingList);
      }

      if (servingList.isEmpty())
      {
        LOG.info("Set the servingList as the first localServingList (obtained for the first subscription");
        servingList = localServingList;
      }
      else
      {
    	Iterator<DatabusServerCoordinates> it = servingList.iterator();
        while (it.hasNext())
        {
          DatabusServerCoordinates rc = it.next();
          if (! localServingList.contains(rc))
          {
            LOG.info("Remove DatabusServerCoordinates from servingList as it is not present in localServingList");
            LOG.info("DatabusSubscription = " + ds);
            LOG.info("DatabusServerCoordinate = " + rc);
            it.remove();

            // If it becomes empty at any point, exit as there is no one relay covering all
            // subscriptions
            if (servingList.isEmpty())
            {
              LOG.info("Exit from the loop as the servingList has become empty");
              return servingList;
            }
          }
        }
      }
    }

    return servingList;
  }

  /**
  *
  * @param groupsServers
  * @param sources
  * @return
  * @throws DatabusClientException
  */
 @Override
 public ServerInfo getRandomRelay(Map<List<DatabusSubscription>, Set<ServerInfo>> groupsServers,
                    List<DatabusSubscription> sources)
 throws DatabusClientException
 {
      List<ServerInfo> candidateServers = findServers(groupsServers, sources);
      if (0 == candidateServers.size())
      {
        LOG.info("Unable to find servers to support sources: " + sources + " Waiting for one to become available");
        return null;
      }
      Random rng = new Random();
      ServerInfo randomRelay = candidateServers.get(rng.nextInt(candidateServers.size()));
      return randomRelay;
 }

  /**
   * This method takes in a consumer registration and a relay cluster information provider
   * and computes a set of relays that would serve all the subscriptions in the registration
   *
   * @param subsSources
   */
  public void updateRelayGroups(DatabusV3ConsumerRegistration consumerReg, RelayClusterInfoProvider rcip)
     throws DatabusClientException
  {
    if (rcip == null)
    {
      LOG.error("No RelayClusterInformationProvider to obtain relay cluster external view");
      return;
    }

    LOG.info("Perform updateRelayGroups for dbName " + consumerReg.getDBName());
    Map<ResourceKey, List<DatabusServerCoordinates>> ev = rcip.getExternalView(consumerReg.getDBName());
    List<DatabusServerCoordinates> servingList = getCandidateServingListForRegistration(consumerReg,ev, true, true);


    for (DatabusServerCoordinates rc : servingList)
    {
      //ServerInfo si = new ServerInfo(rc.getName(), rc.getState().name(), rc.getAddress(), ConsumerRegistration.createStringFromAllSubscriptionFromRegistration(consumerReg));
      ServerInfo si = new ServerInfo(rc.getName(), rc.getState(), rc.getAddress(),
                                     consumerReg.getSubscriptions(),
                                     EspressoSubscriptionUriCodec.getInstance());
      doRegisterRelay(si);

      /**
       * This is to update runtime config parameter to maintain V2/V2 compatibility in code
       * Code access relays from runtimeConfig parameter in case of V2
       */
      RuntimeConfig runtimeConfig = _configManager.getReadOnlyConfig();
      runtimeConfig.addRelay(si);
    }
  }

  /**
   * A method used to make a connection to a server
   */
  protected DatabusSourcesConnection initializeRelayConnection(RegistrationId registrationId,
                                        List<DatabusV2ConsumerRegistration> consumers,
                                        List<DatabusSubscription> subsList)
  throws DatabusClientException
  {

    Set<PhysicalPartition> pps = getPhysicalPartitionGivenSubscriptionList(subsList);
    if (pps.size() != 1) return null; //no individual connection for multi-partition connections

    DatabusSourcesConnection newConn = null;
    List<String> sourcesStrList = DatabusSubscription.getStrList(subsList);
    try
    {
      DatabusSourcesConnection.StaticConfig connConfig =
          getClientStaticConfig().getConnection(sourcesStrList);
      if (null == connConfig)
      {
        connConfig = getClientStaticConfig().getConnectionDefaults();
      }

      CheckpointPersistenceProvider cpPersistenceProvder = getCheckpointPersistenceProvider();
      if (null != cpPersistenceProvder && getClientStaticConfig().getCheckpointPersistence().isClearBeforeUse())
      {
        cpPersistenceProvder.removeCheckpointV3(subsList, registrationId);
      }

      ArrayList<DatabusV2ConsumerRegistration> bstConsumersRegs =
          new ArrayList<DatabusV2ConsumerRegistration>();
      /* Keep bootstrap consumers empty for now ( no support for external bootstrap as yet )
      ServerInfo server0 = _relayGroups.get(subsList).iterator().next();

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
      */

      DatabusV3Registration reg = getRegistration(registrationId);

      PhysicalPartition pp = pps.iterator().next();
      assert(pp != null);
      DbusEventBuffer eventBuffer = connConfig.getEventBuffer().getOrCreateEventBufferWithPhyPartition(pp);
      eventBuffer.setDropOldEvents(true);
      eventBuffer.start(0);

      DbusEventBuffer bootstrapBuffer = null;
      // create bootstrap only if it is enabled (which is currently not the case for v3)
      if(_clientStaticConfig.getRuntime().getBootstrap().isEnabled()) {
        bootstrapBuffer = new DbusEventBuffer(connConfig.getEventBuffer(), pp);
        bootstrapBuffer.setDropOldEvents(false);
        bootstrapBuffer.start(0);
      }

      LOG.info("The sourcesList is " + sourcesStrList);
      LOG.info("The relayGroupStreamConsumers is " + consumers);


      Set<ServerInfo> relays = getServerInfoSet(_relayGroups, subsList);
      Set<ServerInfo> bootstrapServices = getServerInfoSet(_bootstrapGroups, subsList);

      newConn =  new DatabusSourcesConnection(connConfig,
              subsList,
              relays,
              bootstrapServices,
              consumers,
              bstConsumersRegs,
              eventBuffer,
              bootstrapBuffer,
              getDefaultExecutorService(),
              getContainerStatsCollector(),
              (DbusEventsStatisticsCollector)reg.getRelayEventStats(),
              (DbusEventsStatisticsCollector)reg.getBootstrapEventStats(),
              (ConsumerCallbackStats)reg.getRelayCallbackStats(),
              (ConsumerCallbackStats)reg.getBootstrapCallbackStats(),
              getCheckpointPersistenceProvider(),
              getRelayConnFactory(),
              getBootstrapConnFactory(),
              getHttpStatsCollector(),
              registrationId,
              this);
      newConn.start();
      _relayConnections.add(newConn);

    }
    catch (Exception e)
    {
      LOG.error("connection initialization issue for source(s):" + subsList +
          "; please check your configuration", e);
      // NOt being able to connect to a relay is not a fatal exception.
      // The client library should keep retrying until it is able to connect to a relay
      //throw new DatabusClientException(e);
    }

    return newConn;
  }

  @Override
  public void onExternalViewChange( String dbName,
		  Map<ResourceKey, List<DatabusServerCoordinates>> oldResourceToServerCoordinatesMap,
		  Map<DatabusServerCoordinates, List<ResourceKey>> oldServerCoordinatesToResourceMap,
		  Map<ResourceKey, List<DatabusServerCoordinates>> newResourceToServerCoordinatesMap,
		  Map<DatabusServerCoordinates, List<ResourceKey>> newServerCoordinatesToResourceMap)
  {
    LOG.info("Got externalViewChange notification for dbName" + dbName);
    List<RegistrationId> registrationIds = _dbToRegistrationsMap.get(dbName);

    if ( (null == registrationIds) || (registrationIds.isEmpty()))
    {
      return;
    }

    /**
     * To ensure that each external view change is applied
     */
    synchronized ( this )
    {
        for ( RegistrationId id : registrationIds)
        {
          DatabusV3Registration reg = _registrationIdMap.get(id);

          if (!(reg instanceof DatabusV3ConsumerRegistration))
              continue;

          DatabusV3ConsumerRegistration r = (DatabusV3ConsumerRegistration)reg;

          List<DatabusServerCoordinates> oldServerList =
              getCandidateServingListForRegistration(r, oldResourceToServerCoordinatesMap, true, true);
          List<DatabusServerCoordinates> newServerList =
              getCandidateServingListForRegistration(r, newResourceToServerCoordinatesMap, true, true);

          if ( r._lock.tryLock())
          {
        	  try
        	  {
            	  processViewChange(r, oldServerList, newServerList, true);
        	  } finally
        	  {
        		  r._lock.unlock();
        	  }
          }
          else
          {
        	  LOG.info("Skipping externalView Change update for registration " + reg.getId().getId() + " as a flush is likely in progress. It will be updated after flush is done");
          }
        }
    }
  }


  /**
   *
   * Helper method to process External view change for a Consumer registration
   * @param reg : DatabusV3ConsumerRegistration
   * @param oldServerList Old List of Databus Server Coordinates
   * @param newServerList New List of Databus Server Coordinates
   * @param updateRelayGroups Flag to update Sunscription_To_RelayGroup.
   */
  private void processViewChange(DatabusV3ConsumerRegistration reg,
                  List<DatabusServerCoordinates> oldServerList,
                  List<DatabusServerCoordinates> newServerList,
                  boolean updateRelayGroups)
  {
    RegistrationId id = reg.getId();

    Collections.sort(oldServerList);
    Collections.sort(newServerList);

    if (oldServerList.equals(newServerList))
    {
      LOG.info("Server List unchanged for Registration Id : " + id + ", Server List is :" + oldServerList);
    }  else {
      LOG.info(" Server list changed for registration id : "
          + id + ", Old Server Lis is :" + oldServerList
          + ", New Server List is : " + newServerList);


      List<DatabusSubscription> subscriptions = reg.getSubscriptions();

      if ( updateRelayGroups)
      {
        // Clear RelayGroups associated with this subscriptions
        clearRelayRegistrationForSubscriptions(subscriptions);
      }

      HashSet<ServerInfo> serverInfoSet = new HashSet<ServerInfo>();
      for (DatabusServerCoordinates rc : newServerList)
      {
        /*ServerInfo si = new ServerInfo(rc.getName(), rc.getState().name(), rc.getAddress(),
            ConsumerRegistration.createStringFromAllSubscriptionFromRegistration(reg));*/
        ServerInfo si = new ServerInfo(rc.getName(), rc.getState(), rc.getAddress(),
                                       reg.getSubscriptions(),
                                       EspressoSubscriptionUriCodec.getInstance());
        serverInfoSet.add(si);

        if (updateRelayGroups)
          doRegisterRelay(si);
      }

      DatabusSourcesConnection conn = _registrationToSourcesConnectionMap.get(id);
      if ( null != conn )
      {
    	/**
    	 * Synchronize ServerSetChangeMessages on the registration
    	 * This is to avoid this message sent in the middle of a flush
    	 */
    	  LOG.info("Enqueueing ServerSetChange message with new server set :" + serverInfoSet);
    	  conn.getRelayPullThread().enqueueMessage(ServerSetChangeMessage.createSetServersMessage(serverInfoSet));
      }
    }
  }

  @Override
  protected synchronized void doRegisterRelay(ServerInfo serverInfo)
  {
      LOG.info("Registering relay: " + serverInfo.toString());

      List<DatabusSubscription> subList = serverInfo.getSubs();
      if (null == subList || subList.size() == 0)
      {
        List<String> sources = serverInfo.getSources();
        if (null != sources && sources.size() != 0)
        {
          //heuristic whether the sources contain URIs or the legacy format
          if (sources.get(0).startsWith(EspressoSubscriptionUriCodec.SCHEME))
          {
            try
            {
              subList = DatabusSubscription.createFromUriList(sources);
            }
            catch (DatabusException e)
            {
              throw new RuntimeException("unable to parse subscription URIs: " + e.getMessage(), e);
            }
            catch (URISyntaxException e)
            {
              throw new RuntimeException("unable to parse subscription URIs: " + e.getMessage(), e);
            }
          }
          else
          {
            subList = DatabusSubscription.createSubscriptionList(sources);
          }
        }
      }
      Set<ServerInfo> sourceRelays = _relayGroups.get(subList);
      if (null == sourceRelays)
      {
          sourceRelays = new HashSet<ServerInfo>(5);
          _relayGroups.put(subList, sourceRelays);
      }
      sourceRelays.add(serverInfo);
  }

  /**
   * Clear the RelayGroup Set for the subscription List
   * @param subs Subscription List
   */
  protected synchronized void clearRelayRegistrationForSubscriptions(List<DatabusSubscription> subs)
  {
      Set<ServerInfo> sourceRelays = _relayGroups.get(subs);

      if ( null != sourceRelays)
      {
        sourceRelays.clear();
      }
  }

  /**
   * Returns map from RegistrationId to the corresponding DatabusV3Registration object
   * @return
   */
  @Override
  public Map<RegistrationId, DatabusV3Registration> getRegistrationIdMap()
  {
    return _registrationIdMap;
  }

  public Map<String, List<RegistrationId>> getDbToRegistrationsMap() {
    return _dbToRegistrationsMap;
  }

  public Map<RegistrationId, DatabusSourcesConnection> getRegistrationToSourcesConnectionMap() {
    return _registrationToSourcesConnectionMap;
  }

  public List<String> getDbList() {
    return _dbList;
  }

  public RelayClusterInfoProvider getRelayClusterInfoProvider() {
    return _relayClusterInfoProvider;
  }

  /**
   * Obtain the set of physical partitions for a list of subscriptions. The partitions are guaranteed to be
   * in the same database.
   * @param subsList  the list of subscriptions to inspect
   * @return a set of physical partitions
   * @throws DatabusClientException if the subscriptions are not part of the same DB
   */
  private Set<PhysicalPartition> getPhysicalPartitionGivenSubscriptionList(List<DatabusSubscription> subsList)
          throws DatabusClientException
  {
    HashSet<PhysicalPartition> pp = new HashSet<PhysicalPartition>(1);
    String dbName = null;
    for (DatabusSubscription ds: subsList)
    {
      PhysicalPartition ppSubs = ds.getPhysicalPartition();
      pp.add(ppSubs);
      if (null == dbName) dbName = ppSubs.getName();
      else if (!dbName.equals(ppSubs.getName()))
        throw new DatabusClientException("subscription list cannot include more than one database: "
                                         + dbName + ", " + ppSubs.getName());
    }
    return pp;
  }

  @Override
  public DatabusSourcesConnection getDatabusSourcesConnection(String regIdStr)
  {
    if (! _registrationIdMap.containsKey(new RegistrationId(regIdStr)))
    {
      return null;
    }
    return _registrationToSourcesConnectionMap.get(new RegistrationId(regIdStr));
  }

  /**
   * For a given subscription list, identify, the set of all ServerInfo objects that can serve the same
   */
  protected static Set<ServerInfo> getServerInfoSet(
      Map<List<DatabusSubscription>, Set<ServerInfo>> serverInfoGroups,
      List<DatabusSubscription> subsSources)
  {
    // Use a set to avoid duplicating consumerRegistrations
    Set<ServerInfo> serverInfoSet = new HashSet<ServerInfo>();
    for ( List<DatabusSubscription> dsList :  serverInfoGroups.keySet())
    {
      if (dsList.containsAll(subsSources))
      {
        Set<ServerInfo> lsigs = serverInfoGroups.get(dsList);
        serverInfoSet.addAll(lsigs);
      }
    }

    if (serverInfoSet.size() > 0)
    {
      LOG.info("List of relays " + serverInfoSet);
    }
    else
    {
      LOG.info("No relays found for the list of subscriptions");
    }
    return serverInfoSet;
  }

    /**
     * For a given subscription list, identify, the set of all consumer registration objects that are interested in
     *
     * groupsListeners [ (S1,S2) : cReg1 ]
     * subsSources [S1]
     *
     * Return cReg1
     *
     * @param groupsListeners
     * @param dsl
     * @return
     */
    protected static List<DatabusV2ConsumerRegistration> getListOfConsumerRegsFromSubList(
        Map<List<DatabusSubscription>, List<DatabusV2ConsumerRegistration>> groupsListeners,
        List<DatabusSubscription> subsSources)
    {
      List<DatabusV2ConsumerRegistration> consRegList = new ArrayList<DatabusV2ConsumerRegistration>();

      // Use a set to avoid duplicating consumerRegistrations
      Set<DatabusV2ConsumerRegistration> consRegSet = new HashSet<DatabusV2ConsumerRegistration>();
      for ( List<DatabusSubscription> dsList :  groupsListeners.keySet())
      {
        if (dsList.containsAll(subsSources))
        {
          List<DatabusV2ConsumerRegistration> crList = groupsListeners.get(dsList);
          consRegSet.addAll(crList);
        }
      }

      if (consRegSet.size() > 0)
      {
        consRegList.addAll(consRegSet);
        LOG.info("List of consumers " + consRegList);
      }
      else
      {
        LOG.info("No consumers found for the list of subscriptions");
      }
      return consRegList;
  }

  public RelayFindMaxSCNResult fetchMaxSCN(DatabusV3ConsumerRegistration reg,
		  									FetchMaxSCNRequest request)
    throws InterruptedException
  {
      LOG.info("RelayFindMaxSCNResult: Got call to fetchMaxSCN with reg" + reg.getId());
      RelayFindMaxScnResultImpl maxSCNResult = new RelayFindMaxScnResultImpl();

      int partitionId = getPhysicalPartitionId(reg);

      if ( partitionId < 0 )
      {
        LOG.error("RelayFindMaxSCNResult: Partition Number for registration :" + reg + " is invalid !!");
        maxSCNResult.setResultSummary(RelayFindMaxSCNResult.SummaryCode.FAIL);
        return maxSCNResult;
      }
      else
      {
    	  LOG.info("RelayFindMaxSCNResult: partition number " + partitionId );
      }

      // Allow offline relays to be on the candidate list
      List<DatabusServerCoordinates> serverList = getCandidateServingListForRegistration(reg, false, false);

      if ( (serverList == null) || (serverList.isEmpty()))
      {
        LOG.error("RelayFindMaxSCNResult: External view for this registration is empty !! Cannot obtain Max SCN");
        maxSCNResult.setResultSummary(RelayFindMaxSCNResult.SummaryCode.EMPTY_EXTERNAL_VIEW);
        return maxSCNResult;
      }

      // Check if all offline
      boolean allOffline = true;
      for ( DatabusServerCoordinates s : serverList)
      {
    	  if ( s.isOnlineState())
    	  {
    		  allOffline = false;
    		  break;
    	  }
      }

      DatabusServerCoordinates[] serverArray = new DatabusServerCoordinates[serverList.size()];
      serverList.toArray(serverArray);

      RelayMaxSCNFinder maxSCNFinder = new RelayMaxSCNFinder(serverArray, reg.getDBName(), partitionId);

      LOG.error("RelayFindMaxSCNResult: getting max SCN from relays");
      RelayFindMaxScnResultImpl result = maxSCNFinder.getMaxSCNFromRelays(request);

      /**
       * Even if all relays are marked offline, we try querying those relays up to numRetries.
       * If they still fail, we use separate enum to mark this case
       */
      if (  allOffline && (result.getResultSummary() == RelayFindMaxSCNResult.SummaryCode.FAIL))
    	  result.setResultSummary(RelayFindMaxSCNResult.SummaryCode.NO_ONLINE_RELAYS_IN_VIEW);

      LOG.error("RelayFindMaxSCNResult: printing full result" + result);
      return result;
  }

    public RelayFlushMaxSCNResult flush(DatabusV3ConsumerRegistration reg,
    												long flushTimeout,
    												FetchMaxSCNRequest maxSCNRequest)
      throws InterruptedException
    {
    	RelayFindMaxSCNResult maxSCNResult = fetchMaxSCN(reg,maxSCNRequest);
    	return flush(maxSCNResult, reg, flushTimeout);
    }

    public RelayFlushMaxSCNResult flush(RelayFindMaxSCNResult maxSCNResult,
    									DatabusV3ConsumerRegistration reg,
    									long timeout)
    throws InterruptedException
    {
    	RelayFlushMaxSCNResultImpl result = new RelayFlushMaxSCNResultImpl(maxSCNResult);

    	LOG.info("Flush called for registration (" + reg + ") with timeout (" + timeout + ") and fetMaxSCN result (" + maxSCNResult + ")" );

    	if ((maxSCNResult == null)
    			|| (maxSCNResult.getMaxSCN() == null)
    			|| (RelayFindMaxSCNResult.SummaryCode.FAIL == maxSCNResult.getResultSummary())
    			|| (RelayFindMaxSCNResult.SummaryCode.EMPTY_EXTERNAL_VIEW == maxSCNResult.getResultSummary())
    			|| (RelayFindMaxSCNResult.SummaryCode.NO_ONLINE_RELAYS_IN_VIEW == maxSCNResult.getResultSummary()))

    	{
    		LOG.error("FetchMaxSCN query did not succeed !! MaxSCNResult :" + maxSCNResult);
    		if ( RelayFindMaxSCNResult.SummaryCode.EMPTY_EXTERNAL_VIEW == maxSCNResult.getResultSummary())
    			result.setResultStatus(SummaryCode.EMPTY_EXTERNAL_VIEW);
    		else if ( RelayFindMaxSCNResult.SummaryCode.NO_ONLINE_RELAYS_IN_VIEW == maxSCNResult.getResultSummary())
    			result.setResultStatus(SummaryCode.NO_ONLINE_RELAYS_IN_VIEW);
    		else
    			result.setResultStatus(SummaryCode.FAIL);
    		return result;
    	}

    	Set<DatabusServerCoordinates> candidateSet = maxSCNResult.getMaxScnRelays();

    	Set<ServerInfo> serverInfoSet = new HashSet<ServerInfo>();

    	if ( null != candidateSet)
    	{
    		for (DatabusServerCoordinates rc : candidateSet)
    		{
    			ServerInfo si = new ServerInfo(rc.getName(), rc.getState().name(), rc.getAddress(),
    					ConsumerRegistration.createStringFromAllSubscriptionFromRegistration(reg));
    			serverInfoSet.add(si);
    		}
    	}

    	if (serverInfoSet.isEmpty())
    	{
    		LOG.error("Candidate Set of Relays for Flush is empty !! ");
    		result.setResultStatus(SummaryCode.FAIL);
    		return result;
    	}

    	DatabusSourcesConnection conn = getDatabusSourcesConnection(reg.getId().toString());
    	RelayPullThread rp = conn.getRelayPullThread();
    	if ( null == rp || rp.checkForShutdownRequest() || rp.isShutdown() )
    	{
    		LOG.error("RelayPullerThread is in error state");
    		result.setResultStatus(SummaryCode.FAIL);
    		return result;
    	}
    	GenericDispatcher<DatabusCombinedConsumer> gdp = conn.getRelayDispatcher();
    	if ( null == gdp || gdp.checkForShutdownRequest() || gdp.isShutdown() )
    	{
    		LOG.error("RelayDispatcherThread is in error state");
    		result.setResultStatus(SummaryCode.FAIL);
    		return result;
    	}

    	SCN requestedMaxSCN = maxSCNResult.getMaxSCN();
    	SCN currentSCN = conn.getRelayDispatcher().getDispatcherState().getEndWinScn();
    	/**
    	 * Obtain currentSCN from the persisted checkpoint
    	 */
    	try
    	{
    		Checkpoint cp = reg.getCheckpoint().loadCheckpointV3(reg.getSubscriptions(), reg.getId());
    		SCN persistedSCN = null;
    		if (null != cp)
    		{
    			persistedSCN = new SingleSourceSCN(((SingleSourceSCN)requestedMaxSCN).getSourceId(), cp.getWindowScn());
    		}
    		if (null != persistedSCN )
    		{
    			int isPersistedSCNGreater = SCNUtils.compareOnlySequence(persistedSCN, currentSCN);
    			if (isPersistedSCNGreater > 0)
    			{
    				LOG.info("Persisted SCN is greater than SCN on Dispatcher side" +  persistedSCN + currentSCN);
    				currentSCN = persistedSCN;
    			}
    		}
    	} catch (Exception e)
    	{
    		LOG.error("Error in comparing persistedSCN with SCN obtained from dispatcher " + e);
    	}

    	LOG.info("Flush invoked " + " for registration: " + reg.getId() + "with requestedMaxSCN = " + requestedMaxSCN + " currentSCN = " + currentSCN );
    	result.setCurrentMaxSCN(currentSCN);
    	result.setRequestedMaxSCN(requestedMaxSCN);
    	int cmpReturn = (null == currentSCN) ? -1 : SCNUtils.compareOnlySequence(currentSCN, requestedMaxSCN);
    	if (cmpReturn >= 0)
    	{
    		//currentSCN has already reached requestedMaxSCN
    		LOG.info("Current SCN is already greater than or equal to requested Max SCN. No need to flush !! Current SCN :"
    				+ currentSCN + ", Requested SCN :" + requestedMaxSCN + ". Reg Id :" + reg.getId());
    		result.setResultStatus(SummaryCode.MAXSCN_REACHED);
    		return result;
    	}
    	if (SCNUtils.compareOnlySequence(requestedMaxSCN, new SingleSourceSCN(-1, 0)) <= 0)
    	{
    		LOG.info("Requested MaxSCN has sequence number less than or equal to zero. No relays would have those events");
    		result.setResultStatus(SummaryCode.MAXSCN_REACHED);
    		return result;
    	}

    	FlushConsumer flushConsumer = new FlushConsumer(((SingleSourceSCN)requestedMaxSCN).getSequence(), reg.getDBName(), getPhysicalPartitionId(reg));
    	RegistrationId flushRegId = new RegistrationId(FlushConsumer.FLUSH_CONSUMER_REG_PREFIX + "_" + flushRegIdGen.incrementAndGet() + "_" + reg.getId());

    	DatabusV2ConsumerRegistration flushConsumerRegistration = new DatabusV3ConsumerRegistration(
    			this,
    			reg.getDBName(),
    			flushConsumer,
    			flushRegId,
    			reg.getSubscriptions(),
    			reg.getFilterConfig(),
    			reg.getCheckpoint(),
    			reg.getStatus()
    			);

    	conn.getRelayConsumers().add(flushConsumerRegistration);
    	try
    	{
    		// Send Message to Relay Pull Thread to start picking data from the new set of relays
    		conn.getRelayPullThread().enqueueMessage(ServerSetChangeMessage.createSetServersMessage(serverInfoSet));

    		// Wait here for the consumer callback to reach the SCN
    		flushConsumer.waitForTargetSCN(timeout, TimeUnit.MILLISECONDS);

    		result.setCurrentMaxSCN(new SingleSourceSCN(-1, flushConsumer.getCurrentMaxSCN()));

    		if(flushConsumer.targetSCNReached())
    		{
    			result.setResultStatus(SummaryCode.MAXSCN_REACHED);
    		}
    		else
    		{
    			result.setResultStatus(SummaryCode.TIMED_OUT);
    		}

    	}
    	finally
    	{
    		LOG.info("Done with flush for registration :");

    		conn.getRelayConsumers().remove(flushConsumerRegistration);

    		// Reset with the previous set of relays
    		List<DatabusServerCoordinates> newServerList = getCandidateServingListForRegistration(reg, true, true);
    		serverInfoSet = new HashSet<ServerInfo>();

    		if ( null != newServerList)
    		{
    			for (DatabusServerCoordinates rc : newServerList)
    			{
    				ServerInfo si = new ServerInfo(rc.getName(), rc.getState().name(), rc.getAddress(),
    						ConsumerRegistration.createStringFromAllSubscriptionFromRegistration(reg));
    				serverInfoSet.add(si);
    			}
    		}
    		conn.getRelayPullThread().enqueueMessage(ServerSetChangeMessage.createSetServersMessage(serverInfoSet));
    	}

    	return result;
    				}

    private int getPhysicalPartitionId(DatabusV3Registration reg)
    {
      if (reg == null)
    	  return -1;

      List<DatabusSubscription> subsSources = reg.getSubscriptions();

      if ( subsSources.isEmpty())
      {
        return -1;
      }
      else
      {
        return subsSources.get(0).getPhysicalPartition().getId();
      }
    }

    /**
     * De-register this registration Id from the client library. Stop the connection if no registrations remain
     * @param reg : Registration to be de-registered.
     */
    public DeregisterResult deregister(DatabusV3ConsumerRegistration reg)
    {
    	DatabusSourcesConnection conn = _registrationToSourcesConnectionMap.get(reg.getId());

    	if (conn == null)
    	{
    		return DeregisterResultImpl.createRegNotFoundResult(reg.getId());
    	}

    	conn.removeRegistration(reg);

    	List<DatabusV2ConsumerRegistration> consumers = conn.getRelayConsumers();

    	boolean stopConn = true;
    	if ( consumers != null)
    	{
    		Iterator<DatabusV2ConsumerRegistration> itr = consumers.iterator();
    		while (itr.hasNext())
    		{
    			DatabusV2ConsumerRegistration c = itr.next();
    			if ( ! (c instanceof DatabusV3ConsumerRegistration))
    			{
    				stopConn = false;
    				break;
    			}
    			DatabusV3ConsumerRegistration c2 = (DatabusV3ConsumerRegistration)c;

    			if ( ! loggingRegIdSet.contains(c2.getId()))
    			{
    				// means we stepped on some non-logging V3 registration. So we should not shutdown the connection.
    				LOG.info("Databus Connection serving a non logging registration : " + c2.getId()
    						+ " while deregistering registration : " + reg.getId() + ". Not stopping the Pull threads !!");
    				stopConn = false;
    				break;
    			}
    		}
    	}

    	if ( stopConn )
    	{
    		for (DatabusV2ConsumerRegistration c : consumers)
    		{
    			DatabusV3ConsumerRegistration r = (DatabusV3ConsumerRegistration)c;
    			conn.removeRegistration(r);
    			// Only logging Consumers might remain
    			loggingRegIdSet.remove(r.getId());
    		}
    		LOG.info("Stopping Databus Sources Connection as all the registrations associated with it has been deregistered. Current deregistration is for regId :" + reg.getId());
    		conn.stop();
    		_relayConnections.remove(conn);
    	}

    	// Remove Entry from Con
    	_registrationIdMap.remove(reg.getId());

    	// Update _dbToRegistrationsMap
    	List<RegistrationId> regIds = _dbToRegistrationsMap.get(reg.getDBName());
    	regIds.remove(reg.getId());

    	_registrationToSourcesConnectionMap.remove(reg.getId());

    	// Remove reg from _relayGroupStreamConsumers
    	Map<List<DatabusSubscription>, List<DatabusV2ConsumerRegistration>> subsToRegsMap = getRelayGroupStreamConsumers();
    	Iterator<Entry<List<DatabusSubscription>, List<DatabusV2ConsumerRegistration>>> itr = subsToRegsMap.entrySet().iterator();
    	while (itr.hasNext())
    	{
    		Entry<List<DatabusSubscription>, List<DatabusV2ConsumerRegistration>> e = itr.next();
    		List<DatabusV2ConsumerRegistration> regs = e.getValue();

    		regs.remove(reg);
    		LOG.info("Removing reg " + reg.getId().toString() + "from _relayGroupStreamConsumers");
    	}

    	return DeregisterResultImpl.createSuccessDeregisterResult(stopConn);
    }

    /**
     * Override clientStarted flag. Mainly used in testing
     * @param started
     */
    public void setClientStarted(boolean started)
    {
        setStarted(started);
    }

    /**
     * Returns number of partitions in a given DB
     *
     * @param dbName
     * @return
     */
    public int getNumPartitions(String dbName)
    {
      //in the lack of other dat, assume one partition for compatibility with V2
      return null != _relayClusterInfoProvider ? _relayClusterInfoProvider.getNumPartitions(dbName) : 1;
    }
}
