package com.linkedin.databus.client.registration;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


import javax.management.MBeanServer;

import org.apache.log4j.Logger;

import com.linkedin.databus.client.DatabusHttpClientImpl;
import com.linkedin.databus.client.DatabusSourcesConnection;
import com.linkedin.databus.client.DatabusSourcesConnection.StaticConfig;
import com.linkedin.databus.client.consumer.AbstractDatabusCombinedConsumer;
import com.linkedin.databus.client.consumer.DatabusV2ConsumerRegistration;
import com.linkedin.databus.client.consumer.LoggingConsumer;
import com.linkedin.databus.client.pub.CheckpointPersistenceProvider;
import com.linkedin.databus.client.pub.DatabusClientException;
import com.linkedin.databus.client.pub.DatabusCombinedConsumer;
import com.linkedin.databus.client.pub.DatabusRegistration;
import com.linkedin.databus.client.pub.DbusPartitionInfo;
import com.linkedin.databus.client.pub.FetchMaxSCNRequest;
import com.linkedin.databus.client.pub.FlushRequest;
import com.linkedin.databus.client.pub.RegistrationId;
import com.linkedin.databus.client.pub.RelayFindMaxSCNResult;
import com.linkedin.databus.client.pub.RelayFlushMaxSCNResult;
import com.linkedin.databus.client.pub.ServerInfo;
import com.linkedin.databus.client.pub.mbean.ConsumerCallbackStats;
import com.linkedin.databus.client.pub.mbean.ConsumerCallbackStatsMBean;
import com.linkedin.databus.client.pub.monitoring.events.ConsumerCallbackStatsEvent;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DatabusComponentStatus;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollectorMBean;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.filter.DbusKeyCompositeFilterConfig;

public class DatabusV2RegistrationImpl
      implements DatabusRegistration 
{
	private RegistrationState _state;
	private RegistrationId _id;
    private Logger _log;
	private final CheckpointPersistenceProvider _checkpointPersistenceProvider;
    private DbusEventsStatisticsCollector _inboundEventsStatsCollector;
	private DbusEventsStatisticsCollector _bootstrapEventsStatsCollector;
    private ConsumerCallbackStats _relayConsumerStats;
    private ConsumerCallbackStats _bootstrapConsumerStats;
	private final List<DatabusCombinedConsumer> _consumers;
	private final LoggingConsumer _loggingConsumer;
	private final List<String> _sources;
	private DatabusSourcesConnection _sourcesConnection;
	private DatabusRegistration _parent = null;
    private final DatabusHttpClientImpl _client;
	private Status _status = null;
	private DbusKeyCompositeFilterConfig _filterConfig = null;
	
	private List<DatabusV2ConsumerRegistration> _streamConsumerRawRegistrations;
	private List<DatabusV2ConsumerRegistration> _bootstrapConsumerRawRegistrations;
	
    public class Status extends DatabusComponentStatus
	{
      public Status()
      {
        super(getStatusName());
      }
      
      
	}
    
    public DatabusV2RegistrationImpl(RegistrationId id, 
			 DatabusHttpClientImpl client)
    {
    	this(id,client,client.getCheckpointPersistenceProvider(),null,null);
    }
    
    public DatabusV2RegistrationImpl(RegistrationId id, 
    								 DatabusHttpClientImpl client, 
    								 CheckpointPersistenceProvider ckptProvider)
    {
    	this(id,client,ckptProvider,null,null);
    }
    
    
    public DatabusV2RegistrationImpl(RegistrationId id,
    		DatabusHttpClientImpl client,
            CheckpointPersistenceProvider ckptProvider,
            String[] sources,
            AbstractDatabusCombinedConsumer[] consumers)
    {
    	_id = id;
    	_status = new Status();
    	_client = client;
    	_checkpointPersistenceProvider = ckptProvider; 
    	_state = RegistrationState.INIT;
    	_sources = new ArrayList<String>();
    	_consumers = new ArrayList<DatabusCombinedConsumer>();
        _log = Logger.getLogger(getClass().getName() +
                			(null  == _id ? "" : "." + _id.getId()));
        if ( null != sources)
        	_sources.addAll(Arrays.asList(sources));
    	
    	if ( null != consumers)
    		_consumers.addAll(Arrays.asList(consumers));
    	
    	LoggingConsumer c = null;
    	try {
			c = new LoggingConsumer(client.getClientStaticConfig().getLoggingListener());
		} catch (InvalidConfigException e) {
			_log.error("Unable to instantiate logging consumer",e);
		}
    	_loggingConsumer = c;
    }
    
    /**
    *
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
    *
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
    *
    * Adds the specified consumers associated with this registration
    * The added consumers will have the same subscription(s) and filter parameters as the other consumers
    * associated with this registration
    *
    */
   public synchronized void addDatabusConsumers(Collection<DatabusCombinedConsumer> consumers)
   		throws IllegalStateException
   {
	   if (! _state.isPreStartState())
		   throw new IllegalStateException("Cannot add consumers when state is running/shutdown. Current State :" + _state);

	   for (DatabusCombinedConsumer c : consumers)
		   if (! _consumers.contains(c))
			   _consumers.add(c);
   }

   /**
    *
    * Removes the specified consumers associated with this registration.
    *
    **/
   public synchronized void removeDatabusConsumers(Collection<AbstractDatabusCombinedConsumer> consumers)
   {

	   if ( ! _state.isRunning())
		   throw new IllegalStateException("Cannot remove consumers when state is running. Current State :" + _state);

	   _consumers.removeAll(consumers);

   }
   
   /**
    * Callback when registration is added to client Registration Set.
    * @param state
    */
   public synchronized void onRegister()
   {
   	_state = RegistrationState.REGISTERED;
   }
    
    /**
     * Initialize Statistics Collectors
     */
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
    }
    
    /**
     * Initialize Statistics Collectors
     */
    protected void initializeStatsCollectors(String regId, int ownerId, MBeanServer mbeanServer)
    {
	  _inboundEventsStatsCollector =
	      new DbusEventsStatisticsCollector(ownerId,
	                                        regId + ".inbound",
	                                        true,
	                                        false,
	                                        mbeanServer);
	  _bootstrapEventsStatsCollector =
	      new DbusEventsStatisticsCollector(ownerId,
	                                        regId + ".inbound.bs",
	                                        true,
	                                        false,
	                                        mbeanServer);
	  _relayConsumerStats =
	      new ConsumerCallbackStats(ownerId, regId + ".callback.relay",
	                                regId, true, false, new ConsumerCallbackStatsEvent());
      _bootstrapConsumerStats =
          new ConsumerCallbackStats(ownerId, regId + ".callback.bootstrap",
                                    regId, true, false, new ConsumerCallbackStatsEvent());
	  if (null != _client && _client.getClientStaticConfig().isEnablePerConnectionStats())
	  {
        _client.getBootstrapEventsStats().addStatsCollector(regId, _bootstrapEventsStatsCollector );
        _client.getInBoundStatsCollectors().addStatsCollector(regId, _inboundEventsStatsCollector);
        _client.getRelayConsumerStatsCollectors().addStatsCollector(regId, _relayConsumerStats);
        _client.getBootstrapConsumerStatsCollectors().addStatsCollector(regId, _bootstrapConsumerStats);
	  }
    }
        
	@Override
	public synchronized boolean start() 
			 throws IllegalStateException, DatabusClientException 
	{    		
		_log.info("Starting registration (" + toString() + ") !!");

		if (_state.isRunning())
		{
			_log.info("Registration (" + _id + ") already started !!");
			return false;
		}


		if ( _state != RegistrationState.REGISTERED)
			throw new IllegalStateException("Registration (" + _id + ") not in startable state !! Current State is :" + _state);

		if ( (null == _sources) || (_sources.isEmpty()))
			throw new DatabusClientException("Registration (" + _id + ") does not have any sources to start !!");

		if ( (null == _consumers) || (_consumers.isEmpty()))
			throw new DatabusClientException("Registration (" + _id + ") does not have any consumers to start !!");

		List<ServerInfo> relays = _client.getRelays();
		List<ServerInfo> bootstrapServers = _client.getBootstrapServices();

		List<DatabusCombinedConsumer> streamConsumers = new ArrayList<DatabusCombinedConsumer>();
		List<DatabusCombinedConsumer> bootstrapConsumers = new ArrayList<DatabusCombinedConsumer>();

		if ( (null == relays) || ( relays.isEmpty()))
			throw new DatabusClientException("No configured relays in the client to start");

		Set<ServerInfo> candidateRelays = new HashSet<ServerInfo>();

		for (ServerInfo s : relays)
		{
			if (canServe(s, _sources))
				candidateRelays.add(s);
		}

		if (candidateRelays.isEmpty())
			throw new DatabusClientException("No candidate relays for source : " + _sources);

		streamConsumers.addAll(_consumers);

		boolean canConsumerBootstrap = false;
		_streamConsumerRawRegistrations = new ArrayList<DatabusV2ConsumerRegistration>();
		_streamConsumerRawRegistrations.add(new DatabusV2ConsumerRegistration(streamConsumers, _sources, _filterConfig));
		_streamConsumerRawRegistrations.add(new DatabusV2ConsumerRegistration(_loggingConsumer, _sources, _filterConfig));

		for (DatabusCombinedConsumer c : _consumers)
		{
			if ( c.canBootstrap())
			{
				canConsumerBootstrap = true;
				bootstrapConsumers.add(c);
			}
		}

		boolean enableBootstrap = _client.getClientStaticConfig().getRuntime().getBootstrap().isEnabled();
		Set<ServerInfo> candidateBootstrapServers = new HashSet<ServerInfo>();

		if (enableBootstrap && canConsumerBootstrap)
		{
			if ( (null == bootstrapServers) || ( bootstrapServers.isEmpty()))
				throw new DatabusClientException("No configured bootstrap servers in the client to start");

			for (ServerInfo s : bootstrapServers)
			{
				if (canServe(s,_sources))
					candidateBootstrapServers.add(s);
			}

			if (candidateBootstrapServers.isEmpty())
				throw new DatabusClientException("No candidate bootstrap servers for source : " + _sources);

			_bootstrapConsumerRawRegistrations = new ArrayList<DatabusV2ConsumerRegistration>();;
			_bootstrapConsumerRawRegistrations.add(new DatabusV2ConsumerRegistration(bootstrapConsumers, _sources, _filterConfig));
			_bootstrapConsumerRawRegistrations.add(new DatabusV2ConsumerRegistration(_loggingConsumer, _sources, _filterConfig));

		}

		// All validations done. Setup and start
		initializeStatsCollectors();

		DatabusSourcesConnection.StaticConfig connConfig =
				_client.getClientStaticConfig().getConnection(_sources);


		if (null == connConfig)
			connConfig = _client.getClientStaticConfig().getConnectionDefaults();

		DbusEventBuffer.StaticConfig cfg = connConfig.getEventBuffer();
		DbusEventBuffer eventBuffer = new DbusEventBuffer(cfg.getMaxSize(), cfg.getMaxIndividualBufferSize(), cfg.getScnIndexSize(),
				cfg.getReadBufferSize(), cfg.getAllocationPolicy(), new File(cfg.getMmapDirectory().getAbsolutePath() + "_stream_" + _id ),
				cfg.getQueuePolicy(), cfg.getTrace(), null, cfg.getAssertLevel(),
				cfg.getBufferRemoveWaitPeriod(), cfg.getRestoreMMappedBuffers(), cfg.getRestoreMMappedBuffersValidateEvents());

		eventBuffer.setDropOldEvents(true);
		eventBuffer.start(0);

		DbusEventBuffer bootstrapBuffer = null;

		if (enableBootstrap && canConsumerBootstrap)
		{
			bootstrapBuffer = new DbusEventBuffer(cfg.getMaxSize(), cfg.getMaxIndividualBufferSize(), cfg.getScnIndexSize(),
					cfg.getReadBufferSize(), cfg.getAllocationPolicy(), new File(cfg.getMmapDirectory().getAbsolutePath() + "_bootstrap_" + _id ),
					cfg.getQueuePolicy(), cfg.getTrace(), null, cfg.getAssertLevel(),
					cfg.getBufferRemoveWaitPeriod(), cfg.getRestoreMMappedBuffers(), cfg.getRestoreMMappedBuffersValidateEvents());
			bootstrapBuffer.setDropOldEvents(false);
			bootstrapBuffer.start(0);
		}

		List<DatabusSubscription> subs = createSubscriptions(_sources);
		
		if (null != _checkpointPersistenceProvider && _client.getClientStaticConfig().getCheckpointPersistence().isClearBeforeUse())
		{
			_log.info("Clearing checkpoint for sources :" + _sources + " with regId :" + _id);
			_checkpointPersistenceProvider.removeCheckpoint(_sources);
		}
		  
		_sourcesConnection = createConnection(connConfig,subs,candidateRelays,candidateBootstrapServers,eventBuffer,bootstrapBuffer);
		_sourcesConnection.start();
		_state = RegistrationState.STARTED;
		_status.start();

		_state = RegistrationState.STARTED;
		return true;
	}

	private List<DatabusSubscription> createSubscriptions(List<String> sources) 
			throws DatabusClientException
	{
		List<DatabusSubscription> subs = null;
		
		try
		{
			subs = DatabusSubscription.createFromUriList(sources);
		} catch (Exception ex) {
			throw new DatabusClientException(ex);
		} 
		return subs;
	}
	
	/**
	 * Factory method to create sources connection
	 * @param connConfig
	 * @param subs
	 * @param candidateRelays
	 * @param candidateBootstrapServers
	 * @param eventBuffer
	 * @param bootstrapBuffer
	 * @return
	 */
	protected synchronized DatabusSourcesConnection createConnection(StaticConfig connConfig, 
			                                            List<DatabusSubscription> subs, 
			                                            Set<ServerInfo> candidateRelays, 
			                                            Set<ServerInfo> candidateBootstrapServers,
			                                            DbusEventBuffer eventBuffer,
			                                            DbusEventBuffer bootstrapBuffer)
	{
		
		_log.info("Creating Sources Connection : Candidate Relays :" 
		              + candidateRelays + ", CandidateBootstrapServers :"
				      + candidateBootstrapServers + ", Subscriptions :" + subs);
		
		DatabusSourcesConnection sourcesConnection =
				  new DatabusSourcesConnection(
						  connConfig,
						  subs,
						  candidateRelays,
						  candidateBootstrapServers,
						  _streamConsumerRawRegistrations,
						  _bootstrapConsumerRawRegistrations,
						  eventBuffer,
						  bootstrapBuffer,
						  _client.getDefaultExecutorService(),
						  _client.getContainerStatsCollector(),
						  _inboundEventsStatsCollector,
						  _bootstrapEventsStatsCollector,
						  _relayConsumerStats,
						  _bootstrapConsumerStats,
						  _checkpointPersistenceProvider,
						  _client.getRelayConnFactory(),
						  _client.getBootstrapConnFactory(),
						  _client.getHttpStatsCollector(),
						  null, // This should make sure the checkpoint directory structure is compatible with V2.
						  _client,
						  _id.toString()); // Used to uniquely identify logs and mbean name 
		return sourcesConnection;
	}
	
	
	@Override
	public synchronized void shutdown() throws IllegalStateException 
	{

		if (! _state.isRunning())
			throw new IllegalStateException(
					"Registration (" + _id + ") is not in running state to be shutdown. Current state :" + _state);

		_sourcesConnection.unregisterMbeans();
		_sourcesConnection.stop();
		_status.shutdown();
		_state = RegistrationState.SHUTDOWN;
	}

	@Override
	public synchronized void pause() throws IllegalStateException 
	{

		if ( _state == RegistrationState.PAUSED)
			return;

		if ( (_state != RegistrationState.STARTED) && ( _state != RegistrationState.RESUMED))
			throw new IllegalStateException(
					"Registration (" + _id + ") is not in correct state to be paused. Current state :" + _state);

		_sourcesConnection.getConnectionStatus().pause();
		_status.pause();
		_state = RegistrationState.PAUSED;

	}

	@Override
	public synchronized void suspendOnError(Throwable ex) throws IllegalStateException 
	{
		if ( _state == RegistrationState.SUSPENDED_ON_ERROR)
			return;

		if ( !_state.isRunning())
			throw new IllegalStateException(
					"Registration (" + _id + ") is not in correct state to be suspended. Current state :" + _state);

		_sourcesConnection.getConnectionStatus().suspendOnError(ex);
		_status.suspendOnError(ex);
		_state = RegistrationState.SUSPENDED_ON_ERROR;

	}
	
	@Override
	public synchronized void resume() throws IllegalStateException 
	{
		if ( _state == RegistrationState.RESUMED)
			return;

		if ( (_state != RegistrationState.PAUSED) && ( _state != RegistrationState.SUSPENDED_ON_ERROR))
			throw new IllegalStateException(
					"Registration (" + _id + ") is not in correct state to be resumed. Current state :" + _state);

		_sourcesConnection.getConnectionStatus().resume();
		_status.resume();
		_state = RegistrationState.RESUMED;
	}

	@Override
	public RegistrationState getState() {
		return _state;
	}

	@Override
	public synchronized boolean deregister() 
			throws IllegalStateException 
	{
		if ((_state == RegistrationState.DEREGISTERED) || (_state == RegistrationState.INIT))
			return false;

		if ( _state.isRunning())
			shutdown();

		deregisterFromClient();
		_state = RegistrationState.DEREGISTERED;

		return true;
	}

	protected void deregisterFromClient()
	{
		_client.deregister(this);
	}
	
	
	@Override
	public Collection<DatabusSubscription> getSubscriptions() 
	{
		return DatabusSubscription.createSubscriptionList(_sources);
	}

	@Override
	public synchronized DatabusComponentStatus getStatus() 
	{
		return _status;
	}

	@Override
	public synchronized Logger getLogger() {
		return _log;
	}

	@Override
	public DatabusRegistration getParent() {
		return _parent;
	}

	
	protected void setParent(DatabusRegistration parent) {
		_parent = parent;
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

		if (_state.isRunning())
			throw new IllegalStateException("Cannot update server-side filter when registration is in running state. RegId :" + _id 
					+ ", State :" + _state);

		_filterConfig = filterConfig;
		return this;
	}

	@Override
	public List<DbusPartitionInfo> getPartitions() {
		return null;
	}

	@Override
	public Checkpoint getLastPersistedCheckpoint() 
	{
		Checkpoint cp =_checkpointPersistenceProvider.loadCheckpoint(_sources);		
		return cp;
	}

	@Override
	public synchronized boolean storeCheckpoint(Checkpoint ckpt)
			throws IllegalStateException 
	{
		try
		{
			_checkpointPersistenceProvider.storeCheckpoint(_sources, ckpt);
		} catch (IOException ioe) {
			_log.error("Storing checkpoint failed with exception", ioe);
			return false;
		} 
		return true;
	}

	@Override
	public DbusEventsStatisticsCollectorMBean getRelayEventStats() 
	{
		return _inboundEventsStatsCollector;
	}

	@Override
	public DbusEventsStatisticsCollectorMBean getBootstrapEventStats() 
	{
		return _bootstrapEventsStatsCollector;
	}

	@Override
	public ConsumerCallbackStatsMBean getRelayCallbackStats() 
	{
		return _relayConsumerStats;
	}

	@Override
	public ConsumerCallbackStatsMBean getBootstrapCallbackStats() 
	{
		return _bootstrapConsumerStats;
	}

	@Override
	public RelayFindMaxSCNResult fetchMaxSCN(FetchMaxSCNRequest request)
			throws InterruptedException {
		throw new RuntimeException("Not supported yet !!");
	}

	@Override
	public RelayFlushMaxSCNResult flush(RelayFindMaxSCNResult fetchSCNResult,
			FlushRequest flushRequest) throws InterruptedException {
		throw new RuntimeException("Not supported yet !!");
	}

	@Override
	public RelayFlushMaxSCNResult flush(FetchMaxSCNRequest maxScnRequest,
			FlushRequest flushRequest) throws InterruptedException {
		throw new RuntimeException("Not supported yet !!");
	}

	  
	protected synchronized String getStatusName()
	{
	  return "Status" + ((_id != null ) ? "_" + _id.getId() : "");
	}
	
	private static boolean canServe(ServerInfo s, Collection<String> sources)
	{
		List<String> supportedSources = s.getSources();
		
		for (String src : sources)
		{
			if (! supportedSources.contains(src))
				return false;
		}
		
		return true;
	}


	@Override
	public synchronized RegistrationId getRegistrationId() {
		return _id;
	}

	@Override
	public synchronized String toString() {
		return "DatabusV2RegistrationImpl [_state=" + _state + ", _id=" + _id
				+ ", _sources=" + _sources + ", _status=" + _status
				+ ", _filterConfig=" + _filterConfig
				+ ", _streamConsumerRawRegistrations="
				+ _streamConsumerRawRegistrations
				+ ", _bootstrapConsumerRawRegistrations="
				+ _bootstrapConsumerRawRegistrations + "]";
	}

	@Override
	public synchronized DbusKeyCompositeFilterConfig getFilterConfig() {
		return _filterConfig;
	}

	public LoggingConsumer getLoggingConsumer() {
		return _loggingConsumer;
	}	
}
