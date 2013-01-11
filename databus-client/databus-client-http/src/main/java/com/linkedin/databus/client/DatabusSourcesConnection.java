package com.linkedin.databus.client;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.linkedin.databus.client.consumer.BootstrapConsumerCallbackFactory;
import com.linkedin.databus.client.consumer.DatabusV2ConsumerRegistration;
import com.linkedin.databus.client.consumer.MultiConsumerCallback;
import com.linkedin.databus.client.consumer.StreamConsumerCallbackFactory;
import com.linkedin.databus.client.pub.CheckpointPersistenceProvider;
import com.linkedin.databus.client.pub.DatabusCombinedConsumer;
import com.linkedin.databus.client.pub.RegistrationId;
import com.linkedin.databus.client.pub.ServerInfo;
import com.linkedin.databus.client.pub.mbean.ConsumerCallbackStats;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DatabusComponentStatus;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventBuffer.QueuePolicy;
import com.linkedin.databus.core.async.LifecycleMessage;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.data_model.LogicalSource;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus.core.util.NamedThreadFactory;
import com.linkedin.databus.core.util.Range;
import com.linkedin.databus.core.util.UncaughtExceptionTrackingThread;
import com.linkedin.databus2.core.BackoffTimerStaticConfig;
import com.linkedin.databus2.core.BackoffTimerStaticConfigBuilder;
import com.linkedin.databus2.core.container.monitoring.mbean.ContainerStatisticsCollector;
import com.linkedin.databus2.core.container.monitoring.mbean.HttpStatisticsCollector;
import com.linkedin.databus2.core.filter.DbusKeyCompositeFilterConfig;

/**
 * An object that handles the connection for a consumer registration. It may
 * maintain a connection to a relay and/or a bootstrap server.
 */
public class DatabusSourcesConnection {
	public static final int MAX_QUEUED_MESSAGES = 10;
	public static final long MESSAGE_QUEUE_POLL_TIMEOUT_MS = 100;
	public static final int MAX_CONNECT_RETRY_NUM = 3;
	public static final long CONNECT_TIMEOUT_MS = 100;
	public static final long REGISTER_TIMEOUT_MS = 1000;

	public final Logger _log;
	private final String _name;
	private final DatabusSourcesConnection.StaticConfig _connectionConfig;
	private final List<DatabusSubscription> _subscriptions;
	private final RelayPullThread _relayPuller;
	private final GenericDispatcher<DatabusCombinedConsumer> _relayDispatcher;
	private final BootstrapPullThread _bootstrapPuller;
	private final GenericDispatcher<DatabusCombinedConsumer> _bootstrapDispatcher;
	private final DbusEventBuffer _dataEventsBuffer;
	private final DbusEventBuffer _bootstrapEventsBuffer;
	private final ExecutorService _ioThreadPool;
	private final CheckpointPersistenceProvider _checkpointPersistenceProvider;
	private final ContainerStatisticsCollector _containerStatisticsCollector;
	/** Statistics collector about databus events */
	private final DbusEventsStatisticsCollector _inboundEventsStatsCollector;
	private final DbusEventsStatisticsCollector _bootstrapEventsStatsCollector;

	private final HttpStatisticsCollector _relayCallsStatsCollector;
	private final HttpStatisticsCollector _localRelayCallsStatsCollector;
	private final DatabusRelayConnectionFactory _relayConnFactory;
	private final DatabusBootstrapConnectionFactory _bootstrapConnFactory;
	private final List<DatabusV2ConsumerRegistration> _relayConsumers;
	private final ConsumerCallbackStats _relayConsumerStats;
	private final ConsumerCallbackStats _bootstrapConsumerStats;
	private final NannyRunnable _nannyRunnable;

	private final List<DatabusV2ConsumerRegistration> _bootstrapConsumers;
	private final SourcesConnectionStatus _connectionStatus;

	private UncaughtExceptionTrackingThread _relayPullerThread;
	private UncaughtExceptionTrackingThread _relayDispatcherThread;
	private UncaughtExceptionTrackingThread _bootstrapPullerThread;
	private UncaughtExceptionTrackingThread _bootstrapDispatcherThread;
	private final Thread _messageQueuesMonitorThread;
	private Thread _nannyThread;
	private ExecutorService _consumerCallbackExecutor;

	private final boolean _isBootstrapEnabled;
	private final RegistrationId _registrationId;

	public ExecutorService getIoThreadPool() {
		return _ioThreadPool;
	}

	public DatabusSourcesConnection(
			DatabusSourcesConnection.StaticConfig connConfig,
			List<DatabusSubscription> subscriptions, Set<ServerInfo> relays,
			Set<ServerInfo> bootstrapServices,
			List<DatabusV2ConsumerRegistration> consumers,
			List<DatabusV2ConsumerRegistration> bootstrapConsumers,
			DbusEventBuffer dataEventsBuffer,
			DbusEventBuffer bootstrapEventsBuffer,
			ExecutorService ioThreadPool,
			ContainerStatisticsCollector containerStatsCollector,
			DbusEventsStatisticsCollector inboundEventsStatsCollector,
			DbusEventsStatisticsCollector bootstrapEventsStatsCollector,
			ConsumerCallbackStats relayCallbackStats,
			ConsumerCallbackStats bootstrapCallbackStats,
			CheckpointPersistenceProvider checkpointPersistenceProvider,
			DatabusRelayConnectionFactory relayConnFactory,
			DatabusBootstrapConnectionFactory bootstrapConnFactory,
			HttpStatisticsCollector relayCallsStatsCollector,
			RegistrationId registrationId, DatabusHttpClientImpl serverHandle) 
	{
		this(connConfig, 
			subscriptions, 
			relays, 
			bootstrapServices, 
			consumers, 
			bootstrapConsumers, 
			dataEventsBuffer, 
			bootstrapEventsBuffer, 
			ioThreadPool, 
			containerStatsCollector, 
			inboundEventsStatsCollector, 
			bootstrapEventsStatsCollector, 
			relayCallbackStats, 
			bootstrapCallbackStats, 
			checkpointPersistenceProvider, 
			relayConnFactory, 
			bootstrapConnFactory, 
			relayCallsStatsCollector, 
			registrationId, 
			serverHandle, 
			registrationId != null ? registrationId.toString() : null);
	}
	
	public DatabusSourcesConnection(
			DatabusSourcesConnection.StaticConfig connConfig,
			List<DatabusSubscription> subscriptions, Set<ServerInfo> relays,
			Set<ServerInfo> bootstrapServices,
			List<DatabusV2ConsumerRegistration> consumers,
			List<DatabusV2ConsumerRegistration> bootstrapConsumers,
			DbusEventBuffer dataEventsBuffer,
			DbusEventBuffer bootstrapEventsBuffer,
			ExecutorService ioThreadPool,
			ContainerStatisticsCollector containerStatsCollector,
			DbusEventsStatisticsCollector inboundEventsStatsCollector,
			DbusEventsStatisticsCollector bootstrapEventsStatsCollector,
			ConsumerCallbackStats relayCallbackStats,
			ConsumerCallbackStats bootstrapCallbackStats,
			CheckpointPersistenceProvider checkpointPersistenceProvider,
			DatabusRelayConnectionFactory relayConnFactory,
			DatabusBootstrapConnectionFactory bootstrapConnFactory,
			HttpStatisticsCollector relayCallsStatsCollector,
			RegistrationId registrationId, DatabusHttpClientImpl serverHandle,
			String connRawId // Unique Name to be used for generating mbean and logger names.
			) 
	{
		_connectionConfig = connConfig;
		_dataEventsBuffer = dataEventsBuffer;
		_bootstrapEventsBuffer = bootstrapEventsBuffer;
		_subscriptions = subscriptions;
		_ioThreadPool = ioThreadPool;
		_checkpointPersistenceProvider = checkpointPersistenceProvider;
		_containerStatisticsCollector = containerStatsCollector;
		_inboundEventsStatsCollector = inboundEventsStatsCollector;
		_bootstrapEventsStatsCollector = bootstrapEventsStatsCollector;
		_relayConsumerStats = relayCallbackStats;
		_bootstrapConsumerStats = bootstrapCallbackStats;
		_relayConnFactory = relayConnFactory;
		_bootstrapConnFactory = bootstrapConnFactory;
		_relayConsumers = consumers;
		_bootstrapConsumers = bootstrapConsumers;
		_relayCallsStatsCollector = relayCallsStatsCollector;
		_localRelayCallsStatsCollector = null != relayCallsStatsCollector ? relayCallsStatsCollector
				.createForClientConnection(toString()) : null;
		_registrationId = registrationId;
		_name = composeName(connRawId); // will be used as MBean name for
											// example
		_log = Logger.getLogger(DatabusSourcesConnection.class.getName()
				+ ".srcconn-" + _name);
		_connectionStatus = new SourcesConnectionStatus();

		List<DbusKeyCompositeFilterConfig> relayFilterConfigs = new ArrayList<DbusKeyCompositeFilterConfig>();
		List<DbusKeyCompositeFilterConfig> bootstrapFilterConfigs = new ArrayList<DbusKeyCompositeFilterConfig>();

		if (null != consumers) {
			for (DatabusV2ConsumerRegistration c : consumers) {
				DbusKeyCompositeFilterConfig conf = c.getFilterConfig();

				if (null != conf)
					relayFilterConfigs.add(conf);
			}
		}

		if (null != bootstrapConsumers) {
			for (DatabusV2ConsumerRegistration c : bootstrapConsumers) {
				DbusKeyCompositeFilterConfig conf = c.getFilterConfig();

				if (null != conf)
					bootstrapFilterConfigs.add(conf);
			}
		}

		int consumerParallelism = connConfig.getConsumerParallelism();
		if (1 == consumerParallelism) {
			_consumerCallbackExecutor = Executors
					.newSingleThreadExecutor(new NamedThreadFactory("callback"));
		} else {
			_consumerCallbackExecutor = Executors.newFixedThreadPool(
					consumerParallelism, new NamedThreadFactory("callback"));
		}

		MultiConsumerCallback<DatabusCombinedConsumer> relayAsyncCallback = new MultiConsumerCallback<DatabusCombinedConsumer>(
				(null != _relayConsumers ? _relayConsumers
						: new ArrayList<DatabusV2ConsumerRegistration>()),
				_consumerCallbackExecutor,
				connConfig.getConsumerTimeBudgetMs(),
				new StreamConsumerCallbackFactory(), _relayConsumerStats);

		MultiConsumerCallback<DatabusCombinedConsumer> bootstrapAsyncCallback = new MultiConsumerCallback<DatabusCombinedConsumer>(
				(null != _bootstrapConsumers ? _bootstrapConsumers
						: new ArrayList<DatabusV2ConsumerRegistration>()),
				_consumerCallbackExecutor,
				connConfig.getConsumerTimeBudgetMs(),
				new BootstrapConsumerCallbackFactory(), _bootstrapConsumerStats);

		if (_bootstrapEventsBuffer != null) {
			_bootstrapPuller = new BootstrapPullThread(_name
					+ "-BootstrapPuller", this, _bootstrapEventsBuffer,
					bootstrapServices, bootstrapFilterConfigs,
					connConfig.getPullerUtilizationPct(),
					ManagementFactory.getPlatformMBeanServer());
		} else {
			_bootstrapPuller = null;
		}

		_relayDispatcher = new RelayDispatcher(_name + "-RelayDispatcher",
				connConfig, getSubscriptions(), checkpointPersistenceProvider,
				dataEventsBuffer, relayAsyncCallback, _bootstrapPuller,
				ManagementFactory.getPlatformMBeanServer(), serverHandle,
				_registrationId);

		_relayPuller = new RelayPullThread(_name + "-RelayPuller", this,
				_dataEventsBuffer, relays, relayFilterConfigs,
				connConfig.getConsumeCurrent(),
				connConfig.isReadLatestScnOnErrorEnabled(),
				connConfig.getPullerUtilizationPct(),
				ManagementFactory.getPlatformMBeanServer());

		_relayPuller.enqueueMessage(LifecycleMessage.createStartMessage());

		if (_bootstrapEventsBuffer != null) {
			_bootstrapDispatcher = new BootstrapDispatcher(_name
					+ "-BootstrapDispatcher", connConfig, getSubscriptions(),
					checkpointPersistenceProvider, bootstrapEventsBuffer,
					bootstrapAsyncCallback, _relayPuller,
					ManagementFactory.getPlatformMBeanServer(), serverHandle,
					_registrationId);
		} else {
			_bootstrapDispatcher = null;
		}

		_messageQueuesMonitorThread = new Thread(new MessageQueuesMonitor());
		_messageQueuesMonitorThread.setDaemon(true);

		_isBootstrapEnabled = !(null == getBootstrapServices()
				|| getBootstrapServices().isEmpty()
				|| null == getBootstrapConsumers()
				|| 0 == getBootstrapConsumers().size() || _bootstrapEventsBuffer == null);

		_log.info(" Is Service Empty : "
				+ (null == getBootstrapServices() || getBootstrapServices()
						.isEmpty()));
		_log.info(" Is Consumers Empty : "
				+ (null == getBootstrapConsumers() || 0 == getBootstrapConsumers()
						.size()));

		_nannyRunnable = new NannyRunnable();
	}

	// figure out name for the connection - to be used in mbean
	private String composeName(String id) {
		StringBuilder shortSourcesListBuilder = new StringBuilder();
		String separatorChar = "[";
		for (DatabusSubscription sub : getSubscriptions()) {
			shortSourcesListBuilder.append(separatorChar);
			PhysicalPartition p = sub.getPhysicalPartition();
			String sourceName = "AnySource";
			LogicalSource source = sub.getLogicalPartition().getSource();
			if (!source.isAllSourcesWildcard()) {
				sourceName = source.getName();
				int lastDotIdx = sourceName.lastIndexOf('.');
				if (lastDotIdx >= 0)
					sourceName = sourceName.substring(lastDotIdx + 1);
			}
			String partString = "AnyPPart_";
			if (!p.isAnyPartitionWildcard()) {
				partString = p.getId() + "_";
			}
			shortSourcesListBuilder.append(partString + sourceName);
			separatorChar = "_";
		}
		shortSourcesListBuilder.append(']');
		String shortSourcesList = shortSourcesListBuilder.toString();

		return "conn" + shortSourcesList
				+ (id == null ? "" : "_" + id);
	}

	public boolean isBootstrapEnabled() {
		return _isBootstrapEnabled;
	}

	public void start() {
		_log.info("Starting http relay connection for sources:"
				+ _subscriptions);
		_nannyThread = new Thread(_nannyRunnable, _name + ".Nanny");
		_nannyThread.setDaemon(true);

		_connectionStatus.start();
		_messageQueuesMonitorThread.start();
		_nannyThread.start();
	}

	public boolean isRunning() {
		boolean pullThreadRunning = _relayPullerThread.isAlive();
		boolean dispatcherThreadRunning = _relayDispatcherThread.isAlive();

		if (!pullThreadRunning)
			_log.info("Pull thread is DEAD!");
		if (null != _relayPullerThread.getLastException()) {
			_log.error(" Reason: "
					+ _relayPullerThread.getLastException().getMessage(),
					_relayPullerThread.getLastException());
		}

		if (!dispatcherThreadRunning)
			_log.info("Dispatch thread is DEAD!");
		if (null != _relayDispatcherThread.getLastException()) {
			_log.error(" Reason: "
					+ _relayDispatcherThread.getLastException().getMessage(),
					_relayDispatcherThread.getLastException());
		}

		return pullThreadRunning && dispatcherThreadRunning;
	}

	public void await() {
		boolean running = isRunning();
		_log.info("waiting for shutdown: " + running);
		while (running) {
			_relayPuller.awaitShutdown();
			_relayDispatcher.awaitShutdown();

			running = isRunning();
			_log.info("waiting for shutdown: " + running);
		}
	}

	public void stop() {
		_log.info("Stopping ... :" + isRunning());

		if (null != _relayConsumerStats)
			_relayConsumerStats.unregisterAsMbean();
		if (null != _bootstrapConsumerStats)
			_bootstrapConsumerStats.unregisterAsMbean();
		_connectionStatus.shutdown();

	    if (_relayPullerThread.isAlive()) _relayPuller.awaitShutdown();
	    if (_relayDispatcherThread.isAlive()) _relayDispatcher.awaitShutdown();

		_relayDispatcher.awaitShutdown();
		if (_isBootstrapEnabled) {
	    	if (_bootstrapDispatcherThread.isAlive()) _bootstrapDispatcher.awaitShutdown();
	    	if (_bootstrapPullerThread.isAlive()) _bootstrapPuller.awaitShutdown();
		}

		_consumerCallbackExecutor.shutdown();

		_log.info("Stopped ... ");
	}

	public List<String> getSourcesNames() {
		return DatabusSubscription.getStrList(_subscriptions);
	}

	public List<DatabusSubscription> getSubscriptions() {
		return _subscriptions;
	}

	public ConsumerCallbackStats getRelayConsumerStats() {
		return _relayConsumerStats;
	}

	public ConsumerCallbackStats getBootstrapConsumerStats() {
		return _bootstrapConsumerStats;
	}

	public static void main(String args[]) throws Exception {
	}

	public DatabusComponentStatus getConnectionStatus() {
		return _connectionStatus;
	}

	public BootstrapPullThread getBootstrapPuller() {
		return _bootstrapPuller;
	}

	public GenericDispatcher<DatabusCombinedConsumer> getBootstrapDispatcher() {
		return _bootstrapDispatcher;
	}

	public CheckpointPersistenceProvider getCheckpointPersistenceProvider() {
		return _checkpointPersistenceProvider;
	}

	public ContainerStatisticsCollector getContainerStatisticsCollector() {
		return _containerStatisticsCollector;
	}

	public Set<ServerInfo> getRelays() {
		return (_relayPuller != null) ? _relayPuller.getServers() : null;
	}

	public Set<ServerInfo> getBootstrapServices() {
		return (_bootstrapPuller != null) ? _bootstrapPuller.getServers()
				: null;
	}

	public DbusEventsStatisticsCollector getInboundEventsStatsCollector() {
		return _inboundEventsStatsCollector;
	}

	public GenericDispatcher<DatabusCombinedConsumer> getRelayDispatcher() {
		return _relayDispatcher;
	}

	public DatabusRelayConnectionFactory getRelayConnFactory() {
		return _relayConnFactory;
	}

	public DatabusBootstrapConnectionFactory getBootstrapConnFactory() {
		return _bootstrapConnFactory;
	}

	public DbusEventBuffer getDataEventsBuffer() {
		return _dataEventsBuffer;
	}

	DbusEventBuffer getBootstrapEventsBuffer() {
		return _bootstrapEventsBuffer;
	}

	public Checkpoint loadPersistentCheckpoint() {
		if (_checkpointPersistenceProvider != null)
			return _checkpointPersistenceProvider.loadCheckpointV3(
					getSubscriptions(), _registrationId);
		Checkpoint cp = Checkpoint.createFlexibleCheckpoint();
		return cp;
	}

	public List<DatabusV2ConsumerRegistration> getBootstrapConsumers() {
		return _bootstrapConsumers;
	}

	public DatabusSourcesConnection.StaticConfig getConnectionConfig() {
		return _connectionConfig;
	}

	public List<DatabusV2ConsumerRegistration> getRelayConsumers() {
		return _relayConsumers;
	}

	class NannyRunnable implements Runnable {
		public static final int SLEEP_DURATION_MS = 1000;

		@Override
		public void run() {
			while (getConnectionStatus().getStatus() != DatabusComponentStatus.Status.SHUTDOWN) {
				boolean runShutdown = false;
				if (null != _relayPuller
						&& _relayPuller.getComponentStatus().getStatus() == DatabusComponentStatus.Status.SHUTDOWN) {
					_log.error("nanny: detected that the relay puller is shutdown!");
					runShutdown = true;
				}
				if (null != _relayDispatcher
						&& _relayDispatcher.getComponentStatus().getStatus() == DatabusComponentStatus.Status.SHUTDOWN) {
					_log.error("nanny: detected that the relay dispatcher is shutdown!");
					runShutdown = true;
				}
				if (null != _bootstrapPuller
						&& _bootstrapPuller.getComponentStatus().getStatus() == DatabusComponentStatus.Status.SHUTDOWN) {
					_log.error("nanny: detected that the bootstrap puller is shutdown!");
					runShutdown = true;
				}
				if (null != _bootstrapDispatcher
						&& _bootstrapDispatcher.getComponentStatus()
								.getStatus() == DatabusComponentStatus.Status.SHUTDOWN) {
					_log.error("nanny: detected that the bootstrap dispatcher is shutdown!");
					runShutdown = true;
				}

				if (runShutdown) {
					stop();
				}

				try {
					Thread.sleep(SLEEP_DURATION_MS);
				} catch (InterruptedException e) {
					_log.info("nanny: who woke me up?");
				}
			}
		}

	}

	public class SourcesConnectionStatus extends DatabusComponentStatus {
		public SourcesConnectionStatus() {
			super(DatabusSourcesConnection.this._name);
		}

		@Override
		public void start() {
			super.start();

			_relayPullerThread = new UncaughtExceptionTrackingThread(
					_relayPuller, _relayPuller.getName());
			_relayPullerThread.setDaemon(true);
			_relayPullerThread.start();

			_relayDispatcherThread = new UncaughtExceptionTrackingThread(
					_relayDispatcher, _relayDispatcher.getName());
			_relayDispatcherThread.setDaemon(true);
			_relayDispatcherThread.start();

			if (_isBootstrapEnabled) {
				_bootstrapPullerThread = new UncaughtExceptionTrackingThread(
						_bootstrapPuller, _bootstrapPuller.getName());
				_bootstrapPullerThread.setDaemon(true);
				_bootstrapPullerThread.start();

				_bootstrapDispatcherThread = new UncaughtExceptionTrackingThread(
						_bootstrapDispatcher, _bootstrapDispatcher.getName());
				_bootstrapDispatcherThread.setDaemon(true);
				_bootstrapDispatcherThread.start();
			}
		}

		@Override
		public void shutdown() {
			_relayPuller.shutdown();
			_relayDispatcher.shutdown();
			if (_bootstrapPuller != null)
				_bootstrapPuller.shutdown();
			if (_bootstrapDispatcher != null)
				_bootstrapDispatcher.shutdown();

			_relayPullerThread.interrupt();
			_relayDispatcherThread.interrupt();

			if (_isBootstrapEnabled) {
				_bootstrapPullerThread.interrupt();
				_bootstrapDispatcherThread.interrupt();
			}

			super.shutdown();
			_nannyThread.interrupt();
		}

		@Override
		public void pause() {
			_relayPuller.enqueueMessage(LifecycleMessage.createPauseMessage());
			if (_isBootstrapEnabled) {
				_bootstrapPuller.enqueueMessage(LifecycleMessage
						.createPauseMessage());
			}

			super.pause();
		}

		@Override
		public void resume() {
			_relayPuller.enqueueMessage(LifecycleMessage.createResumeMessage());
			if (_isBootstrapEnabled) {
				_bootstrapPuller.enqueueMessage(LifecycleMessage
						.createResumeMessage());
			}

			super.resume();
		}

		@Override
		public void suspendOnError(Throwable cause) {
			_relayPuller.enqueueMessage(LifecycleMessage
					.createSuspendOnErroMessage(cause));
			if (_isBootstrapEnabled) {
				_bootstrapPuller.enqueueMessage(LifecycleMessage
						.createSuspendOnErroMessage(cause));
			}

			super.suspendOnError(cause);
		}
	}

	public static class StaticConfig {
		private final DbusEventBuffer.StaticConfig _eventBuffer;
		private final long _consumerTimeBudgetMs;
		private final int _consumerParallelism;
		private final double _checkpointThresholdPct;
		private final Range _keyRange;
		private final BackoffTimerStaticConfig _bsPullerRetriesBeforeCkptCleanup;
		private final BackoffTimerStaticConfig _pullerRetries;
		private final BackoffTimerStaticConfig _dispatcherRetries;
		private final int _freeBufferThreshold;
		private final boolean _consumeCurrent;
		private final boolean _readLatestScnOnError;
		private final double _pullerBufferUtilizationPct;
		private final int _id;
		private final boolean _enablePullerMessageQueueLogging;
		private final int _numRetriesOnFallOff;

		public StaticConfig(DbusEventBuffer.StaticConfig eventBuffer,
				long consumerTimeBudgetMs, int consumerParallelism,
				double checkpointThresholdPct, Range keyRange,
				BackoffTimerStaticConfig bsPullerRetriesBeforeCkptCleanup,
				BackoffTimerStaticConfig pullerRetries,
				BackoffTimerStaticConfig dispatcherRetries,
				int retriesOnFellOff, int freeBufferThreshold,
				boolean consumeCurrent, boolean readLatestScnOnError,
				double pullerBufferUtilizationPct, int id,
				boolean enablePullerMessageQueueLogging) {
			super();
			_eventBuffer = eventBuffer;
			_consumerTimeBudgetMs = consumerTimeBudgetMs;
			_consumerParallelism = consumerParallelism;
			_checkpointThresholdPct = checkpointThresholdPct;
			_keyRange = keyRange;
			_bsPullerRetriesBeforeCkptCleanup = bsPullerRetriesBeforeCkptCleanup;
			_pullerRetries = pullerRetries;
			_dispatcherRetries = dispatcherRetries;
			_numRetriesOnFallOff = retriesOnFellOff;
			_freeBufferThreshold = freeBufferThreshold;
			_consumeCurrent = consumeCurrent;
			_readLatestScnOnError = readLatestScnOnError;
			_pullerBufferUtilizationPct = pullerBufferUtilizationPct;
			_id = id;
			_enablePullerMessageQueueLogging = enablePullerMessageQueueLogging;
		}

		public boolean getReadLatestScnOnError() {
			return _readLatestScnOnError;
		}

		public boolean isReadLatestScnOnErrorEnabled() {
			return _readLatestScnOnError;
		}

		public double getPullerUtilizationPct() {
			return _pullerBufferUtilizationPct;
		}

		public int getId() {
			return _id;
		}

		public boolean getConsumeCurrent() {
			return _consumeCurrent;
		}

		/** The relay event buffer static configuration */
		public DbusEventBuffer.StaticConfig getEventBuffer() {
			return _eventBuffer;
		}

		/**
		 * Max time in milliseconds that a consumer should use to process an
		 * event before it is considered failed
		 */
		public long getConsumerTimeBudgetMs() {
			return _consumerTimeBudgetMs;
		}

		/**
		 * Max number of consumers that can be called in parallel to process an
		 * event
		 */
		public int getConsumerParallelism() {
			return _consumerParallelism;
		}

		/**
		 * The percentage of event buffer occupancy that will trigger a
		 * checkpoint attempt. This is to ensure that we can make progress in
		 * large event windows without having to reprocess them entirely in case
		 * of a failure.
		 */
		public double getCheckpointThresholdPct() {
			return _checkpointThresholdPct;
		}

		public Range getKeyRange() {
			return _keyRange;
		}

		/**
		 * Pull requests and error retries configuration when talking to the
		 * relays or bootstrap servers
		 */
		public BackoffTimerStaticConfig getPullerRetries() {
			return _pullerRetries;
		}

		/** Error retries configuration calling the consumer code */
		public BackoffTimerStaticConfig getDispatcherRetries() {
			return _dispatcherRetries;
		}

		/**
		 * This config controls how many retries will be made on the same
		 * bootstrap Server before switching and clearing the checkpoint
		 */
		public BackoffTimerStaticConfig getBsPullerRetriesBeforeCkptCleanup() {
			return _bsPullerRetriesBeforeCkptCleanup;
		}

		/**
		 * This config controls how many retries will be made when it received
		 * ScnNotFoundException before
		 * bootstrapping/suspending/reading-latest-event
		 */
		public int getNumRetriesOnFallOff() {
			return _numRetriesOnFallOff;
		}

		/**
		 * Minimum number of bytes that need to be available in the buffer
		 * before the Puller's can request for more events. Ideally this is more
		 * than max event size
		 */
		public int getFreeBufferThreshold() {
			return _freeBufferThreshold;
		}

		public boolean isPullerMessageQueueLoggingEnabled() {
			return _enablePullerMessageQueueLogging;
		}

		@Override
		public String toString() {
			return "StaticConfig [_eventBuffer=" + _eventBuffer
					+ ", _consumerTimeBudgetMs=" + _consumerTimeBudgetMs
					+ ", _consumerParallelism=" + _consumerParallelism
					+ ", _checkpointThresholdPct=" + _checkpointThresholdPct
					+ ", _keyRange=" + _keyRange
					+ ", _bsPullerRetriesBeforeCkptCleanup="
					+ _bsPullerRetriesBeforeCkptCleanup + ", _pullerRetries="
					+ _pullerRetries + ", _dispatcherRetries="
					+ _dispatcherRetries + ", _freeBufferThreshold="
					+ _freeBufferThreshold
					+ ", _enablePullerMessageQueueLogging="
					+ _enablePullerMessageQueueLogging + "]";
		}
	}

	public static class Config implements ConfigBuilder<StaticConfig> {
		private static final long DEFAULT_KEY_RANGE_MIN = -1L;
		private static final long DEFAULT_KEY_RANGE_MAX = -1L;

		private static final long DEFAULT_MAX_BUFFER_SIZE = 10 * 1024 * 1024;
		private static final int DEFAULT_MAX_READBUFFER_SIZE = 1024 * 1024;
		private static final int DEFAULT_MAX_SCNINDEX_SIZE = 1024 * 1024;
		private static final boolean DEFAULT_PULLER_MESSAGE_QUEUE_LOGGING = false;

		private static int DEFAULT_MAX_RETRY_NUM = 3;
		private static int DEFAULT_INIT_SLEEP = 100;
		private static double DEFAULT_SLEEP_INC_FACTOR = 1.1;

		// Default Sleep : InitSleep : 1 sec, then keep incrementing 1 sec for
		// subsequent retry, upto 25 retries.
		private static int DEFAULT_BSPULLER_CKPTCLEANUP_MAX_RETRY_NUM = 25;
		private static int DEFAULT_BSPULLER_CKPTCLEANUP_INIT_SLEEP = 1 * 1000;
		private static int DEFAULT_BSPULLER_CKPTCLEANUP_SLEEP_INC_DELTA = 1000;
		private static int DEFAULT_BSPULLER_CKPTCLEANUP_SLEEP_INC_FACTOR = 1;

		// Default Config woul be to retry 5 times w
		private static int DEFAULT_RETRY_ON_FELLOFF_MAX_RETRY_NUM = 5;

		private DbusEventBuffer.Config _eventBuffer;
		private long _consumerTimeBudgetMs = 300000;
		private int _consumerParallelism = 1;
		private double _checkpointThresholdPct;
		private long _keyMin;
		private long _keyMax;
		private BackoffTimerStaticConfigBuilder _bsPullerRetriesBeforeCkptCleanup;
		private BackoffTimerStaticConfigBuilder _pullerRetries;
		private BackoffTimerStaticConfigBuilder _dispatcherRetries;
		private int _numRetriesOnFallOff;
		// max size event should be less than this ; default to 10K
		private int _freeBufferThreshold = 10000;
		private boolean _consumeCurrent = false;
		private boolean _readLatestScnOnError = false;
		private double _pullerBufferUtilizationPct = 100.0;
		private int _id;
		private boolean _enablePullerMessageQueueLogging;

		public Config() {
			_eventBuffer = new DbusEventBuffer.Config();
			_eventBuffer.setQueuePolicy(QueuePolicy.BLOCK_ON_WRITE.toString());
			_eventBuffer.setEnableScnIndex(false);
			_eventBuffer.setDefaultMemUsage(0.1);
			if (_eventBuffer.getMaxSize() > DEFAULT_MAX_BUFFER_SIZE) {
				_eventBuffer.setMaxSize(DEFAULT_MAX_BUFFER_SIZE);
			}

			if (_eventBuffer.getReadBufferSize() > DEFAULT_MAX_READBUFFER_SIZE) {
				_eventBuffer.setReadBufferSize(DEFAULT_MAX_READBUFFER_SIZE);
			}

			if (_eventBuffer.getScnIndexSize() > DEFAULT_MAX_SCNINDEX_SIZE) {
				_eventBuffer.setScnIndexSize(DEFAULT_MAX_SCNINDEX_SIZE);
			}

			_checkpointThresholdPct = 75.0;
			_keyMin = DEFAULT_KEY_RANGE_MIN;
			_keyMax = DEFAULT_KEY_RANGE_MAX;

			_pullerRetries = new BackoffTimerStaticConfigBuilder();
			_pullerRetries.setInitSleep(DEFAULT_INIT_SLEEP);
			_pullerRetries.setSleepIncFactor(DEFAULT_SLEEP_INC_FACTOR);
			_pullerRetries.setMaxRetryNum(DEFAULT_MAX_RETRY_NUM);

			_bsPullerRetriesBeforeCkptCleanup = new BackoffTimerStaticConfigBuilder();
			_bsPullerRetriesBeforeCkptCleanup
					.setInitSleep(DEFAULT_BSPULLER_CKPTCLEANUP_INIT_SLEEP);
			_bsPullerRetriesBeforeCkptCleanup
					.setSleepIncDelta(DEFAULT_BSPULLER_CKPTCLEANUP_SLEEP_INC_DELTA);
			_bsPullerRetriesBeforeCkptCleanup
					.setMaxRetryNum(DEFAULT_BSPULLER_CKPTCLEANUP_MAX_RETRY_NUM);
			_bsPullerRetriesBeforeCkptCleanup
					.setSleepIncFactor(DEFAULT_BSPULLER_CKPTCLEANUP_SLEEP_INC_FACTOR);

			_numRetriesOnFallOff = DEFAULT_RETRY_ON_FELLOFF_MAX_RETRY_NUM;

			_dispatcherRetries = new BackoffTimerStaticConfigBuilder();
			_dispatcherRetries.setSleepIncFactor(1.1);
			_dispatcherRetries.setMaxRetryNum(-1);
			_enablePullerMessageQueueLogging = DEFAULT_PULLER_MESSAGE_QUEUE_LOGGING;
		}

		public Config(Config other) {
			_eventBuffer = new DbusEventBuffer.Config(other.getEventBuffer());
		}

		public DbusEventBuffer.Config getEventBuffer() {
			return _eventBuffer;
		}

		public void setEventBuffer(DbusEventBuffer.Config eventBuffer) {
			_eventBuffer = eventBuffer;
		}

		@Override
		public StaticConfig build() throws InvalidConfigException {
			Range keyRange = null;
			if ((_keyMin >= 0) && (_keyMax > 0)) {
				keyRange = new Range(_keyMin, _keyMax);
			}

			if (getConsumerTimeBudgetMs() < 0) {
				throw new InvalidConfigException(
						"Invalid consumer time budget:"
								+ getConsumerTimeBudgetMs());
			}
			if (getConsumerParallelism() < 1) {
				throw new InvalidConfigException(
						"Invalid consumer parallelism:"
								+ getConsumerParallelism());
			}

			StaticConfig config = new StaticConfig(_eventBuffer.build(),
					getConsumerTimeBudgetMs(), getConsumerParallelism(),
					getCheckpointThresholdPct(), keyRange,
					_bsPullerRetriesBeforeCkptCleanup.build(),
					_pullerRetries.build(), _dispatcherRetries.build(),
					_numRetriesOnFallOff, _freeBufferThreshold,
					_consumeCurrent, _readLatestScnOnError,
					_pullerBufferUtilizationPct, _id,
					_enablePullerMessageQueueLogging);
			long bufferCapacityInBytes = config.getEventBuffer().getMaxSize();
			long maxWindowSizeInBytes = (long) ((config
					.getCheckpointThresholdPct() / 100.0) * bufferCapacityInBytes);
			if ((maxWindowSizeInBytes + _freeBufferThreshold) > bufferCapacityInBytes) {
				throw new InvalidConfigException(
						"Invalid configuration. Could lead to deadlock: ((checkPointThresholdPct*maxSize) + freeBufferThreshold) > maxSize");
			}
			return config;
		}

		public boolean getReadLatestScnOnError() {
			return _readLatestScnOnError;
		}

		public void setReadLatestScnOnError(boolean r) {
			_readLatestScnOnError = r;
		}

		public int getId() {
			return _id;
		}

		public void setId(int id) {
			_id = id;
		}

		public double getPullerBufferUtilizationPct() {
			return _pullerBufferUtilizationPct;
		}

		public void setPullerBufferUtilizationPct(double p) {
			_pullerBufferUtilizationPct = p;
		}

		public boolean getConsumeCurrent() {
			return _consumeCurrent;
		}

		public void setConsumeCurrent(boolean currentConsume) {
			_consumeCurrent = currentConsume;
		}

		public long getConsumerTimeBudgetMs() {
			return _consumerTimeBudgetMs;
		}

		public void setConsumerTimeBudgetMs(long consumerTimeBudgetMs) {
			_consumerTimeBudgetMs = consumerTimeBudgetMs;
		}

		public int getConsumerParallelism() {
			return _consumerParallelism;
		}

		public void setConsumerParallelism(int consumerParallelism) {
			_consumerParallelism = consumerParallelism;
		}

		public double getCheckpointThresholdPct() {
			return _checkpointThresholdPct;
		}

		public void setCheckpointThresholdPct(double checkpointThresholdPct) {
			_checkpointThresholdPct = checkpointThresholdPct;
		}

		public long getKeyMin() {
			return _keyMin;
		}

		public int getFreeBufferThreshold() {
			return _freeBufferThreshold;
		}

		public void setFreeBufferThreshold(int freeBufferThreshold) {
			_freeBufferThreshold = freeBufferThreshold;
		}

		public void setKeyMin(long keyMin) {
			_keyMin = keyMin;
		}

		public long getKeyMax() {
			return _keyMax;
		}

		public void setKeyMax(long keyMax) {
			_keyMax = keyMax;
		}

		public BackoffTimerStaticConfigBuilder getBsPullerRetriesBeforeCkptCleanup() {
			return _bsPullerRetriesBeforeCkptCleanup;
		}

		public int getNumRetriesOnFallOff() {
			return _numRetriesOnFallOff;
		}

		public void setNumRetriesOnFallOff(int numRetriesOnFallOff) {
			_numRetriesOnFallOff = numRetriesOnFallOff;
		}

		public BackoffTimerStaticConfigBuilder getPullerRetries() {
			return _pullerRetries;
		}

		public BackoffTimerStaticConfigBuilder getDispatcherRetries() {
			return _dispatcherRetries;
		}

		public boolean getEnablePullerMessageQueueLogging() {
			return _enablePullerMessageQueueLogging;
		}

		public void setEnablePullerMessageQueueLogging(
				boolean enablePullerMessageQueueLogging) {
			_enablePullerMessageQueueLogging = enablePullerMessageQueueLogging;
		}

	}

	public DbusEventsStatisticsCollector getBootstrapEventsStatsCollector() {
		return _bootstrapEventsStatsCollector;
	}

	public HttpStatisticsCollector getRelayCallsStatsCollector() {
		return _relayCallsStatsCollector;
	}

	public HttpStatisticsCollector getLocalRelayCallsStatsCollector() {
		return _localRelayCallsStatsCollector;
	}

	public RelayPullThread getRelayPullThread() {
		return _relayPuller;
	}

	public BootstrapPullThread getBootstrapPullThread() {
		return _bootstrapPuller;
	}

	class MessageQueuesMonitor implements Runnable {
		private static final long ERROR_SLEEP_MS = 60000;
		private static final long INFO_SLEEP_MS = 1000;
		private static final long DEBUG_SLEEP_MS = 100;
		private static final long TRACE_SLEEP_MS = 10;

		private String _lastMessage;

		@Override
		public void run() {
			while (_connectionStatus.getStatus() != DatabusComponentStatus.Status.SHUTDOWN) {
				StringBuilder sb = new StringBuilder(1000);

				if (null != _relayPuller)
					_relayPuller.getQueueListString(sb);
				sb.append(' ');
				if (null != _relayDispatcher)
					_relayDispatcher.getQueueListString(sb);
				sb.append(' ');
				if (null != _bootstrapPuller)
					_bootstrapPuller.getQueueListString(sb);
				sb.append(' ');
				if (null != _bootstrapDispatcher)
					_bootstrapDispatcher.getQueueListString(sb);

				String newMessage = sb.toString();
				if (!newMessage.equals(_lastMessage)) {
					_log.info(newMessage);
					_lastMessage = newMessage;
				}

				long sleepDuration = ERROR_SLEEP_MS;
				Level logLevel = _log.getEffectiveLevel();
				if (Level.TRACE == logLevel)
					sleepDuration = TRACE_SLEEP_MS;
				else if (Level.DEBUG == logLevel)
					sleepDuration = DEBUG_SLEEP_MS;
				else if (Level.INFO == logLevel)
					sleepDuration = INFO_SLEEP_MS;

				try {
					Thread.sleep(sleepDuration);
				} catch (InterruptedException ie) {
				}
			}
		}
	}

	public void removeRegistration(DatabusV2ConsumerRegistration reg) {
		_relayDispatcher.getAsyncCallback().removeRegistration(reg);
	}

	public void unregisterMbeans() {
		if (_relayConsumerStats != null) {
			_relayConsumerStats.unregisterAsMbean();
		}
		if (_bootstrapConsumerStats != null) {
			_bootstrapConsumerStats.unregisterAsMbean();
		}
	}

}
