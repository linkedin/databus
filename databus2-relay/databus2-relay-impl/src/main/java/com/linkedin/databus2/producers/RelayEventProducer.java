package com.linkedin.databus2.producers;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;
import org.xeril.util.TimeOutException;

import com.linkedin.databus.client.DatabusBootstrapConnectionFactory;
import com.linkedin.databus.client.DatabusRelayConnectionFactory;
import com.linkedin.databus.client.DatabusSourcesConnection;
import com.linkedin.databus.client.consumer.DatabusV2ConsumerRegistration;
import com.linkedin.databus.client.generic.DatabusConsumerEventBuffer;
import com.linkedin.databus.client.netty.NettyHttpConnectionFactory;
import com.linkedin.databus.client.pub.DatabusCombinedConsumer;
import com.linkedin.databus.client.pub.ServerInfo;
import com.linkedin.databus.client.pub.ServerInfo.ServerInfoBuilder;
import com.linkedin.databus.client.pub.mbean.ConsumerCallbackStats;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DbusClientMode;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventBufferAppendable;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus.core.util.NamedThreadFactory;
import com.linkedin.databus.core.util.RngUtils;
import com.linkedin.databus.monitoring.mbean.EventSourceStatistics;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.seq.MaxSCNReaderWriter;
import com.linkedin.databus2.producers.db.ReadEventCycleSummary;
import com.linkedin.databus2.relay.config.LogicalSourceStaticConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;

/*
 * Relay that uses DatabusSources connection (the client library to connect to other relays - Relay Chaining )
 * Operates with parameters used by other 'Event Producers' including RelayConfig
 */

public class RelayEventProducer implements EventProducer
{

	private DatabusSourcesConnection _dbusConnection = null;
	private String _name = null;
	private DbusEventsStatisticsCollector _statsCollector = null;
	private DatabusConsumerEventBuffer _consumerEventBuffer = null;
	private MaxSCNReaderWriter _scnReaderWriter ;

	// state to capture relay logging and stats
	private Logger _eventsLog;
	private RelayStatsAdapter _relayStatsAdapter;

	// relay logger thread;
	private RelayLogger _relayLogger;

	//netty threadpool artifactes
	private DatabusClientNettyThreadPools _nettyThreadPools;
	private long _restartScnOffset = 0 ;


	public static final String MODULE = RelayEventProducer.class.getName();
	public static final Logger LOG = Logger.getLogger(MODULE);

	public RelayEventProducer(PhysicalSourceStaticConfig config,
			DbusEventBufferAppendable consumerBuffer,
			DbusEventsStatisticsCollector statsCollector,
			MaxSCNReaderWriter scnReaderWriter)
	{
		this(config,consumerBuffer,statsCollector,scnReaderWriter,null);
	}

	public RelayEventProducer(PhysicalSourceStaticConfig config,
			DbusEventBufferAppendable consumerBuffer,
			DbusEventsStatisticsCollector statsCollector,
			MaxSCNReaderWriter scnReaderWriter,
			DatabusClientNettyThreadPools nettyThreadpools)
	{

		try
		{
			_name = config.getName();
			_statsCollector = statsCollector;
			_scnReaderWriter = scnReaderWriter;
			_relayStatsAdapter = new RelayStatsAdapter(_name, statsCollector);
			_eventsLog = Logger
					.getLogger("com.linkedin.databus2.producers.db.events."
							+ _name);
			_restartScnOffset = config.getRestartScnOffset();

			int largestEventSize = config.getLargestEventSizeInBytes();
			//let the internal buffer contain at least 2 windows
			long internalBufferSize = 2 * config.getLargestWindowSizeInBytes() ;
			//10s : write to buffers should be fast; the only exception is when scn's are saved. That's why 10s
			long consumerTimeoutMs = 10 * 1000;
			//15s
			long connTimeoutMs = 15 * 1000;
			long pollIntervalMs = config.getRetries().getInitSleep();
			int consumerParallelism = 1;

			// create logger
			_relayLogger = new RelayLogger(5*1000, "RelayLogger");

			// create consumer;
			 _consumerEventBuffer = new DatabusConsumerEventBuffer(consumerBuffer, statsCollector,scnReaderWriter);

			// get subscription info
			String subscriptionString = createSubscriptionString(config);
			LOG.info("Subscription string=" + subscriptionString);



			 int id = (RngUtils.randomPositiveInt() % 10000) + 1;
			_nettyThreadPools = _nettyThreadPools == null ? DatabusClientNettyThreadPools.createNettyThreadPools(id) : nettyThreadpools;
			_dbusConnection = createDatabusSourcesConnection(_name, id,config.getUri(),
					subscriptionString, _consumerEventBuffer,
					internalBufferSize, largestEventSize, consumerTimeoutMs,
					pollIntervalMs, connTimeoutMs, consumerParallelism, true,
					_nettyThreadPools);

		}
		catch (InvalidConfigException e)
		{
			LOG.fatal("Invalid config in creating a relay event producer" + e);
		}
	}

	public static DatabusSourcesConnection createDatabusSourcesConnection(
	        String producerName,
			String serverName, String subscriptionString,
			DatabusCombinedConsumer consumer, long internalBufferMaxSize,
			int largestEventSize, long consumerTimeoutMs, long pollIntervalMs,
			long connTimeoutMs, int consumerParallelism, boolean blockingBuffer)
			throws InvalidConfigException
	{
		 int id = (RngUtils.randomPositiveInt() % 10000) + 1;
		 return createDatabusSourcesConnection(
		        producerName,
		        id,serverName, subscriptionString, consumer,
				internalBufferMaxSize, largestEventSize, consumerTimeoutMs,
				pollIntervalMs, connTimeoutMs, consumerParallelism, blockingBuffer,
				DatabusClientNettyThreadPools.createNettyThreadPools(id));

	}

	public static DatabusSourcesConnection createDatabusSourcesConnection(
            String producerName,
			int id,String serverName, String subscriptionString,
			DatabusCombinedConsumer consumer, long internalBufferMaxSize,
			int largestEventSize, long consumerTimeoutMs, long pollIntervalMs,
			long connTimeoutMs, int consumerParallelism, boolean blockingBuffer,
			DatabusClientNettyThreadPools nettyThreadPools)
			throws InvalidConfigException
	{
		// the assumption here is that the list of subscriptions will become the
		// list of sources hosted by the relay
		Set<ServerInfo> relayServices = createServerInfo(serverName,
				subscriptionString);
		// null bootstrapService
		Set<ServerInfo> bootstrapServices = null;

		// create subscription objects based on what is required by subscription

		String[] subscriptionList = subscriptionString.split(",");
		List<DatabusSubscription> subsList = DatabusSubscription
				.createSubscriptionList(Arrays.asList(subscriptionList));
		List<String> sourcesStrList = DatabusSubscription.getStrList(subsList);
		LOG.info("The sourcesList is " + sourcesStrList);

		// create registration objects with consumers
		List<DatabusV2ConsumerRegistration> relayConsumers = createDatabusV2ConsumerRegistration(
				consumer, sourcesStrList);
		List<DatabusV2ConsumerRegistration> bstConsumers = null;

		// setup sources connection config
		DatabusSourcesConnection.Config confBuilder = new DatabusSourcesConnection.Config();

        LOG.info("Chained Relay Id=" + id);
		confBuilder.setId(id);
		// consume whatever is in relay
		confBuilder.setConsumeCurrent(true);
		confBuilder.setReadLatestScnOnError(false);
		// set size of largest expected event
		confBuilder.setFreeBufferThreshold(largestEventSize);
		// 10s max consumer timeout
		confBuilder.setConsumerTimeBudgetMs(consumerTimeoutMs);
		// poll interval in ms and infinite retry;
		confBuilder.getPullerRetries().setMaxRetryNum(-1);
		confBuilder.getPullerRetries().setInitSleep(pollIntervalMs);
		confBuilder.setConsumerParallelism(consumerParallelism);
		confBuilder.getDispatcherRetries().setMaxRetryNum(1);

		// internal buffer conf
		DbusEventBuffer.Config bufferConf = new DbusEventBuffer.Config();
		bufferConf.setMaxSize(internalBufferMaxSize);
		int readBufferSize = Math.max((int)(0.2*internalBufferMaxSize), 2*largestEventSize);
		bufferConf.setReadBufferSize(readBufferSize);
		//client buffer's scn index- not used
		bufferConf.setScnIndexSize(64*1024);
		String queuePolicy = blockingBuffer ? "BLOCK_ON_WRITE"
				: "OVERWRITE_ON_WRITE";
		bufferConf.setQueuePolicy(queuePolicy);
		confBuilder.setEventBuffer(bufferConf);

		DatabusSourcesConnection.StaticConfig connConfig = confBuilder.build();

		// internal buffers of databus client library
		DbusEventBuffer buffer = new DbusEventBuffer(
				connConfig.getEventBuffer());
		buffer.start(0);

		DbusEventBuffer bootstrapBuffer = null;
		// Create threadpools and netty managers
		// read - write timeout in ms
		long readTimeoutMs = connTimeoutMs;
		long writeTimeoutMs = connTimeoutMs;
		int version = 2;

		// connection factory
		NettyHttpConnectionFactory defaultConnFacory = new NettyHttpConnectionFactory(
				nettyThreadPools.getBossExecutorService(),
				nettyThreadPools.getIoExecutorService(), null, nettyThreadPools.getTimer(),
				writeTimeoutMs, readTimeoutMs, version,
				nettyThreadPools.getChannelGroup());

		// Create Thread pool for consumer threads
		int maxThreadsNum = 1;
		int keepAliveMs = 1000;
		ThreadPoolExecutor defaultExecutorService = new OrderedMemoryAwareThreadPoolExecutor(
				maxThreadsNum, 0, 0, keepAliveMs, TimeUnit.MILLISECONDS);

        ConsumerCallbackStats relayConsumerStats =
            new ConsumerCallbackStats(id,
                                      producerName + ".inbound.cons",
                                      producerName + ".inbound.cons",
                                      true,
                                      false,
                                      null,
                                      ManagementFactory.getPlatformMBeanServer());

        ConsumerCallbackStats bootstrapConsumerStats =
            new ConsumerCallbackStats(id,
                                      producerName + ".inbound.bs.cons" ,
                                      producerName + ".inbound.bs.cons",
                                      true,
                                      false,
                                      null,
                                      ManagementFactory.getPlatformMBeanServer());

		DatabusRelayConnectionFactory relayConnFactory = defaultConnFacory;
		DatabusBootstrapConnectionFactory bootstrapConnFactory = defaultConnFacory;
		DatabusSourcesConnection conn = new DatabusSourcesConnection(
				connConfig,
				subsList,
				relayServices,
				bootstrapServices,
				relayConsumers,
				bstConsumers,
				buffer,
				bootstrapBuffer,
				defaultExecutorService,
				null,// getContainerStatsCollector(),
				null,// getInboundEventStatisticsCollector(),
				null,// getBootstrapEventsStatsCollector(),
                relayConsumerStats,// relay callback stats,
                bootstrapConsumerStats,// bootstrap callback stats,
				null, // getCheckpointPersistenceProvider(),
				relayConnFactory,
				bootstrapConnFactory,
				null, // getHttpStatsCollector(),
				null, // RegistrationId
				null);

		return conn;
	}

	static String createSubscriptionString(PhysicalSourceStaticConfig config)
	{
		StringBuilder s = new StringBuilder();
		for (LogicalSourceStaticConfig sourceConfig : config.getSources())
		{
			if (s.length() > 0)
				s.append(",");
			s.append(sourceConfig.getName());
		}
		return s.toString();
	}

	static Set<ServerInfo> createServerInfo(String serverName,
			String subscriptions) throws InvalidConfigException
	{
		Set<ServerInfo> serverInfo = new HashSet<ServerInfo>();
		ServerInfoBuilder sBuilder = new ServerInfoBuilder();
		sBuilder.setAddress(serverName + ":" + subscriptions);
		// sBuilder.setSources(subscriptions);
		serverInfo.add(sBuilder.build());
		return serverInfo;
	}

	static List<DatabusV2ConsumerRegistration> createDatabusV2ConsumerRegistration(
			DatabusCombinedConsumer consumer, List<String> sourcesStrList)

	{
		ArrayList<DatabusV2ConsumerRegistration> regs = new ArrayList<DatabusV2ConsumerRegistration>();
		// No server side filtering just as yet
		regs.add(new DatabusV2ConsumerRegistration(consumer, sourcesStrList,
				null));
		return regs;
	}

	@Override
	public synchronized void shutdown()
	{
		if (_dbusConnection != null)
		{
			_dbusConnection.stop();
		}
		_relayLogger.shutdown();
		_relayLogger.interrupt();
	}

	@Override
	public synchronized void waitForShutdown() throws InterruptedException,
			IllegalStateException
	{
		if (_dbusConnection != null)
		{
			_dbusConnection.await();
		}
	}

	@Override
	public void waitForShutdown(long timeout) throws InterruptedException,
			IllegalStateException, TimeOutException
	{
		if (_dbusConnection != null)
		{
			_dbusConnection.await();
		}
	}

	@Override
	public String getName()
	{
		return _name;
	}

	@Override
	public long getSCN()
	{
		if (_statsCollector != null)
		{
			return _statsCollector.getTotalStats().getMaxScn();
		}
		return 0;
	}

	@Override
	public synchronized void start(long sinceSCN)
	{
		if (_dbusConnection != null
				&& !_dbusConnection.getConnectionStatus().isRunningStatus())
		{
			LOG.info("In RelayEventProducer start:  running =" + _dbusConnection.getConnectionStatus().isRunningStatus());
			//translate relay saved scn to client checkpoint
			LOG.info("Requested sinceSCN = " + sinceSCN);
			Checkpoint cp = getCheckpoint(sinceSCN, _scnReaderWriter);

			//check if the relay chaining consumer has been initialized [it could when the leader passes its active buffer]
			if ((cp != null) && (_consumerEventBuffer.getStartSCN() < 0))
			{
				//note that the restartScnOffset comes into the picture only iff the buffer is empty
				long savedScn = cp.getWindowScn();
				LOG.info("Checkpoint read = " + savedScn + " restartScnOffset=" + _restartScnOffset);
				
				long newScn = (savedScn >= _restartScnOffset) ? savedScn - _restartScnOffset : 0;
				cp.setWindowScn(newScn);

				LOG.info("Setting start scn of event buffer to " + cp.getWindowScn());
				_consumerEventBuffer.setStartSCN(cp.getWindowScn());
			}
			LOG.info("Eventbuffer start scn = " + _consumerEventBuffer.getStartSCN());

			//now set the checkpoint in the databus client fetcher
			_dbusConnection.getRelayPullThread().getConnectionState().setCheckpoint(cp);

			//start the connection
			_dbusConnection.start();
			_relayLogger.setDaemon(true);
			_relayLogger.start();
		}
		else
		{
			if (_dbusConnection == null)
			{
				LOG.error("Not started! Connection is null");
			}
			else
			{
				LOG.warn("dbusConnection status="
						+ _dbusConnection.getConnectionStatus().getStatus());
			}
		}
	}

	protected Checkpoint getCheckpoint(long sinceSCN,MaxSCNReaderWriter scnReaderWriter)
	{

		long scn = sinceSCN;
		if ((scn < 0) && (_scnReaderWriter != null))
		{
			try
			{
				scn = _scnReaderWriter.getMaxScn();
			}
			catch (DatabusException e)
			{
				LOG.info("Cannot read persisted SCN " + e);
				scn = -1;
			}
		}
		// return no cp if unable to read from saved SCN
		if (scn <= 0)
		{
			return null;
		}
		Checkpoint cp = new Checkpoint();
		cp.setConsumptionMode(DbusClientMode.ONLINE_CONSUMPTION);
		// always have greater than semantic
		cp.setWindowOffset(-1);
		cp.setWindowScn(scn);
		return cp;
	}

	@Override
	public synchronized boolean isRunning()
	{
		if (_dbusConnection != null)
		{
			return _dbusConnection.isRunning();
		}
		return false;
	}

	@Override
	public synchronized boolean isPaused()
	{
		if (_dbusConnection != null)
		{
			return _dbusConnection.getConnectionStatus().isPausedStatus();
		}
		return false;
	}

	@Override
	public synchronized void unpause()
	{
		if (_dbusConnection != null)
		{
			_dbusConnection.getConnectionStatus().resume();
		}
	}

	@Override
	public synchronized void pause()
	{
		if (_dbusConnection != null)
		{
			_dbusConnection.getConnectionStatus().pause();
		}
	}

	/**
	 * For db stats and ingraphs
	 *
	 * @return
	 */
	public EventSourceStatistics[] getEventSourceStats()
	{
		return _relayStatsAdapter.getEventSourceStatistics();
	}

	/**
	 * A logger that writes RelayEvents and updates Relay specific stats
	 *
	 * @author snagaraj
	 *
	 */
	private class RelayLogger extends Thread
	{
		/** Merge interval **/
		private final int _logIntervalMs;
		private boolean _shutdown = false;

		public RelayLogger(int logIntervalMs, String relayLogger)
		{
			super(relayLogger);
			_logIntervalMs = logIntervalMs;
		}

		public void shutdown()
		{
			_shutdown = true;
		}

		@Override
		public void run()
		{
			LOG.info("Started RelayLogger");
			while (!_shutdown)
			{
				try
				{
					ReadEventCycleSummary readEventCycle = _relayStatsAdapter.getReadEventCycleSummary();
					if ((readEventCycle != null) && (_eventsLog.isDebugEnabled() || (_eventsLog.isInfoEnabled())
								&& (readEventCycle.getTotalEventNum() > 0)
									))
					{
						_eventsLog.info(readEventCycle.toString());
					}

					Thread.sleep(_logIntervalMs);
				}
				catch (InterruptedException e)
				{
					LOG.info("RelayLogger interrupted! Will shutdown");
					_shutdown = true;
				}
			}
		}
	}


	/** Helper class that encapsulates Netty Artifacts **/
	static public class DatabusClientNettyThreadPools
	{
		private final Timer _timer;
		private final ExecutorService _bossExecutorService;
		private final ExecutorService _ioExecutorService;
		private final ChannelGroup _channelGroup;


		static public DatabusClientNettyThreadPools createNettyThreadPools(int id)
		{
			return new DatabusClientNettyThreadPools(id);
		}

		/** Create new threadpools and timer **/
		public DatabusClientNettyThreadPools(int id)
		{
			// connection factory
			 _timer = new HashedWheelTimer(5, TimeUnit.MILLISECONDS);
			 _ioExecutorService = Executors.newCachedThreadPool(new NamedThreadFactory("io" + id ));
  		     _bossExecutorService = Executors.newCachedThreadPool(new NamedThreadFactory("boss" + id ));
			 _channelGroup = new DefaultChannelGroup();
		}

		public DatabusClientNettyThreadPools(int id,
				Timer timer,
				ExecutorService bossExecutorService,
				ExecutorService ioExecutorService, ChannelGroup channelGroup)
		{
			_timer = (timer != null) ? timer :  new HashedWheelTimer(5, TimeUnit.MILLISECONDS);
			_bossExecutorService = (bossExecutorService != null) ? bossExecutorService : Executors.newCachedThreadPool(new NamedThreadFactory("io" + id));
			_ioExecutorService = (ioExecutorService != null) ? ioExecutorService : Executors.newCachedThreadPool(new NamedThreadFactory("boss" + id )) ;
			_channelGroup = (channelGroup != null) ?  channelGroup :  new DefaultChannelGroup() ;
		}

		public Timer getTimer()
		{
			return _timer;
		}

		public ExecutorService getBossExecutorService()
		{
			return _bossExecutorService;
		}

		public ExecutorService getIoExecutorService()
		{
			return _ioExecutorService;
		}

		public ChannelGroup getChannelGroup()
		{
			return _channelGroup;
		}

	}
}
