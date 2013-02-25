package com.linkedin.databus2.core.container.netty;
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


import com.linkedin.databus.core.DatabusComponentStatus;
import com.linkedin.databus.core.DbusEventV1;
import com.linkedin.databus.core.monitoring.mbean.AggregatedDbusEventsStatisticsCollector;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.monitoring.mbean.StatsCollectorMergeable;
import com.linkedin.databus.core.monitoring.mbean.StatsCollectors;
import com.linkedin.databus.core.util.ConfigApplier;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.ConfigManager;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus.core.util.JsonUtils;
import com.linkedin.databus.core.util.NamedThreadFactory;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.JmxStaticConfig;
import com.linkedin.databus2.core.container.JmxStaticConfigBuilder;
import com.linkedin.databus2.core.container.monitoring.mbean.ContainerStatisticsCollector;
import com.linkedin.databus2.core.container.monitoring.mbean.DatabusComponentAdmin;
import com.linkedin.databus2.core.container.request.CommandsRegistry;
import com.linkedin.databus2.core.container.request.ContainerAdminRequestProcessor;
import com.linkedin.databus2.core.container.request.ContainerStatsRequestProcessor;
import com.linkedin.databus2.core.container.request.JavaStatsRequestProcessor;
import com.linkedin.databus2.core.container.request.RequestProcessorRegistry;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import javax.management.MBeanServer;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import javax.naming.NameAlreadyBoundException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.PropertyConfigurator;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.DirectChannelBufferFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Log4JLoggerFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.ThreadNameDeterminer;
import org.jboss.netty.util.ThreadRenamingRunnable;
import org.jboss.netty.util.Timer;

/**
 * A serving container
 */
public abstract class ServerContainer
{
  public static final String MODULE = ServerContainer.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);
  public static final int GLOBAL_STATS_MERGE_INTERVAL_MS=500;

  private static final int SHUTDOWN_TIMEOUT_MS = 30000;
  private Timer _networkTimeoutTimer = null;

  protected final StaticConfig _containerStaticConfig;
  private final MBeanServer _mbeanServer;
  private final ThreadPoolExecutor _defaultExecutorService;
  private final ExecutorService _ioExecutorService;
  private final ExecutorService _bossExecutorService;
  //FIXME Deadlock problems with the TrackingExecutorService
  /*private final CallTracker _defaultExecutorServiceTracker;
  private final EnabledTrackingExecutorServiceState _enabledTrackingExecutorServiceState;
  private final DisabledTrackingExecutorServiceState _disabledTrackingExecutorServiceState;
  private final TrackingExecutorService _defaultTrackingExecutorService;*/
  private final ExecutionHandler _nettyExecHandler;
  private final ConfigManager<RuntimeConfig> _containerRuntimeConfigMgr;
  private final ContainerStatisticsCollector _containerStatsCollector;
  private final ReentrantLock _controlLock = new ReentrantLock(true);
  private final Condition _shutdownCondition = _controlLock.newCondition();
  private final Condition _shutdownFinishedCondition = _controlLock.newCondition();
  protected final StatsCollectors<DbusEventsStatisticsCollector> _inBoundStatsCollectors;
  protected final StatsCollectors<DbusEventsStatisticsCollector> _outBoundStatsCollectors;
  private final DatabusComponentAdmin _componentAdmin;
  private final DatabusComponentStatus _componentStatus;
  protected final GlobalStatsCalc _globalStatsMerger;

  private final Thread _globalStatsThread;

  /**
   * Eventually the processor registry will be migrated to the commands registry. Until all
   * commands have been migrated, we have to keep it. */
  protected final RequestProcessorRegistry _processorRegistry;
  protected final CommandsRegistry _commandsRegistry;
  private JMXConnectorServer _jmxConnServer;
  private JmxShutdownThread _jmxShutdownThread;
  private NettyShutdownThread _nettyShutdownThread;
  private Thread _containerShutdownHook;
  private volatile boolean _shutdown = false;
  private boolean _shutdownRequest = false;
  private boolean _started = false;

  protected ServerBootstrap _httpBootstrap;
  protected ServerBootstrap _tcpBootstrap;

  protected Channel _httpServerChannel;
  protected Channel _tcpServerChannel;

  /** Helper field until we migrate the callers of the static CLI functions to the Cli class */
  private static Cli _staticCliToBeDeprecated = new Cli();

  /**
   * Channel group for the server channel and all per-connection channels. Used to close all of
   * them on shutdown
   **/
  protected ChannelGroup _tcpChannelGroup;
  protected ChannelGroup _httpChannelGroup;
  public static final String JMX_DOMAIN = "com.linkedin.databus2";

  @Deprecated
  public static Properties processCommandLineArgs(String[] cliArgs)
         throws IOException, DatabusException
  {
    _staticCliToBeDeprecated.processCommandLineArgs(cliArgs);
    return _staticCliToBeDeprecated.getConfigProps();
  }

  public RequestProcessorRegistry getProcessorRegistry()
  {
    return _processorRegistry;
  }

  public ServerContainer(Config configBuilder) throws IOException, InvalidConfigException,
                                                      DatabusException
  {
    this(configBuilder.build());
  }

  public ServerContainer(StaticConfig config) throws IOException, InvalidConfigException,
                                                     DatabusException
  {
   _containerStaticConfig = config;
   //by default we have 5ms timeout precision
   _networkTimeoutTimer = new HashedWheelTimer(5, TimeUnit.MILLISECONDS);

   _processorRegistry = new RequestProcessorRegistry();
   _commandsRegistry = new CommandsRegistry();

   _mbeanServer = _containerStaticConfig.getOrCreateMBeanServer();
   _containerStaticConfig.getRuntime().setManagedInstance(this);

   //TODO (DDSDBUS-105) HIGH we have to use the builder here instead of a read-only copy because we have a
   //bootstrapping circular reference RuntimeConfig -> ContainerStatisticsCollector.RuntimeConfig
   // -> ContainerStatisticsCollector -> RuntimeConfig
   RuntimeConfigBuilder runtimeConfig = _containerStaticConfig.getRuntime();

   //ExecutorConfigBuilder ioThreadsConfig = runtimeConfig.getIoExecutor();
   /* commented out because of deadlock problems
    * _ioExecutorService = new ThreadPoolExecutor(
       ioThreadsConfig.getCoreThreadsNum(),
       ioThreadsConfig.getMaxThreadsNum(),
       ioThreadsConfig.getKeepAliveMs(),
       TimeUnit.MILLISECONDS,
       ioThreadsConfig.getMaxQueueSize() <= 0 ?
           new LinkedBlockingQueue<Runnable>() :
           new ArrayBlockingQueue<Runnable>(ioThreadsConfig.getMaxQueueSize()));*/
   _ioExecutorService = Executors.newCachedThreadPool(new NamedThreadFactory("io" + _containerStaticConfig.getId()));
   _bossExecutorService = Executors.newCachedThreadPool(new NamedThreadFactory("boss" + _containerStaticConfig.getId()));

    _defaultExecutorService =
      new OrderedMemoryAwareThreadPoolExecutor(runtimeConfig.getDefaultExecutor().getMaxThreadsNum(),
                             0,
                             0,
                             runtimeConfig.getDefaultExecutor().getKeepAliveMs(),
                             TimeUnit.MILLISECONDS,
                             new NamedThreadFactory("worker" + _containerStaticConfig.getId()));

   _containerStatsCollector = _containerStaticConfig.getOrCreateContainerStatsCollector();
    DbusEventsStatisticsCollector inboundEventStatisticsCollector = new AggregatedDbusEventsStatisticsCollector(getContainerStaticConfig().getId(),
                                                        "eventsInbound",
                                                        true,
                                                        true,
                                                        getMbeanServer());

    DbusEventsStatisticsCollector outboundEventStatisticsCollector = new AggregatedDbusEventsStatisticsCollector(getContainerStaticConfig().getId(),
                                                        "eventsOutbound",
                                                        true,
                                                        true,
                                                        getMbeanServer());


    _inBoundStatsCollectors = new StatsCollectors<DbusEventsStatisticsCollector>(inboundEventStatisticsCollector);
    _outBoundStatsCollectors = new StatsCollectors<DbusEventsStatisticsCollector>(outboundEventStatisticsCollector);

   _containerRuntimeConfigMgr = new ConfigManager<RuntimeConfig>(
       _containerStaticConfig.getRuntimeConfigPropertyPrefix(), _containerStaticConfig.getRuntime());

    //FIXME MED using _defaultTrackingExecutorService for _nettyExecHandler seems to hang
    /* _defaultExecutorServiceTracker = new CallTrackerImpl(new CallTrackerImpl.Config());
    _enabledTrackingExecutorServiceState =
          new EnabledTrackingExecutorServiceState("enabledTrackingExecutorServiceState",
                                                  _defaultExecutorService,
                                                  _defaultExecutorServiceTracker,
                                                  false);
    _disabledTrackingExecutorServiceState =
          new DisabledTrackingExecutorServiceState(_defaultExecutorService, new SystemClock());

    _defaultTrackingExecutorService =
      new TrackingExecutorService(_enabledTrackingExecutorServiceState,
                                  _disabledTrackingExecutorServiceState,
                                  runtimeConfig.getDefaultExecutor().isTrackerEnabled());*/
    _nettyExecHandler = new ExecutionHandler(_defaultExecutorService);

    _componentStatus = createComponentStatus();
    _componentAdmin = createComponentAdmin();
    _componentAdmin.registerAsMBean();

    _globalStatsMerger = new GlobalStatsCalc(GLOBAL_STATS_MERGE_INTERVAL_MS);
    _globalStatsThread = new Thread(_globalStatsMerger, "GlobalStatsThread");
    _globalStatsThread.setDaemon(true);

    initializeContainerNetworking();
    initializeContainerJmx();
    initializeContainerCommandProcessors();
    initializeStatsMerger();
  }

  private void initializeStatsMerger()
  {
    _globalStatsMerger.registerStatsCollector(_inBoundStatsCollectors);
    _globalStatsMerger.registerStatsCollector(_outBoundStatsCollectors);
  }

  protected void initializeContainerCommandProcessors() throws DatabusException
  {
    _processorRegistry.register(ContainerStatsRequestProcessor.COMMAND_NAME,
                                new ContainerStatsRequestProcessor(null, this));
    _processorRegistry.register(JavaStatsRequestProcessor.COMMAND_NAME,
                                new JavaStatsRequestProcessor(null));
    String healthcheckPrefix = ContainerAdminRequestProcessor.extractCommandRoot(
        _containerStaticConfig.getHealthcheckPath());
    LOG.info("healthcheck command root: " + healthcheckPrefix);
    _processorRegistry.register(healthcheckPrefix,
        new ContainerAdminRequestProcessor(null, _componentStatus,
                                           _containerStaticConfig.getHealthcheckPath()));
  }

  protected abstract DatabusComponentAdmin createComponentAdmin();

  protected DatabusComponentStatus createComponentStatus()
  {
    return new DatabusComponentStatus("Relay",
                                      DatabusComponentStatus.Status.INITIALIZING,
                                      DatabusComponentStatus.INITIALIZING_MESSAGE);
  }

  protected void initializeContainerNetworking() throws IOException, DatabusException
  {
    //instruct netty not to rename our threads in the I/O and boss thread pools
    ThreadRenamingRunnable.setThreadNameDeterminer(ThreadNameDeterminer.CURRENT);

    _httpBootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(_bossExecutorService,
                                                                       _ioExecutorService));
    _httpBootstrap.setPipelineFactory(new HttpServerPipelineFactory(this));
    _httpBootstrap.setOption("bufferFactory", DirectChannelBufferFactory.getInstance(DbusEventV1.byteOrder));
    _httpBootstrap.setOption("child.bufferFactory", DirectChannelBufferFactory.getInstance(DbusEventV1.byteOrder));

    if (_containerStaticConfig.getTcp().isEnabled())
    {
      _tcpBootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(_bossExecutorService,
                                                                            _ioExecutorService));
      _tcpBootstrap.setPipelineFactory(new TcpServerPipelineFactory(this));
      _tcpBootstrap.setOption("bufferFactory", DirectChannelBufferFactory.getInstance(DbusEventV1.byteOrder));
      _tcpBootstrap.setOption("child.bufferFactory", DirectChannelBufferFactory.getInstance(DbusEventV1.byteOrder));

      //LOG.debug("endianness:" + ((ChannelBufferFactory)_tcpBootstrap.getOption("bufferFactory")).getDefaultOrder());
    }
  }

  protected void initializeContainerJmx()
  {

    if (_containerStaticConfig.getJmx().isRmiEnabled())
    {
      try
      {
        JMXServiceURL jmxServiceUrl =
            new JMXServiceURL("service:jmx:rmi://" +
                              _containerStaticConfig.getJmx().getJmxServiceHost() + ":" +
                              _containerStaticConfig.getJmx().getJmxServicePort() +"/jndi/rmi://" +
                              _containerStaticConfig.getJmx().getRmiRegistryHost() + ":" +
                              _containerStaticConfig.getJmx().getRmiRegistryPort() + "/jmxrmi" +
                              _containerStaticConfig.getJmx().getJmxServicePort());

        _jmxConnServer = JMXConnectorServerFactory.newJMXConnectorServer(jmxServiceUrl, null,
                                                                         getMbeanServer());
      }
      catch (Exception e)
      {
        LOG.warn("Unable to instantiate JMX server", e);
      }
    }
  }

  /** Starts the container synchronously and waits for its shutdown*/
  public void startAndBlock()
  {
    start();
    awaitShutdown();
  }

  /** Starts the container */
  synchronized public void start()
  {
	if ( ! _started )
	{
		doStart();
		_componentStatus.start();
		_started = true;
		LOG.info("Databus service started!");
	} else {
		LOG.info("Databus service has already been started. Skipping this request !!");
	}
  }

  /** Please use start */
  public void startAsynchronously()
  {
	  Thread runThread = new Thread(new Runnable()
	  {
		  @Override
		  public void run()
		  {
			  startAndBlock();
		  }
	  },"ServerContainerStartAsync");
	  runThread.start();
  }

  protected void doStart()
  {
    _controlLock.lock();
    try
    {
      // Bind and start to accept incoming connections.
      int portNum = getContainerStaticConfig().getHttpPort();
      _tcpChannelGroup = new DefaultChannelGroup();
      _httpChannelGroup = new DefaultChannelGroup();

      _httpServerChannel = _httpBootstrap.bind(new InetSocketAddress(portNum));
      _httpChannelGroup.add(_httpServerChannel);
      LOG.info("Serving container " + getContainerStaticConfig().getId() +
               " HTTP listener on port " + portNum);

      if (_containerStaticConfig.getTcp().isEnabled())
      {
        int tcpPortNum = _containerStaticConfig.getTcp().getPort();
        _tcpServerChannel = _tcpBootstrap.bind(new InetSocketAddress(tcpPortNum));
        _tcpChannelGroup.add(_tcpServerChannel);

        LOG.info("Serving container " + getContainerStaticConfig().getId() +
                 " TCP listener on port " + tcpPortNum);
      }

      _nettyShutdownThread = new NettyShutdownThread();
      Runtime.getRuntime().addShutdownHook(_nettyShutdownThread);

      // Start the producer thread after 5 seconds
      if (null != _jmxConnServer && _containerStaticConfig.getJmx().isRmiEnabled())
      {
        try
        {
          _jmxShutdownThread = new JmxShutdownThread(_jmxConnServer);
          Runtime.getRuntime().addShutdownHook(_jmxShutdownThread);

          _jmxConnServer.start();
          LOG.info("JMX server listening on port " + _containerStaticConfig.getJmx().getJmxServicePort());
        }
        catch (IOException ioe)
        {
          if (ioe.getCause() != null && ioe.getCause() instanceof NameAlreadyBoundException)
          {
            LOG.warn("Unable to bind JMX server connector. Likely cause is that the previous instance was not cleanly shutdown: killed in Eclipse?");
            if (_jmxConnServer.isActive())
            {
              LOG.warn("JMX server connector seems to be running anyway. ");
            }
            else
            {
              LOG.warn("Unable to determine if JMX server connector is running");
            }
          }
          else
          {
            LOG.error("Unable to start JMX server connector", ioe);
          }
        }
      }

      _globalStatsThread.start();
    }
    catch (RuntimeException ex)
    {
    	LOG.error("Got runtime exception :" + ex, ex);
    	throw ex;
    }
    finally
    {
      _controlLock.unlock();
    }
  }

  public void awaitShutdown()
  {
    _controlLock.lock();
    try
    {
      while (! _shutdownRequest)
      {
        LOG.info("waiting for shutdown request for container id: " + _containerStaticConfig.getId());
        _shutdownCondition.awaitUninterruptibly();
      }
    }
    finally
    {
      _controlLock.unlock();
    }

    _controlLock.lock();
    try
    {
      while (!_shutdown)
      {
        LOG.info("Waiting for shutdown complete for serving container: " + _containerStaticConfig.getId());
        _shutdownFinishedCondition.awaitUninterruptibly();
      }
    }
    finally
    {
      _controlLock.unlock();
    }
  }

  public void awaitShutdown(long timeoutMs) throws TimeoutException, InterruptedException
  {
    long startTs = System.currentTimeMillis();
    long endTs = startTs + timeoutMs;
    _controlLock.lock();
    try
    {
      long waitTime;
      while (! _shutdownRequest && (waitTime = endTs - System.currentTimeMillis()) > 0)
      {
        LOG.info("waiting for shutdown request for container id: " + _containerStaticConfig.getId());
        if (!_shutdownCondition.await(waitTime, TimeUnit.MILLISECONDS)) break;
      }
    }
    finally
    {
      _controlLock.unlock();
    }

    if (!_shutdownRequest)
    {
      LOG.error("timeout waiting for a shutdown request");
      throw new TimeoutException("timeout waiting for shutdown request");
    }

    _controlLock.lock();
    try
    {
      long waitTime;
      while (!_shutdown && (waitTime = endTs - System.currentTimeMillis()) > 0)
      {
        LOG.info("Waiting for shutdown complete for serving container: " + _containerStaticConfig.getId());
        if (!_shutdownFinishedCondition.await(waitTime, TimeUnit.MILLISECONDS)) break;
      }
    }
    finally
    {
      _controlLock.unlock();
    }

    if (!_shutdown)
    {
      LOG.error("timeout waiting for shutdown");
      throw new TimeoutException("timeout waiting for shutdown to complete");
    }
  }

  protected void doShutdown()
  {
    unregisterShutdownHook();

    LOG.info("Initializing shutdown for serving container: " + _containerStaticConfig.getId());
    if (null != _jmxShutdownThread && Thread.State.NEW == _jmxShutdownThread.getState())
    {
      try
      {
        Runtime.getRuntime().removeShutdownHook(_jmxShutdownThread);
        _jmxShutdownThread.start();
      }
      catch (IllegalStateException ise)
      {
        LOG.error("Error removing shutdown hook", ise);
      }
    }
    if (null != _nettyShutdownThread && Thread.State.NEW == _nettyShutdownThread.getState())
    {
      try
      {
        Runtime.getRuntime().removeShutdownHook(_nettyShutdownThread);
        _nettyShutdownThread.start();
      }
      catch (IllegalStateException ise)
      {
        LOG.error("Error removing shutdown hook", ise);
      }
    }

    if (_globalStatsMerger != null && !_globalStatsMerger.isHalted())
    {
      _globalStatsMerger.halt();
      _globalStatsThread.interrupt();
    }

    // unregister all mbeans
    getContainerStatsCollector().unregisterMBeans();
    getInboundEventStatisticsCollector().unregisterMBeans();
    getOutboundEventStatisticsCollector().unregisterMBeans();
    for (DbusEventsStatisticsCollector coll: _inBoundStatsCollectors.getStatsCollectors())
    {
    	coll.unregisterMBeans();
    }
    for (DbusEventsStatisticsCollector coll: _outBoundStatsCollectors.getStatsCollectors())
    {
    	coll.unregisterMBeans();
    }

    _componentAdmin.unregisterAsMBeans();

    LOG.info("joining shutdown threads");

    long startTime = System.currentTimeMillis();
    long timeRemaining = SHUTDOWN_TIMEOUT_MS;

    while (null != _jmxShutdownThread && _jmxShutdownThread.isAlive() && timeRemaining > 0)
    {
      try
      {
        _jmxShutdownThread.join(timeRemaining);
      }
      catch (InterruptedException ie) {}
      timeRemaining = SHUTDOWN_TIMEOUT_MS - (System.currentTimeMillis() - startTime);
    }
    LOG.info("JMX shutdown for container " + _containerStaticConfig.getId() + ":" +
             (null != _jmxShutdownThread ? !_jmxShutdownThread.isAlive() : true) +
             "; ms remaining: " + timeRemaining);

    while (null != _nettyShutdownThread && _nettyShutdownThread.isAlive() && timeRemaining > 0)
    {
      try
      {
        _nettyShutdownThread.join(timeRemaining);
      }
      catch (InterruptedException ie) {}
      timeRemaining = SHUTDOWN_TIMEOUT_MS - (System.currentTimeMillis() - startTime);
    }
    LOG.info("Netty shutdown for container " + _containerStaticConfig.getId() + ":" +
             (null != _nettyShutdownThread ? !_nettyShutdownThread.isAlive() : true) +
             "; ms remaining: " + timeRemaining);

    LOG.info("Done with shutdown for serving container: " + _containerStaticConfig.getId());
  }

  public void shutdown()
  {
    shutdownAsynchronously();
    try
    {
      awaitShutdown(SHUTDOWN_TIMEOUT_MS);
    }
    catch (TimeoutException e)
    {
      LOG.error("shutdown timed out");
    }
    catch (InterruptedException e)
    {
      LOG.warn("shutdown cancelled because of interruption");
    }
  }

  public void shutdownUninteruptibly()
  {
    shutdownAsynchronously();
    awaitShutdown();
  }

  public void shutdownAsynchronously()
  {
    LOG.info("Initiating asynchronous shutdown for serving container: " + _containerStaticConfig.getId());
    _controlLock.lock();
    try
    {
      if (_shutdown) return;

      if (! _shutdownRequest)
      {
        _shutdownRequest = true;
        _shutdownCondition.signalAll();

        Thread shutdownThread = new Thread(new ShutdownRunnable(),
                                           "shutdown thread for container: " + _containerStaticConfig.getId());
        shutdownThread.setDaemon(true);
        shutdownThread.start();
      }
    }
    finally
    {
      _controlLock.unlock();
    }
  }

  //This method shall only be called by AdminMBean impl class
  public DatabusComponentStatus.Status getStatus()
  {
    return _componentStatus.getStatus();
  }

  // This method shall only be called by AdminMBean impl class
  public String getStatusMessage()
  {
    return _componentStatus.getMessage();
  }

  public boolean isRunningStatus()
  {
    return _componentStatus.isRunningStatus();
  }

  public MBeanServer getMbeanServer()
  {
    return _mbeanServer;
  }

  public StaticConfig getContainerStaticConfig()
  {
    return _containerStaticConfig;
  }

  public ExecutionHandler getNettyExecHandler()
  {
    return _nettyExecHandler;
  }

  public ConfigManager<RuntimeConfig> getContainerRuntimeConfigMgr()
  {
    return _containerRuntimeConfigMgr;
  }

  public ThreadPoolExecutor getDefaultExecutorService()
  {
    return _defaultExecutorService;
  }

  public ContainerStatisticsCollector getContainerStatsCollector()
  {
    return _containerStatsCollector;
  }

  public GlobalStatsCalc getGlobalStatsMerger()
  {
    return _globalStatsMerger;
  }

  /**
   * Adds a hook that will cleanly shutdown the container if the JVM is
   * stopped. A NOOP if one has already been created. */
  public void registerShutdownHook()
  {
    _controlLock.lock();
    try
    {
      if (null != _containerShutdownHook) return;
      _containerShutdownHook = new Thread(new Runnable()
      {
        @Override
        public void run()
        {
          shutdown();
        }
      }, "shutdownHook-" + _containerStaticConfig.getId());
      Runtime.getRuntime().addShutdownHook(_containerShutdownHook);
    }
    finally
    {
      _controlLock.unlock();
    }
  }

  /**
   * Removes the previously created shutdown hook. A NOOP if none has been
   * set up. */
  public synchronized void unregisterShutdownHook()
  {
    _controlLock.lock();
    try
    {
      if (null != _containerShutdownHook)
      {
        Runtime.getRuntime().removeShutdownHook(_containerShutdownHook);
        _containerShutdownHook = null;
      }
    }
    catch (IllegalStateException e)
    {
      //noop -- we are already shutting down
    }
    finally
    {
      _controlLock.unlock();
    }
  }

  /** Command-line interface to the server container */
  public static class Cli
  {
    public static final String HELP_OPT_LONG_NAME = "help";
    public static final char HELP_OPT_CHAR = 'h';
    public static final String LOG4J_PROPS_OPT_LONG_NAME = "log_props";
    public static final char LOG4J_PROPS_OPT_CHAR = 'l';
    public static final String CONTAINER_PROPS_OPT_LONG_NAME = "container_props";
    public static final char CONTAINER_PROPS_OPT_CHAR = 'p';
    public static final String CMD_LINE_PROPS_OPT_LONG_NAME = "cmdline_props";
    public static final char CMD_LINE_PROPS_OPT_CHAR = 'c';
    public static final String DEBUG_OPT_LONG_NAME = "debug";
    public static final char DEBUG_OPT_CHAR = 'd';

    private final String _usage;
    protected Options _cliOptions;
    protected CommandLine _cmd;
    protected Properties _configProps;
    HelpFormatter _helpFormatter;
    protected Level _defaultLogLevel = Level.INFO;

    public Cli()
    {
      this("java <class> [options]");
    }

    public Cli(String usage)
    {
      _usage = usage;
      _cliOptions = new Options();
      constructCommandLineOptions();
      _helpFormatter = new HelpFormatter();
      _helpFormatter.setWidth(150);
    }

    public void printCliHelp()
    {
      _helpFormatter.printHelp(getUsage(), _cliOptions);
    }

    public String getUsage()
    {
      return _usage;
    }

    public Options getCliOptions()
    {
      return _cliOptions;
    }

    public CommandLine getCmdLine()
    {
      return _cmd;
    }

    public void processCommandLineArgs(String[] cliArgs) throws IOException, DatabusException
    {

      CommandLineParser cliParser = new GnuParser();

      _cmd = null;
      try
      {
        _cmd = cliParser.parse(_cliOptions, cliArgs);
      }
      catch (ParseException pe)
      {
        System.err.println("HttpServer: failed to parse command-line options: " + pe.toString());
        printCliHelp();
        System.exit(1);
      }

      if (_cmd.hasOption(HELP_OPT_CHAR))
      {
        printCliHelp();
        System.exit(0);
      }

      if (_cmd.hasOption(DEBUG_OPT_CHAR))
      {
        Logger.getRootLogger().setLevel(Level.DEBUG);
      }
      else
      {
        Logger.getRootLogger().setLevel(_defaultLogLevel);
      }

      if (_cmd.hasOption(LOG4J_PROPS_OPT_CHAR))
      {
        String log4jPropFile = _cmd.getOptionValue(LOG4J_PROPS_OPT_CHAR);
        PropertyConfigurator.configure(log4jPropFile);
        LOG.info("Using custom logging settings from file " + log4jPropFile);
      }
      else
      {
        PatternLayout defaultLayout = new PatternLayout("%d{ISO8601} +%r [%t] (%p) {%c{1}} %m%n");
        ConsoleAppender defaultAppender = new ConsoleAppender(defaultLayout);

        Logger.getRootLogger().removeAllAppenders();
        Logger.getRootLogger().addAppender(defaultAppender);

        LOG.info("Using default logging settings");
      }

      _configProps = new Properties(System.getProperties());
      if (_cmd.hasOption(CONTAINER_PROPS_OPT_CHAR))
      {
        for (String propFile: _cmd.getOptionValues(CONTAINER_PROPS_OPT_CHAR))
        {
          LOG.info("Loading container config from properties file " + propFile);
          FileInputStream fis = new FileInputStream(propFile);
          try
          {
            _configProps.load(fis);
          }
          catch (Exception e)
          {
            LOG.error("error processing properties; ignoring:" + e.getMessage(), e);
          }
          finally
          {
            fis.close();
          }
        }
      }
      else
      {
        LOG.info("Using system properties for container config");
      }

      if (_cmd.hasOption(CMD_LINE_PROPS_OPT_CHAR))
      {
        String cmdLinePropString = _cmd.getOptionValue(CMD_LINE_PROPS_OPT_CHAR);
        updatePropsFromCmdLine(cmdLinePropString);
      }
      if (Logger.getRootLogger().isTraceEnabled())
      {
        //Print out Netty Logging only if we want a very detailed log
        InternalLoggerFactory.setDefaultFactory(new Log4JLoggerFactory());
      }
    }

    private void updatePropsFromCmdLine(String cmdLinePropString)
    {
      String[] cmdLinePropSplit = cmdLinePropString.split(";");
      for(String s : cmdLinePropSplit)
      {
        String[] onePropSplit = s.split("=");
        if (onePropSplit.length != 2)
        {
          LOG.error("CMD line property setting " + s + "is not valid!");
        }
        else
        {
          LOG.info("CMD line Property overwride: " + s);
          _configProps.put(onePropSplit[0], onePropSplit[1]);
        }
      }
    }

    @SuppressWarnings("static-access")
    protected void constructCommandLineOptions()
    {
      Option helpOption = OptionBuilder.withLongOpt(HELP_OPT_LONG_NAME)
                                       .withDescription("Prints command-line options info")
                                       .create(HELP_OPT_CHAR);
      Option log4jPropsOption = OptionBuilder.withLongOpt(LOG4J_PROPS_OPT_LONG_NAME)
                                             .withDescription("Log4j properties to use")
                                             .hasArg()
                                             .withArgName("property_file")
                                             .create(LOG4J_PROPS_OPT_CHAR);
      Option debugPropsOption = OptionBuilder.withLongOpt(DEBUG_OPT_LONG_NAME)
                                             .withDescription("Turns on debugging info")
                                             .create(DEBUG_OPT_CHAR);
      Option containerPropsOption = OptionBuilder.withLongOpt(CONTAINER_PROPS_OPT_LONG_NAME)
                                                 .withDescription("Container config properties to use")
                                                 .hasArg()
                                                 .withArgName("property_file")
                                                 .create(CONTAINER_PROPS_OPT_CHAR);
      Option cmdLinePropsOption = OptionBuilder.withLongOpt(CMD_LINE_PROPS_OPT_LONG_NAME)
                                                 .withDescription("Cmd line override of config properties. Semicolon separated.")
                                                 .hasArg()
                                                 .withArgName("Semicolon_separated_properties")
                                                 .create(CMD_LINE_PROPS_OPT_CHAR);


      _cliOptions.addOption(helpOption);
      _cliOptions.addOption(log4jPropsOption);
      _cliOptions.addOption(debugPropsOption);
      _cliOptions.addOption(containerPropsOption);
      _cliOptions.addOption(cmdLinePropsOption);
    }

    public Properties getConfigProps()
    {
      return _configProps;
    }

    public Level getDefaultLogLevel()
    {
      return _defaultLogLevel;
    }

    public void setDefaultLogLevel(Level defaultLogLevel)
    {
      _defaultLogLevel = defaultLogLevel;
    }

  }

  /** Static configuration for the TCP command interface */
  public static class TcpStaticConfig
  {
    private final boolean _enabled;
    private final int _port;

    public TcpStaticConfig(boolean enabled, int port)
    {
      super();
      _enabled = enabled;
      _port = port;
    }

    /** A flag if the TCP command interface is to be enabled*/
    public boolean isEnabled()
    {
      return _enabled;
    }

    /** The port for the TCP command interface */
    public int getPort()
    {
      return _port;
    }

    @Override
    public String toString()
    {
      return toJsonString(false);
    }

    public String toJsonString(boolean pretty)
    {
      return JsonUtils.toJsonStringSilent(this, pretty);
    }
  }

  /** */
  public static class TcpStaticConfigBuilder implements ConfigBuilder<TcpStaticConfig>
  {
    private boolean _enabled;
    private int _port;

    public TcpStaticConfigBuilder()
    {
      _enabled = false;
      _port = 8180;
    }

    public boolean isEnabled()
    {
      return _enabled;
    }

    public int getPort()
    {
      return _port;
    }

    public void setEnabled(boolean enabled)
    {
      _enabled = enabled;
    }

    public void setPort(int port)
    {
      _port = port;
    }

    @Override
    public TcpStaticConfig build() throws InvalidConfigException
    {
      if (_enabled && 0 >= _port) throw new InvalidConfigException("invalid TCP port: " + _port);
      TcpStaticConfig newConfig = new TcpStaticConfig(_enabled, _port);
      LOG.info("TCP command interface configuration:" + newConfig);
      return newConfig;
    }
  }

  public static class ExecutorConfig implements ConfigApplier<ExecutorConfig>
  {
    private final int _maxThreadsNum;
    private final int _coreThreadsNum;
    private final int _keepAliveMs;
    private final boolean _trackerEnabled;
    private final int _maxQueueSize;

    public ExecutorConfig(int maxThreadsNum,
                          int coreThreadsNum,
                          int keepAliveMs,
                          boolean trackerEnabled,
                          int maxQueueSize) throws InvalidConfigException
    {
      super();
      _maxThreadsNum = maxThreadsNum;
      _coreThreadsNum = coreThreadsNum;
      _keepAliveMs = keepAliveMs;
      _trackerEnabled = trackerEnabled;
      _maxQueueSize = maxQueueSize;
    }

    /** The maximum number of executor threads */
    public int getMaxThreadsNum()
    {
      return _maxThreadsNum;
    }

    /** The default number of executor threads */
    public int getCoreThreadsNum()
    {
      return _coreThreadsNum;
    }

    /** The time in milliseconds to keep an idle thread before killing it */
    public int getKeepAliveMs()
    {
      return _keepAliveMs;
    }

    /** Is the executor service stats tracking enabled. */
    public boolean isTrackerEnabled()
    {
      return _trackerEnabled;
    }

    @Override
    public void applyNewConfig(ExecutorConfig oldConfig)
    {
      // FIXME Add implementation
    }

    @Override
    public boolean equals(Object other)
    {
      if(other == null || !(other instanceof ExecutorConfig)) return false;
      if(this == other) return true;
      return equalsConfig((ExecutorConfig)other);
    }

    @Override
    public boolean equalsConfig(ExecutorConfig otherConfig)
    {
      if(otherConfig == null) return false;
      return getMaxThreadsNum() == otherConfig.getMaxThreadsNum() &&
      getCoreThreadsNum() == otherConfig.getCoreThreadsNum() &&
      getKeepAliveMs() == otherConfig.getKeepAliveMs() &&
      isTrackerEnabled() == otherConfig.isTrackerEnabled();
    }

    @Override
    public int hashCode()
    {
      return _maxThreadsNum ^ _coreThreadsNum ^ _keepAliveMs ^ (_trackerEnabled ? 0xFFFFFFFF : 0) ;
    }

    /** The max number of queued tasks; <= 0 means no limit*/
    public int getMaxQueueSize()
    {
      return _maxQueueSize;
    }
  }

  public static class ExecutorConfigBuilder implements ConfigBuilder<ExecutorConfig>
  {
    public static final Integer DEFAULT_MAX_THREADS_NUM = 100;
    public static final Integer DEFAULT_CORE_THREADS_NUM = 50;
    public static final Integer DEFAULT_KEEPALIVE_MS = 60000;
    public static final Boolean DEFAULT_TRACKER_ENABLED = true;
    public static final Integer DEFAULT_MAX_QUEUE_SIZE = 1000;

    private int _maxThreadsNum               = DEFAULT_MAX_THREADS_NUM;
    private int _coreThreadsNum              = DEFAULT_CORE_THREADS_NUM;
    private int _keepAliveMs                 = DEFAULT_KEEPALIVE_MS;
    private boolean _trackerEnabled          = DEFAULT_TRACKER_ENABLED;
    private int _maxQueueSize                = DEFAULT_MAX_QUEUE_SIZE;
    private ServerContainer _managedInstance = null;

    public ExecutorConfigBuilder()
    {
    }

    public int getMaxThreadsNum()
    {
      return _maxThreadsNum;
    }

    public void setMaxThreadsNum(int maxThreadsNum)
    {
      _maxThreadsNum = maxThreadsNum;
    }

    public int getCoreThreadsNum()
    {
      return _coreThreadsNum;
    }

    public void setCoreThreadsNum(int coreThreadsNum)
    {
      _coreThreadsNum = coreThreadsNum;
    }

    public int getKeepAliveMs()
    {
      return _keepAliveMs;
    }

    public void setKeepAliveMs(int keepAliveMs)
    {
      _keepAliveMs = keepAliveMs;
    }

    public boolean isTrackerEnabled()
    {
      return _trackerEnabled;
    }

    public void setTrackerEnabled(boolean trackerEnabled)
    {
      _trackerEnabled = trackerEnabled;
    }

    @Override
    public ExecutorConfig build() throws InvalidConfigException
    {
      if (null == _managedInstance) throw new InvalidConfigException("Missing server container");
      return new ExecutorConfig(_maxThreadsNum, _coreThreadsNum,
                                                 _keepAliveMs, _trackerEnabled,
                                                 _maxQueueSize);
    }

    ServerContainer getManagedInstance()
    {
      return _managedInstance;
    }

    void setManagedInstance(ServerContainer managedInstance)
    {
      _managedInstance = managedInstance;
    }

    public int getMaxQueueSize()
    {
      return _maxQueueSize;
    }

    public void setMaxQueueSize(int maxQueueSize)
    {
      _maxQueueSize = maxQueueSize;
    }

  }

  public static class RuntimeConfig implements ConfigApplier<RuntimeConfig>
  {
    private final int _requestProcessingBudgetMs;
    private final ExecutorConfig _defaultExecutorConfig;
    private final ExecutorConfig _ioExecutorConfig;

    public RuntimeConfig(int requestProcessingBudgetMs,
                         ExecutorConfig defaultExecutorConfig,
                         ExecutorConfig ioExecutorConfig
                        )
                         throws InvalidConfigException
    {
      super();
      _requestProcessingBudgetMs = requestProcessingBudgetMs;
      _defaultExecutorConfig = defaultExecutorConfig;
      _ioExecutorConfig = ioExecutorConfig;
    }

    /** Maximum time in millisecods to process a request before interrupting the request processing */
    public int getRequestProcessingBudgetMs()
    {
      return _requestProcessingBudgetMs;
    }

    /** Runtime configuration for the request executor */
    public ExecutorConfig getDefaultExecutor()
    {
      return _defaultExecutorConfig;
    }

    @Override
    public void applyNewConfig(RuntimeConfig oldConfig)
    {
      LOG.info("Changing container config");
      //FIXME add logic
      if (null == oldConfig || !getDefaultExecutor().equals(oldConfig.getDefaultExecutor()))
      {
        getDefaultExecutor().applyNewConfig(null != oldConfig ? oldConfig.getDefaultExecutor() : null);
      }

    }

    @Override
    public boolean equals(Object other)
    {
      if(other == null || !(other instanceof RuntimeConfig)) return false;
      if(this == other) return true;
      return equalsConfig((RuntimeConfig)other);
    }

    @Override
    public boolean equalsConfig(RuntimeConfig otherConfig)
    {
      if(otherConfig == null) return false;

      return (getRequestProcessingBudgetMs() == otherConfig.getRequestProcessingBudgetMs()) &&
             (getDefaultExecutor().equals(otherConfig.getDefaultExecutor()));

    }

    @Override
    public int hashCode()
    {
      return _requestProcessingBudgetMs ^ _defaultExecutorConfig.hashCode();

    }

    public ExecutorConfig getIoExecutorConfig()
    {
      return _ioExecutorConfig;
    }
  }

  public static class RuntimeConfigBuilder implements ConfigBuilder<RuntimeConfig>
  {
    private int _requestProcessingBudgetMs                = 100;
    private ExecutorConfigBuilder _defaultExecutor;
    private ExecutorConfigBuilder _ioExecutor;
    private ServerContainer _managedInstance              = null;

    public RuntimeConfigBuilder()
    {
      _defaultExecutor = new ExecutorConfigBuilder();
      _ioExecutor = new ExecutorConfigBuilder();
      _ioExecutor.setCoreThreadsNum(5);
      _ioExecutor.setMaxThreadsNum(2 * Runtime.getRuntime().availableProcessors());
    }

    public int getRequestProcessingBudgetMs()
    {
      return _requestProcessingBudgetMs;
    }

    public void setRequestProcessingBudgetMs(int requestProcessingBudgetMs)
    {
      _requestProcessingBudgetMs = requestProcessingBudgetMs;
    }

    public ExecutorConfigBuilder getDefaultExecutor()
    {
      return _defaultExecutor;
    }

    public void setDefaultExecutor(ExecutorConfigBuilder defaultExecutor)
    {
      _defaultExecutor = defaultExecutor;
    }

    @Override
    public RuntimeConfig build() throws InvalidConfigException
    {
      if (null != _managedInstance)
      {
        return new RuntimeConfig(_requestProcessingBudgetMs,
                                                  _defaultExecutor.build(),
                                                  _ioExecutor.build());
      }

      throw new InvalidConfigException("Server container instance not set");
    }


    public ServerContainer getManagedInstance()
    {
      return _managedInstance;
    }

    public void setManagedInstance(ServerContainer managedInstance)
    {
      if (null != managedInstance)
      {
        _managedInstance = managedInstance;
        _defaultExecutor.setManagedInstance(managedInstance);
        _ioExecutor.setManagedInstance(managedInstance);
      }
    }


    public ExecutorConfigBuilder getIoExecutor()
    {
      return _ioExecutor;
    }

    public void setIoExecutor(ExecutorConfigBuilder ioExecutor)
    {
      _ioExecutor = ioExecutor;
    }
  }

  public static class StaticConfig
  {
    private final int _id;
    private final JmxStaticConfig _jmxConfig;
    private final int _httpPort;
    private final MBeanServer _existingMbeanServer;
    private final String _runtimeConfigPropertyPrefix;
    private final RuntimeConfigBuilder _runtimeConfigBuilder;
    private final String _healthcheckPath;
    private final long _readTimeoutMs;
    private final long _writeTimeoutMs;
    private final TcpStaticConfig _tcp;
    private final boolean _enableHttpCompression;

    public StaticConfig(int id, JmxStaticConfig jmxConfig, int httpPort,
                        MBeanServer existingMbeanServer,
                        String runtimeConfigPropertyPrefix,
                        RuntimeConfigBuilder runtimeConfigBuilder,
                        String healthcheckPath,
                        long readTimeoutMs,
                        long writeTimeoutMs,
                        TcpStaticConfig tcp,
                        boolean enableHttpCompression)
    {
      super();
      _id = id;
      _jmxConfig = jmxConfig;
      _httpPort = httpPort;
      _existingMbeanServer = existingMbeanServer;
      _runtimeConfigPropertyPrefix = runtimeConfigPropertyPrefix;
      _runtimeConfigBuilder = runtimeConfigBuilder;
      _healthcheckPath = healthcheckPath;
      _readTimeoutMs = readTimeoutMs;
      _writeTimeoutMs = writeTimeoutMs;
      _tcp = tcp;
      _enableHttpCompression = enableHttpCompression;
    }

    /** HTTP port to listen on */
    public int getHttpPort()
    {
      return _httpPort;
    }

    /** JMX static configuration */
    public JmxStaticConfig getJmx()
    {
      return _jmxConfig;
    }

    public MBeanServer getExistingMbeanServer()
    {
      return _existingMbeanServer;
    }

    public String getRuntimeConfigPropertyPrefix()
    {
      return _runtimeConfigPropertyPrefix;
    }

    /** Runtime  configuration */
    public RuntimeConfigBuilder getRuntime()
    {
      return _runtimeConfigBuilder;
    }

    /** Container ID */
    public int getId()
    {
      return _id;
    }


    public MBeanServer getOrCreateMBeanServer()
    {
      return (null != getExistingMbeanServer()) ? getExistingMbeanServer() :
                                                  ManagementFactory.getPlatformMBeanServer();
    }

    public ContainerStatisticsCollector getOrCreateContainerStatsCollector()
    {
      ContainerStatisticsCollector result =
          new ContainerStatisticsCollector(getRuntime().getManagedInstance(), "containerStats",
                                          true,
                                           true, getOrCreateMBeanServer());

      return result;
    }


    /** Path on which the healthcheck will respond (sans the initial /) */
    public String getHealthcheckPath()
    {
      return _healthcheckPath;
    }

    /** Timeout for reading parts of HTTP request */
    public long getReadTimeoutMs()
    {
      return _readTimeoutMs;
    }

    /** Timeout for confirmation from the peer when sending a response */
    public long getWriteTimeoutMs()
    {
      return _writeTimeoutMs;
    }

    /** TCP command interface static configuration*/
    public TcpStaticConfig getTcp()
    {
      return _tcp;
    }

    /** Whether to enable HTTP compression (deflate) for outbound data*/
    public boolean getEnableHttpCompression()
    {
      return _enableHttpCompression;
    }

	@Override
	public String toString() {
		return "StaticConfig [_id=" + _id + ", _jmxConfig=" + _jmxConfig
				+ ", _httpPort=" + _httpPort + ", _existingMbeanServer="
				+ _existingMbeanServer + ", _runtimeConfigPropertyPrefix="
				+ _runtimeConfigPropertyPrefix + ", _runtimeConfigBuilder="
				+ _runtimeConfigBuilder + ", _statsCollector="
				+ _healthcheckPath + ", _readTimeoutMs=" + _readTimeoutMs
				+ ", _writeTimeoutMs=" + _writeTimeoutMs + ", _tcp=" + _tcp
				+ ", _enableHttpCompression=" + _enableHttpCompression
				+ "]";
	}
  }

  public static class Config implements ConfigBuilder<StaticConfig>
  {
    private int _id;
    private int _httpPort                       = 9000;
    private JmxStaticConfigBuilder _jmx;
    private MBeanServer _existingMbeanServer    = null;
    private String _runtimeConfigPropertyPrefix = "databus.container.runtime";
    private RuntimeConfigBuilder _runtime;
    private int _defaultId;
    private String _healthcheckPath             = "admin";
    private long _readTimeoutMs                 = 15000;
    private long _writeTimeoutMs                = 15000;
    private final TcpStaticConfigBuilder _tcp;
    private boolean _enableHttpCompression      = false;

    public Config()
    {
      _jmx = new JmxStaticConfigBuilder();
      _runtime = new RuntimeConfigBuilder();
      _tcp = new TcpStaticConfigBuilder();
      _id = -1;
    }

    public int getHttpPort()
    {
      return _httpPort;
    }

    public void setHttpPort(int httpPort)
    {
      _httpPort = httpPort;
    }

    public JmxStaticConfigBuilder getJmx()
    {
      return _jmx;
    }

    public void setJmx(JmxStaticConfigBuilder jmx)
    {
      _jmx = jmx;
    }

    public MBeanServer getExistingMbeanServer()
    {
      return _existingMbeanServer;
    }

    public void setExistingMbeanServer(MBeanServer existingMbeanServer)
    {
      _existingMbeanServer = existingMbeanServer;
    }

    @Override
    public StaticConfig build() throws InvalidConfigException
    {
      _defaultId = -1;
      try
      {
        int hostHash = InetAddress.getLocalHost().hashCode();
        _defaultId = (hostHash == Integer.MIN_VALUE) ? Integer.MAX_VALUE : Math.abs(hostHash);
        _defaultId = _defaultId % 0x7FFF0000;
      }
      catch (UnknownHostException uhe)
      {
        LOG.error("Error getting localhost info", uhe);
      }

      if (_id == -1) _id = (_defaultId + _httpPort);

      String realHealthcheckPath = _healthcheckPath.startsWith("/") ? _healthcheckPath.substring(1)
                                                                    : _healthcheckPath;

      LOG.info("Using http port:" + _httpPort);
      LOG.info("Using container id: " + _id);
      LOG.info("Using healthcheck path: " + realHealthcheckPath);
      return new StaticConfig(_id, _jmx.build(), _httpPort, _existingMbeanServer,
                              _runtimeConfigPropertyPrefix, _runtime,
                              realHealthcheckPath,
                              _readTimeoutMs,
                              _writeTimeoutMs,
                              _tcp.build(),
                              _enableHttpCompression);
    }


    public String getRuntimeConfigPropertyPrefix()
    {
      return _runtimeConfigPropertyPrefix;
    }

    public void setRuntimeConfigPropertyPrefix(String runtimeConfigPropertyPrefix)
    {
      _runtimeConfigPropertyPrefix = runtimeConfigPropertyPrefix;
    }

    public RuntimeConfigBuilder getRuntime()
    {
      return _runtime;
    }

    public void setRuntime(RuntimeConfigBuilder runtime)
    {
      _runtime = runtime;
    }

    public int getId()
    {
      return _id;
    }

    public void setId(int id)
    {
      _id = id;
    }

    public void setIdFromName(String name)
    {
      int id = name.hashCode();
      if(id < 0)
      {
        id = (id == Integer.MIN_VALUE) ? Integer.MAX_VALUE : Math.abs(id);
      }
      setId(id);
    }

    public String getHealthcheckPath()
    {
      return _healthcheckPath;
    }

    public void setHealthcheckPath(String healthcheckPath)
    {
      _healthcheckPath = healthcheckPath;
    }

    public long getReadTimeoutMs()
    {
      return _readTimeoutMs;
    }

    public void setReadTimeoutMs(long readTimeoutMs)
    {
      _readTimeoutMs = readTimeoutMs;
    }

    public long getWriteTimeoutMs()
    {
      return _writeTimeoutMs;
    }

    public void setWriteTimeoutMs(long writeTimeoutMs)
    {
      _writeTimeoutMs = writeTimeoutMs;
    }

    public TcpStaticConfigBuilder getTcp()
    {
      return _tcp;
    }

    public boolean getEnableHttpCompression()
    {
      return _enableHttpCompression;
    }

    public void setEnableHttpCompression(boolean enableHttpCompression)
    {
      _enableHttpCompression = enableHttpCompression;
    }
  }

  public StatsCollectors<DbusEventsStatisticsCollector> getInBoundStatsCollectors()
  {
	  return _inBoundStatsCollectors;
  }

  public StatsCollectors<DbusEventsStatisticsCollector> getOutBoundStatsCollectors()
  {
	  return _outBoundStatsCollectors;
  }

  public DbusEventsStatisticsCollector getInboundEventStatisticsCollector()
  {
    return _inBoundStatsCollectors.getStatsCollector();
  }

  public DbusEventsStatisticsCollector getOutboundEventStatisticsCollector()
  {
    return  _outBoundStatsCollectors.getStatsCollector();
  }

  public abstract void pause();

  public abstract void resume();

  public abstract void suspendOnError(Throwable cause);

  public DatabusComponentAdmin getComponentAdmin()
  {
    return _componentAdmin;
  }

  public DatabusComponentStatus getComponentStatus()
  {
    return _componentStatus;
  }

  public ExecutorService getIoExecutorService()
  {
    return _ioExecutorService;
  }

  protected ChannelGroup getTcpChannelGroup()
  {
    return _tcpChannelGroup;
  }

  protected ChannelGroup getHttpChannelGroup()
  {
    return _httpChannelGroup;
  }

  public Timer getNetworkTimeoutTimer()
  {
    return _networkTimeoutTimer;
  }

  /** The new registry for parsing and execution of commands */
  public CommandsRegistry getCommandsRegistry()
  {
    return _commandsRegistry;
  }

  public boolean hasClientStarted()
  {
	  return _started;
  }

  /**
   * Override _started flag. Used in testing
   */
  protected void setStarted(boolean started)
  {
	  _started = started;
  }

  /**
   * Utility class that can keep track of global stats across physical sources/buffers
   *
   */

  static public class GlobalStatsCalc implements Runnable
  {

		private boolean _stopRequested=false;
		private boolean _stopped=false;
		final private int _sleepInMs;
		final private List<StatsCollectors<? extends StatsCollectorMergeable<?>>> _stats;

		public GlobalStatsCalc(int sleepInMs)
		{
			_sleepInMs = sleepInMs;
			_stats = new ArrayList<StatsCollectors<? extends StatsCollectorMergeable<?>>>(5);
		}

		public synchronized void registerStatsCollector(StatsCollectors<?  extends StatsCollectorMergeable<?>> coll)
		{
		  _stats.add(coll);
		}

        public synchronized void deregisterStatsCollector(StatsCollectors<?  extends StatsCollectorMergeable<?>> coll)
        {
          _stats.remove(coll);
        }

		public void halt()
		{
			_stopRequested=true;
		}

		public boolean isHalted()
		{
			return _stopped;
		}

		@Override
		public void run()
		{
			try
			{
				_stopped=false;
				while (!_stopRequested)
				{
				  synchronized(this)
				  {
					for( StatsCollectors<?  extends StatsCollectorMergeable<?>> stats : _stats)
					{
						stats.mergeStatsCollectors();
					}
				  }
                  if (!_stopRequested)
                    Thread.sleep(_sleepInMs);
				}
			}
			catch (InterruptedException e)
			{

			}
			_stopped=true;
		}
	  }

  public ExecutorService getBossExecutorService()
  {
    return _bossExecutorService;
  }

  class NettyShutdownThread extends Thread
  {

    public NettyShutdownThread()
    {
      super("Databus2 Netty Shutdown Thread");
    }

    @Override
    public void run()
    {
      LOG.info("Starting shutdown procedure for Netty ...");
      if (null != _httpServerChannel)
      {
        LOG.info("closing http server channel ...");
        _httpServerChannel.close().awaitUninterruptibly();
        _httpChannelGroup.remove(_httpServerChannel);
      }
      if (null != _tcpServerChannel)
      {
        LOG.info("closing tcp server channel ...");
        _tcpServerChannel.close().awaitUninterruptibly();
        _tcpChannelGroup.remove(_tcpServerChannel);
      }

      if (null != _tcpChannelGroup)
      {
        LOG.info("closing tcp channel group with " + _tcpChannelGroup.size() + " channels ...");
        _tcpChannelGroup.close().awaitUninterruptibly();
      }
      if (null != _httpChannelGroup)
      {
        LOG.info("closing http channel group with " + _httpChannelGroup.size() + " channels ...");
        _httpChannelGroup.close().awaitUninterruptibly();
      }
      if (null != _httpBootstrap)
      {
        LOG.info("releasing netty http resources ...");
        _httpBootstrap.releaseExternalResources();
      }
      if (null != _tcpBootstrap)
      {
        LOG.info("releasing netty tcp resources ...");
        _tcpBootstrap.releaseExternalResources();
      }

      LOG.info("stopping network timeout timer");
      _networkTimeoutTimer.stop();
      LOG.info("Done shutting down Netty.");
    }

  }

  private class ShutdownRunnable implements Runnable
  {

    @Override
    public void run()
    {
      _controlLock.lock();
      try
      {
        try
        {
          doShutdown();
        }
        catch (Exception e)
        {
          LOG.error("shutdown error", e);
        }
        _shutdown = true;
        _shutdownFinishedCondition.signalAll();
      }
      finally
      {
        _controlLock.unlock();
      }
    }

  }

}

class JmxShutdownThread extends Thread
{
  public static final String MODULE = JmxShutdownThread.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private final JMXConnectorServer _jmxConnServer;

  public JmxShutdownThread(JMXConnectorServer jmxConnServer)
  {
    super("Databus2 JMX Shutdown Thread");
    _jmxConnServer = jmxConnServer;
  }

  @Override
  public void run()
  {
    LOG.info("Starting shutdown procedure for JMX server ...");
    try
    {
      if (null != _jmxConnServer && _jmxConnServer.isActive())
      {
        _jmxConnServer.stop();
      }
      LOG.info("JMX server shutdown.");
    }
    catch (Exception e)
    {
      LOG.error("Error shutting down JMX server", e);
    }
  }
}
