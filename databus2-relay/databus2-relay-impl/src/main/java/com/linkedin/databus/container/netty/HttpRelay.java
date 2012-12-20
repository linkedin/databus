/*
 * Copyright 2009 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.linkedin.databus.container.netty;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.log4j.Logger;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ServerChannel;

import com.linkedin.databus.container.request.BufferInfoRequestProcessor;
import com.linkedin.databus.container.request.GenerateDataEventsRequestProcessor;
import com.linkedin.databus.container.request.LoadDataEventsRequestProcessor;
import com.linkedin.databus.container.request.PhysicalBuffersRequestProcessor;
import com.linkedin.databus.container.request.PhysicalSourcesRequestProcessor;
import com.linkedin.databus.container.request.ReadEventsRequestProcessor;
import com.linkedin.databus.container.request.RegisterRequestProcessor;
import com.linkedin.databus.container.request.RelayCommandRequestProcessor;
import com.linkedin.databus.container.request.RelayStatsRequestProcessor;
import com.linkedin.databus.container.request.SourcesRequestProcessor;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventBufferMetaInfo;
import com.linkedin.databus.core.DbusEventBufferMult;
import com.linkedin.databus.core.DbusEventBufferMult.PhysicalPartitionKey;
import com.linkedin.databus.core.EventLogReader;
import com.linkedin.databus.core.EventLogWriter;
import com.linkedin.databus.core.UnsupportedKeyException;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.util.ConfigApplier;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus.core.util.ConfigManager;
import com.linkedin.databus.core.util.DatabusEventProducer;
import com.linkedin.databus.core.util.DatabusEventRandomProducer;
import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus.core.util.PhysicalSourceConfigBuilder;
import com.linkedin.databus2.core.BufferNotFoundException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.monitoring.mbean.DatabusComponentAdmin;
import com.linkedin.databus2.core.container.monitoring.mbean.HttpStatisticsCollector;
import com.linkedin.databus2.core.container.netty.ServerContainer;
import com.linkedin.databus2.core.container.request.ConfigRequestProcessor;
import com.linkedin.databus2.core.container.request.EchoRequestProcessor;
import com.linkedin.databus2.core.container.request.RequestProcessorRegistry;
import com.linkedin.databus2.core.container.request.SleepRequestProcessor;
import com.linkedin.databus2.producers.EventCreationException;
import com.linkedin.databus2.relay.AddRemovePartitionInterface;
import com.linkedin.databus2.relay.config.DataSourcesStaticConfig;
import com.linkedin.databus2.relay.config.DataSourcesStaticConfigBuilder;
import com.linkedin.databus2.relay.config.LogicalSourceConfig;
import com.linkedin.databus2.relay.config.LogicalSourceStaticConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import com.linkedin.databus2.schemas.FileSystemSchemaRegistryService;
import com.linkedin.databus2.schemas.SchemaRegistryConfigBuilder;
import com.linkedin.databus2.schemas.SchemaRegistryService;
import com.linkedin.databus2.schemas.SchemaRegistryStaticConfig;
import com.linkedin.databus2.schemas.SourceIdNameRegistry;
import com.linkedin.databus2.schemas.StandardSchemaRegistryFactory;

/**
 * The HTTP container for the databus relay
 */
public class HttpRelay extends ServerContainer implements AddRemovePartitionInterface
{
    private static final String MODULE = HttpRelay.class.getName();
    private static final Logger LOG = Logger.getLogger(MODULE);

    private DbusEventBufferMult _eventBufferMult;
    private final SchemaRegistryService _schemaRegistryService;
    protected final StaticConfig _relayStaticConfig;
    private final ConfigManager<RuntimeConfig> _relayConfigManager;
    private final HttpStatisticsCollector _httpStatisticsCollector;
    private final SourceIdNameRegistry _sourcesIdNameRegistry;
    protected List<PhysicalSourceStaticConfig> _pConfigs;
    // The map maintains the highest last known schemaVersion for each source parsed
    // from the events from RPL-dbus. If there is a version change, we can detect by checking
    // with the version info stored in the map.
    final Map<String, Short> _sourceSchemaVersionMap = new ConcurrentHashMap<String, Short>();

    public HttpRelay(Config config, PhysicalSourceStaticConfig [] pConfigs)
    throws IOException, InvalidConfigException, DatabusException
    {
      this(config.build(), pConfigs);
    }

    public HttpRelay(StaticConfig config, PhysicalSourceStaticConfig[] pConfigs)
           throws IOException, InvalidConfigException, DatabusException
    {
      this(config, pConfigs, SourceIdNameRegistry.createFromIdNamePairs(config.getSourceIds()),
           (new StandardSchemaRegistryFactory(config.getSchemaRegistry())).createSchemaRegistry());
    }

    public HttpRelay(StaticConfig config, PhysicalSourceStaticConfig [] pConfigs,
                     SourceIdNameRegistry sourcesIdNameRegistry,
                     SchemaRegistryService schemaRegistry)
    throws IOException, InvalidConfigException, DatabusException
    {
      super(config.getContainer());
      _relayStaticConfig = config;

      _sourcesIdNameRegistry = sourcesIdNameRegistry;

      //3 possible scenarios:
      // 1. _pConfigs is initialized by this moment
      // 2. _pConfigs is null - create a fake one for backward compatiblity
      // 3. _pConfigs is not null but empty - we are trying to create an empty relay
      if(pConfigs == null) { // nothing provided - create a fake ones
        initPConfigs(config);
     } else if(pConfigs.length == 0) { //nothing available yet, probably CM case
       _pConfigs = new ArrayList<PhysicalSourceStaticConfig>();
     } else {
       _pConfigs = new ArrayList<PhysicalSourceStaticConfig>();
       for(PhysicalSourceStaticConfig pC : pConfigs)
         _pConfigs.add(pC);
     }

      if(_eventBufferMult == null) {
        PhysicalSourceStaticConfig[] psscArr = new PhysicalSourceStaticConfig[_pConfigs.size()];
        _pConfigs.toArray(psscArr);
        _eventBufferMult = new DbusEventBufferMult(psscArr,  config.getEventBuffer());
      }
      _eventBufferMult.setDropOldEvents(true);

      if (null != _eventBufferMult.getAllPhysicalPartitionKeys())
      {
        for (PhysicalPartitionKey pkey: _eventBufferMult.getAllPhysicalPartitionKeys())
        {
          addPhysicalPartitionCollectors(pkey.getPhysicalPartition());
        }
      }

      _sourcesIdNameRegistry.getAllSources();

      _schemaRegistryService = schemaRegistry;
      if (null == _schemaRegistryService)
      {
        throw new InvalidConfigException("Unable to initialize schema registry");
      }

      HttpStatisticsCollector httpStatsColl = _relayStaticConfig.getHttpStatsCollector()
                                              .getExistingStatsCollector();
      if (null == httpStatsColl)
      {
        httpStatsColl = new HttpStatisticsCollector(getContainerStaticConfig().getId(),
                                                    "httpOutbound",
                                                    _relayStaticConfig.getRuntime().getHttpStatsCollector().isEnabled(),
                                                    true,
                                                    getMbeanServer());
      }
      _httpStatisticsCollector = httpStatsColl;

      _relayStaticConfig.getRuntime().setManagedInstance(this);
      _relayConfigManager = new ConfigManager<RuntimeConfig>("databus.relay.runtime.",
                                                             _relayStaticConfig.getRuntime());

      initializeRelayNetworking();
      initializeRelayCommandProcessors();
    }

    @Override
    protected DatabusComponentAdmin createComponentAdmin()
    {
      return new DatabusComponentAdmin(this,
                                       getMbeanServer(),
                                       HttpRelay.class.getSimpleName());
    }

    public Map<String, Short> getSourceSchemaVersionMap()
    {
      return _sourceSchemaVersionMap;
    }

    public StaticConfig getRelayStaticConfig()
    {
      return _relayStaticConfig;
    }

    protected void initializeRelayNetworking() throws IOException, DatabusException
    {
      _httpBootstrap.setPipelineFactory(new HttpRelayPipelineFactory(this, _httpBootstrap.getPipelineFactory()));
    }

    protected void initializeRelayCommandProcessors() throws DatabusException
    {
      _processorRegistry.register(ConfigRequestProcessor.COMMAND_NAME,
                                 new ConfigRequestProcessor(null, this));
      _processorRegistry.register(RelayStatsRequestProcessor.COMMAND_NAME,
                                 new RelayStatsRequestProcessor(null, this));
      _processorRegistry.register(SourcesRequestProcessor.COMMAND_NAME,
                                 new SourcesRequestProcessor(null, this));
      _processorRegistry.register(RegisterRequestProcessor.COMMAND_NAME,
                                 new RegisterRequestProcessor(null, this));
      _processorRegistry.register(ReadEventsRequestProcessor.COMMAND_NAME,
                                  new ReadEventsRequestProcessor(null, this));
      _processorRegistry.register(PhysicalSourcesRequestProcessor.COMMAND_NAME,
                                 new PhysicalSourcesRequestProcessor(null, this));
      _processorRegistry.register(PhysicalBuffersRequestProcessor.COMMAND_NAME,
                                  new PhysicalBuffersRequestProcessor(null, this));
      _processorRegistry.register(BufferInfoRequestProcessor.COMMAND_NAME,
                                  new BufferInfoRequestProcessor(null,_eventBufferMult));
      _processorRegistry.register(RelayCommandRequestProcessor.COMMAND_NAME,
                                  new RelayCommandRequestProcessor(null, this));
    }

    @Override
    protected void doStart()
    {
      super.doStart();
    }

    public void disconnectDBusClients()
    {
      disconnectDBusClients(null);
    }

    synchronized public void disconnectDBusClients(Channel exceptThis)
    {
      LOG.info("disconnectDBusClients");
      if(_httpChannelGroup != null)
      {
        LOG.info("Total " + _httpChannelGroup.size() + " channels");
        for(Channel channel : _httpChannelGroup)
        {
          // Keep the server channel and REST channel
          if((channel instanceof ServerChannel) ||
              (exceptThis != null && channel.getId().equals(exceptThis.getId()))) {
            LOG.info("Skipping closing channel" + channel.getId());
          } else {
            LOG.info("closing channel" + channel.getId());
            channel.close();
          }
        }
      }
    }

    @Override
    protected void doShutdown()
    {
      super.doShutdown();
      getHttpStatisticsCollector().unregisterMBeans();
      if (null != _schemaRegistryService &&
          _schemaRegistryService instanceof FileSystemSchemaRegistryService)
      {
        LOG.info("stopping file-system schema registry refresh thread");
        ((FileSystemSchemaRegistryService)_schemaRegistryService).stopSchemasRefreshThread();
        LOG.info("file-system schema registry refresh thread stopped.");
      }
    }

    // read configurations used for integration testing
    private static PhysicalSourceStaticConfig [] readPhysicalConfigs(String baseDir, Config config)
    throws Exception {

      // source configuration moved here
      // NOTE. some of tests REQUIRE specific values
      // This values are incorporated into sources-TEST.json
      // (id=1,2,3, sourceNames="source1, source2, source1"
      String dbRelayConfigFileTest = "integration-test/config/sources-TEST.json";
      String dbRelayConfigFileTest1 = "integration-test/config/sources-TEST1.json";

      String[] sources = new String[]{dbRelayConfigFileTest,dbRelayConfigFileTest1};
      PhysicalSourceConfigBuilder builder = new PhysicalSourceConfigBuilder(baseDir, sources);

      PhysicalSourceStaticConfig[] pConfigs = builder.build();

      for(PhysicalSourceStaticConfig pCfg : pConfigs) {
        for(LogicalSourceStaticConfig lSC : pCfg.getSources()) {
          config.setSourceName("" + lSC.getId(), lSC.getName());
        }
      }
      return pConfigs;
    }

    /**
     * add a new buffer - usually invoked by Helix
     * @param pConfig
     * @param config
     * @throws DatabusException
     * @throws Exception
     */
    public DbusEventBuffer addNewBuffer(PhysicalSourceStaticConfig pConfig, HttpRelay.StaticConfig config)
        throws DatabusException
     {
      DbusEventBufferMult eventMult = getEventBuffer();
      DbusEventBuffer buf = eventMult.addNewBuffer(pConfig, config.getEventBuffer());
      return buf;
    }

    public void addOneProducer(PhysicalSourceStaticConfig pConfig)
        throws DatabusException, EventCreationException,
        UnsupportedKeyException, SQLException, InvalidConfigException {
      // do nothing by default.
    }

    public void removeOneProducer(PhysicalSourceStaticConfig pConfig) {
      // do nothing by default
    }

    /** remove a new buffer - usually invoked by Helix */
    public void removeBuffer(PhysicalSourceStaticConfig pConfig) {
      DbusEventBufferMult eventMult = getEventBuffer();
      eventMult.removeBuffer(pConfig);
    }

    @Override
    public void dropDatabase(String dbName)
    throws DatabusException
    {
    	_schemaRegistryService.dropDatabase(dbName);

    	DbusEventBufferMult eventMult = getEventBuffer();

    	/*
    	 * Close the buffers
    	 */
    	for (DbusEventBuffer dBuf : eventMult.bufIterable())
    	{

    		PhysicalPartition pp = dBuf.getPhysicalPartition();
    		if (pp.getName().equals(dbName))
    		{
        		dBuf.closeBuffer();
        		dBuf.removeMMapFiles();
        		PhysicalPartitionKey pKey = new PhysicalPartitionKey(pp);
        		eventMult.removeBuffer(pKey, null);
    		}
    	}
		eventMult.deallocateRemovedBuffers(true);
    	return;
    }


    public void resetBuffer(PhysicalPartition p, long prevScn, long binlogOffset)
    throws BufferNotFoundException
    {
      DbusEventBufferMult eventMult = getEventBuffer();
      eventMult.resetBuffer(p, prevScn);
      // TODO Set binlogOffset of the server
    }

    public int[] getBinlogOffset(int serverId)
    throws DatabusException
    {
      throw new DatabusException("Unimplemented method");
    }

    public Map<String, String> printInfo()
    throws DatabusException
    {
      throw new DatabusException("Unimplemented method");
    }

    public static void main(String[] args) throws Exception
    {
      Properties startupProps = ServerContainer.processCommandLineArgs(args);

      Config config = new Config();

      String baseDir = System.getProperty(PhysicalSourceConfig.PHYSICAL_SOURCE_BASE_DIR,
                                          PhysicalSourceConfig.PHYSICAL_SOURCE_BASE_DIR_DEFAULT);
      PhysicalSourceStaticConfig [] pConfigs = readPhysicalConfigs(baseDir, config);

      ConfigLoader<StaticConfig> staticConfigLoader =
          new ConfigLoader<StaticConfig>("databus.relay.", config);

      StaticConfig staticConfig = staticConfigLoader.loadConfig(startupProps);

      HttpRelay relay = new HttpRelay(staticConfig, pConfigs);

      RequestProcessorRegistry processorRegistry = relay.getProcessorRegistry();

      DatabusEventProducer randomEventProducer =
          new DatabusEventRandomProducer(relay.getEventBuffer(), 10, 100, 1000,
                                         staticConfig.getSourceIds());

      // specify stats collector for this producer
      ((DatabusEventRandomProducer)randomEventProducer).setStatsCollector(relay.getInboundEventStatisticsCollector());

      processorRegistry.register(EchoRequestProcessor.COMMAND_NAME, new EchoRequestProcessor(null));
      processorRegistry.register(SleepRequestProcessor.COMMAND_NAME, new SleepRequestProcessor(null));
      processorRegistry.register(
                                 GenerateDataEventsRequestProcessor.COMMAND_NAME,
                                 new GenerateDataEventsRequestProcessor(null,
                                                                        relay,
                                                                        randomEventProducer));
      processorRegistry.register(LoadDataEventsRequestProcessor.COMMAND_NAME,
                                 new LoadDataEventsRequestProcessor(relay.getDefaultExecutorService(),
                                                                    relay));

      LOG.info("source = " + relay.getSourcesIdNameRegistry().getAllSources());
      try
      {
        relay.registerShutdownHook();
        relay.startAndBlock();
      }
      catch (Exception e)
      {
        LOG.error("Error starting the relay", e);
      }
      LOG.info("Exiting relay");
    }

    public static class Cli extends ServerContainer.Cli
    {
      public static final char DB_RELAY_CONFIG_FILE_OPT_CHAR = 'Y';
      public static final String DB_RELAY_CONFIG_FILE_OPT_NAME = "db_relay_config";

      protected String []_physicalSrcConfigFiles;

      public Cli()
      {
        this("java " + HttpRelay.class.getName() + " [options]");
      }

      public Cli(String usage)
      {
        super(usage);
      }

      public String[] getPhysicalSrcConfigFiles()
      {
        if (null == _physicalSrcConfigFiles) return null;
        return Arrays.copyOf(_physicalSrcConfigFiles, _physicalSrcConfigFiles.length);
      }

      @Override
      public void processCommandLineArgs(String[] cliArgs) throws IOException,
          DatabusException
      {
        super.processCommandLineArgs(cliArgs);

        if (_cmd.hasOption(DB_RELAY_CONFIG_FILE_OPT_NAME))
        {
          String opt = _cmd.getOptionValue(DB_RELAY_CONFIG_FILE_OPT_NAME);
          _physicalSrcConfigFiles = opt.split(",");
          for (int i = 0; i < _physicalSrcConfigFiles.length; ++i)
              _physicalSrcConfigFiles[i] = _physicalSrcConfigFiles[i].trim();
          LOG.info("Physical Sources Config files = " + Arrays.toString(_physicalSrcConfigFiles));
        }
      }

      @SuppressWarnings("static-access")
      @Override
      protected void constructCommandLineOptions()
      {
        super.constructCommandLineOptions();
        Option physConfigOption = OptionBuilder.withLongOpt(DB_RELAY_CONFIG_FILE_OPT_NAME)
                                               .hasArg()
                                               .hasArg()
                                               .withArgName("physical sources config File")
                                               .create(DB_RELAY_CONFIG_FILE_OPT_CHAR);

        _cliOptions.addOption(physConfigOption);
      }

    }

    public class RuntimeConfig implements ConfigApplier<RuntimeConfig>
    {
      private final ServerContainer.RuntimeConfig _container;
      private final HttpStatisticsCollector.RuntimeConfig _httpStatsCollector;

      public RuntimeConfig(ServerContainer.RuntimeConfig container,
                           HttpStatisticsCollector.RuntimeConfig httpStatsCollector)
      {
        super();
        _container = container;
        _httpStatsCollector = httpStatsCollector;
      }

      @Override
      public void applyNewConfig(RuntimeConfig oldConfig)
      {
        LOG.debug("Applying new relay config");
        if (null == oldConfig || ! getContainer().equals(oldConfig.getContainer()))
        {
          _container.applyNewConfig(null != oldConfig ? oldConfig.getContainer() : null);
        }
        if (null == oldConfig || ! getHttpStatsCollector().equals(oldConfig.getHttpStatsCollector()))
        {
          getHttpStatsCollector().applyNewConfig(null != oldConfig ?
                                                 oldConfig.getHttpStatsCollector() :
                                                 null);
        }
      }

      @Override
      public boolean equals(Object other)
      {
        if (null == other || !(other instanceof RuntimeConfig)) return false;
        if(this == other) return true;
        return equalsConfig((RuntimeConfig)other);
      }

      @Override
      public boolean equalsConfig(RuntimeConfig otherConfig)
      {
        if (null == otherConfig) return false;

        return getContainer().equals(otherConfig.getContainer()) &&
                getHttpStatsCollector().equals(otherConfig.getHttpStatsCollector());
      }

      @Override
      public int hashCode()
      {
        return _container.hashCode() ^ _httpStatisticsCollector.hashCode();
      }

      /** Runtime configuration options for the Netty container */
      public ServerContainer.RuntimeConfig getContainer()
      {
        return _container;
      }

      /** Runtime configuration options for the HTTP calls statistics collect*/
      public HttpStatisticsCollector.RuntimeConfig getHttpStatsCollector()
      {
        return _httpStatsCollector;
      }
    }

    public static class RuntimeConfigBuilder implements ConfigBuilder<RuntimeConfig>
    {
      private final ServerContainer.RuntimeConfigBuilder _container;
      private HttpRelay _managedInstance = null;
      private HttpStatisticsCollector.RuntimeConfigBuilder _httpStatsCollector;

      public RuntimeConfigBuilder(ServerContainer.RuntimeConfigBuilder container)
      {
        _container = container;
        _httpStatsCollector = new HttpStatisticsCollector.RuntimeConfigBuilder();
      }

      public HttpRelay getManagedInstance()
      {
        return _managedInstance;
      }

      public void setManagedInstance(HttpRelay managedInstance)
      {
        _managedInstance = managedInstance;
        _container.setManagedInstance(managedInstance);
        _httpStatsCollector.setManagedInstance(_managedInstance.getHttpStatisticsCollector());
      }

      public ServerContainer.RuntimeConfigBuilder getContainer()
      {
        return _container;
      }

      @Override
      public RuntimeConfig build() throws InvalidConfigException
      {
        if (null == _managedInstance) throw new InvalidConfigException("Missing relay");
        return _managedInstance.new RuntimeConfig(_container.build(),
                                                  _httpStatsCollector.build());
      }

      public HttpStatisticsCollector.RuntimeConfigBuilder getHttpStatsCollector()
      {
        return _httpStatsCollector;
      }

      public void setHttpStatsCollector(HttpStatisticsCollector.RuntimeConfigBuilder httpStatsCollector)
      {
        _httpStatsCollector = httpStatsCollector;
      }
    }

    public static class StaticConfig
    {
      private final ServerContainer.StaticConfig _containerConfig;
      private final DbusEventBuffer.StaticConfig _eventBufferConfig;
      private final SchemaRegistryStaticConfig _schemaRegistryConfig;
      private final List<IdNamePair> _sourceIds;
      private final RuntimeConfigBuilder _runtime;
      private final HttpStatisticsCollector.StaticConfig _httpStatsCollector;
      private final DatabusEventRandomProducer.StaticConfig _randomProducer;
      private final EventLogWriter.StaticConfig _eventLogWriterConfig;
      private final EventLogReader.StaticConfig _eventLogReaderConfig;
      private final boolean _startDbPuller;
      private final DataSourcesStaticConfig _dataSources;
      private final PhysicalSourceStaticConfig[] _physicalSourcesConfigs;

      public StaticConfig(DbusEventBuffer.StaticConfig eventBufferConfig,
                          ServerContainer.StaticConfig containerConfig,
                          SchemaRegistryStaticConfig schemaRegistryConfig,
                          List<IdNamePair> sourceIds,
                          RuntimeConfigBuilder runtime,
                          HttpStatisticsCollector.StaticConfig httpStatsCollector,
                          DbusEventsStatisticsCollector.StaticConfig inboundEventsStatsCollector,
                          DbusEventsStatisticsCollector.StaticConfig outboundEventsStatsCollector,
                          DatabusEventRandomProducer.StaticConfig randomProducer,
                          EventLogWriter.StaticConfig eventLogWriterConfig,
                          EventLogReader.StaticConfig eventLogReaderConfig,
                          boolean startDbPuller,
                          DataSourcesStaticConfig dataSources,
                          PhysicalSourceStaticConfig[] physicalSourcesConfigs)
      {
        super();
        _eventBufferConfig = eventBufferConfig;
        _containerConfig = containerConfig;
        _schemaRegistryConfig = schemaRegistryConfig;
        _sourceIds = sourceIds;
        _runtime = runtime;
        _httpStatsCollector = httpStatsCollector;
        _randomProducer = randomProducer;
        _eventLogWriterConfig = eventLogWriterConfig;
        _eventLogReaderConfig = eventLogReaderConfig;
        _startDbPuller = startDbPuller;
        _dataSources = dataSources;
        _physicalSourcesConfigs = physicalSourcesConfigs;
      }

      /** Configuration options for the relay event buffer */
      public DbusEventBuffer.StaticConfig getEventBuffer()
      {
        return _eventBufferConfig;
      }

      /** Configuration options for the relay random events producer (testing) */
      public DatabusEventRandomProducer.StaticConfig getRandomProducer()
      {
        return _randomProducer;
      }

      /** Configuration options for the relay netty container */
      public ServerContainer.StaticConfig getContainer()
      {
        return _containerConfig;
      }

      /** Configuration options for the schema registry */
      public SchemaRegistryStaticConfig getSchemaRegistry()
      {
        return _schemaRegistryConfig;
      }

      /** Databus sources registered in the relay */
      public List<IdNamePair> getSourceIds()
      {
        return _sourceIds;
      }

      /** Relay runtime configuration options */
      public RuntimeConfigBuilder getRuntime()
      {
        return _runtime;
      }

      /** Configuration options for the HTTP calls statistics collector */
      public HttpStatisticsCollector.StaticConfig getHttpStatsCollector()
      {
        return _httpStatsCollector;
      }

      /** Configuration options for the event log writer. */
      public EventLogWriter.StaticConfig getEventLogWriterConfig()
      {
        return _eventLogWriterConfig;
      }

      /** Configuration options for the event log reader. */
      public EventLogReader.StaticConfig getEventLogReaderConfig()
      {
        return _eventLogReaderConfig;
      }

      /**Configuration option for starting db puller thread on startup */
      public boolean getStartDbPuller()
      {
        return _startDbPuller;
      }

      /**
       * Configuration for all physical data sources used by the relay.
       *
       * NOTE: WIP*/
      public DataSourcesStaticConfig getDataSources()
      {
        return _dataSources;
      }

      /** Physical sources config */
      public PhysicalSourceStaticConfig[] getPhysicalSourcesConfigs()
      {
        if (null == _physicalSourcesConfigs) return null;
        return Arrays.copyOf(_physicalSourcesConfigs, _physicalSourcesConfigs.length);
      }

    }

    public static class StaticConfigBuilderBase
    {
      protected DbusEventBuffer.Config _eventBuffer;
      protected ServerContainer.Config _container;
      protected SchemaRegistryConfigBuilder _schemaRegistry;
      protected final HashMap<String, String> _sourceName;
      protected RuntimeConfigBuilder _runtime;
      protected HttpStatisticsCollector.Config _httpStatsCollector;
      protected DbusEventsStatisticsCollector.Config _outboundEventsStatsCollector;
      protected DbusEventsStatisticsCollector.Config _inboundEventsStatsCollector;
      protected DatabusEventRandomProducer.Config _randomProducer;
      protected EventLogWriter.Config _eventLogWriter;
      protected EventLogReader.Config _eventLogReader;
      protected String _startDbPuller;
      protected DataSourcesStaticConfigBuilder _dataSources;
      protected ArrayList<PhysicalSourceConfig> _physicalSourcesConfigs;
      protected String _physicalSourcesConfigsPattern;

      public StaticConfigBuilderBase() throws IOException
      {
        _eventBuffer = new DbusEventBuffer.Config();
        _schemaRegistry = new SchemaRegistryConfigBuilder();
        setContainer(new ServerContainer.Config());
        _sourceName = new HashMap<String, String>(100);
        _runtime = new RuntimeConfigBuilder(_container.getRuntime());
        _httpStatsCollector = new HttpStatisticsCollector.Config();
        _outboundEventsStatsCollector = new DbusEventsStatisticsCollector.Config();
        _inboundEventsStatsCollector = new DbusEventsStatisticsCollector.Config();
        _randomProducer = new DatabusEventRandomProducer.Config();
        _eventLogWriter = new EventLogWriter.Config();
        _eventLogReader = new EventLogReader.Config();
        _dataSources = new DataSourcesStaticConfigBuilder();
        _physicalSourcesConfigs = new ArrayList<PhysicalSourceConfig>();
        setStartDbPuller("false");
      }

      public ServerContainer.Config getContainer()
      {
        return _container;
      }

      public DbusEventBuffer.Config getEventBuffer()
      {
        return _eventBuffer;
      }

      public void setEventBuffer(DbusEventBuffer.Config eventBufferConfig)
      {
        System.out.println("DEBUG: setEventBuffer Called");
        _eventBuffer = eventBufferConfig;
      }

      public DatabusEventRandomProducer.Config getRandomProducer()
      {
          return _randomProducer;
      }

      public void setRandomProducer(DatabusEventRandomProducer.Config randomProducer)
      {
        _randomProducer = randomProducer;
      }

      public void setContainer(ServerContainer.Config container)
      {
        _container = container;
        _container.setRuntimeConfigPropertyPrefix("com.linkedin.databus.relay");
      }

      public SchemaRegistryConfigBuilder getSchemaRegistry()
      {
        return _schemaRegistry;
      }

      public void setSourceName(String idStr, String name)
      {
        _sourceName.put(idStr, name);
      }

      public String getSourceName(String idStr)
      {
        return _sourceName.get(idStr);
      }

      public RuntimeConfigBuilder getRuntime()
      {
        return _runtime;
      }

      public void setRuntime(RuntimeConfigBuilder runtime)
      {
        _runtime = runtime;
      }

      public HttpStatisticsCollector.Config getHttpStatsCollector()
      {
        return _httpStatsCollector;
      }

      public void setHttpStatsCollector(HttpStatisticsCollector.Config httpStatsCollector)
      {
        _httpStatsCollector = httpStatsCollector;
      }

      public DbusEventsStatisticsCollector.Config getOutboundEventsStatsCollector()
      {
        return _outboundEventsStatsCollector;
      }

      public void setOutboundEventsStatsCollector(DbusEventsStatisticsCollector.Config eventsStatsCollector)
      {
        _outboundEventsStatsCollector = eventsStatsCollector;
      }

      public DbusEventsStatisticsCollector.Config getInboundEventsStatsCollector()
      {
        return _inboundEventsStatsCollector;
      }

      public void setInboundEventsStatsCollector(DbusEventsStatisticsCollector.Config inboundEventsStatsCollector)
      {
        _inboundEventsStatsCollector = inboundEventsStatsCollector;
      }

      public EventLogWriter.Config getEventLogWriter()
      {
        return _eventLogWriter;
      }

      public void setEventLogWriter(EventLogWriter.Config eventLogWriterConfig)
      {
        _eventLogWriter = eventLogWriterConfig;
      }

      public EventLogReader.Config getEventLogReader()
      {
        return _eventLogReader;
      }

      public void setEventLogReader(EventLogReader.Config eventLogReader)
      {
        _eventLogReader = eventLogReader;
      }

      public void setStartDbPuller(String startDbPuller)
      {
        _startDbPuller = startDbPuller;
      }

      public String getStartDbPuller()
      {
        return _startDbPuller;
      }

      public DataSourcesStaticConfigBuilder getDataSources()
      {
        return _dataSources;
      }

      public PhysicalSourceConfig getPhysicalSourcesConfigs(int index)
      {
        while (_physicalSourcesConfigs.size() <= index) _physicalSourcesConfigs.add(new PhysicalSourceConfig());
        return _physicalSourcesConfigs.get(index);
      }

      public void setPhysicalSourcesConfigs(int index, PhysicalSourceConfig conf)
      {
        while (_physicalSourcesConfigs.size() <= index) _physicalSourcesConfigs.add(new PhysicalSourceConfig());
        _physicalSourcesConfigs.set(index, conf);
      }

      public String getPhysicalSourcesConfigsPattern()
      {
        return _physicalSourcesConfigsPattern;
      }

      public void setPhysicalSourcesConfigsPattern(String physicalSourcesConfigsPattern)
      {
        _physicalSourcesConfigsPattern = physicalSourcesConfigsPattern;
      }

      protected PhysicalSourceStaticConfig[] buildInitPhysicalSourcesConfigs()
                throws InvalidConfigException
      {
        String[] sourcesConfigFiles = null;
        String physConfDirName = null;
        if (null != _physicalSourcesConfigsPattern && _physicalSourcesConfigsPattern.trim().length() >0)
        {
          File patternFile = new File(_physicalSourcesConfigsPattern);
          physConfDirName = patternFile.getParent();
          final String globPattern = patternFile.getName().replace(".", "\\.").replace("*", ".*")
                                                .replace("?", ".");

          sourcesConfigFiles = patternFile.getParentFile().list(new FilenameFilter()
          {
            @Override
            public boolean accept(File dir, String name)
            {
              return name.matches(globPattern);
            }
          });

          LOG.info("loading physical sources configs from: " + Arrays.toString(sourcesConfigFiles));
        }

        int physConfigsSize = _physicalSourcesConfigs.size();
        if (null != sourcesConfigFiles) physConfigsSize += sourcesConfigFiles.length;

        PhysicalSourceStaticConfig[] physConfigs = new PhysicalSourceStaticConfig[physConfigsSize];
        int physConfIdx = 0;
        for (PhysicalSourceConfig confBuilder: _physicalSourcesConfigs)
        {
          physConfigs[physConfIdx++] = confBuilder.build();
        }

        if (null != sourcesConfigFiles)
        {
          PhysicalSourceConfigBuilder fileConfBuilder =
              new PhysicalSourceConfigBuilder(physConfDirName, sourcesConfigFiles);
          PhysicalSourceStaticConfig[] confsFromFiles = fileConfBuilder.build();
          System.arraycopy(confsFromFiles, 0, physConfigs, physConfIdx, confsFromFiles.length);
        }

        return physConfigs;
      }

    }

    public static class Config extends StaticConfigBuilderBase implements ConfigBuilder<StaticConfig>
    {

      public Config() throws IOException
      {
        super();
      }

      @Override
      public StaticConfig build() throws InvalidConfigException
      {
        ArrayList<IdNamePair> sourceIds = new ArrayList<IdNamePair>(_sourceName.size());
        for (String srcIdStr: _sourceName.keySet())
        {
          try
          {
            long srcId = Long.parseLong(srcIdStr);
            sourceIds.add(new IdNamePair(srcId, _sourceName.get(srcIdStr)));
          }
          catch (NumberFormatException nfe)
          {
            throw new InvalidConfigException("Invalid source id: " + srcIdStr);
          }
        }

        PhysicalSourceStaticConfig[] physConfigs = buildInitPhysicalSourcesConfigs();

        // TODO (DDSDBUS-77) Add config verification
        return new StaticConfig(_eventBuffer.build(), _container.build(), _schemaRegistry.build(),
                                sourceIds, getRuntime(), _httpStatsCollector.build(),
                                _inboundEventsStatsCollector.build(),
                                _outboundEventsStatsCollector.build(),
                                _randomProducer.build(),
                                _eventLogWriter.build(),
                                _eventLogReader.build(),
                                Boolean.parseBoolean(_startDbPuller),
                                _dataSources.build(),
                                physConfigs);
      }

    }

    public DbusEventBufferMult getEventBuffer()
    {
      return _eventBufferMult;
    }

    public SchemaRegistryService getSchemaRegistryService()
    {
      return _schemaRegistryService;
    }

    public ConfigManager<RuntimeConfig> getRelayConfigManager()
    {
      return _relayConfigManager;
    }

    public HttpStatisticsCollector getHttpStatisticsCollector()
    {
      return _httpStatisticsCollector;
    }

    @Override
    public void pause()
    {
      //TODO: for jmx admin mbean, to be implemented by sub-classes
    }

    @Override
    public void resume()
    {
      //TODO: for jmx admin mbean, to be implemented by sub-classes
    }

    @Override
    public void suspendOnError(Throwable cause)
    {
      //TODO: implement me
    }

    // create a "fake" configuration for backward compatiblity - in case a new configuration is not available
    private void initPConfigs(HttpRelay.StaticConfig config) throws InvalidConfigException {
      if(_pConfigs != null)
        return;

      StringBuilder logListIds =
          new StringBuilder("Creating default physical source config. Sources are: ");

      // default ph config
      PhysicalSourceConfig pConfig =
          PhysicalSourceConfig.createFromLogicalSources(_sourcesIdNameRegistry.getAllSources());

      //
      for(LogicalSourceConfig ls : pConfig.getSources())
        logListIds.append(ls.getId() + ":" + ls.getName() + ",");
      LOG.info(logListIds);
      // set the memeber
      _pConfigs = new ArrayList<PhysicalSourceStaticConfig>(1);
      _pConfigs.add(pConfig.build());
    }

    public List<PhysicalSourceStaticConfig> getPhysicalSources() {
      return _pConfigs;
    }

    public SourceIdNameRegistry getSourcesIdNameRegistry()
    {
      return _sourcesIdNameRegistry;
    }

    @Override
    public void addPartition(PhysicalSourceStaticConfig pConfig) throws DatabusException
    {
      addNewBuffer(pConfig, _relayStaticConfig);
    }

    @Override
    public void removePartition(PhysicalSourceStaticConfig pConfig)
    {
      removeBuffer(pConfig);
    }

    public void saveBufferMetaInfo(boolean infoOnly) throws IOException {
      getEventBuffer().saveBufferMetaInfo(infoOnly);
    }

    public void validateRelayBuffers() throws DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException {
      getEventBuffer().validateRelayBuffers();
    }

    @Override
    public void shutdown() {
      super.shutdown(); // shuts down all the connections
      getEventBuffer().rollbackAllBuffers();
      getEventBuffer().close(); // will save the state of MMapped Buffers
    }

    public void addPhysicalPartitionCollectors(PhysicalPartition pPartition)
    {
      String statsCollectorName = pPartition.toSimpleString();
      if (null != _inBoundStatsCollectors)
        synchronized (_inBoundStatsCollectors)
        {
          if (null == _inBoundStatsCollectors.getStatsCollector(statsCollectorName))
          {
            _inBoundStatsCollectors.addStatsCollector(statsCollectorName, new DbusEventsStatisticsCollector(getContainerStaticConfig().getId(),
                    statsCollectorName+".inbound",
                    true,
                    false,
                    getMbeanServer()));
          }
        }

      if (null != _outBoundStatsCollectors)
        synchronized (_outBoundStatsCollectors)
        {
          if (null == _outBoundStatsCollectors.getStatsCollector(statsCollectorName))
          {
            _outBoundStatsCollectors.addStatsCollector(statsCollectorName, new DbusEventsStatisticsCollector(getContainerStaticConfig().getId(),
                    statsCollectorName+".outbound",
                    true,
                    false,
                    getMbeanServer()));
          }
        }
    }
}
