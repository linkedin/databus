package com.linkedin.databus.bootstrap.utils;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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
import org.codehaus.jackson.map.ObjectMapper;

import com.linkedin.databus.bootstrap.common.BootstrapConfig;
import com.linkedin.databus.bootstrap.common.BootstrapReadOnlyConfig;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.producers.db.MonitoredSourceInfo;
import com.linkedin.databus2.relay.OracleEventProducerFactory;
import com.linkedin.databus2.relay.config.LogicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceConfig;
import com.linkedin.databus2.schemas.FileSystemSchemaRegistryService;
import com.linkedin.databus2.schemas.SchemaRegistryConfigBuilder;
import com.linkedin.databus2.schemas.SchemaRegistryStaticConfig;


public class BootstrapAvroFileSeederMain 
{
	public static final String MODULE = BootstrapAvroFileSeederMain.class.getName();
	public static final Logger LOG = Logger.getLogger(MODULE);

    public static final String HELP_OPT_LONG_NAME = "help";
	public static final String BOOTSTRAP_DB_PROPS_OPT_LONG_NAME = "bootstrap_db_props";
	public static final String PHYSICAL_CONFIG_OPT_LONG_NAME = "physical_config";
    public static final String LOG4J_PROPS_OPT_LONG_NAME = "log_props";
    public static final char   HELP_OPT_CHAR = 'h';
	public static final char   BOOTSTRAP_DB_PROP_OPT_CHAR = 'p';
	public static final char   PHYSICAL_CONFIG_OPT_CHAR = 'c';
	public static final char LOG4J_PROPS_OPT_CHAR = 'l';
	
	private static Properties  _sBootstrapConfigProps = null;
	private static String      _sSourcesConfigFile    = null;
	private static StaticConfig _sStaticConfig        = null;
	private static List<MonitoredSourceInfo> _sources = null;
	private static BootstrapDBSeeder _sSeeder         = null;
	private static BootstrapAvroFileEventReader _sReader = null;
	private static BootstrapSeederWriterThread _sWriterThread = null;
	private static BootstrapEventBuffer  _sBootstrapBuffer = null;
	
    public static BootstrapDBSeeder getSeeder()
    {
      return _sSeeder;
    }

    public static BootstrapAvroFileEventReader getReader()
    {
      return _sReader;
    }

    public static Properties getBootstrapConfigProps()
    {
      return _sBootstrapConfigProps;
    }

    public static String getSourcesConfigFile()
    {
      return _sSourcesConfigFile;
    }

    public static StaticConfig getStaticConfig()
    {
      return _sStaticConfig;
    }

    public static List<MonitoredSourceInfo> getSources()
    {
      return _sources;
    }

  /**
	 * @param args
	 */
	public static void main(String[] args)
	  throws Exception
	{
	    init(args);
	    _sSeeder.startSeeding();
	    //_sReader.readEventsFromAllSources(0);
	    _sReader.start();
	    _sWriterThread.start();
	}

	public static void init(String[] args)
	  throws Exception
	{
		parseArgs(args);

	    File sourcesJson = new File(_sSourcesConfigFile);

	    ObjectMapper mapper = new ObjectMapper();
	    PhysicalSourceConfig physicalSourceConfig = mapper.readValue(sourcesJson, PhysicalSourceConfig.class);
	    physicalSourceConfig.checkForNulls();

	    Config config = new Config();

	    ConfigLoader<StaticConfig> configLoader =
	          		new ConfigLoader<StaticConfig>("databus.seed.", config);
	    _sStaticConfig = configLoader.loadConfig(_sBootstrapConfigProps);

	    // Make sure the URI from the configuration file identifies an Oracle JDBC source.
	    String uri = physicalSourceConfig.getUri();
	    if(!uri.startsWith("jdbc:oracle"))
	    {
	      throw new InvalidConfigException("Invalid source URI (" +
	    		  physicalSourceConfig.getUri() + "). Only jdbc:oracle: URIs are supported.");
	    }

	    OracleEventProducerFactory factory = new BootstrapSeederOracleEventProducerFactory(_sStaticConfig.getController().getPKeyNameMap());
	    
	    // Parse each one of the logical sources
	    _sources = new ArrayList<MonitoredSourceInfo>();
	    FileSystemSchemaRegistryService schemaRegistryService =
	    	    FileSystemSchemaRegistryService.build(_sStaticConfig.getSchemaRegistry().getFileSystem());

	    for(LogicalSourceConfig sourceConfig : physicalSourceConfig.getSources())
	    {
	      MonitoredSourceInfo source =
	          factory.buildOracleMonitoredSourceInfo(sourceConfig.build(), physicalSourceConfig.build(), schemaRegistryService);
	      _sources.add(source);
	    }
	    _sSeeder = new BootstrapDBSeeder(_sStaticConfig.getBootstrap(),_sources,_sStaticConfig.getCheckpointPersistanceScript());

	    _sBootstrapBuffer = new BootstrapEventBuffer(_sStaticConfig.getController().getCommitInterval() * 2);

	    _sWriterThread = new BootstrapSeederWriterThread(_sBootstrapBuffer, _sSeeder);

	    _sReader = new BootstrapAvroFileEventReader(_sStaticConfig.getController(),
	                                                _sources,
		                                             _sSeeder.getLastRows(),
	    										    _sBootstrapBuffer);
	}

	@SuppressWarnings("static-access")
	public static void parseArgs(String[] args) throws IOException
	{
		CommandLineParser cliParser = new GnuParser();


        Option helpOption = OptionBuilder.withLongOpt(HELP_OPT_LONG_NAME)
                                       .withDescription("Help screen")
                                       .create(HELP_OPT_CHAR);

	    Option sourcesOption = OptionBuilder.withLongOpt(PHYSICAL_CONFIG_OPT_LONG_NAME)
	                                   .withDescription("Bootstrap producer properties to use")
	                                   .hasArg()
	                                   .withArgName("property_file")
	                                   .create(PHYSICAL_CONFIG_OPT_CHAR);

	    Option dbOption = OptionBuilder.withLongOpt(BOOTSTRAP_DB_PROPS_OPT_LONG_NAME)
        							.withDescription("Bootstrap producer properties to use")
        							.hasArg()
        							.withArgName("property_file")
        							.create(BOOTSTRAP_DB_PROP_OPT_CHAR);

	    Option log4jPropsOption = OptionBuilder.withLongOpt(LOG4J_PROPS_OPT_LONG_NAME)
	    									.withDescription("Log4j properties to use")
	    									.hasArg()
	    									.withArgName("property_file")
	    									.create(LOG4J_PROPS_OPT_CHAR);

	    Options options = new Options();
	    options.addOption(helpOption);
	    options.addOption(sourcesOption);
	    options.addOption(dbOption);
	    options.addOption(log4jPropsOption);

	    CommandLine cmd = null;
	    try
	    {
	      cmd = cliParser.parse(options, args);
	    }
	    catch (ParseException pe)
	    {
	      LOG.fatal("Bootstrap Physical Config: failed to parse command-line options.", pe);
	      throw new RuntimeException("Bootstrap Physical Config: failed to parse command-line options.", pe);
	    }

	    if (cmd.hasOption(LOG4J_PROPS_OPT_CHAR))
	    {
	      String log4jPropFile = cmd.getOptionValue(LOG4J_PROPS_OPT_CHAR);
	      PropertyConfigurator.configure(log4jPropFile);
	      LOG.info("Using custom logging settings from file " + log4jPropFile);
	    }
	    else
	    {
	      PatternLayout defaultLayout = new PatternLayout("%d{ISO8601} +%r [%t] (%p) {%c} %m%n");
	      ConsoleAppender defaultAppender = new ConsoleAppender(defaultLayout);

	      Logger.getRootLogger().removeAllAppenders();
	      Logger.getRootLogger().addAppender(defaultAppender);
	      Logger.getRootLogger().setLevel(Level.INFO);  //using info as the default log level
	      LOG.info("Using default logging settings. Log Level is :" + Logger.getRootLogger().getLevel());
	    }

	    if (cmd.hasOption(HELP_OPT_CHAR))
	    {
	      printCliHelp(options);
	      System.exit(0);
	    }

	    if (! cmd.hasOption(PHYSICAL_CONFIG_OPT_CHAR))
	    	throw new RuntimeException("Sources Config is not provided; use --help for usage");

	    if (!cmd.hasOption(BOOTSTRAP_DB_PROP_OPT_CHAR) )
	    	throw new RuntimeException("Bootstrap config is not provided; use --help for usage");

	    _sSourcesConfigFile = cmd.getOptionValue(PHYSICAL_CONFIG_OPT_CHAR);

	    String propFile = cmd.getOptionValue(BOOTSTRAP_DB_PROP_OPT_CHAR);
	    LOG.info("Loading bootstrap DB config from properties file " + propFile);

	    _sBootstrapConfigProps = new Properties();
	    _sBootstrapConfigProps.load(new FileInputStream(propFile));

	}

    private static void printCliHelp(Options cliOptions)
    {
      HelpFormatter helpFormatter = new HelpFormatter();
      helpFormatter.printHelp("java " + BootstrapSeederMain.class.getName(), cliOptions);
    }
	public static class StaticConfig
	{
		public SchemaRegistryStaticConfig getSchemaRegistry()
		{
			return _schemaRegistry;
		}

		public BootstrapReadOnlyConfig getBootstrap()
		{
			return _bootstrap;
		}

		public BootstrapAvroFileEventReader.StaticConfig getController()
		{
			return _controller;
		}

		public Map<String,String> getCheckpointPersistanceScript()
		{
			return _checkpointPersistanceScript;
		}

		public StaticConfig(BootstrapAvroFileEventReader.StaticConfig controller,
				SchemaRegistryStaticConfig schemaRegistry,
				BootstrapReadOnlyConfig bootstrap,
				Map<String,String> checkpointPersistanceScript)
		{
			super();
			_controller = controller;
			_schemaRegistry = schemaRegistry;
			_bootstrap = bootstrap;
			_checkpointPersistanceScript = checkpointPersistanceScript;
		}

		private final BootstrapAvroFileEventReader.StaticConfig _controller;
		private final SchemaRegistryStaticConfig _schemaRegistry;
		private final BootstrapReadOnlyConfig   _bootstrap;
		private final Map<String,String>        _checkpointPersistanceScript;
	}

	public static class Config implements ConfigBuilder<StaticConfig>
	{
		public Config()
		{
			_controller = new BootstrapAvroFileEventReader.Config();
			_checkpointPersistanceScript = new HashMap<String, String>();

			try
			{
				_bootstrap = new BootstrapConfig();
			} catch (IOException io) {
				LOG.error("Got exception while instantiating BootstrapConfig !!",io);
				throw new RuntimeException("Got exception while instantiating BootstrapConfig !!",io);
			}

			_schemaRegistry = new SchemaRegistryConfigBuilder();
		}

		@Override
		public StaticConfig build() throws InvalidConfigException
		{
			return new StaticConfig(_controller.build(),
					_schemaRegistry.build(), _bootstrap.build(), _checkpointPersistanceScript);
		}


		public SchemaRegistryConfigBuilder getSchemaRegistry()
		{
			return _schemaRegistry;
		}

		public BootstrapConfig getBootstrap()
		{
			return _bootstrap;
		}

		public BootstrapAvroFileEventReader.Config getController()
		{
			return _controller;
		}


		public void setSchemaRegistry(SchemaRegistryConfigBuilder schemaRegistry)
		{
			this._schemaRegistry = schemaRegistry;
		}

		public void setBootstrap(BootstrapConfig bootstrap)
		{
			this._bootstrap = bootstrap;
		}

		public void setController(BootstrapAvroFileEventReader.Config controller)
		{
			this._controller = controller;
		}

		public void setCheckpointPersistanceScript(String source, String script)
		{
			_checkpointPersistanceScript.put(source,script);
		}

		public String getCheckpointPersistanceScript(String source)
		{
			String script = _checkpointPersistanceScript.get(source);

			if ( null == script )
			{
				_checkpointPersistanceScript.put(source,DEFAULT_SCRIPT);
				return DEFAULT_SCRIPT;
			}
			return script;
		}

		private  BootstrapAvroFileEventReader.Config _controller;
		private  SchemaRegistryConfigBuilder _schemaRegistry;
		private  BootstrapConfig   _bootstrap;
		private final  Map<String,String>  _checkpointPersistanceScript;

		private static final String DEFAULT_SCRIPT = "";
	}

}
