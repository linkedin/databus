package com.linkedin.databus.bootstrap.utils;
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


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;


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
import com.linkedin.databus2.relay.OracleJarUtils;
import com.linkedin.databus2.relay.config.LogicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceConfig;
import com.linkedin.databus2.schemas.FileSystemSchemaRegistryService;
import com.linkedin.databus2.schemas.SchemaRegistryConfigBuilder;
import com.linkedin.databus2.schemas.SchemaRegistryStaticConfig;


public class BootstrapSeederMain
{
	public static final String MODULE = BootstrapSeederMain.class.getName();
	public static final Logger LOG = Logger.getLogger(MODULE);

    public static final String HELP_OPT_LONG_NAME = "help";
	public static final String BOOTSTRAP_DB_PROPS_OPT_LONG_NAME = "bootstrap_db_props";
	public static final String PHYSICAL_CONFIG_OPT_LONG_NAME = "physical_config";
    public static final String LOG4J_PROPS_OPT_LONG_NAME = "log_props";
    public static final String VALIDATION_TYPE_OPT_LONG_NAME="validation-type";
    public static final char   HELP_OPT_CHAR = 'h';
	public static final char   BOOTSTRAP_DB_PROP_OPT_CHAR = 'p';
	public static final char   PHYSICAL_CONFIG_OPT_CHAR = 'c';
	public static final char LOG4J_PROPS_OPT_CHAR = 'l';
	public static final char VALIDATION_TYPE_OPT_CHAR='v';

	private static Properties  _sBootstrapConfigProps = null;
	private static String      _sSourcesConfigFile    = null;
	private static DataSource  _sDataStore      = null;
	private static StaticConfig _sStaticConfig        = null;
	private static List<MonitoredSourceInfo> _sources = null;
	private static BootstrapDBSeeder _sSeeder         = null;
	private static BootstrapSrcDBEventReader _sReader = null;
	private static BootstrapSeederWriterThread _sWriterThread = null;
	private static BootstrapEventBuffer  _sBootstrapBuffer = null;

	private static String _validationType = "normal";

    public static BootstrapDBSeeder getSeeder()
    {
      return _sSeeder;
    }

    public static String getValidationType ()
    {
    	return _validationType;
    }
    
    public static BootstrapSrcDBEventReader getReader()
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

    public static DataSource getDataStore()
    {
      return _sDataStore;
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

	    // Load the source configuration JSON file
	    //File sourcesJson = new File("integration-test/config/sources-member2.json");
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

	    // Create the OracleDataSource used to get DB connection(s)
	    try
	    {
	    	Class oracleDataSourceClass = OracleJarUtils.loadClass("oracle.jdbc.pool.OracleDataSource");
	    	Object ods = oracleDataSourceClass.newInstance();
		    Method setURLMethod = oracleDataSourceClass.getMethod("setURL", String.class);
		    setURLMethod.invoke(ods, uri);
	    	_sDataStore = (DataSource) ods;
	    } catch (Exception e)
	    {
	    	String errMsg = "Error creating a data source object ";
	    	LOG.error(errMsg, e);
	    	throw e;
	    }

	    //TODO: Need a better way than relaying on RelayFactory for generating MonitoredSourceInfo
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

	    _sReader = new BootstrapSrcDBEventReader(_sDataStore,_sBootstrapBuffer,
	                                             _sStaticConfig.getController(),
	                                             _sources,
	                                             _sSeeder.getLastRows(),
	                                             _sSeeder.getLastKeys(),
	                                             0,
	                                             _sStaticConfig.getCheckpointPersistanceScript());
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
	    
	    Option validationType = OptionBuilder.withLongOpt(VALIDATION_TYPE_OPT_LONG_NAME)
	    						.withDescription("Type of validation algorithm , normal[cmp two sorted streams of oracle and bootstrap db]  or point[entry in bootstrap checked in oracle]")
	    						.hasArg()
	    						.withArgName("validation_type")
	    						.create(VALIDATION_TYPE_OPT_CHAR);

	    Options options = new Options();
	    options.addOption(helpOption);
	    options.addOption(sourcesOption);
	    options.addOption(dbOption);
	    options.addOption(log4jPropsOption);
	    options.addOption(validationType);
	    

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
	    if (!cmd.hasOption(VALIDATION_TYPE_OPT_CHAR))
	    {
	    	_validationType = "normal";
	    } 
	    else 
	    {	
	    	String vtype = cmd.getOptionValue(VALIDATION_TYPE_OPT_CHAR);
	    	if (vtype.equals("point") || vtype.equals("normal") || vtype.equals("pointBs"))
	    	{
	    		_validationType = vtype;
	    	} 
	    	else
	    	{
	    		throw new RuntimeException("Validation type has to be one of 'normal' or 'point' or 'pointBs'");
	    	}
	    }

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

		public BootstrapSrcDBEventReader.StaticConfig getController()
		{
			return _controller;
		}

		public Map<String,String> getCheckpointPersistanceScript()
		{
			return _checkpointPersistanceScript;
		}

		public StaticConfig(BootstrapSrcDBEventReader.StaticConfig controller,
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

		private final BootstrapSrcDBEventReader.StaticConfig _controller;
		private final SchemaRegistryStaticConfig _schemaRegistry;
		private final BootstrapReadOnlyConfig   _bootstrap;
		private final Map<String,String>        _checkpointPersistanceScript;
 	}

	public static class Config implements ConfigBuilder<StaticConfig>
	{
		public Config()
		{
			_controller = new BootstrapSrcDBEventReader.Config();
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

		public BootstrapSrcDBEventReader.Config getController()
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

		public void setController(BootstrapSrcDBEventReader.Config controller)
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

		private  BootstrapSrcDBEventReader.Config _controller;
		private  SchemaRegistryConfigBuilder _schemaRegistry;
		private  BootstrapConfig   _bootstrap;
		private final  Map<String,String>  _checkpointPersistanceScript;

		private static final String DEFAULT_SCRIPT = "";
	}
}
