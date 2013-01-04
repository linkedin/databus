package com.linkedin.databus.bootstrap.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
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
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.PropertyConfigurator;

import com.linkedin.databus.bootstrap.common.BootstrapConfig;
import com.linkedin.databus.bootstrap.common.BootstrapConn;
import com.linkedin.databus.bootstrap.common.BootstrapDBMetaDataDAO;
import com.linkedin.databus.bootstrap.common.BootstrapReadOnlyConfig;
import com.linkedin.databus.core.util.ConfigLoader;

public class BootstrapDropSource
{

  public static final String MODULE = BootstrapDropSource.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  public static final String HELP_OPT_LONG_NAME = "help";
  public static final char   HELP_OPT_CHAR = 'h';
  public static final String BOOTSTRAP_DB_PROPS_OPT_LONG_NAME = "bootstrap_db_props";
  public static final char   BOOTSTRAP_DB_PROP_OPT_CHAR = 'p';
  public static final String CMD_LINE_PROPS_OPT_LONG_NAME = "cmdline_props";
  public static final char CMD_LINE_PROPS_OPT_CHAR = 'c';
  public static final String LOG4J_PROPS_OPT_LONG_NAME = "log_props";
  public static final char LOG4J_PROPS_OPT_CHAR = 'l';
  public static final String SOURCE_ID_OPT_LONG_NAME = "source_id";
  public static final char   SOURCE_ID_OPT_CHAR = 's';

  private static Properties  _sBootstrapConfigProps = null;
  private int           _srcId               = 0;

  private BootstrapDBMetaDataDAO           _dbDao       = null;
  private BootstrapReadOnlyConfig _config              = null;

  public BootstrapDropSource(BootstrapReadOnlyConfig config, int srcId)
  {
    _srcId = srcId;
    _config = config;
  }

  /*
   * @return a bootstrapDB connection object.
   * Note: The connection object is still owned by BootstrapConn. SO dont close it
   */
    public Connection getConnection()
    {
      Connection conn = null;

      if (_dbDao == null)
      {
        LOG.info("<<<< Creating Bootstrap Connection!! >>>>");
        BootstrapConn dbConn = new BootstrapConn();
        try
        {
           dbConn.initBootstrapConn(false,
                                            _config.getBootstrapDBUsername(),
                                            _config.getBootstrapDBPassword(),
                                            _config.getBootstrapDBHostname(),
                                            _config.getBootstrapDBName());
           _dbDao = new BootstrapDBMetaDataDAO(dbConn,        		
        		   							_config.getBootstrapDBHostname(),
        		   							_config.getBootstrapDBUsername(),
        		   							_config.getBootstrapDBPassword(),
        		   							_config.getBootstrapDBName(),
        		   							false);
        } catch (Exception e) {
            LOG.fatal("Unable to open BootstrapDB Connection !!", e);
            throw new RuntimeException("Got exception when getting bootstrap DB Connection.",e);
        }
      }

      try
      {
        conn = _dbDao.getBootstrapConn().getDBConn();
      } catch ( SQLException e) {
        LOG.fatal("Not able to open BootstrapDB Connection !!", e);
        throw new RuntimeException("Got exception when getting bootstrap DB Connection.",e);
      }
      return conn;
    }

  /**
   * @param args
   */
  public static void main(String[] args)
    throws Exception
  {
    int srcId = parseArgs(args);
    BootstrapConfig config = new BootstrapConfig();

    ConfigLoader<BootstrapReadOnlyConfig> configLoader =
                new ConfigLoader<BootstrapReadOnlyConfig>("bootstrap.", config);

    BootstrapReadOnlyConfig staticConfig = configLoader.loadConfig(_sBootstrapConfigProps);

    BootstrapDropSource cleaner = new BootstrapDropSource(staticConfig, srcId);
    cleaner.cleanup();
  }


  public void cleanup()
    throws Exception
  {
	getConnection();
	
    _dbDao.dropSourceInDB(_srcId);
  }

  @SuppressWarnings("static-access")
  public static int parseArgs(String[] args) throws IOException
  {
      CommandLineParser cliParser = new GnuParser();


      Option helpOption = OptionBuilder.withLongOpt(HELP_OPT_LONG_NAME)
                                     .withDescription("Help screen")
                                     .create(HELP_OPT_CHAR);

      Option sourceIdOption = OptionBuilder.withLongOpt(SOURCE_ID_OPT_LONG_NAME)
                                     .withDescription("Source ID for which tables need to be dropped")
                                     .hasArg()
                                     .withArgName("Source ID")
                                     .create(SOURCE_ID_OPT_CHAR);

      Option dbOption = OptionBuilder.withLongOpt(BOOTSTRAP_DB_PROPS_OPT_LONG_NAME)
                                  .withDescription("Bootstrap producer properties to use")
                                  .hasArg()
                                  .withArgName("property_file")
                                  .create(BOOTSTRAP_DB_PROP_OPT_CHAR);

      Option cmdLinePropsOption = OptionBuilder.withLongOpt(CMD_LINE_PROPS_OPT_LONG_NAME)
                                    .withDescription("Cmd line override of config properties. Semicolon separated.")
                                    .hasArg()
                                    .withArgName("Semicolon_separated_properties")
                                    .create(CMD_LINE_PROPS_OPT_CHAR);

      Option log4jPropsOption = OptionBuilder.withLongOpt(LOG4J_PROPS_OPT_LONG_NAME)
                                    .withDescription("Log4j properties to use")
                                    .hasArg()
                                    .withArgName("property_file")
                                    .create(LOG4J_PROPS_OPT_CHAR);

      Options options = new Options();
      options.addOption(helpOption);
      options.addOption(sourceIdOption);
      options.addOption(dbOption);
      options.addOption(cmdLinePropsOption);
      options.addOption(log4jPropsOption);

      CommandLine cmd = null;
      try
      {
        cmd = cliParser.parse(options, args);
      }
      catch (ParseException pe)
      {
        LOG.error("Bootstrap Physical Config: failed to parse command-line options.", pe);
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

          LOG.info("Using default logging settings");
      }

      if (cmd.hasOption(HELP_OPT_CHAR))
      {
        printCliHelp(options);
        System.exit(0);
      }

      if ( !cmd.hasOption(SOURCE_ID_OPT_CHAR))
        throw new RuntimeException("Source ID is not provided");

      if (!cmd.hasOption(BOOTSTRAP_DB_PROP_OPT_CHAR) )
          throw new RuntimeException("Bootstrap config is not provided");

      String propFile = cmd.getOptionValue(BOOTSTRAP_DB_PROP_OPT_CHAR);
      LOG.info("Loading bootstrap DB config from properties file " + propFile);

      _sBootstrapConfigProps = new Properties();
	  FileInputStream f = new FileInputStream(propFile);
	  try
	  {
		_sBootstrapConfigProps.load(f);
	  } finally {
		f.close();
	  }
	  
      if (cmd.hasOption(CMD_LINE_PROPS_OPT_CHAR))
      {
        String cmdLinePropString = cmd.getOptionValue(CMD_LINE_PROPS_OPT_CHAR);
        updatePropsFromCmdLine(_sBootstrapConfigProps, cmdLinePropString);
      }

      return Integer.parseInt(cmd.getOptionValue(SOURCE_ID_OPT_CHAR));
  }

  private static void updatePropsFromCmdLine(Properties props, String cmdLinePropString)
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
        props.put(onePropSplit[0], onePropSplit[1]);
      }
    }
  }

  private static void printCliHelp(Options cliOptions)
  {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp("java " + BootstrapDBCleanerMain.class.getName(), cliOptions);
  }
}
