package com.linkedin.databus.bootstrap.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
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

import com.linkedin.databus.bootstrap.common.BootstrapCleanerConfig;
import com.linkedin.databus.bootstrap.common.BootstrapCleanerStaticConfig;
import com.linkedin.databus.bootstrap.common.BootstrapConfig;
import com.linkedin.databus.bootstrap.common.BootstrapDBCleaner;
import com.linkedin.databus.bootstrap.common.BootstrapReadOnlyConfig;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus2.core.container.request.BootstrapDatabaseTooOldException;
import com.linkedin.databus2.util.DBHelper;

/*
 *
 * Bootstrap DB Cleaner StandAlone Script.
 * 
 * Used for manually cleaning bootstrap DB.
 *
 */
public class BootstrapDBCleanerMain
{
  public static final String MODULE = BootstrapSeederMain.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  public static final String HELP_OPT_LONG_NAME = "help";
  public static final char   HELP_OPT_CHAR = 'h';
  public static final String BOOTSTRAP_SOURCES_OPT_LONG_NAME = "sources";
  public static final char   BOOTSTRAP_SOURCES_PROP_OPT_CHAR = 's';
  public static final String BOOTSTRAP_DB_PROPS_OPT_LONG_NAME = "bootstrap_db_props";
  public static final char   BOOTSTRAP_DB_PROP_OPT_CHAR = 'p';
  public static final String CLEANER_CMD_LINE_PROPS_OPT_LONG_NAME = "cleaner_cmdline_props";
  public static final char CLEANER_CMD_LINE_PROPS_OPT_CHAR = 'c';
  public static final String LOG4J_PROPS_OPT_LONG_NAME = "log_props";
  public static final char LOG4J_PROPS_OPT_CHAR = 'l';

  private static BootstrapCleanerStaticConfig _sCleanerConfig        = null;
  private static BootstrapReadOnlyConfig _sBootstrapConfig        = null;
  private static List<String>            _sSources  = null;
  
  private static Properties  _sBootstrapConfigProps = null;

  /**
   * @param args
   */
  public static void main(String[] args)
    throws Exception
  {
    parseArgs(args);
    BootstrapCleanerConfig config = new BootstrapCleanerConfig();
    BootstrapConfig bsConfig = new BootstrapConfig();
    
    ConfigLoader<BootstrapCleanerStaticConfig> configLoader =
                new ConfigLoader<BootstrapCleanerStaticConfig>("databus.bootstrap.cleaner.", config);

    ConfigLoader<BootstrapReadOnlyConfig> configLoader2 =
            new ConfigLoader<BootstrapReadOnlyConfig>("databus.bootstrap.db.", bsConfig);
    
    _sCleanerConfig = configLoader.loadConfig(_sBootstrapConfigProps);
    _sBootstrapConfig = configLoader2.loadConfig(_sBootstrapConfigProps);

    
    BootstrapDBCleaner cleaner = new BootstrapDBCleaner("StandAloneCleaner", 
    													_sCleanerConfig, 
    													_sBootstrapConfig, 
    													null, 
    													_sSources);
    
    
    if ((null == _sSources) || (_sSources.isEmpty()))
    {
    	_sSources = getSourceNames(cleaner.getBootstrapDao().getBootstrapConn().getDBConn());
    	LOG.info("Going to run cleaner for all sources :" + _sSources);
		try
		{
			cleaner.setSources(cleaner.getBootstrapDao().getSourceIdAndStatusFromName(_sSources,false));
		} catch (BootstrapDatabaseTooOldException bto) {
			LOG.error("Not expected to receive this exception as activeCheck is turned-off", bto);
			throw new RuntimeException(bto);
		}	
    }
   
    cleaner.doClean();
  }


  @SuppressWarnings("static-access")
  public static void parseArgs(String[] args) throws IOException
  {
      CommandLineParser cliParser = new GnuParser();


      Option helpOption = OptionBuilder.withLongOpt(HELP_OPT_LONG_NAME)
                                     .withDescription("Help screen")
                                     .create(HELP_OPT_CHAR);

      Option dbOption = OptionBuilder.withLongOpt(BOOTSTRAP_DB_PROPS_OPT_LONG_NAME)
                                  .withDescription("Bootstrap Cleaner and DB properties to use")
                                  .hasArg()
                                  .withArgName("property_file")
                                  .create(BOOTSTRAP_DB_PROP_OPT_CHAR);
      
      Option cmdLinePropsOption1 = OptionBuilder.withLongOpt(CLEANER_CMD_LINE_PROPS_OPT_LONG_NAME)
      								.withDescription("Cmd line override of cleaner config properties. Semicolon separated.")
      								.hasArg()
      								.withArgName("Semicolon_separated_properties")
      								.create(CLEANER_CMD_LINE_PROPS_OPT_CHAR);
      
	  Option log4jPropsOption = OptionBuilder.withLongOpt(LOG4J_PROPS_OPT_LONG_NAME)
									.withDescription("Log4j properties to use")
									.hasArg()
									.withArgName("property_file")
									.create(LOG4J_PROPS_OPT_CHAR);
	  
      Option sourcesOption = OptionBuilder.withLongOpt(BOOTSTRAP_SOURCES_OPT_LONG_NAME)
              .withDescription("Comma seperated list of sourceNames. If not provided, will cleanup all sources in the bootstrap DB.")
              .hasArg()
              .withArgName("comma-seperated sources")
              .create(BOOTSTRAP_SOURCES_PROP_OPT_CHAR);
      
      Options options = new Options();
      options.addOption(helpOption);
      options.addOption(dbOption);
      options.addOption(cmdLinePropsOption1);
	  options.addOption(log4jPropsOption);
	  options.addOption(sourcesOption);

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

      if (cmd.hasOption(BOOTSTRAP_SOURCES_PROP_OPT_CHAR))
      {
    	  _sSources = new ArrayList<String>();
    	  String srcListStr = cmd.getOptionValue(BOOTSTRAP_SOURCES_PROP_OPT_CHAR);
          LOG.info("Going to run cleaner for only these sources : " + srcListStr);
          String[] srcList = srcListStr.split(",");
          for (String s: srcList)
        	  _sSources.add(s);
      }
      
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
      if (cmd.hasOption(CLEANER_CMD_LINE_PROPS_OPT_CHAR))
      {
        String cmdLinePropString = cmd.getOptionValue(CLEANER_CMD_LINE_PROPS_OPT_CHAR);
        updatePropsFromCmdLine(_sBootstrapConfigProps, cmdLinePropString);
      }
  }

  private static void printCliHelp(Options cliOptions)
  {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp("java " + BootstrapDBCleanerMain.class.getName(), cliOptions);
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
  
  /*
   * Get the sourceIds which are bootstrapped in this DB
   *
   * @return list of SrcIds
   */
  private static List<String> getSourceNames(Connection conn)
  	throws SQLException
  {
	  List<String> srcNames = new ArrayList<String>();
	  String sql = "select src from bootstrap_sources";
	  ResultSet rs = null;
	  PreparedStatement stmt = null;
	  try
	  {
		  stmt = conn.prepareStatement(sql);
		  rs = stmt.executeQuery();

		  while (rs.next())
		  {
			  String name = rs.getString(1);
			  srcNames.add(name);
		  }
	  } finally {
		  DBHelper.close(rs,stmt,null);
	  }

	  return srcNames;
  }
}
