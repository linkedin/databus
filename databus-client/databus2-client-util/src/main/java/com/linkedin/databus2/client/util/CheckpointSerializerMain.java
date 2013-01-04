package com.linkedin.databus2.client.util;

import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
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
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;

import com.linkedin.databus.client.DatabusHttpClientImpl;
import com.linkedin.databus.client.DatabusHttpClientImpl.CheckpointPersistenceStaticConfig;
import com.linkedin.databus.client.DatabusHttpClientImpl.CheckpointPersistenceStaticConfigBuilder;
import com.linkedin.databus.client.pub.CheckpointPersistenceProvider;
import com.linkedin.databus.client.pub.FileSystemCheckpointPersistenceProvider;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DbusClientMode;
import com.linkedin.databus.core.util.ConfigLoader;

/** Utility that can be used to serialize a checkpoint to a file */
public class CheckpointSerializerMain
{
  public static final String MODULE = CheckpointSerializerMain.class.getSimpleName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  public static final String HELP_OPT_NAME = "help";
  public static final String HELP_OPT_DESCR = "prints this help screen";
  public static final String ACTION_OPT_NAME = "action";
  public static final String ACTION_OPT_DESCR = "action to run: PRINT, CHANGE, DELETE";
  public static final String CLIENT_PROPS_FILE_OPT_NAME = "client_props";
  public static final String CLIENT_PROPS_FILE_OPT_DESCR = "specifies a client configuration properties file";
  public static final String CP3_PROPS_FILE_OPT_NAME = "cp3_props";
  public static final String CP3_PROPS_FILE_OPT_DESCR = "specifies a checkpoint persistence provider (CP3) configuration properties file ";
  public static final String PROPS_PREFIX_OPT_NAME = "props_prefix";
  public static final String PROPS_PREFIX_OPT_DESCR = "properties name prefix for the configuration file (client or CP3)";
  public static final String SOURCES_OPT_NAME = "sources";
  public static final String SOURCES_OPT_DESCR = "comma-separated source list for the checkpoint; -1 for flexible checkpoint";
  public static final String SCN_OPT_NAME = "sequence_num";
  public static final String SCN_OPT_DESCR = "new scn for the checkpoint to be saved";
  public static final String TYPE_OPT_NAME = "type";
  public static final String TYPE_OPT_DESCR = "the type of the checkpoint to be saved: BOOTSTRAP_SNAPSHOT, BOOTSTRAP_CATCHUP, ONLINE_CONSUMPTION";
  public static final String SINCE_SCN_OPT_NAME = "since_sequence_num";
  public static final String SINCE_SCN_OPT_DESCR = "sequence number for when the bootstrap started (snapshot and catchup)";
  public static final String START_SCN_OPT_NAME = "start_sequence_num";
  public static final String START_SCN_OPT_DESCR = "start sequence number for bootstrap checkpoints (snapshot and catchup)";
  public static final String TARGET_SCN_OPT_NAME = "target_sequence_num";
  public static final String TARGET_SCN_OPT_DESCR = "target sequence number for bootstrap checkpoints (catchup only)";
  public static final String BOOTSTRAP_SOURCE_OPT_NAME = "bootstrap_source";
  public static final String BOOTSTRAP_SOURCE_OPT_DESCR = "the current bootstrap source name";

  public static final char   ACTION_OPT_CHAR = 'a';
  public static final char   BOOTSTRAP_SOURCE_OPT_CHAR = 'b';
  public static final char   CLIENT_PROPS_FILE_OPT_CHAR = 'c';
  public static final char   CP3_PROPS_FILE_OPT_CHAR = 'C';
  public static final char   PROPS_PREFIX_OPT_CHAR = 'f';
  public static final char   HELP_OPT_CHAR = 'h';
  public static final char   SOURCES_OPT_CHAR = 'S';
  public static final char   SCN_OPT_CHAR = 's';
  public static final char   TYPE_OPT_CHAR = 't';
  public static final char   SINCE_SCN_OPT_CHAR = 'x';
  public static final char   START_SCN_OPT_CHAR = 'y';
  public static final char   TARGET_SCN_OPT_CHAR = 'z';

  private static enum Action
  {
    PRINT,
    CHANGE,
    DELETE
  }

  private static Action _action;
  private static String[] _sources;
  private static Properties _clientProps;
  private static Properties _cp3Props;
  private static String _propPrefix;
  private static Long _scn;
  private static Long _sinceScn;
  private static Long _startScn;
  private static Long _targetScn;
  private static DbusClientMode _cpType;
  private static String _bootstrapSource;

  @SuppressWarnings("static-access")
  private static Options createOptions()
  {
    Option helpOption = OptionBuilder.withLongOpt(HELP_OPT_NAME)
                                     .withDescription(HELP_OPT_DESCR)
                                     .create(HELP_OPT_CHAR);
    Option clientPropsOption = OptionBuilder.withLongOpt(CLIENT_PROPS_FILE_OPT_NAME)
                                            .withDescription(CLIENT_PROPS_FILE_OPT_DESCR)
                                            .hasArg()
                                            .withArgName("properties_file")
                                            .create(CLIENT_PROPS_FILE_OPT_CHAR);
    Option cp3PropsOption = OptionBuilder.withLongOpt(CP3_PROPS_FILE_OPT_NAME)
                                         .withDescription(CP3_PROPS_FILE_OPT_DESCR)
                                         .hasArg()
                                         .withArgName("properties_file")
                                         .create(CP3_PROPS_FILE_OPT_CHAR);
    Option propsPrefixOption = OptionBuilder.withLongOpt(PROPS_PREFIX_OPT_NAME)
                                            .withDescription(PROPS_PREFIX_OPT_DESCR)
                                            .hasArg()
                                            .withArgName("prefix_string")
                                            .create(PROPS_PREFIX_OPT_CHAR);
    Option sourcesOption = OptionBuilder.withLongOpt(SOURCES_OPT_NAME)
                                        .withDescription(SOURCES_OPT_DESCR)
                                        .hasArg()
                                        .withArgName("sources_list")
                                        .create(SOURCES_OPT_CHAR);
    Option scnOptOption = OptionBuilder.withLongOpt(SCN_OPT_NAME)
                                       .withDescription(SCN_OPT_DESCR)
                                       .hasArg()
                                       .withArgName("sequence_number")
                                       .create(SCN_OPT_CHAR);
    Option actionOption = OptionBuilder.withLongOpt(ACTION_OPT_NAME)
                                       .withDescription(ACTION_OPT_DESCR)
                                       .hasArg()
                                       .withArgName("action")
                                       .create(ACTION_OPT_CHAR);
    Option sinceScnOptOption = OptionBuilder.withLongOpt(SINCE_SCN_OPT_NAME)
                                            .withDescription(SINCE_SCN_OPT_DESCR)
                                            .hasArg()
                                            .withArgName("sequence_number")
                                            .create(SINCE_SCN_OPT_CHAR);
    Option startScnOptOption = OptionBuilder.withLongOpt(START_SCN_OPT_NAME)
                                            .withDescription(START_SCN_OPT_DESCR)
                                            .hasArg()
                                            .withArgName("sequence_number")
                                            .create(START_SCN_OPT_CHAR);
    Option targetScnOptOption = OptionBuilder.withLongOpt(TARGET_SCN_OPT_NAME)
                                             .withDescription(TARGET_SCN_OPT_DESCR)
                                             .hasArg()
                                             .withArgName("sequence_number")
                                             .create(TARGET_SCN_OPT_CHAR);
    Option typeOption = OptionBuilder.withLongOpt(TYPE_OPT_NAME)
                                     .withDescription(TYPE_OPT_DESCR)
                                     .hasArg()
                                     .withArgName("checkpoint_type")
                                     .create(TYPE_OPT_CHAR);
    Option bootstrapSourceOption = OptionBuilder.withLongOpt(BOOTSTRAP_SOURCE_OPT_NAME)
                                                .withDescription(BOOTSTRAP_SOURCE_OPT_DESCR)
                                                .hasArg()
                                                .withArgName("bootstrap_source_name")
                                                .create(BOOTSTRAP_SOURCE_OPT_CHAR);

    Options options = new Options();
    options.addOption(helpOption);
    options.addOption(actionOption);
    options.addOption(clientPropsOption);
    options.addOption(cp3PropsOption);
    options.addOption(propsPrefixOption);
    options.addOption(sourcesOption);
    options.addOption(scnOptOption);
    options.addOption(sinceScnOptOption);
    options.addOption(startScnOptOption);
    options.addOption(targetScnOptOption);
    options.addOption(typeOption);
    options.addOption(bootstrapSourceOption);

    return options;
  }

  private static void parseArgs(String[] args) throws Exception
  {
      CommandLineParser cliParser = new GnuParser();

      Options options = createOptions();

      CommandLine cmd = null;
      try
      {
        cmd = cliParser.parse(options, args);
      }
      catch (ParseException pe)
      {
        throw new RuntimeException("failed to parse command-line options.", pe);
      }

      if (cmd.hasOption(HELP_OPT_CHAR) || 0 == cmd.getOptions().length)
      {
        printCliHelp(options);
        System.exit(0);
      }

      try
      {
        _action = Action.valueOf(cmd.getOptionValue(ACTION_OPT_CHAR).toUpperCase());
      }
      catch (Exception e)
      {
        throw new RuntimeException("invalid action: " + cmd.getOptionValue(ACTION_OPT_CHAR), e);
      }

      if (! cmd.hasOption(SOURCES_OPT_CHAR))
      {
        throw new RuntimeException("expected sources list; see --help for more info");
      }

      String sourcesListStr = cmd.getOptionValue(SOURCES_OPT_CHAR);
      _sources = sourcesListStr.split(",");
      if (null == _sources || 0 == _sources.length)
      {
        throw new RuntimeException("empty sources list");
      }
      for (int i = 0; i < _sources.length; ++i) _sources[i] = _sources[i].trim();

      if (Action.PRINT != _action && ! cmd.hasOption(CLIENT_PROPS_FILE_OPT_CHAR) &&
          ! cmd.hasOption(CP3_PROPS_FILE_OPT_CHAR))
      {
        throw new RuntimeException("expected client or CP3 configuration; see --help for more info");
      }

      String defaultPropPrefix = null;
      if (cmd.hasOption(CLIENT_PROPS_FILE_OPT_CHAR))
      {
        try
        {
          _clientProps = loadProperties(cmd.getOptionValue(CLIENT_PROPS_FILE_OPT_CHAR));
          defaultPropPrefix = "databus2.client";
        }
        catch (Exception e)
        {
          throw new RuntimeException("unable to load client properties", e);
        }
      }
      else if (cmd.hasOption(CP3_PROPS_FILE_OPT_CHAR))
      {
        try
        {
          _cp3Props = loadProperties(cmd.getOptionValue(CP3_PROPS_FILE_OPT_CHAR));
          defaultPropPrefix = "databus2.client.checkpointPersistence";
        }
        catch (Exception e)
        {
          throw new RuntimeException("unable to load CP3 properties", e);
        }
      }

      _propPrefix = cmd.hasOption(PROPS_PREFIX_OPT_CHAR) ? cmd.getOptionValue(PROPS_PREFIX_OPT_CHAR)
                                                         : defaultPropPrefix;
      if (null != _propPrefix && ! _propPrefix.endsWith("."))
      {
        _propPrefix = _propPrefix + ".";
      }

      if (! cmd.hasOption(ACTION_OPT_CHAR))
      {
        throw new RuntimeException("action expected; see --help for more info");
      }

      _scn = parseLongOption(cmd, SCN_OPT_CHAR, "sequence number");
      _sinceScn = parseLongOption(cmd, SINCE_SCN_OPT_CHAR, "last sequence number");
      _startScn = parseLongOption(cmd, START_SCN_OPT_CHAR, "start sequence number");
      _targetScn = parseLongOption(cmd, TARGET_SCN_OPT_CHAR, "target sequence number");

      if (cmd.hasOption(TYPE_OPT_CHAR))
      {
        try
        {
          _cpType = DbusClientMode.valueOf(cmd.getOptionValue(TYPE_OPT_CHAR).toUpperCase());
        }
        catch (Exception e)
        {
          throw new RuntimeException("invalid checkpoint type:" + cmd.getOptionValue(TYPE_OPT_CHAR),
                                     e);
        }
      }

      if (cmd.hasOption(BOOTSTRAP_SOURCE_OPT_CHAR))
      {
        _bootstrapSource = cmd.getOptionValue(BOOTSTRAP_SOURCE_OPT_CHAR);
      }
  }

  private static Long parseLongOption(CommandLine cmd, char optionChar, String argName)
  {
    Long result = null;

    if (cmd.hasOption(optionChar))
    {
      try
      {
        result = Long.parseLong(cmd.getOptionValue(optionChar));
      }
      catch (NumberFormatException nfe)
      {
        throw new RuntimeException("invalid " + argName + ": " + cmd.getOptionValue(optionChar), nfe);
      }
    }

    return result;
  }

  private static Properties loadProperties(String fileName) throws IOException
  {
    Properties result = new Properties();
    FileReader freader = new FileReader(fileName);
    try
    {
      result.load(freader);
    }
    finally
    {
      freader.close();
    }

    return result;
  }

  private static void printCliHelp(Options cliOptions)
  {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.setWidth(120);
    helpFormatter.printHelp("java " + CheckpointSerializerMain.class.getName(), cliOptions);
  }

  private static Checkpoint updateCheckpoint(Checkpoint cpOld)
          throws JsonParseException, JsonMappingException, IOException
  {
    Checkpoint cpNew = null != cpOld ? new Checkpoint(cpOld.toString()) : new Checkpoint();
    if (null != _scn)
    {
      if (-1L != _scn)
      {
        cpNew.setWindowScn(_scn);
        cpNew.setWindowOffset(0);
      }
      else
      {
        cpNew.setFlexible();
      }
    }

    if (null != _sinceScn)
    {
      cpNew.setBootstrapSinceScn(_sinceScn);
    }

    if (null != _startScn)
    {
      cpNew.setBootstrapStartScn(_startScn);
    }

    if (null != _targetScn)
    {
      cpNew.setBootstrapTargetScn(_targetScn);
    }

    if (null != _cpType)
    {
      cpNew.setConsumptionMode(_cpType);
      switch (_cpType)
      {
        case ONLINE_CONSUMPTION: cpNew.setWindowOffset(0); break;
        case BOOTSTRAP_CATCHUP:
        {
          if (null != _bootstrapSource) cpNew.setCatchupSource(_bootstrapSource);
          cpNew.setCatchupOffset(-1);
          break;
        }
        case BOOTSTRAP_SNAPSHOT:
        {
          if (null != _bootstrapSource) cpNew.setSnapshotSource(_bootstrapSource);
          cpNew.setSnapshotOffset(-1);
          break;
        }
      }
    }

    return cpNew;
  }

  public static void main(String[] args) throws Exception
  {
    parseArgs(args);

    PatternLayout defaultLayout = new PatternLayout("%d{ISO8601} +%r [%t] (%p) {%c{1}} %m%n");
    ConsoleAppender defaultAppender = new ConsoleAppender(defaultLayout);

    Logger.getRootLogger().removeAllAppenders();
    Logger.getRootLogger().addAppender(defaultAppender);

    Logger.getRootLogger().setLevel(Level.INFO);
    Logger.getRootLogger().info("NOTE. This tool works only with V2/V1 checkpoints");


    CheckpointPersistenceProvider cp3 = null;
    if (null != _cp3Props)
    {
      CheckpointPersistenceStaticConfigBuilder cp3ConfBuilder =
          new CheckpointPersistenceStaticConfigBuilder();
      ConfigLoader<CheckpointPersistenceStaticConfig> configLoader =
          new ConfigLoader<DatabusHttpClientImpl.CheckpointPersistenceStaticConfig>(
              _propPrefix,
              cp3ConfBuilder);
      configLoader.loadConfig(_cp3Props);
      CheckpointPersistenceStaticConfig cp3Conf = cp3ConfBuilder.build();
      if (cp3Conf.getType() != CheckpointPersistenceStaticConfig.ProviderType.FILE_SYSTEM)
      {
        throw new RuntimeException("don't know what to do with cp3 type:" + cp3Conf.getType());
      }

      cp3 = new FileSystemCheckpointPersistenceProvider(cp3Conf.getFileSystem(), 2);
    }
    else if (null != _clientProps)
    {
      DatabusHttpClientImpl.Config clientConfBuilder =
          new DatabusHttpClientImpl.Config();
      ConfigLoader<DatabusHttpClientImpl.StaticConfig> configLoader =
          new ConfigLoader<DatabusHttpClientImpl.StaticConfig>(_propPrefix, clientConfBuilder);
      configLoader.loadConfig(_clientProps);

      DatabusHttpClientImpl.StaticConfig clientConf = clientConfBuilder.build();
      if (clientConf.getCheckpointPersistence().getType() !=
          CheckpointPersistenceStaticConfig.ProviderType.FILE_SYSTEM)
      {
        throw new RuntimeException("don't know what to do with cp3 type:" +
                                   clientConf.getCheckpointPersistence().getType());
      }

      cp3 = new FileSystemCheckpointPersistenceProvider(
          clientConf.getCheckpointPersistence().getFileSystem(), 2);
    }

    List<String> sourceList = Arrays.asList(_sources);
    Checkpoint cpOld = null != cp3 ? cp3.loadCheckpoint(sourceList) : new Checkpoint();
    Checkpoint cpNew;
    if (Action.PRINT == _action)
    {
      cpNew = updateCheckpoint(cpOld);
    }
    else if (Action.CHANGE == _action)
    {
      cpNew = updateCheckpoint(cpOld);

      cp3.storeCheckpoint(sourceList, cpNew);

      //reread as a sanity check
      cpNew = cp3.loadCheckpoint(sourceList);
    }
    else if (Action.DELETE == _action)
    {
      cp3.removeCheckpoint(sourceList);
      cpNew = cp3.loadCheckpoint(sourceList);
    }
    else
    {
      throw new RuntimeException("don't know what to do with action: " + _action);
    }

    if (null != cpOld) System.out.println("old: " + cpOld.toString());
    else System.out.println("old: null");

    if (null != cpNew) System.out.println("new: " + cpNew.toString());
    else System.out.println("new: null");

  }

}
