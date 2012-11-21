package com.linkedin.databus2.tools.dtail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.xeril.util.io.resource.FileResource;
import org.xeril.util.io.resource.Resource;

import com.linkedin.cfg.impl.ap.FabricReader;
import com.linkedin.cfg.model.ConfigProperty;
import com.linkedin.databus.client.DatabusHttpClientImpl;
import com.linkedin.databus.client.DatabusHttpClientImpl.CheckpointPersistenceStaticConfig;
import com.linkedin.databus.client.pub.DatabusClientException;
import com.linkedin.databus.client.pub.ServerInfo.ServerInfoBuilder;
import com.linkedin.databus.client.pub.ServerInfo.ServerInfoSetBuilder;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DbusClientMode;
import com.linkedin.databus.core.util.ConfigHelper;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.netty.ServerContainer;
import com.linkedin.databus2.core.filter.DbusKeyCompositeFilterConfig;
import com.linkedin.databus2.core.filter.KeyFilterConfigHolder.PartitionType;
import com.linkedin.databus2.tools.dtail.DtailPrinter.PrintVerbosity;

public class Dtail
{
  public static final Logger LOG = Logger.getLogger(Dtail.class);
  public static final String BOB_SCN_STRING = "BOB";
  public static final String EOB_SCN_STRING = "EOB";

  private static final long BOB_SCN = -1000;
  private static final long EOB_SCN = -2000;

  public static enum OutputFormat
  {
    JSON,
    EVENT_JSON,
    NOOP
  }

  public static enum Fabric
  {
    DEV("dev"),
    STG_ALPHA("STG-ALPHA"),
    ALPHA(STG_ALPHA),
    STG_BETA("STG-BETA"),
    BETA(STG_BETA),
    EI_EAT1("EI"),
    EI(EI_EAT1),
    PROD_ELA4("PROD-ELA4"),
    ELA4(PROD_ELA4),
    PROD_ECH3("PROD-ECH3"),
    ECH3(PROD_ECH3),
    MANUAL("MANUAL");

    private final String _fabricString;
    private final Fabric _canonical;

    private Fabric(String fabricString)
    {
      this(fabricString, null);
    }

    private Fabric(Fabric canonical)
    {
      this(canonical.getFabricString(), canonical);
    }

    private Fabric(String fabricString, Fabric canonical)
    {
      _fabricString = fabricString;
      _canonical = canonical;
    }

    public String getFabricString()
    {
      return _fabricString;
    }

    public Fabric getCanonical()
    {
      return null != _canonical ? _canonical : this;
    }
  }

  static class Cli extends ServerContainer.Cli
  {
    public static final String BOOTSTRAP_OPT_NAME = "with-bootstrap";
    public static final char BOOTSTRAP_OPT_CHAR = 'b';
    public static final String BOOTSTRAP_SERVERS_OPT_NAME = "bstservers";
    public static final String CONFIG_ROOT_OPT_NAME = "config_root";
    public static final String DURATION_OPTION_NAME = "duration";
    public static final char DURATION_OPTION_CHAR = 'u';
    public static final String FABRIC_OPT_NAME = "fabric";
    public static final char FABRIC_OPT_CHAR = 'f';
    public static final String EVENT_NUM_OPT_NAME = "event_num";
    public static final char EVENT_NUM_OPT_CHAR = 'n';
    public static final String MOD_PARTITION_OPT_NAME = "mod_part";
    public static final String OUTPUT_FORMAT_OPT_NAME = "output_format";
    public static final char OUTPUT_FORMAT_OPT_CHAR = 'F';
    public static final String OUTPUT_OPT_NAME = "output";
    public static final char OUTPUT_OPT_CHAR = 'o';
    public static final String RESUME_OPT_NAME = "resume";
    public static final String RELAYS_OPT_NAME = "relays";
    public static final char RELAYS_OPT_CHAR = 'R';
    public static final String SCN_OPT_NAME = "scn";
    public static final String SILENT_OPT_NAME = "silent";
    public static final String SOURCES_OPT_NAME = "sources";
    public static final char SOURCES_OPT_CHAR = 's';
    public static final String STATS_OPT_NAME = "stats";
    public static final String VERBOSE_OPT_NAME = "verbose";
    public static final char VERBOSE_OPT_CHAR = 'v';
    public static final String PRINT_VERBOSITY_OPT_NAME = "print_verbosity";
    public static final char PRINT_VERBOSITY_OPT_CHAR = 'V';

    public static final OutputFormat DEFAUL_OUTPUT_FORMAT = OutputFormat.JSON;
    public static final PrintVerbosity DEFAULT_PRINT_VERBOSITY = PrintVerbosity.EVENT;

    private String _sourcesString;
    private String[] _sources;
    private String _relaysOverride;
    private String _bstserversOverride;
    private OutputFormat _outputFormat = DEFAUL_OUTPUT_FORMAT;
    private PrintVerbosity _printVerbosity = DEFAULT_PRINT_VERBOSITY;
    private OutputStream _out = System.out;
    private Fabric _fabric;
    private File _configRoot = new File(".");
    private String _checkpointDirName;
    private long _maxEventNum = Long.MAX_VALUE;
    private long _durationMs = Long.MAX_VALUE;
    private boolean _showStats = false;
    private long _modPartBase = -1;
    private String _modPartIds;
    private boolean _bootstrapEnabled = false;
    private long _sinceScn = BOB_SCN;

    public Cli()
    {
      super("java " + Dtail.class.getSimpleName() + " [options]");
      setDefaultLogLevel(Level.ERROR);
    }

    private static Fabric guessFabric() throws UnknownHostException
    {
      Fabric guess = Fabric.DEV;
      InetAddress localhost = InetAddress.getLocalHost();
      String localHostname = localhost.getHostName();
      if (localHostname.startsWith("ela4-"))
      {
        guess = Fabric.PROD_ELA4;
      }
      else if (localHostname.startsWith("ech3-"))
      {
        guess = Fabric.PROD_ECH3;
      }
      else if (localHostname.startsWith("eat1-") && localHostname.contains(".stg"))
      {
        guess = Fabric.EI_EAT1;
      }
      else if (localHostname.startsWith("esv4-") && localHostname.contains(".stg"))
      {
        guess = Fabric.STG_BETA;
      }

      LOG.info("guessed fabric: " + guess);
      return guess;
    }

    @SuppressWarnings("static-access")
    @Override
    protected void constructCommandLineOptions()
    {
      super.constructCommandLineOptions();

      Option sourcesOption = OptionBuilder.withLongOpt(SOURCES_OPT_NAME)
                                          .hasArg()
                                          .withArgName("sources")
                                          .withDescription("comma-separated list of sources:")
                                          .create(SOURCES_OPT_CHAR);
      Option printVerbosityOption = OptionBuilder.withLongOpt(PRINT_VERBOSITY_OPT_NAME)
                                            .hasArg()
                                            .withArgName("print_verbosity")
                                            .withDescription("print verbosity: " +
                                                Arrays.toString(PrintVerbosity.values()) +
                                                "; default: " + DEFAULT_PRINT_VERBOSITY)
                                            .create(PRINT_VERBOSITY_OPT_CHAR);
      Option outputFormatOption = OptionBuilder.withLongOpt(OUTPUT_FORMAT_OPT_NAME)
                                               .hasArg()
                                               .withArgName("output_format")
                                               .withDescription("output format: " +
                                               Arrays.toString(OutputFormat.values()) +
                                               "; default: " + DEFAUL_OUTPUT_FORMAT)
                                               .create(OUTPUT_FORMAT_OPT_CHAR);
      Option outputOption = OptionBuilder.withLongOpt(OUTPUT_OPT_NAME)
                                         .hasArg()
                                         .withArgName("output_file")
                                         .withDescription("output file or - for STDOUT")
                                         .create(OUTPUT_OPT_CHAR);
      Option resumeOption = OptionBuilder.withLongOpt(RESUME_OPT_NAME)
                                         .hasArg()
                                         .withArgName("checkpoint_dir")
                                         .withDescription("resumes from a previous checkpoint")
                                         .create();
      Option verboseOption = OptionBuilder.withLongOpt(VERBOSE_OPT_NAME)
                                          .withDescription("verbose logging: INFO or above")
                                          .create(VERBOSE_OPT_CHAR);
      Option silentOption = OptionBuilder.withLongOpt(SILENT_OPT_NAME)
                                         .withDescription("turn off logging")
                                         .create();
      Option fabricOption = OptionBuilder.withLongOpt(FABRIC_OPT_NAME)
                                         .hasArg()
                                         .withArgName("fabric")
                                         .withDescription("fabric: " +
                                             Arrays.toString(Fabric.values()) +
                                             "; default: guess")
                                         .create(FABRIC_OPT_CHAR);
      Option configRootOption = OptionBuilder.withLongOpt(CONFIG_ROOT_OPT_NAME)
                                             .hasArg()
                                             .withArgName("config_rootdir")
                                             .withDescription("directory with all config files; default: .")
                                             .create();
      Option eventNumOption = OptionBuilder.withLongOpt(EVENT_NUM_OPT_NAME)
                                             .hasArg()
                                             .withArgName("num")
                                             .withDescription("max number of events to return; default: no limit")
                                             .create(EVENT_NUM_OPT_CHAR);
      Option relaysOption = OptionBuilder.withLongOpt(RELAYS_OPT_NAME)
                                         .hasArg()
                                         .withArgName("relay_list")
                                         .withDescription("semicolon-separated list of server:port")
                                         .create(RELAYS_OPT_CHAR);
      Option durationOption = OptionBuilder.withLongOpt(DURATION_OPTION_NAME)
                                           .hasArg()
                                           .withArgName("duration_value")
                                           .withDescription("max consumption duration: value[ns|us|ms|s|min|hr|d]; default: no limit")
                                           .create(DURATION_OPTION_CHAR);
      Option statsOption = OptionBuilder.withLongOpt(STATS_OPT_NAME)
                                        .withDescription("print statistics at the end; Default: off")
                                        .create();
      Option modPartOption = OptionBuilder.withLongOpt(MOD_PARTITION_OPT_NAME)
                                          .hasArg()
                                          .withArgName("div:[id1,id2,...]")
                                          .withDescription("returns only events for which hash(key) mod div in {id1, ...}")
                                          .create();
      Option bootstrapOption = OptionBuilder.withLongOpt(BOOTSTRAP_OPT_NAME)
                                            .hasOptionalArg()
                                            .withArgName("[true|false]")
                                            .withDescription("enable/disable bootstrap; Default: disabled")
                                            .create(BOOTSTRAP_OPT_CHAR);
      Option sinceScnOption = OptionBuilder.withLongOpt(SCN_OPT_NAME)
                                           .hasArg()
                                           .withArgName("scn")
                                           .withDescription("starts consumption from the given scn; special values: BOB for current beginning of relay buffer, " +
                                           		"EOB for current end of buffer; Default: BOB")
                                           .create();
      Option bstserversOption = OptionBuilder.withLongOpt(BOOTSTRAP_SERVERS_OPT_NAME)
                                             .hasArg()
                                             .withArgName("bootstrap_server_list")
                                             .withDescription("semicolon-separated list of server:port")
                                             .create();

      _cliOptions.addOption(configRootOption);
      _cliOptions.addOption(fabricOption);
      _cliOptions.addOption(eventNumOption);
      _cliOptions.addOption(outputFormatOption);
      _cliOptions.addOption(outputOption);
      _cliOptions.addOption(printVerbosityOption);
      _cliOptions.addOption(resumeOption);
      _cliOptions.addOption(silentOption);
      _cliOptions.addOption(sourcesOption);
      _cliOptions.addOption(verboseOption);
      _cliOptions.addOption(relaysOption);
      _cliOptions.addOption(durationOption);
      _cliOptions.addOption(statsOption);
      _cliOptions.addOption(modPartOption);
      _cliOptions.addOption(bootstrapOption);
      _cliOptions.addOption(sinceScnOption);
      _cliOptions.addOption(bstserversOption);
    }

    public static void error(String msg, int code)
    {
      System.err.println(Dtail.class.getSimpleName() + ": " + msg);
      System.exit(code);
    }

    private void processLoggingOptions()
    {
      if (_cmd.hasOption(DEBUG_OPT_CHAR))
      {
        Logger.getRootLogger().setLevel(Level.DEBUG);
      }
      else if (_cmd.hasOption(VERBOSE_OPT_CHAR))
      {
        Logger.getRootLogger().setLevel(Level.INFO);
      }
      else if (_cmd.hasOption(SILENT_OPT_NAME))
      {
        Logger.getRootLogger().setLevel(Level.OFF);
      }
      else
      {
        Logger.getRootLogger().setLevel(Level.ERROR);
      }
    }

    private void processSources()
    {
      if (! _cmd.hasOption(SOURCES_OPT_CHAR))
      {
        error("sources list expected", 1);
      }

      _sourcesString = _cmd.getOptionValue(SOURCES_OPT_CHAR);
      LOG.debug("sources from cmd=" + _sourcesString);
      _sources = _sourcesString.split(",");
    }

    private void processOutputFormat()
    {
      if (_cmd.hasOption(OUTPUT_FORMAT_OPT_CHAR))
      {
        try
        {
          _outputFormat = OutputFormat.valueOf(_cmd.getOptionValue(OUTPUT_FORMAT_OPT_CHAR).toUpperCase());
        }
        catch (IllegalArgumentException e)
        {
          error("invalid output format: " + _cmd.getOptionValue(OUTPUT_FORMAT_OPT_CHAR), 2);
        }
      }
    }

    private void processVerbosity()
    {
      if (_cmd.hasOption(PRINT_VERBOSITY_OPT_CHAR))
      {
        try
        {
          _printVerbosity = PrintVerbosity.valueOf(_cmd.getOptionValue(PRINT_VERBOSITY_OPT_CHAR).toUpperCase());
        }
        catch (IllegalArgumentException e)
        {
          error("invalid print verbosity: " + _cmd.getOptionValue(PRINT_VERBOSITY_OPT_CHAR), 4);
        }
      }
    }

    private void processOutput()
    {
      if (_cmd.hasOption(OUTPUT_OPT_CHAR))
      {
        String outputStr = _cmd.getOptionValue(OUTPUT_OPT_CHAR);
        if (outputStr.startsWith("hdfs://"))
        {
          error("HDFS is not supported yet", 10);
        }
        else if (! outputStr.equals("-"))
        {
          try
          {
            _out = new FileOutputStream(outputStr);
          }
          catch (IOException e)
          {
            error("unable to open output: " + outputStr, 11);
          }
        }
      }
    }

    private void processFabric() throws UnknownHostException
    {
      if (_cmd.hasOption(FABRIC_OPT_CHAR))
      {
        String fabricStr = _cmd.getOptionValue(FABRIC_OPT_CHAR).trim();
        try
        {
          _fabric = Fabric.valueOf(fabricStr.toUpperCase());
        }
        catch (IllegalArgumentException e)
        {
          error("invalid fabric: '" + fabricStr + "'", 12);
        }
        _fabric = _fabric.getCanonical();
      }
      else
      {
        _fabric = guessFabric();
      }
    }

    private void processConfigRoot()
    {
      if (_cmd.hasOption(CONFIG_ROOT_OPT_NAME))
      {
        String configRootName = _cmd.getOptionValue(CONFIG_ROOT_OPT_NAME);
        File f = new File(configRootName);
        if (!f.exists())
        {
          error("config directory " + configRootName + " does not exist", 15);
        }

        if (!f.isDirectory())
        {
          error("config directory " + configRootName + " is not a directory", 16);
        }

        _configRoot = f;
      }
      LOG.info("using config root: " + _configRoot.getAbsolutePath());
    }

    private void processResume()
    {
      if (_cmd.hasOption(RESUME_OPT_NAME))
      {
        _checkpointDirName = _cmd.getOptionValue(RESUME_OPT_NAME);
      }
    }

    private void processMaxEventNum()
    {
      if (_cmd.hasOption(EVENT_NUM_OPT_CHAR))
      {
        try
        {
          _maxEventNum = Long.parseLong(_cmd.getOptionValue(EVENT_NUM_OPT_CHAR));
        }
        catch (NumberFormatException e)
        {
          error("invalid max events number: " + _cmd.getOptionValue(EVENT_NUM_OPT_CHAR), 17);
        }
      }
    }

    private void processRelaysOverride()
    {
      if (_cmd.hasOption(RELAYS_OPT_CHAR))
      {
        _relaysOverride = _cmd.getOptionValue(RELAYS_OPT_CHAR);
        if (! _relaysOverride.endsWith(String.valueOf(ServerInfoSetBuilder.SERVER_INFO_SEPARATOR)))
        {
          _relaysOverride = _relaysOverride + ServerInfoSetBuilder.SERVER_INFO_SEPARATOR;
        }
        LOG.info("Using relays override: " + _relaysOverride);
        _relaysOverride = _relaysOverride.replaceAll(
                                                     String.valueOf(ServerInfoSetBuilder.SERVER_INFO_SEPARATOR),
                                                                    ServerInfoBuilder.SOURCES_LIST_SEPARATOR + _sourcesString +
                                                                    ServerInfoSetBuilder.SERVER_INFO_SEPARATOR);
      }
    }

    private void processDuration() throws InvalidConfigException
    {
      if (_cmd.hasOption(DURATION_OPTION_CHAR))
      {
        String durationStr = _cmd.getOptionValue(DURATION_OPTION_CHAR);
        LOG.info("Using duration: " + durationStr);
        _durationMs = ConfigHelper.parseDuration(durationStr, TimeUnit.MILLISECONDS);
      }
    }

    private void processStats()
    {
      _showStats = _cmd.hasOption(STATS_OPT_NAME);
    }

    private void processModPartition()
    {
      if (_cmd.hasOption(MOD_PARTITION_OPT_NAME))
      {
        String modPartStr = _cmd.getOptionValue(MOD_PARTITION_OPT_NAME);
        String[] parts = modPartStr.split(":");
        if (parts.length != 2)
        {
          error("invalid  mod partition specification: " + modPartStr, 18);
        }
        _modPartBase = Long.parseLong(parts[0]);
        if (_modPartBase <= 0)
        {
          error("invalid  mod partition specification: " + modPartStr, 18);
        }
        _modPartIds = parts[1];
      }
    }

    private void processBootstrap()
    {
      if (_cmd.hasOption(BOOTSTRAP_OPT_CHAR))
      {
        String bstValue = _cmd.getOptionValue(BOOTSTRAP_OPT_CHAR);
        if (null == bstValue || 0 == bstValue.trim().length())
        {
          _bootstrapEnabled = true;
        }
        else
        {
          _bootstrapEnabled = Boolean.valueOf(bstValue.trim().toLowerCase());
        }
        LOG.info("with boostrap enabled: " + _bootstrapEnabled);
      }
    }

    private void processSinceScn()
    {
      if (_cmd.hasOption(SCN_OPT_NAME))
      {
        String scnOption = _cmd.getOptionValue(SCN_OPT_NAME);
        if (scnOption.equals(BOB_SCN_STRING)) _sinceScn = BOB_SCN;
        else if (scnOption.equals(EOB_SCN_STRING)) _sinceScn = EOB_SCN;
        else
        {
          _sinceScn = Long.parseLong(scnOption);
        }
      }
      LOG.info("starting from SCN: " + _sinceScn);
    }

    private void processBootstrapServers()
    {
      if (_cmd.hasOption(BOOTSTRAP_SERVERS_OPT_NAME))
      {
        _bstserversOverride = _cmd.getOptionValue(BOOTSTRAP_SERVERS_OPT_NAME);
        if (! _bstserversOverride.endsWith(String.valueOf(ServerInfoSetBuilder.SERVER_INFO_SEPARATOR)))
        {
          _bstserversOverride = _bstserversOverride + ServerInfoSetBuilder.SERVER_INFO_SEPARATOR;
        }
        LOG.info("Using bootstrap servers override: " + _bstserversOverride);
        _bstserversOverride = _bstserversOverride.replaceAll(
                                                             String.valueOf(ServerInfoSetBuilder.SERVER_INFO_SEPARATOR),
                                                                            ServerInfoBuilder.SOURCES_LIST_SEPARATOR + _sourcesString +
                                                                            ServerInfoSetBuilder.SERVER_INFO_SEPARATOR);
      }
    }

    @Override
    public void processCommandLineArgs(String[] cliArgs) throws IOException, DatabusException
    {
      super.processCommandLineArgs(cliArgs);

      processLoggingOptions();
      processSources();
      processOutputFormat();
      processVerbosity();
      processOutput();
      processFabric();
      processConfigRoot();
      processResume();
      processMaxEventNum();
      processRelaysOverride();
      processDuration();
      processStats();
      processModPartition();
      processBootstrap();
      processSinceScn();
      processBootstrapServers();
    }

    public String[] getSources()
    {
      return _sources;
    }

    public OutputFormat getOutputFormat()
    {
      return _outputFormat;
    }

    public PrintVerbosity getPrintVerbosity()
    {
      return _printVerbosity;
    }

    public OutputStream getOut()
    {
      return _out;
    }

    public Fabric getFabric()
    {
      return _fabric;
    }

    public File getConfigRoot()
    {
      return _configRoot;
    }

    public String getCheckpointDirName()
    {
      return _checkpointDirName;
    }

    public long getMaxEventNum()
    {
      return _maxEventNum;
    }

    public String getRelaysOverride()
    {
      return _relaysOverride;
    }

    public long getDurationMs()
    {
      return _durationMs;
    }

    public boolean isShowStats()
    {
      return _showStats;
    }

    public long getModPartBase()
    {
      return _modPartBase;
    }

    public String getModPartIds()
    {
      return _modPartIds;
    }

    public boolean isBootstrapEnabled()
    {
      return _bootstrapEnabled;
    }

    public long getSinceScn()
    {
      return _sinceScn;
    }

    public String getBstserversOverride()
    {
      return _bstserversOverride;
    }

  }

  private class ShutdownThread extends Thread
  {

    @Override
    public void run()
    {
       LOG.info("Shutdown hook started");
       if (_cli.isShowStats() && null != _consumer)
       {
          LOG.info("Generating stats");
	  _consumer.printStats();
       }
       LOG.info("Shutdown hook finished");
    }

  }

  private final Cli _cli;
  private final Map<String, ConfigProperty> _fabricConfig;
  private final DatabusHttpClientImpl _client;
  DtailPrinter _consumer = null;

  public Dtail(Cli cli) throws IOException, DatabusException, DatabusClientException
  {
    _cli = cli;

    Properties props = _cli.getConfigProps();
    DatabusHttpClientImpl.Config configBuilder =
        DatabusHttpClientImpl.createConfigBuilder("dtail.", props);

    _fabricConfig = readFabricConfig(_cli.getConfigRoot(), _cli.getFabric());

    String relaysOverride = _cli.getRelaysOverride();
    if ( null == relaysOverride || 0 == relaysOverride.length() || relaysOverride.length() == 0)
    {
      loadRelaysInfo(configBuilder);
    }
    else
    {
      configBuilder.getRuntime().setRelaysList(relaysOverride);
    }

    String bstserverOverrides = _cli.getBstserversOverride();
    if (cli.isBootstrapEnabled() && (null == bstserverOverrides || 0 == bstserverOverrides.length() || 0 == bstserverOverrides.length()))
    {
      loadBstServersInfo(configBuilder);
    }
    else
    {
      configBuilder.getRuntime().getBootstrap().setServicesList(bstserverOverrides);
    }

    if (null == _cli.getCheckpointDirName())
    {
      configBuilder.getCheckpointPersistence().setType(CheckpointPersistenceStaticConfig.ProviderType.NONE.toString());
    }
    else
    {
      configBuilder.getCheckpointPersistence().setType(CheckpointPersistenceStaticConfig.ProviderType.FILE_SYSTEM.toString());
      configBuilder.getCheckpointPersistence().getFileSystem().setRootDirectory(_cli.getCheckpointDirName());
    }

    if (cli.isBootstrapEnabled())
    {
      configBuilder.getRuntime().getBootstrap().setEnabled(true);
    }

    Checkpoint ckpt = new Checkpoint();
    if (BOB_SCN == cli.getSinceScn())
    {
      ckpt.setConsumptionMode(DbusClientMode.ONLINE_CONSUMPTION);
      ckpt.setWindowScn(0L);
      ckpt.setFlexible();
    }
    else if (EOB_SCN == cli.getSinceScn())
    {
      Cli.error("EOB checkpoint not supported by dtail yet", 101);
    }
    else
    {
      ckpt.setConsumptionMode(DbusClientMode.ONLINE_CONSUMPTION);
      ckpt.setWindowScn(cli.getSinceScn());
      ckpt.setWindowOffset(-1);
    }

    _client = new DatabusHttpClientImpl(configBuilder);
    if (null != _client.getCheckpointPersistenceProvider())
    {
      _client.getCheckpointPersistenceProvider().storeCheckpoint(Arrays.asList(cli.getSources()), ckpt);
    }

    //register consumers
    DtailPrinter.StaticConfigBuilder consConfBuilder = new DtailPrinter.StaticConfigBuilder();
    consConfBuilder.setPrintPrintVerbosity(cli.getPrintVerbosity());
    consConfBuilder.setMaxEventsNum(cli.getMaxEventNum());
    consConfBuilder.setMaxDurationMs(cli.getDurationMs());
    consConfBuilder.setPrintStats(cli.isShowStats());

    switch (cli.getOutputFormat())
    {
    case JSON: _consumer = new JsonDtailPrinter(_client, consConfBuilder.build(), cli.getOut()); break;
    case NOOP: _consumer = new NoopDtailPrinter(_client, consConfBuilder.build(), cli.getOut()); break;
    default: Cli.error("unsupported output format: " + cli.getOutputFormat(), 3);
    }

    DbusKeyCompositeFilterConfig filterConfig = null;

    if (_cli.getModPartBase() > 0)
    {
      DbusKeyCompositeFilterConfig.Config builder = new DbusKeyCompositeFilterConfig.Config();
      for (String src: cli.getSources())
      {
        builder.getFilter(src).setType(PartitionType.MOD.toString());
        builder.getFilter(src).getMod().setNumBuckets(cli.getModPartBase());
        builder.getFilter(src).getMod().setBuckets(cli.getModPartIds());
      }
      filterConfig = new DbusKeyCompositeFilterConfig(builder.build());
    }

    _client.registerDatabusStreamListener(_consumer, filterConfig, cli.getSources());
    _client.registerDatabusBootstrapListener(_consumer, filterConfig, cli.getSources());

    Runtime.getRuntime().addShutdownHook(new ShutdownThread());
  }

  private static Map<String, ConfigProperty> readFabricConfig(File root, Fabric fabric)
          throws IOException
  {
    String fabricConfigFName = fabric.getFabricString() + ".fabric";
    LOG.info("Reading config:" + (new File(root, fabricConfigFName)).getAbsolutePath());
    Resource fabricFileResource = FileResource.create(root, fabricConfigFName);
    FabricReader fabricReader = new FabricReader();
    List<ConfigProperty> props = fabricReader.load(fabricFileResource);

    HashMap<String, ConfigProperty> result = new HashMap<String, ConfigProperty>(props.size());
    for (ConfigProperty prop: props)
    {
      result.put(prop.getName(), prop);
    }

    return result;
  }

  public void start()
  {
    _client.start();
    _client.awaitShutdown();
  }

  private void loadBstServersInfo(DatabusHttpClientImpl.Config configBuilder)
          throws InvalidConfigException
  {
    addBootstrapInfo(configBuilder, "bizfollow");
    addBootstrapInfo(configBuilder, "cappr");
    addBootstrapInfo(configBuilder, "conn");
    addBootstrapInfo(configBuilder, "following");
    addBootstrapInfo(configBuilder, "forum");
    addBootstrapInfo(configBuilder, "fuse");
    addBootstrapInfo(configBuilder, "liar");
    addBootstrapInfo(configBuilder, "mbrrec");
    addBootstrapInfo(configBuilder, "member2");
    addBootstrapInfo(configBuilder, "news");
  }

  private void loadRelaysInfo(DatabusHttpClientImpl.Config configBuilder)
      throws InvalidConfigException
  {
    addRelayInfo(configBuilder, "bizfollow");
    addRelayInfo(configBuilder, "cappr");
    addRelayInfo(configBuilder, "conn");
    addRelayInfo(configBuilder, "following");
    addRelayInfo(configBuilder, "forum");
    addRelayInfo(configBuilder, "fuse");
    addRelayInfo(configBuilder, "liar");
    addRelayInfo(configBuilder, "mbrrec");
    addRelayInfo(configBuilder, "member2");
    addRelayInfo(configBuilder, "news");
  }

  private void addSourceInfo(DatabusHttpClientImpl.Config configBuilder, String dbname)
      throws InvalidConfigException
  {
    addRelayInfo(configBuilder, dbname);
    if (_cli.isBootstrapEnabled()) addBootstrapInfo(configBuilder, dbname);
  }

  private void addRelayInfo(DatabusHttpClientImpl.Config configBuilder, String dbname)
          throws InvalidConfigException
  {
    LOG.debug("Discovering relay for " + dbname);
    ConfigProperty hostProp = resolveProperty("databus2.relay." + dbname + ".host");
    if (null == hostProp) throw new InvalidConfigException("unable to find relay hosts for " + dbname);
    String host = hostProp.getValue().toString();
    configBuilder.getRuntime().getRelay(dbname).setHost(host);

    ConfigProperty portProp = resolveProperty("databus2.relay." + dbname + ".port");
    if (null == portProp) throw new InvalidConfigException("unable to find relay port for " + dbname);
    String portString = portProp.getValue().toString();
    configBuilder.getRuntime().getRelay(dbname).setPort(Integer.parseInt(portString));

    ConfigProperty sourcesProp = resolveProperty("databus2.relay." + dbname + ".sources");
    if (null == sourcesProp) throw new InvalidConfigException("unable to find relay sources for " + dbname);
    String sources = sourcesProp.getValue().toString();
    configBuilder.getRuntime().getRelay(dbname).setSources(sources);
    LOG.debug("Discovered: " + host + ":" + portString + "-->" + sources);
  }

  private void addBootstrapInfo(DatabusHttpClientImpl.Config configBuilder, String dbname)
          throws InvalidConfigException
  {
    LOG.debug("Discovering bootstrap for " + dbname);
    ConfigProperty hostProp = resolveProperty("databus2.bootstrap.server." + dbname + ".host");
    if (null == hostProp) throw new InvalidConfigException("unable to find bootstrap hosts for " + dbname);
    String host = hostProp.getValue().toString();
    configBuilder.getRuntime().getBootstrap().getService(dbname).setHost(host);

    ConfigProperty portProp = resolveProperty("databus2.bootstrap.server." + dbname + ".port");
    if (null == portProp) throw new InvalidConfigException("unable to find bootstrap port for " + dbname);
    String portString = portProp.getValue().toString();
    configBuilder.getRuntime().getBootstrap().getService(dbname).setPort(Integer.parseInt(portString));

    ConfigProperty sourcesProp = resolveProperty("databus2.relay." + dbname + ".sources");
    if (null == sourcesProp) throw new InvalidConfigException("unable to find bootstrap sources for " + dbname);
    String sources = sourcesProp.getValue().toString();
    configBuilder.getRuntime().getBootstrap().getService(dbname).setSources(sources);
    LOG.debug("Discovered: " + host + ":" + portString + "-->" + sources);
  }

  /** A dirty HACK until I figure out how to resolve properties through Cfg2 */
  private ConfigProperty resolveProperty(String name)
  {
    ConfigProperty res = null;
    res = _fabricConfig.get(name);
    if (null == res) return null;

    String v = res.getValue().toString();
    if (v.startsWith("${") && v.endsWith("}"))
    {
      return resolveProperty(v.substring(2, v.length() - 1));
    }

    return res;
  }

  public static void main(String[] args) throws Exception
  {
    Cli cli = new Cli();
    cli.processCommandLineArgs(args);

    Dtail dtail = new Dtail(cli);
    dtail.start();
  }

}
