package com.linkedin.databus2.tools.dtail;
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
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.log4j.Logger;

import com.linkedin.databus.client.DatabusHttpClientImpl;
import com.linkedin.databus.client.DatabusHttpClientImpl.CheckpointPersistenceStaticConfig;
import com.linkedin.databus.client.pub.DatabusClientException;
import com.linkedin.databus.client.pub.DatabusCombinedConsumer;
import com.linkedin.databus.client.pub.ServerInfo.ServerInfoBuilder;
import com.linkedin.databus.client.pub.ServerInfo.ServerInfoSetBuilder;
import com.linkedin.databus.core.BaseCli;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DbusClientMode;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.filter.DbusKeyCompositeFilterConfig;
import com.linkedin.databus2.core.filter.KeyFilterConfigHolder.PartitionType;

/**
 * The implementation of a command line tool to consume and print events from one or more
 * Databus sources.
 */
public class Dtail
{
  public static final Logger LOG = Logger.getLogger(Dtail.class);

  static class Cli extends DtailCliBase
  {
    public static final String BOOTSTRAP_OPT_NAME = "with-bootstrap";
    public static final char BOOTSTRAP_OPT_CHAR = 'b';
    public static final String BOOTSTRAP_SERVERS_OPT_NAME = "bstservers";
    public static final String CONFIG_ROOT_OPT_NAME = "config_root";
    public static final String FABRIC_OPT_NAME = "fabric";
    public static final char FABRIC_OPT_CHAR = 'f';
    public static final String MOD_PARTITION_OPT_NAME = "mod_part";
    public static final String RELAYS_OPT_NAME = "relays";
    public static final char RELAYS_OPT_CHAR = 'R';
    public static final String SOURCES_OPT_NAME = "sources";
    public static final char SOURCES_OPT_CHAR = 's';
    public static final String CONSUMER_CLASS_NAME = "consumer";

    protected String _sourcesString;
    protected String[] _sources;
    protected String _relaysOverride;
    protected String _bstserversOverride;
    protected File _configRoot = new File(".");
    protected long _modPartBase = -1;
    protected String _modPartIds;
    protected boolean _bootstrapEnabled = false;
    protected Class<?> _consumerClass;

    public Cli()
    {
      super(constructCliHelp(), Logger.getLogger(Dtail.class));
    }

    private static BaseCli.CliHelp constructCliHelp()
    {
      return new BaseCli.CliHelpBuilder()
         .className(Dtail.class)
         .startHeader()
           .addSection("Description")
           .addLine("A command-line tool to consume and inspect events from Databus for Oracle")
           .addSection("Options")
         .finish()
         .startFooter()
           .addSection("Examples")
           .addLine("* Print all recent events for source com.linkedin.events.example.Person from " +
                    "the relay relay.host:12345")
           .addLine()
           .addLine("bin/dtail -s com.linkedin.events.example.Person -R relay.host:12345")
           .addLine()
           .addLine("* Print 100 events for sources com.linkedin.events.example.Person and " +
                    "com.linkedin.events.example.Company from the " +
                    "relay relay.host.com:12345 since scn 987654321")
           .addLine()
           .addLine("bin/dtail -s com.linkedin.events.example.Person," +
                    "com.linkedin.events.example.Company -R relay.host.com:12345 " +
                    "--scn 987654321 -n 100")
           .addLine()
           .addLine("* Bootstrap from SCN 0 for source com.linkedin.events.example.Person using" +
                    " relay relay.host:12345 and bootstrap server " +
                    " bootsrap.host:11111 using checkpoint directory my_ckpt/ and print out " +
                    " JSON format suitable for JSON deserialization")
           .addLine()
           .addLine("bin/dtail -s com.linkedin.events.example.Person --scn 0 " +
                    "--relays relay.host:12345 " +
                    "-b --bstservers bootsrap.host:11111 " +
                    " --resume my_ckpt/ -F AVRO_JSON")
           .addLine()
           .addLine("* Consume events for 5 minutes for source com.linkedin.events.example.Person "+
                    "from the relay relay.host.com:12345 since scn 987654321 and print out event" +
                    "metadata rather than the payload")
           .addLine()
           .addLine("bin/dtail -s com.linkedin.events.example.Person," +
                    "com.linkedin.events.example.Company -R relay.host.com:12345 " +
                    "--scn 987654321 -u 5min -F EVENT_INFO")
           .addLine()
         .finish()
         .build();
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
      Option configRootOption = OptionBuilder.withLongOpt(CONFIG_ROOT_OPT_NAME)
                                             .hasArg()
                                             .withArgName("config_rootdir")
                                             .withDescription("directory with all config files; default: .")
                                             .create();
      Option relaysOption = OptionBuilder.withLongOpt(RELAYS_OPT_NAME)
                                         .hasArg()
                                         .withArgName("relay_list")
                                         .withDescription("semicolon-separated list of server:port")
                                         .create(RELAYS_OPT_CHAR);
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
      Option bstserversOption = OptionBuilder.withLongOpt(BOOTSTRAP_SERVERS_OPT_NAME)
                                             .hasArg()
                                             .withArgName("bootstrap_server_list")
                                             .withDescription("semicolon-separated list of server:port")
                                             .create();
      Option consumerClassOption = OptionBuilder.withLongOpt(CONSUMER_CLASS_NAME)
                                              .hasArg()
                                              .withArgName("callback_class")
                                              .withDescription("a name of a class that implements " +
                                              		"the DatabusCombinedConsumer interface and " +
                                              		"a default constructor. Add your jars to the" +
                                              		"lib/ directory.")
                                              .create();

      _cliOptions.addOption(configRootOption);
      _cliOptions.addOption(sourcesOption);
      _cliOptions.addOption(relaysOption);
      _cliOptions.addOption(modPartOption);
      _cliOptions.addOption(bootstrapOption);
      _cliOptions.addOption(bstserversOption);
      _cliOptions.addOption(consumerClassOption);
    }

    private boolean processSources()
    {
      if (! _cmd.hasOption(SOURCES_OPT_CHAR))
      {
        printError("sources list expected", true);
        return false;
      }

      _sourcesString = _cmd.getOptionValue(SOURCES_OPT_CHAR);
      LOG.debug("sources from cmd=" + _sourcesString);
      _sources = _sourcesString.split(",");
      return true;
    }

    private boolean processConfigRoot()
    {
      if (_cmd.hasOption(CONFIG_ROOT_OPT_NAME))
      {
        String configRootName = _cmd.getOptionValue(CONFIG_ROOT_OPT_NAME);
        File f = new File(configRootName);
        if (!f.exists())
        {
          printError("config directory " + configRootName + " does not exist", false);
          return false;
        }

        if (!f.isDirectory())
        {
          printError("config directory " + configRootName + " is not a directory", false);
          return false;
        }

        _configRoot = f;
      }
      LOG.info("using config root: " + _configRoot.getAbsolutePath());
      return true;
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

    private boolean processModPartition()
    {
      if (_cmd.hasOption(MOD_PARTITION_OPT_NAME))
      {
        String modPartStr = _cmd.getOptionValue(MOD_PARTITION_OPT_NAME);
        String[] parts = modPartStr.split(":");
        if (parts.length != 2)
        {
          printError("invalid mod partition specification: " + modPartStr, true);
          return false;
        }
        _modPartBase = Long.parseLong(parts[0]);
        if (_modPartBase <= 0)
        {
          printError("invalid  mod partition specification: " + modPartStr, true);
          return false;
        }
        _modPartIds = parts[1];
      }
      return true;
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

    protected boolean processConsumerClass()
    {
      if (_cmd.hasOption(CONSUMER_CLASS_NAME))
      {
        String className = _cmd.getOptionValue(CONSUMER_CLASS_NAME);
        try
        {
          _consumerClass = getClass().getClassLoader().loadClass(className);
        }
        catch (ClassNotFoundException e)
        {
          printError("unable to find consumer callback class " + className, false);
          return false;
        }

        if (! DatabusCombinedConsumer.class.isAssignableFrom(_consumerClass))
        {
          printError("consumer callback class " + _consumerClass +
                " does not implement DatabusCombinedConsumer", false);
          return false;
        }
      }
      return true;
    }

    @Override
    public boolean processCommandLineArgs(String[] cliArgs)
    {
      super.processCommandLineArgs(cliArgs);

      if (!processSources())
      {
        return false;
      }
      if (!processConfigRoot())
      {
        return false;
      }
      processRelaysOverride();
      if (!processModPartition())
      {
        return false;
      }
      processBootstrap();
      processBootstrapServers();
      if (!processConsumerClass())
      {
        return false;
      }

      return true;
    }

    public String[] getSources()
    {
      return _sources;
    }

    public File getConfigRoot()
    {
      return _configRoot;
    }

    public String getRelaysOverride()
    {
      return _relaysOverride;
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

    public String getBstserversOverride()
    {
      return _bstserversOverride;
    }

    public Class<?> getConsumerClass()
    {
      return _consumerClass;
    }

  }

  private class ShutdownThread extends Thread
  {

    @Override
    public void run()
    {
       LOG.info("Shutdown hook started");
       if (_cli.isShowStats() && null != _consumer && (_consumer instanceof DtailPrinter))
       {
          LOG.info("Generating stats");
	     ((DtailPrinter)_consumer).printStats();
       }
       LOG.info("Shutdown hook finished");
    }

  }

  private final Cli _cli;
  private final DatabusHttpClientImpl _client;
  DatabusCombinedConsumer _consumer = null;

  public Dtail(Cli cli) throws IOException, DatabusException, DatabusClientException,
                               InstantiationException, IllegalAccessException
  {
    _cli = cli;

    Properties props = _cli.getConfigProps();
    DatabusHttpClientImpl.Config configBuilder =
        DatabusHttpClientImpl.createConfigBuilder("dtail.", props);

    String relaysOverride = _cli.getRelaysOverride();
    configBuilder.getRuntime().setRelaysList(relaysOverride);

    String bstserverOverrides = _cli.getBstserversOverride();
    configBuilder.getRuntime().getBootstrap().setServicesList(bstserverOverrides);

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
    if (DtailCliBase.BOB_SCN == cli.getSinceScn())
    {
      ckpt.setConsumptionMode(DbusClientMode.ONLINE_CONSUMPTION);
      ckpt.setWindowScn(0L);
      ckpt.setFlexible();
    }
    else if (DtailCliBase.EOB_SCN == cli.getSinceScn())
    {
      throw new DatabusException("EOB checkpoint not supported by dtail yet");
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

    if (null != cli.getConsumerClass())
    {
      _consumer = (DatabusCombinedConsumer)cli.getConsumerClass().newInstance();
    }
    else
    {
      switch (cli.getOutputFormat())
      {
      case JSON: _consumer = new JsonDtailPrinter(_client, consConfBuilder.build(), cli.getOut()); break;
      case AVRO_JSON: _consumer = new AvroJsonDtailPrinter(_client, consConfBuilder.build(),
                                                           cli.getOut()); break;
      case AVRO_BIN: _consumer = new AvroBinaryDtailPrinter(_client, consConfBuilder.build(),
                                                           cli.getOut()); break;
      case NOOP: _consumer = new NoopDtailPrinter(_client, consConfBuilder.build(), cli.getOut()); break;
      case EVENT_INFO: _consumer = new EventInfoDtailPrinter(_client, consConfBuilder.build(),
                                                             cli.getOut()); break;
      default: throw new InvalidConfigException("unsupported output format: " + cli.getOutputFormat());
      }
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

  public void start()
  {
    _client.start();
    _client.awaitShutdown();
  }

  public static void main(String[] args) throws Exception
  {
    Cli cli = new Cli();
    if (!cli.processCommandLineArgs(args))
    {
      System.exit(1);
    }

    Dtail dtail = new Dtail(cli);
    dtail.start();
  }

}
