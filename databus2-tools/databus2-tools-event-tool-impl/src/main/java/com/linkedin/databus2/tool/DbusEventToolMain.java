package com.linkedin.databus2.tool;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DbusClientMode;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventBufferMult;
import com.linkedin.databus.core.Encoding;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus.core.util.DatabusEventRandomProducer;
import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus.relay.bizfollow.DatabusEventBizFollowRandomProducer;
import com.linkedin.databus.relay.liar.DatabusEventLiarRandomProducer;
import com.linkedin.databus.relay.member2.DatabusEventProfileRandomProducer;
import com.linkedin.databus2.core.filter.AllowAllDbusFilter;
import com.linkedin.databus2.relay.config.PhysicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import com.linkedin.databus2.schemas.FileSystemSchemaRegistryService;
import com.linkedin.databus2.schemas.SchemaRegistryConfigBuilder;
import com.linkedin.databus2.schemas.SchemaRegistryService;
import com.linkedin.databus2.schemas.SchemaRegistryStaticConfig;

public class DbusEventToolMain
{
  public static final String MODULE = DbusEventToolMain.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private static final char HELP_OPT_CHAR = 'h';
  private static final String HELP_OPT_NAME = "help";
  private static final char INPUT_FORMAT_OPT_CHAR = 'i';
  private static final String INPUT_FORMAT_OPT_NAME = "input_format";
  private static final char OUTPUT_FORMAT_OPT_CHAR = 'o';
  private static final String OUTPUT_FORMAT_OPT_NAME = "output_format";
  private static final char INPUT_FILE_OPT_CHAR = 'I';
  private static final String INPUT_FILE_OPT_NAME = "input_file";
  private static final char OUTPUT_FILE_OPT_CHAR = 'O';
  private static final String OUTPUT_FILE_OPT_NAME = "output_file";
  private static final char EVENT_BUFFER_CONF_OPT_CHAR = 'b';
  private static final String EVENT_BUFFER_CONF_OPT_NAME = "buffer_conf";
  private static final char ACTION_OPT_CHAR = 'a';
  private static final String ACTION_OPT_NAME = "action";
  private static final char SCHEMA_REGISTRY_CONF_OPT_CHAR = 'S';
  private static final String SCHEMA_REGISTRY_CONF_OPT_NAME = "schema_reg_conf";
  private static final char EVENT_PRODUCER_CONF_OPT_CHAR = 'R';
  private static final String EVENT_PRODUCER_CONF_OPT_NAME = "rnd_producer_conf";
  private static final String EVENTS_PER_SECOND_OPT_NAME = "events_per_sec";
  private static final String DURATION_MS_OPT_NAME = "duration_ms";
  private static final String EVENTS_NUM_OPT_NAME = "events_num";
  private static final String PRODUCER_SOURCE_OPT_NAME = "producer_sources";
  private static final String SCN_OPT_NAME = "seq";
  private static final String KEY_MIN_OPT_NAME = "key_min";
  private static final String KEY_MAX_OPT_NAME = "key_max";

  enum Action
  {
    CONVERT,
    GENERATE
  }

  enum ProducerSource
  {
    BIZFOLLOW,
    LIAR,
    PROFILE
  }

  private static Options _cliOptions;
  private static CommandLine _cmdLine;

  private static Action _action;
  private static Encoding _inputEncoding = Encoding.JSON;
  private static Encoding _outputEncoding = Encoding.BINARY;
  private static InputStream _input = System.in;
  private static OutputStream _output = System.out;
  private static DbusEventBuffer.Config _bufferConf = new DbusEventBuffer.Config();
  private static Checkpoint _readCheckpoint;
  private static SchemaRegistryConfigBuilder _schemasRegistryConfig = new SchemaRegistryConfigBuilder();
  private static DatabusEventRandomProducer.Config _producerConfig = new DatabusEventRandomProducer.Config();
  private static int _eventsPerSec = 1000;
  private static long _durationMs = 60000;
  private static long _eventsNum = _eventsPerSec * (_durationMs / 1000);
  private static ProducerSource _producerSource = ProducerSource.BIZFOLLOW;
  private static long _scn = 1;
  private static long _keyMin = 1;
  private static long _keyMax = 100;

  @SuppressWarnings("static-access")
  private static void generateCliOptions()
  {
    _cliOptions = new Options();

    Option helpOption = OptionBuilder.withLongOpt(HELP_OPT_NAME)
                                     .create(HELP_OPT_CHAR);
    Option inputFormatOption = OptionBuilder.withLongOpt(INPUT_FORMAT_OPT_NAME)
                                            .withDescription("events input format: JSON or BINARY")
                                            .withArgName("format")
                                            .hasArg()
                                            .create(INPUT_FORMAT_OPT_CHAR);
    Option outputFormatOption = OptionBuilder.withLongOpt(OUTPUT_FORMAT_OPT_NAME)
                                             .withDescription("events output format: JSON or BINARY")
                                             .withArgName("format")
                                             .hasArg()
                                             .create(OUTPUT_FORMAT_OPT_CHAR);
    Option inputFileOption = OptionBuilder.withLongOpt(INPUT_FILE_OPT_NAME)
                                          .withDescription("input file name or - for STDIN")
                                          .hasArg()
                                          .withArgName("file")
                                          .create(INPUT_FILE_OPT_CHAR);
    Option outputFileOption = OptionBuilder.withLongOpt(OUTPUT_FILE_OPT_NAME)
                                           .withDescription("output file name or - for STDOUT")
                                           .hasArg()
                                           .withArgName("file")
                                           .create(OUTPUT_FILE_OPT_CHAR);
    Option bufferConfOption = OptionBuilder.withLongOpt(EVENT_BUFFER_CONF_OPT_NAME)
                                           .withDescription("event buffer configuration")
                                           .hasArg()
                                           .withArgName("conf_file")
                                            .create(EVENT_BUFFER_CONF_OPT_CHAR);
    Option actionOption = OptionBuilder.withLongOpt(ACTION_OPT_NAME)
                                       .withDescription("action to run: convert, generate")
                                       .hasArg()
                                       .withArgName("action")
                                       .create(ACTION_OPT_CHAR);
    Option schemaRegConfOption = OptionBuilder.withLongOpt(SCHEMA_REGISTRY_CONF_OPT_NAME)
                                              .withDescription("schema registry configuration")
                                              .hasArg()
                                              .withArgName("config_file")
                                              .create(SCHEMA_REGISTRY_CONF_OPT_CHAR);
    Option eventProducerConfOption = OptionBuilder.withLongOpt(EVENT_PRODUCER_CONF_OPT_NAME)
                                                  .withDescription("random event producer configuration")
                                                  .hasArg()
                                                  .withArgName("config_file")
                                                  .create(EVENT_PRODUCER_CONF_OPT_CHAR);
    Option eventsPerSecOption = OptionBuilder.withLongOpt(EVENTS_PER_SECOND_OPT_NAME)
                                             .withDescription("number of events per second to generate")
                                             .hasArg()
                                             .withArgName("num")
                                             .create(EVENTS_PER_SECOND_OPT_NAME);
    Option durationOption = OptionBuilder.withLongOpt(DURATION_MS_OPT_NAME)
                                          .withDescription("how long to generate events in millis")
                                          .hasArg()
                                          .withArgName("millis")
                                          .create(DURATION_MS_OPT_NAME);
    Option eventsNumOption = OptionBuilder.withLongOpt(EVENTS_NUM_OPT_NAME)
                                           .withDescription("number of events to generate")
                                           .hasArg()
                                           .withArgName("num")
                                           .create(EVENTS_NUM_OPT_NAME);
    Option producerSrcOption = OptionBuilder.withLongOpt(PRODUCER_SOURCE_OPT_NAME)
                                            .withDescription("source to be generated by the producer: "
                                                             + "BIZFOLLOW,LIAR,PROFILE")
                                            .hasArg()
                                            .withArgName("source")
                                            .create(PRODUCER_SOURCE_OPT_NAME);
    Option scnOption = OptionBuilder.withLongOpt(SCN_OPT_NAME)
                                    .withDescription("sequence number to start generating events")
                                    .hasArg()
                                    .withArgName("num")
                                    .create(SCN_OPT_NAME);
    Option keyMinOption = OptionBuilder.withLongOpt(KEY_MIN_OPT_NAME)
                                       .withDescription("minimum event key to generate")
                                       .hasArg()
                                       .withArgName("num")
                                       .create(KEY_MIN_OPT_NAME);
    Option keyMaxOption = OptionBuilder.withLongOpt(KEY_MAX_OPT_NAME)
                                       .withDescription("maximum event key to generate")
                                       .hasArg()
                                       .withArgName("num")
                                       .create(KEY_MAX_OPT_NAME);

    _cliOptions.addOption(helpOption);
    _cliOptions.addOption(inputFormatOption);
    _cliOptions.addOption(outputFormatOption);
    _cliOptions.addOption(inputFileOption);
    _cliOptions.addOption(outputFileOption);
    _cliOptions.addOption(bufferConfOption);
    _cliOptions.addOption(actionOption);
    _cliOptions.addOption(schemaRegConfOption);
    _cliOptions.addOption(eventProducerConfOption);
    _cliOptions.addOption(eventsPerSecOption);
    _cliOptions.addOption(durationOption);
    _cliOptions.addOption(eventsNumOption);
    _cliOptions.addOption(producerSrcOption);
    _cliOptions.addOption(scnOption);
    _cliOptions.addOption(keyMinOption);
    _cliOptions.addOption(keyMaxOption);
  }

  private static void parseCliOptions(String[] args) throws Exception
  {
    GnuParser cliParser = new GnuParser();
    _cmdLine = cliParser.parse(_cliOptions, args);

    _action = parseEnumArgument(ACTION_OPT_NAME, false, Action.CONVERT, Action.class);
    _inputEncoding = parseEnumArgument(INPUT_FORMAT_OPT_NAME, false, _inputEncoding,
                                       Encoding.class);
    _outputEncoding = parseEnumArgument(OUTPUT_FORMAT_OPT_NAME, false, _outputEncoding,
                                        Encoding.class);

    if (_cmdLine.hasOption(INPUT_FILE_OPT_CHAR) && ! _cmdLine.getOptionValue(INPUT_FILE_OPT_CHAR).equals("-"))
    {
      _input = new FileInputStream(_cmdLine.getOptionValue(INPUT_FILE_OPT_CHAR));
    }

    if (_cmdLine.hasOption(OUTPUT_FILE_OPT_CHAR) && ! _cmdLine.getOptionValue(OUTPUT_FILE_OPT_CHAR).equals("-"))
    {
      _output = new FileOutputStream(_cmdLine.getOptionValue(OUTPUT_FILE_OPT_CHAR));
    }

    if (_cmdLine.hasOption(EVENT_BUFFER_CONF_OPT_CHAR))
    {
      loadConfigFromFile(_cmdLine.getOptionValue(EVENT_BUFFER_CONF_OPT_CHAR), _bufferConf,
                         "buffer.");
    }

    if (_cmdLine.hasOption(SCHEMA_REGISTRY_CONF_OPT_CHAR))
    {
      loadConfigFromFile(_cmdLine.getOptionValue(SCHEMA_REGISTRY_CONF_OPT_CHAR),
                         _schemasRegistryConfig,
                         "schema-reg.");
    }

    if (_cmdLine.hasOption(EVENT_PRODUCER_CONF_OPT_CHAR))
    {
      loadConfigFromFile(_cmdLine.getOptionValue(EVENT_PRODUCER_CONF_OPT_CHAR), _producerConfig,
                         "rnd-producer.");
    }

    _eventsPerSec = parseIntArgument(EVENTS_PER_SECOND_OPT_NAME, false, _eventsPerSec);
    _eventsNum = _eventsPerSec * (_durationMs / 1000);

    _durationMs = parseLongArgument(DURATION_MS_OPT_NAME, false, _durationMs);
    _eventsNum = _eventsPerSec * (_durationMs / 1000);

    if (_cmdLine.hasOption(EVENTS_NUM_OPT_NAME))
    {
      _eventsNum = parseLongArgument(EVENTS_NUM_OPT_NAME, false, _eventsNum);
      if (_cmdLine.hasOption(DURATION_MS_OPT_NAME))
      {
        _eventsPerSec = (int)((_eventsNum * 1000) / _durationMs);
      }
      else
      {
        _durationMs = _eventsNum * 1000/ _eventsPerSec;
      }
    }

    if (_cmdLine.hasOption(PRODUCER_SOURCE_OPT_NAME))
    {
      _producerSource = ProducerSource.valueOf(_cmdLine.getOptionValue(PRODUCER_SOURCE_OPT_NAME).toUpperCase());
    }

    _scn = parseLongArgument(SCN_OPT_NAME, false, 1);
    _keyMin = parseLongArgument(KEY_MIN_OPT_NAME, false, 1);
    _keyMax = parseLongArgument(KEY_MAX_OPT_NAME, false, 1000);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private static <T extends Enum>T parseEnumArgument(String argName,
                                                     boolean required,
                                                     T defaultValue,
                                                     Class<T> enumClass)
  {
    T result = defaultValue;
    if (_cmdLine.hasOption(argName))
    {
      try
      {
        result = (T)T.valueOf(enumClass, _cmdLine.getOptionValue(argName).toUpperCase());
      }
      catch (NumberFormatException nfe)
      {
        exitWithError("invalid value for argument " + argName + ": ", 5, true);
      }
    }
    else if (required)
    {
      exitWithError("expected value for argument " + argName, 6, true);
    }

    return result;
  }

  private static long parseLongArgument(String argName, boolean required, long defaultValue)
  {
    long result = defaultValue;
    if (_cmdLine.hasOption(argName))
    {
      try
      {
        result = Long.parseLong(_cmdLine.getOptionValue(argName));
      }
      catch (NumberFormatException nfe)
      {
        exitWithError("invalid value for argument " + argName + ": ", 5, true);
      }
    }
    else if (required)
    {
      exitWithError("expected value for argument " + argName, 6, true);
    }

    return result;
  }

  private static int parseIntArgument(String argName, boolean required, int defaultValue)
  {
    int result = defaultValue;
    if (_cmdLine.hasOption(argName))
    {
      try
      {
        result = Integer.parseInt(_cmdLine.getOptionValue(argName));
      }
      catch (NumberFormatException nfe)
      {
        exitWithError("invalid value for argument " + argName + ": ", 5, true);
      }
    }
    else if (required)
    {
      exitWithError("expected value for argument " + argName, 6, true);
    }

    return result;
  }

  private static <C> void loadConfigFromFile(String fileName, ConfigBuilder<C> builder,
                                             String propPrefix) throws Exception
  {
    ConfigLoader<C> configLoader = new ConfigLoader<C>(propPrefix, builder);
    Properties props = new Properties();
    FileInputStream input = new FileInputStream(fileName);
    try
    {
      props.load(input);
      configLoader.loadConfig(props);
    }
    finally
    {
      input.close();
    }
  }

  private static void printUsage()
  {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.setWidth(100);
    helpFormatter.printHelp("java " + DbusEventToolMain.class.getName(), _cliOptions);
  }

  private static void exitWithError(String message, int exitCode, boolean printUsage)
  {
    if (printUsage) printUsage();
    System.err.println(DbusEventToolMain.class.getSimpleName() + ":" + message);
    System.exit(exitCode);
  }

  private static boolean hasHelpOption()
  {
    return _cmdLine.hasOption(HELP_OPT_CHAR);
  }

  private static void init()
  {
    _readCheckpoint = new Checkpoint();
    _readCheckpoint.setWindowScn(_scn - 1);
    _readCheckpoint.setWindowOffset(-1);
    _readCheckpoint.setConsumptionMode(DbusClientMode.ONLINE_CONSUMPTION);
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception
  {
    DbusEvent.byteOrder = ByteOrder.LITTLE_ENDIAN;

    PatternLayout defaultLayout = new PatternLayout("%d{ISO8601} +%r [%t] (%p) {%c} %m%n");
    ConsoleAppender defaultAppender = new ConsoleAppender(defaultLayout);

    Logger.getRootLogger().removeAllAppenders();
    Logger.getRootLogger().addAppender(defaultAppender);

    Logger.getRootLogger().setLevel(Level.OFF);
    Logger.getRootLogger().setLevel(Level.ERROR);

    init();
    generateCliOptions();
    parseCliOptions(args);

    if (hasHelpOption())
    {
      printUsage();
      System.exit(0);
    }

    if (null == _action)
    {
      exitWithError("action expected", 1, true);
    }
    else if (Action.CONVERT == _action)
    {
      convertEvents();
    }
    else if (Action.GENERATE == _action)
    {
      generateEvents();
    }
    else
    {
      exitWithError("unsupported action: " + _action, 2, false);
    }
  }

  private static void convertEvents() throws Exception
  {
    DbusEventBuffer buffer = new DbusEventBuffer(_bufferConf);
    ReadableByteChannel inChannel = Channels.newChannel(_input);
    WritableByteChannel outChannel = Channels.newChannel(_output);

    DbusEventConverter eventConverter = new DbusEventConverter(_inputEncoding, _outputEncoding,
                                                               buffer);
    eventConverter.convertBatch(inChannel, outChannel, _readCheckpoint);
  }

  private static void generateEvents() throws Exception
  {
    SchemaRegistryStaticConfig schemaRegConf = _schemasRegistryConfig.build();
    SchemaRegistryService schemaService = null;
    if (schemaRegConf.getType() == SchemaRegistryStaticConfig.RegistryType.FILE_SYSTEM)
    {
      schemaService = FileSystemSchemaRegistryService.build(schemaRegConf.getFileSystem());
    }
    else
    {
      exitWithError("unsupported schema registry type:" + schemaRegConf.getType(), 2, false);
    }

    DbusEventBuffer buffer = new DbusEventBuffer(_bufferConf);
    DatabusEventRandomProducer eventProducer = null;
    ArrayList<IdNamePair> sources = new ArrayList<IdNamePair>();
    switch (_producerSource)
    {
      case BIZFOLLOW:
      {
        sources.add(new IdNamePair(101L, "com.linkedin.events.bizfollow.bizfollow.BizFollow"));
        DatabusEventRandomProducer.Config configBuilder = new DatabusEventRandomProducer.Config();
        DbusEventBufferMult eventBufferMult = new DbusEventBufferMult();
        PhysicalSourceConfig pSourceConfig = new PhysicalSourceConfig("test", "", 101);
        pSourceConfig.setSlowSourceQueryThreshold(10);
        pSourceConfig.setRestartScnOffset(10);
        PhysicalSourceStaticConfig psourceConf = pSourceConfig.build();
        eventBufferMult.addBuffer(psourceConf, buffer);
        eventProducer = new DatabusEventBizFollowRandomProducer(eventBufferMult, _eventsPerSec,
                                                                _durationMs,
                                                                sources, schemaService,
                                                                configBuilder.build());
        break;
      }
      case LIAR:
      {
        sources.add(new IdNamePair(201L, "com.linkedin.events.liar.jobrelay.LiarJobRelay"));
        sources.add(new IdNamePair(202L, "com.linkedin.events.liar.memberrelay.LiarMemberRelay"));
        DbusEventBufferMult eventBufferMult = new DbusEventBufferMult();
        
        PhysicalSourceConfig pSourceConfig = new PhysicalSourceConfig("test", "", 201);
        pSourceConfig.setSlowSourceQueryThreshold(10);
        pSourceConfig.setRestartScnOffset(10);
        PhysicalSourceStaticConfig psourceConf1 = pSourceConfig.build();
        
        pSourceConfig = new PhysicalSourceConfig("test", "", 202);
        pSourceConfig.setSlowSourceQueryThreshold(10);
        pSourceConfig.setRestartScnOffset(10);
        PhysicalSourceStaticConfig psourceConf2 = pSourceConfig.build();
        
        eventBufferMult.addBuffer(psourceConf1, buffer);
        eventBufferMult.addBuffer(psourceConf2, buffer);
        eventProducer = new DatabusEventLiarRandomProducer(eventBufferMult, _eventsPerSec,
                                                           _durationMs,
                                                           sources, schemaService);
        break;
      }
      case PROFILE:
      {
        sources.add(new IdNamePair(401L, "com.linkedin.events.profile.Profile"));
        DbusEventBufferMult eventBufferMult = new DbusEventBufferMult();
        
        PhysicalSourceConfig pSourceConfig = new PhysicalSourceConfig("test", "", 401);
        pSourceConfig.setSlowSourceQueryThreshold(10);
        pSourceConfig.setRestartScnOffset(10);        
        PhysicalSourceStaticConfig psourceConf = pSourceConfig.build();

        eventBufferMult.addBuffer(psourceConf, buffer);
        eventProducer = new DatabusEventProfileRandomProducer(eventBufferMult, _eventsPerSec,
                                                              _durationMs,
                                                              sources, schemaService);
        break;
      }
      default: exitWithError("unsupported producer source: " + _producerSource, 3, false);
    }

    LOG.info("Generating events");
    eventProducer.setDaemon(true);
    eventProducer.startGeneration(_scn, _eventsPerSec, _durationMs, _eventsNum, 100, _keyMin, _keyMax,
                                  sources, new DbusEventsStatisticsCollector(1, "test", true, false,
                                                                             null));
    Thread.sleep((long)(_durationMs * 1.1));

    LOG.info("Writing events");
    WritableByteChannel outChannel = Channels.newChannel(_output);
    int bytesWritten = buffer.streamEvents(_readCheckpoint, Integer.MAX_VALUE, outChannel,
                                            _outputEncoding, new AllowAllDbusFilter(), null);
    LOG.info("bytes written:" + bytesWritten);
    outChannel.close();
    _output.close();
  }

}

class DbusEventConverter
{
  public static final String MODULE = DbusEventConverter.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private final Encoding _inputEncoding;
  private final Encoding _outputEncoding;
  private final DbusEventBuffer _buffer;

  public DbusEventConverter(Encoding inputEncoding, Encoding outputEncoding, DbusEventBuffer buffer)
  {
    _inputEncoding = inputEncoding;
    _outputEncoding = outputEncoding;
    _buffer = buffer;
  }

  void convertBatch(ReadableByteChannel inputChannel, WritableByteChannel outputChannel,
                    Checkpoint cp)
       throws Exception
  {
    _buffer.start(0);
    int eventsRead = _buffer.readEvents(inputChannel, _inputEncoding);
    LOG.info("events read:" + eventsRead);

    int bytesWritten = _buffer.streamEvents(cp, Integer.MAX_VALUE, outputChannel,
                                            _outputEncoding, new AllowAllDbusFilter(), null);
    LOG.info("bytes written:" + bytesWritten);
    outputChannel.close();
  }


}
