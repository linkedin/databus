package com.linkedin.databus.util;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.relay.config.DatabusRelaySourcesInFiles;
import com.linkedin.databus2.relay.config.LogicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceConfig;
import com.linkedin.databus2.schemas.FileSystemSchemaRegistryService;
import com.linkedin.databus2.schemas.VersionedSchema;
import com.linkedin.databus2.schemas.utils.SchemaHelper;

public class DevRelayConfigGenerator
{
  public static final Logger LOG = Logger.getLogger(DevRelayConfigGenerator.class);
  public static final String NAME = DevRelayConfigGenerator.class.getName();

  public static class Cli
  {
    public static final char SCHEMA_REGISTRY_LOCATION_OPT_CHAR = 'l';
    public static final char SCHEMA_NAME_OPT_CHAR = 'd';
    public static final char DB_URI_OPT_CHAR = 'u';
    public static final char SRC_NAMES_OPT_CHAR = 's';
    public static final char HELP_OPT_CHAR = 'h';
    public static final char OUTPUT_DIRECTORY_OPT_CHAR = 'o';

    public static final String SCHEMA_REGISTRY_LOCATION_LONG_STR = "schema_registry";
    public static final String SCHEMA_NAME_LONG_STR = "schema_name";
    public static final String DB_URI_LONG_STR = "source_uri";
    public static final String SRC_NAMES_LONG_STR = "source_names";
    public static final String HELP_OPT_LONG_NAME = "help";
    public static final String OUTPUT_DIRECTORY_LONG_STR = "output_directory";

    private final String _usage;
    protected Options _cliOptions;
    protected CommandLine _cmd;
    HelpFormatter _helpFormatter;

    private String _schemaRegistryLocation;
    private String _schemaName;
    private String _dbUri;
    private List<String> _srcNames;
    private String _outputDirectory;

    public Cli()
    {
      _usage = "java " + NAME + " [options]";
      _cliOptions = new Options();
      constructCommandLineOptions();
      _helpFormatter = new HelpFormatter();
      _helpFormatter.setWidth(150);
    }



    /**
     * Creates the command-line options
     */
    @SuppressWarnings("static-access")
    private void constructCommandLineOptions()
    {
      Option helpOption = OptionBuilder.withLongOpt(HELP_OPT_LONG_NAME)
                .withDescription("Prints command-line options info")
                .create(HELP_OPT_CHAR);

      Option schemaRegistryLocation = OptionBuilder.withLongOpt(SCHEMA_REGISTRY_LOCATION_LONG_STR)
          .withDescription("Absolute path to the Schema Registry directory")
          .hasArg().create(SCHEMA_REGISTRY_LOCATION_OPT_CHAR);

      Option schemaNameOption = OptionBuilder.withLongOpt(SCHEMA_NAME_LONG_STR)
          .withDescription("DB Schema (Database) name in which the tables/views to be databusified is present")
          .hasArg().create(SCHEMA_NAME_OPT_CHAR);

      Option dbUriOption = OptionBuilder.withLongOpt(DB_URI_LONG_STR)
          .withDescription("The URI for Oracle (Format : jdbc:oracle:thin:<user>/<password>@<dbhost>:1521:<SID>) or GG trail file location (Format : gg:///mnt/gg/extract/dbext<num>:x<num>)")
          .hasArg().create(DB_URI_OPT_CHAR);

      Option srcNamesOption = OptionBuilder.withLongOpt(SRC_NAMES_LONG_STR)
                                    .withDescription("Comma seperated list of source names (e.g : com.linkedin.events.liar.jobrelay.LiarJobRelay,com.linkedin.events.liar.memberrelay.LiarMemberRelay)")
                                    .hasArg().create(SRC_NAMES_OPT_CHAR);

      Option outputDirectoryOption = OptionBuilder.withLongOpt(OUTPUT_DIRECTORY_LONG_STR)
          .withDescription("Output Directory to generate the relay configs")
          .hasArg().create(OUTPUT_DIRECTORY_OPT_CHAR);

      _cliOptions.addOption(helpOption);
      _cliOptions.addOption(schemaRegistryLocation);
      _cliOptions.addOption(schemaNameOption);
      _cliOptions.addOption(dbUriOption);
      _cliOptions.addOption(srcNamesOption);
      _cliOptions.addOption(outputDirectoryOption);

    }

    public void printCliHelp()
    {
      _helpFormatter.printHelp(getUsage(), _cliOptions);
    }

    public String getUsage()
    {
      return _usage;
    }

    public boolean processCommandLineArgs(String[] cliArgs) throws IOException, DatabusException
    {
      CommandLineParser cliParser = new GnuParser();
      _cmd = null;

      try
      {
        _cmd = cliParser.parse(_cliOptions, cliArgs);
      } catch (ParseException pe)
      {
        System.err.println(NAME + ": failed to parse command-line options: " + pe.toString());
        printCliHelp();
        return false;
      }


      if (_cmd.hasOption(HELP_OPT_CHAR))
      {
        printCliHelp();
        return false;
      }

      if (!_cmd.hasOption(SCHEMA_REGISTRY_LOCATION_OPT_CHAR))
      {
        System.err.println("Please specify the schema registry location");
        return false;
      } else {
        _schemaRegistryLocation = _cmd.getOptionValue(SCHEMA_REGISTRY_LOCATION_OPT_CHAR);

        File f = new File(_schemaRegistryLocation);

        if (!f.isDirectory())
        {
          System.err.println("Schema Registry (" + _schemaRegistryLocation + ") is not a valid directory !! Please specify one");
          return false;
        }
      }

      if (!_cmd.hasOption(OUTPUT_DIRECTORY_OPT_CHAR))
      {
        System.err.println("Please specify the output directory");
        return false;
      } else {
        _outputDirectory = _cmd.getOptionValue(OUTPUT_DIRECTORY_LONG_STR);

        File f = new File(_outputDirectory);

        if (!f.isDirectory())
        {
          System.err.println("Output Directory (" + _outputDirectory + ") is not a valid directory !! Please specify one");
          return false;
        }
      }

      if (!_cmd.hasOption(SCHEMA_NAME_OPT_CHAR))
      {
        System.out.println("Please specify the Schema name !!");
        return false;
      } else {
        _schemaName = _cmd.getOptionValue(SCHEMA_NAME_OPT_CHAR);
      }

      if ( !_cmd.hasOption(DB_URI_OPT_CHAR))
      {
        System.err.println("Plase specify the DB URI !!");
        return false;
      } else {
        _dbUri = _cmd.getOptionValue(DB_URI_OPT_CHAR);
      }

      if ( !_cmd.hasOption(SRC_NAMES_OPT_CHAR))
      {
        System.err.println("Please specify comma seperated list of source names");
        return false;
      } else {
        _srcNames = Arrays.asList(_cmd.getOptionValue(SRC_NAMES_LONG_STR).split(","));
      }

      return true;
    }

    public String getOutputDirectory()
    {
      return _outputDirectory;
    }

    public String get_usage()
    {
      return _usage;
    }

    public String getSchemaRegistryLocation()
    {
      return _schemaRegistryLocation;
    }

    public String getSchemaName()
    {
      return _schemaName;
    }

    public String getDbUri()
    {
      return _dbUri;
    }

    public List<String> getSrcNames()
    {
      return _srcNames;
    }
  }

  /**
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception
  {
    Cli cli = new Cli();
    if (! cli.processCommandLineArgs(args))
    {
      return;
    }

    generateRelayConfig(cli.getSchemaRegistryLocation(),
                        cli.getSchemaName(),
                        cli.getDbUri(),
                        cli.getOutputDirectory(),
                        cli.getSrcNames(),
                        new SchemaMetaDataManager(cli.getSchemaRegistryLocation()));
    System.out.println("DONE !!");
  }


  public static void generateRelayConfig(String schemaRegistryLocation,
                                   String dbName,
                                   String uri,
                                   String outputDir,
                                   List<String> srcNames,
                                   SchemaMetaDataManager manager)
       throws Exception
  {
    PhysicalSourceConfig config = new PhysicalSourceConfig();
    FileSystemSchemaRegistryService s = FileSystemSchemaRegistryService.build(
                                             new FileSystemSchemaRegistryService.StaticConfig(new File(schemaRegistryLocation), 0, false, false));
    dbName = dbName.trim().toLowerCase();
    config.setName(dbName);
    config.setUri(uri);
    for (String srcName : srcNames)
    {
      VersionedSchema schema = null;
      schema = s.fetchLatestVersionedSchemaBySourceName(srcName);

      String dbObjectName = SchemaHelper.getMetaField(schema.getSchema(), "dbFieldName");

      LogicalSourceConfig c = new LogicalSourceConfig();
      c.setId(manager.getSrcId(srcName));
      c.setName(srcName);
      c.setUri(dbName + "." + dbObjectName);
      c.setPartitionFunction("constant:1");
      config.addSource(c);
    }

    DatabusRelaySourcesInFiles relaySourcesInFiles = new DatabusRelaySourcesInFiles(outputDir);
    relaySourcesInFiles.add(dbName, config);
    boolean success = relaySourcesInFiles.save();
    if ( ! success )
      throw new RuntimeException("Unable to create the dev relay config for DB :" + dbName);
  }
}
