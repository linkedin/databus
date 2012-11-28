package com.linkedin.databus2.core.schema.tools;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;

import com.linkedin.databus2.core.schema.tools.AvroConverter.AvroFormat;

public class AvroConvertMain
{
  public static final String MODULE = AvroConvertMain.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception
  {

    ConsoleAppender app = new ConsoleAppender(new SimpleLayout());
    Logger.getRootLogger().removeAllAppenders();
    Logger.getRootLogger().addAppender(app);

    AvroConvertCli cli = new AvroConvertCli();

    try
    {
      cli.parseCommandLine(args);
    }
    catch (ParseException pe)
    {
      System.err.println(pe.getMessage());
      cli.printUsage();
      System.exit(1);
    }

    if (!cli.hasOptions() || cli.hasHelpOption())
    {
      cli.printUsage();
      System.exit(0);
    }

    int verbosity = cli.getVerbosity();
    switch (verbosity)
    {
      case 0: Logger.getRootLogger().setLevel(Level.ERROR); break;
      case 1: Logger.getRootLogger().setLevel(Level.INFO); break;
      case 2: Logger.getRootLogger().setLevel(Level.DEBUG); break;
      default: Logger.getRootLogger().setLevel(Level.ALL); break;
    }

    AvroFormat inputFormat = cli.getInputFormat(AvroFormat.JSON);
    LOG.info("Using input format: " + inputFormat);

    AvroFormat outputFormat = cli.getOutputFormat(AvroFormat.JSON);
    LOG.info("Using output format: " + outputFormat);

    String inputSchemaName = cli.getInputSchema(null);
    if (null == inputSchemaName)
    {
      System.err.println("Input schema expected");
      cli.printUsage();
      System.exit(4);
    }

    Schema inputSchema = null;
    try
    {
      inputSchema = openSchema(inputSchemaName);
    }
    catch (IOException ioe)
    {
      System.err.println("Unable to open input schema: " + ioe);
      System.exit(2);
    }
    LOG.info("Using input schema:" + inputSchemaName);

    String outputSchemaName = cli.getOutputSchema(inputSchemaName);
    Schema outputSchema = null;
    try
    {
      outputSchema = outputSchemaName.equals(inputSchemaName) ? inputSchema : openSchema(outputSchemaName);
    }
    catch (IOException ioe)
    {
      System.err.println("Unable to open output schema: " + ioe);
      System.exit(3);
    }
    LOG.info("Using output schema:" + outputSchemaName);

    String inputFileName = cli.getInputFileName("-");
    InputStream input = inputFileName.equals("-") ? System.in : new FileInputStream(inputFileName);
    LOG.info("Using input: " + inputFileName);

    String outputFileName = cli.getOutputFileName("-");
    OutputStream output = outputFileName.equals("-") ? System.out : new FileOutputStream(outputFileName);
    LOG.info("Using output: " + outputFileName);

    AvroConverter avroConverter = new AvroConverter(inputFormat, outputFormat, inputSchema,
                                                    outputSchema);
    avroConverter.convert(input, output);
    if (! inputFileName.equals("-")) input.close();
    if (! outputFileName.equals("-")) output.close();
  }

  private static Schema openSchema(String schemaName) throws IOException
  {
    return Schema.parse(new File(schemaName));
  }

}

class AvroConvertCli
{
  public static final String HELP_OPT_NAME = " help";
  public static final char HELP_OPT_CHAR = 'h';
  public static final String INPUT_SCHEMA_OPT_NAME = "input_schema";
  public static final char INPUT_SCHEMA_OPT_CHAR = 'I';
  public static final String INPUT_FORMAT_OPT_NAME = "input_format";
  public static final char INPUT_FORMAT_OPT_CHAR = 'i';
  public static final String OUTPUT_SCHEMA_OPT_NAME = "output_schema";
  public static final char OUTPUT_SCHEMA_OPT_CHAR = 'O';
  public static final String OUTPUT_FORMAT_OPT_NAME = "output_format";
  public static final char OUTPUT_FORMAT_OPT_CHAR = 'o';
  public static final String INPUT_FILE_OPT_NAME = "input_file";
  public static final char INPUT_FILE_OPT_CHAR = 'f';
  public static final String OUTPUT_FILE_OPT_NAME = "output_file";
  public static final char OUTPUT_FILE_OPT_CHAR = 'F';
  public static final char VERBOSE_OPT_CHAR = 'v';

  private final Options _options = new Options();
  private CommandLine _cmdLine;

  @SuppressWarnings("static-access")
  public AvroConvertCli()
  {

    Option helpOption = OptionBuilder.withLongOpt(HELP_OPT_NAME)
                                     .withDescription("this help screen")
                                     .create(HELP_OPT_CHAR);
    Option inpFormatOption = OptionBuilder.withLongOpt(INPUT_FORMAT_OPT_NAME)
                                          .withDescription("Input format: JSON, JSON_LINES, BINARY")
                                          .hasArg()
                                          .withArgName("format")
                                          .create(INPUT_FORMAT_OPT_CHAR);
    Option outFormatOption = OptionBuilder.withLongOpt(OUTPUT_FORMAT_OPT_NAME)
                                          .withDescription("Output format: JSON, JSON_LINES, BINARY")
                                          .hasArg()
                                          .withArgName("format")
                                          .create(OUTPUT_FORMAT_OPT_CHAR);
    Option inpSchemaOption = OptionBuilder.withLongOpt(INPUT_SCHEMA_OPT_NAME)
                                          .withDescription("Input schema file")
                                          .hasArg()
                                          .withArgName("file")
                                          .create(INPUT_SCHEMA_OPT_CHAR);
    Option outSchemaOption = OptionBuilder.withLongOpt(OUTPUT_SCHEMA_OPT_NAME)
                                          .withDescription("Output schema file")
                                          .hasArg()
                                          .withArgName("file")
                                          .create(OUTPUT_SCHEMA_OPT_CHAR);
    Option inpFileOption = OptionBuilder.withLongOpt(INPUT_FILE_OPT_NAME)
                                        .withDescription("input file name or - for STDIN")
                                        .hasArg()
                                        .withArgName("file | - ")
                                        .create(INPUT_FILE_OPT_CHAR);
    Option outFileOption = OptionBuilder.withLongOpt(OUTPUT_FILE_OPT_NAME)
                                        .withDescription("output file name or - for STDOUT")
                                        .hasArg()
                                        .withArgName("file | - ")
                                        .create(OUTPUT_FILE_OPT_CHAR);
    Option verboseOption = OptionBuilder.withDescription("verbose; more occurrences increase verbosity")
                                        .create(VERBOSE_OPT_CHAR);

    _options.addOption(helpOption);
    _options.addOption(inpFormatOption);
    _options.addOption(outFormatOption);
    _options.addOption(inpSchemaOption);
    _options.addOption(outSchemaOption);
    _options.addOption(inpFileOption);
    _options.addOption(outFileOption);
    _options.addOption(verboseOption);
  }

  public void parseCommandLine(String[] args) throws ParseException
  {
    GnuParser cmdLineParser = new GnuParser();
    _cmdLine = cmdLineParser.parse(_options, args);
  }

  public void printUsage()
  {
    HelpFormatter help = new HelpFormatter();
    help.setWidth(100);
    help.printHelp("java " + AvroConvertMain.class.getName() + " [options]", _options);
  }

  public boolean hasOptions()
  {
    return _cmdLine.getOptions().length > 0;
  }

  public boolean hasHelpOption()
  {
    return _cmdLine.hasOption(HELP_OPT_CHAR);
  }

  public AvroConverter.AvroFormat getInputFormat(AvroConverter.AvroFormat defaultFormat)
  {
    String inpFormatString = _cmdLine.getOptionValue(INPUT_FORMAT_OPT_CHAR, defaultFormat.toString());
    return AvroConverter.AvroFormat.valueOf(inpFormatString.toUpperCase());
  }

  public AvroConverter.AvroFormat getOutputFormat(AvroConverter.AvroFormat defaultFormat)
  {
    String outFormatString = _cmdLine.getOptionValue(OUTPUT_FORMAT_OPT_CHAR, defaultFormat.toString());
    return AvroConverter.AvroFormat.valueOf(outFormatString.toUpperCase());
  }

  public String getInputSchema(String defaultValue)
  {
    return _cmdLine.getOptionValue(INPUT_SCHEMA_OPT_CHAR, defaultValue);
  }

  public String getOutputSchema(String defaultValue)
  {
    return _cmdLine.getOptionValue(OUTPUT_SCHEMA_OPT_CHAR, defaultValue);
  }

  public String getInputFileName(String defaultValue)
  {
    return _cmdLine.getOptionValue(INPUT_FILE_OPT_CHAR, defaultValue);
  }

  public String getOutputFileName(String defaultValue)
  {
    return _cmdLine.getOptionValue(OUTPUT_FILE_OPT_CHAR, defaultValue);
  }

  public int getVerbosity()
  {
    int cnt = 0;
    for (Option o: _cmdLine.getOptions()) if (o.getOpt().equals("v")) ++cnt;
    return cnt;
  }

}
