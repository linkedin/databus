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
package com.linkedin.databus2.ggParser.staxparser.validator;

import java.io.File;
import java.io.IOException;

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

import com.linkedin.databus.core.ConcurrentAppendableCompositeFileInputStream;
import com.linkedin.databus.core.TrailFileNotifier.TrailFileManager;
import com.linkedin.databus.core.TrailFilePositionSetter;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.producers.db.GGXMLTrailTransactionFinder;

/**
 * The entry point for a command-line too to validate existing XMLFORMAT GoldenGate trail files.
 */
public class XmlFormatTrailValidator
{

  public static final Logger LOG = Logger.getLogger(XmlFormatTrailValidator.class);
  public static final String NAME = XmlFormatTrailValidator.class.getName();

  /** Command-line interface */
  private static class Cli
  {
    public static final char CONTINUOUS_OPT_CHAR = 'c';
    public static final char DEBUG_OPT_CHAR = 'd';
    public static final char DTD_VALIDATION_OPT_CHAR = 'D';
    public static final char USE_XPATH_QUERY_CHAR = 'x';
    public static final char HELP_OPT_CHAR = 'h';
    public static final char LOG4J_PROPS_OPT_CHAR = 'l';

    public static final String DEBUG_OPT_LONG_NAME = "debug";
    public static final String DTD_VALIDATION_OPT_LONG_NAME = "validate_dtd";
    public static final String CONTINUE_ON_ERROR_LONG_NAME = "continue_on_error";
    public static final String CONTINUOUS_OPT_LONG_NAME = "continuous";
    public static final String USE_XPATH_QUERY_NAME = "use_xpath_query";
    public static final String HELP_OPT_LONG_NAME = "help";
    public static final String LOG4J_PROPS_OPT_LONG_NAME = "log_props";

    private final String _usage;
    protected Options _cliOptions;
    protected CommandLine _cmd;
    HelpFormatter _helpFormatter;
    protected Level _defaultLogLevel = Level.INFO;
    private String _trailDirName;
    private String _trailFilePrefix;
    private boolean _continuous = false;
    private boolean _enableDtdValidation = false;
    // Enables regex search over xpath queries while locating initial SCN
    private boolean _enableRegex = true;
    private String _errorLogFile = null;

    public Cli()
    {
      _usage = "java " + NAME + " [options] trail_dir trail_prefix ";
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
      Option log4jPropsOption = OptionBuilder.withLongOpt(LOG4J_PROPS_OPT_LONG_NAME)
                .withDescription("Log4j properties to use")
                .hasArg()
                .withArgName("property_file")
                .create(LOG4J_PROPS_OPT_CHAR);
      Option debugPropsOption = OptionBuilder.withLongOpt(DEBUG_OPT_LONG_NAME)
                .withDescription("Turns on debugging info")
                .create(DEBUG_OPT_CHAR);
      Option dtdValidationOption = OptionBuilder.withLongOpt(DTD_VALIDATION_OPT_LONG_NAME)
                .withDescription("Turns on DTD validation")
                .create(DTD_VALIDATION_OPT_CHAR);
      Option useXpathQueryOption = OptionBuilder.withLongOpt(USE_XPATH_QUERY_NAME)
              .withDescription("Uses xpath queries to locate the SCNs within a transaction")
              .create(USE_XPATH_QUERY_CHAR);
      Option continuousOption = OptionBuilder.withLongOpt(CONTINUOUS_OPT_LONG_NAME)
                .withDescription("Continuously poll for new data")
                .create(CONTINUOUS_OPT_CHAR);
      Option continueOnErrorOption = OptionBuilder.withLongOpt(CONTINUE_ON_ERROR_LONG_NAME)
                .withDescription("Log errors and continue")
                .hasArg()
                .withArgName("log_file")
                .create();

      _cliOptions.addOption(debugPropsOption);
      _cliOptions.addOption(dtdValidationOption);
      _cliOptions.addOption(continuousOption);
      _cliOptions.addOption(helpOption);
      _cliOptions.addOption(useXpathQueryOption);
      _cliOptions.addOption(log4jPropsOption);
      _cliOptions.addOption(continueOnErrorOption);
    }

    public void printCliHelp()
    {
      _helpFormatter.printHelp(getUsage(), _cliOptions);
    }

    public String getUsage()
    {
      return _usage;
    }

    /**
     * Process the command-line arguments
     * @return true iff success
     */
    public boolean processCommandLineArgs(String[] cliArgs) throws IOException, DatabusException
    {

      CommandLineParser cliParser = new GnuParser();

      _cmd = null;
      try
      {
        _cmd = cliParser.parse(_cliOptions, cliArgs);
      }
      catch (ParseException pe)
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

      final int numArgs = _cmd.getArgs().length;
      if (2 != numArgs)
      {
        System.err.println(NAME + ": incorrect command-line arguments " + numArgs + ". Should be 2");
        printCliHelp();
        return false;
      }

      if (_cmd.hasOption(DEBUG_OPT_CHAR))
      {
        Logger.getRootLogger().setLevel(Level.DEBUG);
      }
      else
      {
        Logger.getRootLogger().setLevel(_defaultLogLevel);
      }

      if (_cmd.hasOption(LOG4J_PROPS_OPT_CHAR))
      {
        String log4jPropFile = _cmd.getOptionValue(LOG4J_PROPS_OPT_CHAR);
        PropertyConfigurator.configure(log4jPropFile);
        LOG.info("Using custom logging settings from file " + log4jPropFile);
      }
      else
      {
        PatternLayout defaultLayout = new PatternLayout("%d{ISO8601} +%r [%t] (%p) {%c{1}} %m%n");
        ConsoleAppender defaultAppender = new ConsoleAppender(defaultLayout);

        Logger.getRootLogger().removeAllAppenders();
        Logger.getRootLogger().addAppender(defaultAppender);

        LOG.info("Using default logging settings");
      }

      _continuous = _cmd.hasOption(CONTINUOUS_OPT_CHAR);
      _enableDtdValidation = _cmd.hasOption(DTD_VALIDATION_OPT_CHAR);

      if (_cmd.hasOption(CONTINUE_ON_ERROR_LONG_NAME))
      {
        _errorLogFile = _cmd.getOptionValue(CONTINUE_ON_ERROR_LONG_NAME);
      }

      if (_cmd.hasOption(USE_XPATH_QUERY_CHAR) || _cmd.hasOption(USE_XPATH_QUERY_NAME))
      {
        _enableRegex = false;
      }
      else
      {
        _enableRegex = true;
      }

      _trailDirName = _cmd.getArgs()[0];
      _trailFilePrefix = _cmd.getArgs()[1];
      return true;
    }

    public String getTrailDirName()
    {
      return _trailDirName;
    }

    public String getTrailFilePrefix()
    {
      return _trailFilePrefix;
    }

    public boolean isContinuous()
    {
      return _continuous;
    }

    public boolean isDtdValidationEnabled()
    {
      return _enableDtdValidation;
    }

    public boolean getEnableRegex()
    {
      return _enableRegex;
    }
    /**
     * @return the errorLogFile
     */
    protected String getErrorLogFName()
    {
      return _errorLogFile;
    }
  }

  final TrailFilePositionSetter _trailFilePositionSetter;
  final TrailFilePositionSetter.FilePositionResult _filePositionResult;
  final TrailFileManager _filter;
  final ConcurrentAppendableCompositeFileInputStream _compositeInputStream;
  final XmlFormatTrailParser _parser;
  final boolean _continuous;
  final boolean _dtdValidationEnabled;
  final boolean _enableRegex;

  public XmlFormatTrailValidator(Cli cli) throws Exception
  {
    this(cli.getTrailDirName(), cli.getTrailFilePrefix(), cli.isContinuous(),
         cli.isDtdValidationEnabled(), cli.getErrorLogFName(), cli.getEnableRegex());
  }

  public XmlFormatTrailValidator(String xmlDir, String xmlPrefix, boolean continuous,
                                 boolean dtdValidationEnabled, String errorLogFName,
                                 boolean enableRegex)
      throws Exception
  {
    LOG.info("Reading " + xmlDir + "/" + xmlPrefix + "*; continuous=" + continuous +
             "; dtdValidation=" + dtdValidationEnabled + "; enableRegex=" + enableRegex);

    _continuous = continuous;
    _dtdValidationEnabled = dtdValidationEnabled;
    _enableRegex = enableRegex;

    //find the start of the oldest transaction
    _trailFilePositionSetter = new TrailFilePositionSetter(xmlDir, xmlPrefix);
    _filePositionResult = _trailFilePositionSetter.locateFilePosition(
        TrailFilePositionSetter.USE_EARLIEST_SCN, new GGXMLTrailTransactionFinder(_enableRegex));

    LOG.info("starting from position " + _filePositionResult);

    //set up the parser
    _filter = new TrailFilePositionSetter.FileFilter(new File(xmlDir), xmlPrefix);
    _compositeInputStream = new ConcurrentAppendableCompositeFileInputStream(
        xmlDir,
        _filePositionResult.getTxnPos().getFile(),
        _filePositionResult.getTxnPos().getFileOffset(),
        _filter,
        !continuous);
    _compositeInputStream.initializeStream();
    _parser = new XmlFormatTrailParser(_compositeInputStream, _dtdValidationEnabled, null,
                                       errorLogFName);
  }

  public XmlFormatTrailValidator(String xmlDir, String xmlPrefix, boolean continuous,
      boolean dtdValidationEnabled, String errorLogFName)
  throws Exception
  {
    this(xmlDir, xmlPrefix, continuous, dtdValidationEnabled, errorLogFName, true);
  }

  public boolean run() throws IOException
  {
    boolean success = false;
    LOG.info("starting validation ...");
    if (_continuous)
    {
      Thread runThread = new Thread(_parser, "XmlFormatTrailParser");
      runThread.start();
      success = true;
    }
    else
    {
      _parser.run();
      Throwable error = _parser.getLastError();
      LOG.info("validation complete: " + (null == error ? "SUCCESS" : error.toString()) );
      if (null != error)
      {
        _parser.printErrorContext(System.err);
      }
      else
      {
        success = true;
      }
      LOG.info("PARSE ERRORS: " + _parser.getErrorCount());
    }

    return success;
  }

  public static void main(String[] args) throws Exception
  {
    Cli cli = new Cli();
    if (! cli.processCommandLineArgs(args))
    {
      return;
    }

    XmlFormatTrailValidator validator = new XmlFormatTrailValidator(cli);
    if (! validator.run())
    {
      System.exit(255);
    }
  }

}
