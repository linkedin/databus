package com.linkedin.databus.core;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.PropertyConfigurator;
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

/**
 * Base CLI implementation. Provides common options.
 * Default options supported:
 * <preformat>
 * -h,--help                        Prints command-line options info
 * -l,--log_props <property_file>   Log4j properties to use
 * -q,--quiet                       turn off logging
 * -v                               verbose
 * -vv                              more verbose
 * -vvv                             most verbose
 * </preformat>
 * */
public class BaseCli
{
  /** Possible verbosities for logging - get around the problem that the int <--> Level mapping
   * is not consistent */
  protected static final Level[] VERBOSITIES =
    {
      Level.OFF, Level.ERROR, Level.WARN, Level.INFO, Level.DEBUG, Level.ALL
    };

  /**
   * Encapsulates the information to be printed as part of the help screen for a CLI tool.
   * The structure is:
   * <pre>
   * usage
   * header
   * options (generate by Apache CLI)
   * footer
   * </pre>
   */
  public static class CliHelp
  {
    private final String _className;
    private final String _usage;
    private final String _header;
    private final String _footer;

    public CliHelp(String className, String usage, String header, String footer)
    {
      super();
      _usage = usage;
      _header = header;
      _footer = footer;
      _className = className;
    }

    public String getUsage()
    {
      return _usage;
    }

    public String getHeader()
    {
      return _header;
    }

    public String getFooter()
    {
      return _footer;
    }

    public String getClassName()
    {
      return _className;
    }
  }

  public static interface HeaderFooterBuilder
  {
    HeaderFooterBuilder add(String s);
    HeaderFooterBuilder addLine();
    HeaderFooterBuilder addLine(String ln);
    HeaderFooterBuilder addSection(String sectionName);
    CliHelpBuilder finish();
    @Override
    String toString();
  }

  public static class StdHeaderFooterBuilder implements HeaderFooterBuilder
  {

    protected final StringBuilder _s = new StringBuilder();
    protected final CliHelpBuilder _parent;

    public StdHeaderFooterBuilder(CliHelpBuilder parent)
    {
      super();
      _parent = parent;
    }

    @Override
    public HeaderFooterBuilder addLine(String ln)
    {
      _s.append(ln);
      _s.append("\n");
      return this;
    }

    @Override
    public HeaderFooterBuilder addSection(String sectionName)
    {
      addLine();
      addLine(sectionName.toUpperCase());
      for (int i = 0; i< sectionName.length(); ++i) add("-");
      addLine();
      return this;
    }

    @Override
    public String toString()
    {
      return _s.toString();
    }

    /**
     * @see com.linkedin.databus.core.BaseCli.HeaderFooterBuilder#add(java.lang.String)
     */
    @Override
    public HeaderFooterBuilder add(String s)
    {
      _s.append(s);
      return this;
    }

    /**
     * @see com.linkedin.databus.core.BaseCli.HeaderFooterBuilder#finishSection()
     */
    @Override
    public CliHelpBuilder finish()
    {
      return _parent;
    }

    /**
     * @see com.linkedin.databus.core.BaseCli.HeaderFooterBuilder#addLine()
     */
    @Override
    public HeaderFooterBuilder addLine()
    {
      //the default Apache Cli HelpFormatter does not like completely blank lines :(
      return addLine("\177");
    }
  }

  public static class CliHelpBuilder
  {
    private String _usage;
    private HeaderFooterBuilder _header;
    private HeaderFooterBuilder _footer;
    private String _className = "<class>";

    public CliHelpBuilder className(String className)
    {
      _className = className;
      _header = new StdHeaderFooterBuilder(this);
      _footer = new StdHeaderFooterBuilder(this);
      return this;
    }

    public CliHelpBuilder className(Class<?> clazz)
    {
      return className(clazz.getName());
    }

    public String className()
    {
      return _className;
    }

    public HeaderFooterBuilder startHeader()
    {
      return _header;
    }

    public CliHelpBuilder headerBuilder(HeaderFooterBuilder builder)
    {
      _header = builder;
      return this;
    }

    public HeaderFooterBuilder startFooter()
    {
      return _footer;
    }

    public CliHelpBuilder footerBuilder(HeaderFooterBuilder builder)
    {
      _footer = builder;
      return this;
    }

    public String header()
    {
      return _header.toString();
    }

    public String footer()
    {
      return _footer.toString();
    }

    public CliHelpBuilder usage(String usage)
    {
      _usage = usage;
      return this;
    }

    public String usage()
    {
      return _usage;
    }

    public CliHelp build()
    {
      if (null == _usage)
      {
        _usage = "java " + _className + " [options]";
      }

      return new CliHelp(_className, _usage, null != _header ? _header.toString() : "",
                        null != _footer ? _footer.toString() : "");
    }
  }

  protected static final String CMD_LINE_PROPS_OPT_LONG_NAME = "cmdline_props";
  protected static final char CMD_LINE_PROPS_OPT_CHAR = 'c';
  protected static final String HELP_OPT_LONG_NAME = "help";
  protected static final char HELP_OPT_CHAR = 'h';
  protected static final String LOG4J_PROPS_OPT_LONG_NAME = "log_props";
  protected static final char LOG4J_PROPS_OPT_CHAR = 'l';
  protected static final String PROPS_FILE_OPT_LONG_NAME = "props_file";
  protected static final char PROPS_FILE_OPT_CHAR = 'p';
  protected static final String QUIET_OPT_LONG_NAME = "quiet";
  protected static final char QUIET_OPT_CHAR = 'q';
  protected static final String VERBOSE1_OPT_LONG_NAME = "v";
  protected static final String VERBOSE2_OPT_LONG_NAME = "vv";
  protected static final String VERBOSE3_OPT_LONG_NAME = "vvv";

  private final CliHelp _help;
  final protected Options _cliOptions;
  final protected HelpFormatter _helpFormatter;
  final protected Logger _log;
  protected CommandLine _cmd;
  protected int _defaultLogLevelIdx = 3; //Level.INFO
  final protected Properties _configProps = new Properties(System.getProperties());

  public BaseCli(String usage, Logger log)
  {
    this(new CliHelpBuilder().usage(usage).build(), log);
  }

  public BaseCli(CliHelp help, Logger log)
  {
    _log = null == log ? Logger.getLogger(getClass()) : log;
    _help = help;
    _cliOptions = new Options();
    _helpFormatter = new HelpFormatter();
    _helpFormatter.setWidth(150);
  }

  public static String createDefaultUsageString(String className)
  {
    return "java " + className + " [options]";
  }

  public static String createDefaultUsageString(Class<?> clazz)
  {
    return createDefaultUsageString(clazz.getName());
  }

  public Properties getConfigProps()
  {
    return _configProps;
  }

  protected String getProgramName()
  {
    return _help.getClassName();
  }

  protected void printError(String message, boolean printHelp)
  {
    System.err.println(getProgramName() + ": " + message);
    if (printHelp)
    {
      System.out.println();
      printCliHelp();
    }
  }

  private void updatePropsFromCmdLine(String cmdLinePropString)
  {
    String[] cmdLinePropSplit = cmdLinePropString.split(";");
    for(String s : cmdLinePropSplit)
    {
      String[] onePropSplit = s.split("=");
      if (onePropSplit.length != 2)
      {
        _log.error("CMD line property setting " + s + "is not valid!");
      }
      else
      {
        _log.info("CMD line Property overwride: " + s);
        _configProps.put(onePropSplit[0], onePropSplit[1]);
      }
    }
  }

  /**
	* Creates command-line options
	*/
  @SuppressWarnings("static-access")
  protected void constructCommandLineOptions()
  {
    _cliOptions.addOption(
        OptionBuilder.withLongOpt(HELP_OPT_LONG_NAME)
                     .withDescription("Prints command-line options info")
                     .create(HELP_OPT_CHAR));
    _cliOptions.addOption(
        OptionBuilder.withLongOpt(CMD_LINE_PROPS_OPT_LONG_NAME)
                     .withDescription("Command-line override for config properties; a " +
                                      "semicolon-separated list of key=vale.")
                     .hasArg()
                     .withArgName("Semicolon_separated_properties")
                     .create(CMD_LINE_PROPS_OPT_CHAR));
    _cliOptions.addOption(
        OptionBuilder.withLongOpt(LOG4J_PROPS_OPT_LONG_NAME)
                     .withDescription("Log4j properties to use")
                     .hasArg()
                     .withArgName("property_file")
                     .create(LOG4J_PROPS_OPT_CHAR));
    _cliOptions.addOption(
         OptionBuilder.withLongOpt(PROPS_FILE_OPT_LONG_NAME)
                      .withDescription("Config properties file to use")
                      .hasArg()
                      .withArgName("property_file")
                      .create(PROPS_FILE_OPT_CHAR));
    _cliOptions.addOption(
        OptionBuilder.withLongOpt(QUIET_OPT_LONG_NAME)
                     .withDescription("quiet (no logging)")
                     .create(QUIET_OPT_CHAR));
    _cliOptions.addOption(
        OptionBuilder.withDescription("verbose")
                     .create(VERBOSE1_OPT_LONG_NAME));
    _cliOptions.addOption(
        OptionBuilder.withDescription("more verbose")
                     .create(VERBOSE2_OPT_LONG_NAME));
    _cliOptions.addOption(
        OptionBuilder.withDescription("most verbose")
                     .create(VERBOSE3_OPT_LONG_NAME));
  }

  /**
   * Parses the command line arguments
   * @param cliArgs		the command line arguments
   * @return true iff parsing was successful
   */
  public boolean processCommandLineArgs(String[] cliArgs)
  {
    constructCommandLineOptions();
    CommandLineParser cliParser = new GnuParser();

    _cmd = null;
    try
    {
      _cmd = cliParser.parse(_cliOptions, cliArgs);
    }
    catch (ParseException pe)
    {
      printError("failed to parse command-line options: " + pe.toString(), true);
      return false;
    }

    if (_cmd.hasOption(HELP_OPT_CHAR))
    {
      printCliHelp();
      return false;
    }

    int verbosityInc = 0;
    if (_cmd.hasOption(VERBOSE3_OPT_LONG_NAME))
    {
      //-vvv is always Level.ALL
      verbosityInc = VERBOSITIES.length;
    }
    else if (_cmd.hasOption(VERBOSE2_OPT_LONG_NAME))
    {
      verbosityInc = 2;
    }
    else if (_cmd.hasOption(VERBOSE1_OPT_LONG_NAME))
    {
      verbosityInc = 1;
    }

    Level effectiveLevel = VERBOSITIES[Math.min(_defaultLogLevelIdx + verbosityInc,
                                                VERBOSITIES.length - 1)];
    if (_cmd.hasOption(QUIET_OPT_CHAR))
    {
      effectiveLevel = Level.OFF;
    }
    Logger.getRootLogger().setLevel(effectiveLevel);

    if (_cmd.hasOption(LOG4J_PROPS_OPT_CHAR))
    {
      String log4jPropFile = _cmd.getOptionValue(LOG4J_PROPS_OPT_CHAR);
      PropertyConfigurator.configure(log4jPropFile);
      _log.debug("Using custom logging settings from file " + log4jPropFile);
    }
    else
    {
      PatternLayout defaultLayout = new PatternLayout("%d{ISO8601} +%r [%t] (%p) {%c{1}} %m%n");
      ConsoleAppender defaultAppender = new ConsoleAppender(defaultLayout);

      Logger.getRootLogger().removeAllAppenders();
      Logger.getRootLogger().addAppender(defaultAppender);
      _log.debug("Using default logging settings");
    }

    processProperties();

    return true;
  }

  private void processProperties()
  {
    if (_cmd.hasOption(PROPS_FILE_OPT_CHAR))
    {
      for (String propFile: _cmd.getOptionValues(PROPS_FILE_OPT_CHAR))
      {
        _log.info("Loading container config from properties file " + propFile);
        FileInputStream fis = null;
        try
        {
          fis = new FileInputStream(propFile);
          _configProps.load(fis);
        }
        catch (Exception e)
        {
          _log.error("error processing properties; ignoring:" + e.getMessage());
        }
        finally
        {
          if (fis != null)
          {
            try
            {
              fis.close();
            }
            catch (IOException e)
            {
              //not much to do -- ignore
            }
          }
        }
      }
    }
    else
    {
      _log.info("Using system properties for container config");
    }

    if (_cmd.hasOption(CMD_LINE_PROPS_OPT_CHAR))
    {
      String cmdLinePropString = _cmd.getOptionValue(CMD_LINE_PROPS_OPT_CHAR);
      updatePropsFromCmdLine(cmdLinePropString);
    }
  }

  /** Print out the help for this program */
  public void printCliHelp()
  {
    _helpFormatter.printHelp(_help.getUsage(), _help.getHeader(), _cliOptions, _help.getFooter());
  }

  /** Gets out the usage for this program*/
  public String getUsage()
  {
    return _help.getUsage();
  }

  /** For testing */
  public static void main(String[] args)
  {
    BasicConfigurator.configure();
    BaseCli cli = new BaseCli(new CliHelpBuilder().className(BaseCli.class)
                                                  .startHeader()
                                                  .addSection("Description")
                                                  .addLine("Description Line 1")
                                                  .addLine("Description Line 2")
                                                  .addSection("Options")
                                                  .finish()
                                                  .startFooter()
                                                  .addSection("Examples")
                                                  .addLine("* Example Line 1")
                                                  .addLine("\177\t Example Line 2")
                                                  .addSection("Notes")
                                                  .addLine("Be careful")
                                                  .finish()
                                                  .build(),
                                                  null);
    cli.processCommandLineArgs(args);
  }

}
