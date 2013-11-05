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
package com.linkedin.databus2.tools.dtail;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.log4j.Logger;

import com.linkedin.databus.core.BaseCli;
import com.linkedin.databus.core.util.ConfigHelper;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.tools.dtail.DtailPrinter.PrintVerbosity;

/**
 * Base class for Dtail Cli
 */
public class DtailCliBase extends BaseCli
{
  public static final String DURATION_OPTION_NAME = "duration";
  public static final char DURATION_OPTION_CHAR = 'u';
  public static final String EVENT_NUM_OPT_NAME = "event_num";
  public static final char EVENT_NUM_OPT_CHAR = 'n';
  public static final String OUTPUT_FORMAT_OPT_NAME = "output_format";
  public static final char OUTPUT_FORMAT_OPT_CHAR = 'F';
  public static final String OUTPUT_OPT_NAME = "output";
  public static final char OUTPUT_OPT_CHAR = 'o';
  public static final String PRINT_VERBOSITY_OPT_NAME = "print_verbosity";
  public static final char PRINT_VERBOSITY_OPT_CHAR = 'V';
  public static final String RESUME_OPT_NAME = "resume";
  public static final String SCN_OPT_NAME = "scn";
  public static final String STATS_OPT_NAME = "stats";
  public static final String VERBOSE_OPT_NAME = "verbose";
  public static final char VERBOSE_OPT_CHAR = 'v';

  public static enum OutputFormat
  {
    JSON,
    AVRO_JSON,
    AVRO_BIN,
    NOOP,
    EVENT_INFO
  }
  public static final String BOB_SCN_STRING = "BOB";
  public static final String EOB_SCN_STRING = "EOB";

  public static final long BOB_SCN = -1000;
  public static final long EOB_SCN = -2000;

  public static final OutputFormat DEFAUL_OUTPUT_FORMAT = OutputFormat.JSON;
  public static final PrintVerbosity DEFAULT_PRINT_VERBOSITY = PrintVerbosity.EVENT;

  protected String _checkpointDirName;
  protected long _durationMs = Long.MAX_VALUE;
  protected long _maxEventNum = Long.MAX_VALUE;
  protected OutputFormat _outputFormat = DEFAUL_OUTPUT_FORMAT;
  protected PrintVerbosity _printVerbosity = DEFAULT_PRINT_VERBOSITY;
  protected OutputStream _out = System.out;
  protected boolean _showStats = false;
  protected long _sinceScn = BOB_SCN;

  public DtailCliBase(BaseCli.CliHelp help, Logger log)
  {
    super(help, log);
    _defaultLogLevelIdx = 1;
  }

  @SuppressWarnings("static-access")
  @Override
  protected void constructCommandLineOptions()
  {
    super.constructCommandLineOptions();

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
    Option eventNumOption = OptionBuilder.withLongOpt(EVENT_NUM_OPT_NAME)
                                           .hasArg()
                                           .withArgName("num")
                                           .withDescription("max number of events to return; default: no limit")
                                           .create(EVENT_NUM_OPT_CHAR);
    Option durationOption = OptionBuilder.withLongOpt(DURATION_OPTION_NAME)
                                         .hasArg()
                                         .withArgName("duration_value")
                                         .withDescription("max consumption duration: value[ns|us|ms|s|min|hr|d]; default: no limit")
                                         .create(DURATION_OPTION_CHAR);
    Option statsOption = OptionBuilder.withLongOpt(STATS_OPT_NAME)
                                      .withDescription("print statistics at the end; Default: off")
                                      .create();
    Option sinceScnOption = OptionBuilder.withLongOpt(SCN_OPT_NAME)
                                         .hasArg()
                                         .withArgName("scn")
                                         .withDescription("starts consumption from the given scn; special values: BOB for current beginning of relay buffer, " +
                                              "EOB for current end of buffer; Default: BOB")
                                         .create();

    _cliOptions.addOption(eventNumOption);
    _cliOptions.addOption(outputFormatOption);
    _cliOptions.addOption(outputOption);
    _cliOptions.addOption(printVerbosityOption);
    _cliOptions.addOption(resumeOption);
    _cliOptions.addOption(verboseOption);
    _cliOptions.addOption(durationOption);
    _cliOptions.addOption(statsOption);
    _cliOptions.addOption(sinceScnOption);
  }

  @Override
  public boolean processCommandLineArgs(String[] cliArgs)
  {
    super.processCommandLineArgs(cliArgs);

    if (!processOutputFormat())
    {
      return false;
    }
    if (!processVerbosity())
    {
      return false;
    }
    if (!processOutput())
    {
      return false;
    }
    processResume();
    if (!processMaxEventNum())
    {
      return false;
    }
    if (!processDuration())
    {
      return false;
    }
    processStats();
    processSinceScn();

    return true;
  }

  private boolean processOutputFormat()
  {
    if (_cmd.hasOption(OUTPUT_FORMAT_OPT_CHAR))
    {
      try
      {
        _outputFormat = OutputFormat.valueOf(_cmd.getOptionValue(OUTPUT_FORMAT_OPT_CHAR)
                                                 .toUpperCase());
      }
      catch (IllegalArgumentException e)
      {
        printError("invalid output format: " + _cmd.getOptionValue(OUTPUT_FORMAT_OPT_CHAR), true);
        return false;
      }
    }
    return true;
  }

  private boolean processVerbosity()
  {
    if (_cmd.hasOption(PRINT_VERBOSITY_OPT_CHAR))
    {
      try
      {
        _printVerbosity = PrintVerbosity.valueOf(_cmd.getOptionValue(PRINT_VERBOSITY_OPT_CHAR)
                                                     .toUpperCase());
      }
      catch (IllegalArgumentException e)
      {
        printError("invalid print verbosity: " + _cmd.getOptionValue(PRINT_VERBOSITY_OPT_CHAR),
                   true);
        return false;
      }
    }
    return true;
  }

  private boolean processOutput()
  {
    if (_cmd.hasOption(OUTPUT_OPT_CHAR))
    {
      String outputStr = _cmd.getOptionValue(OUTPUT_OPT_CHAR);
      if (outputStr.startsWith("hdfs://"))
      {
        printError("HDFS is not supported yet", false);
        return false;
      }
      else if (! outputStr.equals("-"))
      {
        try
        {
          _out = new FileOutputStream(outputStr);
        }
        catch (IOException e)
        {
          printError("unable to open output: " + outputStr, false);
          return false;
        }
      }
    }
    return true;
  }

  private void processResume()
  {
    if (_cmd.hasOption(RESUME_OPT_NAME))
    {
      _checkpointDirName = _cmd.getOptionValue(RESUME_OPT_NAME);
    }
  }

  private boolean processMaxEventNum()
  {
    if (_cmd.hasOption(EVENT_NUM_OPT_CHAR))
    {
      try
      {
        _maxEventNum = Long.parseLong(_cmd.getOptionValue(EVENT_NUM_OPT_CHAR));
      }
      catch (NumberFormatException e)
      {
        printError("invalid max events number: " + _cmd.getOptionValue(EVENT_NUM_OPT_CHAR), false);
        return false;
      }
    }
    return true;
  }

  private boolean processDuration()
  {
    if (_cmd.hasOption(DURATION_OPTION_CHAR))
    {
      String durationStr = _cmd.getOptionValue(DURATION_OPTION_CHAR);
      _log.info("Using duration: " + durationStr);
      try
      {
        _durationMs = ConfigHelper.parseDuration(durationStr, TimeUnit.MILLISECONDS);
      }
      catch (InvalidConfigException e)
      {
        printError("invalid duration: " + durationStr, true);
        return false;
      }
    }
    return true;
  }

  private void processStats()
  {
    _showStats = _cmd.hasOption(STATS_OPT_NAME);
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
    _log.info("starting from SCN: " + _sinceScn);
  }

  public String getCheckpointDirName()
  {
    return _checkpointDirName;
  }

  public long getDurationMs()
  {
    return _durationMs;
  }

  public boolean isShowStats()
  {
    return _showStats;
  }

  public long getMaxEventNum()
  {
    return _maxEventNum;
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

  public long getSinceScn()
  {
    return _sinceScn;
  }

}
