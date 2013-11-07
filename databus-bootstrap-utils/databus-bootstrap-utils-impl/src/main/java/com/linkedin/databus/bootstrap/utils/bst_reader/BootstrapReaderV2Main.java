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
package com.linkedin.databus.bootstrap.utils.bst_reader;

import java.io.Console;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.OptionBuilder;
import org.apache.log4j.Logger;

import com.linkedin.databus.bootstrap.common.BootstrapConfig;
import com.linkedin.databus.bootstrap.common.BootstrapConfigBase;
import com.linkedin.databus.bootstrap.common.BootstrapReadOnlyConfig;
import com.linkedin.databus.bootstrap.utils.BootstrapDBReader;
import com.linkedin.databus.bootstrap.utils.BootstrapReaderEventHandler;
import com.linkedin.databus.bootstrap.utils.bst_reader.filter.BootstrapReaderFilter;
import com.linkedin.databus.bootstrap.utils.bst_reader.filter.PayloadFieldEqFilter;
import com.linkedin.databus.bootstrap.utils.bst_reader.filter.PayloadFieldGtFilter;
import com.linkedin.databus.bootstrap.utils.bst_reader.filter.PayloadFieldLtFilter;
import com.linkedin.databus.core.BaseCli;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.schemas.FileSystemVersionedSchemaSetProvider;
import com.linkedin.databus2.schemas.ResourceVersionedSchemaSetProvider;
import com.linkedin.databus2.schemas.VersionedSchemaSet;
import com.linkedin.databus2.schemas.VersionedSchemaSetProvider;

/**
 * A helper utility to read  the contents of the bootstrap database
 */
public class BootstrapReaderV2Main
{
  public static final Logger LOG = Logger.getLogger(BootstrapReaderV2Main.class);

  public static class Cli extends BaseCli
  {
    /**
     *
     */
    private static final int QUERY_BATCH_SIZE = 50;
    public static final String BST_DBNAME_OPT_NAME = "db";
    public static final char BST_DBNAME_OPT_CHAR = 'D';
    public static final String MYSQL_HOST_OPT_NAME = "mysql_host";
    public static final char MYSQL_HOST_OPT_CHAR = 'H';
    public static final String BST_USER_OPT_NAME = "user";
    public static final char BST_USER_OPT_CHAR = 'u';
    public static final String BST_PASSWORD_OPT_NAME = "password";
    public static final char BST_PASSWORD_OPT_CHAR = 'p';
    public static final String WITH_SNAPSHOT_OPT_NAME = "with-snapshot-table";
    public static final char WITH_SNAPSHOT_OPT_CHAR = 'S';
    public static final String WITH_LOG_OPT_NAME = "with-log-tables";
    public static final char WITH_LOG_OPT_CHAR = 'L';
    public static final String SCNS_OPT_NAME = "scns";
    public static final String KEYS_OPT_NAME = "keys";
    public static final String SRCID_OPT_NAME = "srcid";
    public static final String SRCNAME_OPT_NAME = "srcname";
    public static final String SCHEMAS_PATH_OPT_NAME = "schemas_registry";
    public static final String PAYLOAD_COND_OPT_NAME = "payload_matches";

    private static final Pattern PAYLOAD_COND_REGEX = Pattern.compile("(\\w+)(<=|<|>|>=|==|!=|~)(.+)");

    private final BootstrapConfig _bstCfg;
    private short _srcId = (short)-1;
    private final MetaDataFiltersBuilder _metaBuilder =
        new MetaDataFiltersBuilder();
    private final List<String> _tableNames = new ArrayList<String>();
    private final List<BootstrapReaderFilter> _eventFilters =
        new ArrayList<BootstrapReaderFilter>();
    private VersionedSchemaSetProvider _schemaSetProvider =
        new ResourceVersionedSchemaSetProvider(this.getClass().getClassLoader());

    public Cli(Logger log) throws IOException
    {
      super(constructCliHelp(), log);
      _bstCfg = new BootstrapConfig();
      _bstCfg.setBootstrapBatchSize(QUERY_BATCH_SIZE);
    }

    private static BaseCli.CliHelp constructCliHelp()
    {
      return new BaseCli.CliHelpBuilder()
         .className(BootstrapReaderV2Main.class)
         .startHeader()
           .addSection("Description")
           .addLine("A simple tool to inspect the contents for MySQL bootstrap database")
           .addSection("Options")
         .finish()
         .startFooter()
           .addSection("Examples")
           .addLine("* Find records in tab_201 with key 12345")
           .addLine()
           .add("java ")
           .add(BootstrapReaderV2Main.class.getName())
           .addLine("-H example.host.com --with-snaptshot-table --keys 12345,12345 --srcid 201")
           .addLine()
           .addLine("* Find records in tab_201 with key from \"abc\" to \"abd\" and scn <= 1000 " +
                    "(key range works as expected only for true string keys)")
           .addLine()
           .add("java ")
           .add(BootstrapReaderV2Main.class.getName())
           .addLine("-H example.host.com --with-snaptshot-table --keys abc,abd --scn ,1000 --srcid 201")
           .addLine()
           .addLine("* Find records in tab_201, log_201_100, ..., log_201_200 with key > 123  which " +
                    "are not deleted (works for number keys but requires a full table scan)")
           .addLine()
           .add("java ")
           .add(BootstrapReaderV2Main.class.getName())
           .addLine("-H example.host.com --with-snaptshot-table --with-log-tables 100,200  " +
                    "--srcid 201 --payload_matches key>123 --payload_matches isDeleted==N")
           .addLine()
         .finish()
         .build();
    }

    @SuppressWarnings("static-access")
    @Override
    protected void constructCommandLineOptions()
    {
      super.constructCommandLineOptions();
      _cliOptions.addOption(
          OptionBuilder.withLongOpt(BST_DBNAME_OPT_NAME)
                       .withDescription("bootstrap database name; default is " +
                                        BootstrapConfigBase.DEFAULT_BOOTSTRAP_DB_NAME)
                       .hasArg()
                       .withArgName("db_name")
                       .create(BST_DBNAME_OPT_CHAR));
      _cliOptions.addOption(
          OptionBuilder.withLongOpt(MYSQL_HOST_OPT_NAME)
                       .withDescription("bootstrap MySQL host; default is " +
                                        BootstrapConfigBase.DEFAULT_BOOTSTRAP_DB_HOSTNAME)
                       .hasArg()
                       .withArgName("host_name")
                       .create(MYSQL_HOST_OPT_CHAR));
      _cliOptions.addOption(
          OptionBuilder.withLongOpt(BST_USER_OPT_NAME)
                       .withDescription("bootstrap user name; default is " +
                                        BootstrapConfigBase.DEFAULT_BOOTSTRAP_DB_USERNAME)
                       .hasArg()
                       .withArgName("user_name")
                       .create(BST_USER_OPT_CHAR));
      _cliOptions.addOption(
          OptionBuilder.withLongOpt(BST_PASSWORD_OPT_NAME)
                       .withDescription("bootstrap user password; default is " +
                                        BootstrapConfigBase.DEFAULT_BOOTSTRAP_DB_PASSWORD)
                       .hasOptionalArg()
                       .withArgName("password")
                       .create(BST_PASSWORD_OPT_CHAR));
      _cliOptions.addOption(
          OptionBuilder.withLongOpt(WITH_SNAPSHOT_OPT_NAME)
                       .withDescription("scan snapshot table")
                       .create(WITH_SNAPSHOT_OPT_CHAR));
      _cliOptions.addOption(
          OptionBuilder.withLongOpt(WITH_LOG_OPT_NAME)
                       .withDescription("scan log tables. If min_log_id is not specified, the" +
                                        " first available log will be used. if max_log_id is not" +
                                        " specified, the last available log table will be used.")
                       .hasOptionalArg()
                       .withArgName("[[min_log_id],[max_log_id]]")
                       .create(WITH_LOG_OPT_CHAR));
      _cliOptions.addOption(
          OptionBuilder.withDescription("scn range to return")
                       .hasArg()
                       .withArgName("[min_scn],[max_scn]")
                       .create(SCNS_OPT_NAME));
      _cliOptions.addOption(
          OptionBuilder.withDescription("key range to return")
                       .hasArg()
                       .withArgName("[min_key],[max_key]")
                       .create(KEYS_OPT_NAME));
      _cliOptions.addOption(
          OptionBuilder.withDescription("logical source id to scan; exactly one of --srcid and " +
                                        "--srcname should be specified")
                       .hasArg()
                       .withArgName("srcid")
                       .create(SRCID_OPT_NAME));
      _cliOptions.addOption(
          OptionBuilder.withDescription("logical source name to scan; exactly one of --srcid and " +
                                        "--srcname should be specified")
                       .hasArg()
                       .withArgName("source_name")
                       .create(SRCNAME_OPT_NAME));
      _cliOptions.addOption(
          OptionBuilder.withDescription("path to the schemas registry with the Avro schemas to use for decoding")
                       .hasArg()
                       .withArgName("schemas_path")
                       .create(SCHEMAS_PATH_OPT_NAME));
      _cliOptions.addOption(
          OptionBuilder.withDescription("payload predicate: <OP> can be <=,<,>,>=,==,!=,~")
                       .hasArg()
                       .withArgName("field_name<OP>constant")
                       .create(PAYLOAD_COND_OPT_NAME));
    }

    @Override
    public boolean processCommandLineArgs(String[] cliArgs)
    {
      if (!super.processCommandLineArgs(cliArgs))
      {
        return false;
      }

      if (!processBootstrapOptions())
      {
        return false;
      }
      if (!processTables())
      {
        return false;
      }
      if (!processMetadataFilters())
      {
        return false;
      }
      if (!processEventFilters())
      {
        return false;
      }
      if (!processSchemasPath())
      {
        return false;
      }

      return true;
    }

    private boolean processTables()
    {
      if (_cmd.hasOption(SRCID_OPT_NAME))
      {
        try
        {
          _srcId = Short.parseShort(_cmd.getOptionValue(SRCID_OPT_NAME));
        }
        catch (NumberFormatException e)
        {
          printError("invalid logical source id: " + _cmd.getOptionValue(SRCID_OPT_NAME), false);
          return false;
        }
      }
      if (_cmd.hasOption(SRCNAME_OPT_NAME))
      {
        //TODO add support for mapping a logical source name to an id
        printError("option not supported yet: " + SRCNAME_OPT_NAME, false);
        return false;
      }
      if (_srcId <= 0)
      {
        printError("no logical source specified", true);
        return false;
      }
      if (_cmd.hasOption(WITH_SNAPSHOT_OPT_CHAR))
      {
        _tableNames.add("tab_" + _srcId);
      }
      if (_cmd.hasOption(WITH_LOG_OPT_CHAR))
      {
        int minLogId = -1;
        int maxLogId = -1;

        String logOpt = _cmd.getOptionValue(WITH_LOG_OPT_CHAR);
        if (null != logOpt)
        {
          int commaPos = logOpt.indexOf(',');
          try
          {
            if (-1 == commaPos)
            {
              minLogId = Integer.parseInt(logOpt);
              maxLogId = minLogId;
            }
            else
            {
              if (commaPos > 0)
              {
                minLogId = Integer.parseInt(logOpt.substring(0, commaPos));
              }
              if (commaPos < logOpt.length() - 1)
              {
                maxLogId = Integer.parseInt(logOpt.substring(commaPos + 1));
              }
            }
          }
          catch (NumberFormatException e)
          {
            printError("invalid log table number(s): " + logOpt, true);
            return false;
          }
        }

        if (0 > minLogId || 0 > maxLogId)
        {
          //TODO add the ability to figure out the oldest and/or newest log table
          printError("log table number wildcards are not supported yet", false);
          return false;
        }

        for (int lid = minLogId; lid <= maxLogId; ++lid)
        {
          _tableNames.add("log_" + _srcId + "_" + lid);
        }
      }

      if (0 == _tableNames.size())
      {
        printError("At least one of --" + WITH_SNAPSHOT_OPT_NAME + " or --" + WITH_LOG_OPT_NAME +
                   " is required.", true);
        return false;
      }

      return true;
    }

    private boolean processEventFilters()
    {
      if (_cmd.hasOption(PAYLOAD_COND_OPT_NAME))
      {
        for (String cond: _cmd.getOptionValues(PAYLOAD_COND_OPT_NAME))
        {
          if (!processEventFilter(cond))
          {
            return false;
          }
        }
      }
      return true;
    }

    private boolean processEventFilter(String cond)
    {
      Matcher m  = PAYLOAD_COND_REGEX.matcher(cond);
      if (! m.matches())
      {
        printError("invalid payload predicate: " + cond, true);
        return false;
      }
      //try to guess the type of the constant
      //TODO make this smarter -- inspect the schema
      String constStr = m.group(3);
      Object constArg = constStr;
      Long constLong = null;
      Double constDouble = null;
      try
      {
        constLong = Long.parseLong(constStr);
        constArg = constLong;
      }
      catch (NumberFormatException e)
      {
        //try next type
      }
      if (null == constLong)
      {
        try
        {
          constDouble = Double.parseDouble(constStr);
          constArg = constDouble;
        }
        catch (NumberFormatException e)
        {
          //use string
        }
      }
      String oper = m.group(2);
      if ("<".equals(oper))
      {
        BootstrapReaderFilter f = new PayloadFieldLtFilter(m.group(1), constArg);
        _eventFilters.add(f);
      }
      else if (">".equals(oper))
      {
        BootstrapReaderFilter f = new PayloadFieldGtFilter(m.group(1), constArg);
        _eventFilters.add(f);
      }
      else if ("==".equals(oper))
      {
        BootstrapReaderFilter f = new PayloadFieldEqFilter(m.group(1), constArg);
        _eventFilters.add(f);
      }
      else
      {
        printError("unsupported relationship: " + oper, false);
        return false;
      }
      //TODO add column name validation
      return true;
    }

    private boolean processMetadataFilters()
    {
      if (_cmd.hasOption(KEYS_OPT_NAME))
      {
        String keysOpt = _cmd.getOptionValue(KEYS_OPT_NAME);
        String[] optParts = keysOpt.split(",");
        if (optParts.length > 2)
        {
          printError("invalid key range specifier:" + keysOpt, true);
          return false;
        }
        if (optParts[0].length() > 0)
        {
          _metaBuilder.minKey(optParts[0]);
        }
        if (2 == optParts.length && optParts[1].length() > 0)
        {
          _metaBuilder.maxKey(optParts[1]);
        }
      }

      if (_cmd.hasOption(SCNS_OPT_NAME))
      {
        String scnsOpt = _cmd.getOptionValue(SCNS_OPT_NAME);
        String[] optParts = scnsOpt.split(",");
        if (optParts.length > 2)
        {
          printError("invalid SCN range specifier:" + scnsOpt, true);
          return false;
        }
        try
        {
          if (optParts[0].length() > 0)
          {
            _metaBuilder.minScn(Long.parseLong(optParts[0]));
          }
          if (2 == optParts.length && optParts[1].length() > 0)
          {
            _metaBuilder.maxScn(Long.parseLong(optParts[1]));
          }
        }
        catch (NumberFormatException e)
        {
          printError("invalid SCN range specifier:" + scnsOpt, true);
          return false;
        }
      }

      return true;
    }

    private boolean processBootstrapOptions()
    {
       if (_cmd.hasOption(BST_DBNAME_OPT_CHAR))
       {
         _bstCfg.setBootstrapDBName(_cmd.getOptionValue(BST_DBNAME_OPT_CHAR));
       }
       if (_cmd.hasOption(MYSQL_HOST_OPT_CHAR))
       {
         _bstCfg.setBootstrapDBHostname(_cmd.getOptionValue(MYSQL_HOST_OPT_CHAR));
       }
       if (_cmd.hasOption(BST_USER_OPT_CHAR))
       {
         _bstCfg.setBootstrapDBUsername(_cmd.getOptionValue(BST_USER_OPT_CHAR));
       }
       if (_cmd.hasOption(BST_PASSWORD_OPT_CHAR))
       {
         String pwd = _cmd.getOptionValue(BST_PASSWORD_OPT_CHAR);
         if (null == pwd)
         {
           Console console = System.console();
           if (null == console)
           {
             printError("no password specified", true);
             return false;
           }
           char[] pwdChars = console.readPassword("Enter bootstrap MySQL password:");
           if (null == pwdChars)
           {
             printError("no password specified", true);
             return false;
           }
           pwd = new String(pwdChars);
         }
         _bstCfg.setBootstrapDBPassword(pwd);
       }
       return true;
    }

    public boolean processSchemasPath()
    {
      if (_cmd.hasOption(SCHEMAS_PATH_OPT_NAME))
      {
        _schemaSetProvider =
            new FileSystemVersionedSchemaSetProvider(
                new File(_cmd.getOptionValue(SCHEMAS_PATH_OPT_NAME)));
      }
      return true;
    }

    public BootstrapReadOnlyConfig getBstCfg() throws InvalidConfigException
    {
      return _bstCfg.build();
    }

    public MetaDataFilters getMetadataFilters()
    {
      return _metaBuilder.build();
    }

    public List<String> getTableNames()
    {
      return Collections.unmodifiableList(_tableNames);
    }

    public List<BootstrapReaderFilter> getEventFilters()
    {
      return _eventFilters;
    }

    public VersionedSchemaSet getSchemaSet()
    {
      return _schemaSetProvider.loadSchemas();
    }
  }

  public static void main(String[] args) throws Exception
  {
    Cli cli = new Cli(null);
    if (!cli.processCommandLineArgs(args))
    {
      System.exit(1);
    }

    BootstrapReaderEventHandler eventHandler = new BootstrapDBReader.DumpEventHandler();

    for (String table: cli.getTableNames())
    {
      LOG.info("scanning table " + table);
      BootstrapTableReaderV2 reader = new
          BootstrapTableReaderV2(table, cli.getMetadataFilters(), cli.getSchemaSet(), eventHandler,
                                  cli.getEventFilters(), cli.getBstCfg(), null);
      reader.execute();
    }
  }

}
