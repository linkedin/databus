package com.linkedin.databus.util;
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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;

import com.linkedin.databus.core.BaseCli;

/**
 * Command-line interface for {@link InteractiveSchemaGenerator}
 * <preformat>
 * usage: java com.linkedin.databus.util.InteractiveSchemaGenerator [options]
 * -A,--automatic                   Answer all questions proved through cli automatically and interactive for the rest of the options.
 * -b,--database <db_name>          the name of database
 * -D,--dburl <url>                 DB URL to read view/table definitions
 * -f,--fields <fields>             comma-separated list of table/view fields to include in the schema
 * -h,--help                        Prints command-line options info
 * -k,--pk <keys>                   comma-separated list of primary keys of view/table
 * -l,--log_props <property_file>   Log4j properties to use
 * -p,--password <password>         DB password
 * -q,--quiet                       turn off logging
 * -S,--schema_reg_path <path>      path where to checkout the schema registry
 * -t,--table <table_name>          name of the table/view whose schema to generate
 * -u,--username <user>             DB username
 * -v                               verbose
 * -vv                              more verbose
 * -vvv                             most verbose
 * </preformat>
 */
public class InteractiveSchemaGeneratorCli extends BaseCli
{
  private static char AUTOMATIC_OPT_CHAR = 'A';
  private static char DBNAME_OPT_CHAR = 'b';
  private static char DBURL_OPT_CHAR = 'D';
  private static char FIELDS_OPT_CHAR = 'f';
  private static char PK_OPT_CHAR = 'k';
  private static char PASSWORD_OPT_CHAR = 'p';
  private static char TABLE_OPT_CHAR = 't';
  private static char USER_OPT_CHAR = 'u';
  private static char SCHEMA_REGISTRY_PATH_OPT_CHAR = 'S';
  private static char NUMBEROVERRIDE_OPT_CHAR = 'N';


  private static String AUTOMATIC_OPT_NAME = "automatic";
  private static String DBNAME_OPT_NAME = "database";
  private static String DBURL_OPT_NAME = "dburl";
  private static String FIELDS_OPT_NAME = "fields";
  private static String PASSWORD_OPT_NAME = "password";
  private static String PK_OPT_NAME = "pk";
  private static String TABLE_OPT_NAME = "table";
  private static String USER_OPT_NAME = "username";
  private static String SCHEMA_REGISTRY_PATH_OPT_NAME = "schema_reg_path";
  private static String NUMBEROVERRIDE_OPT_NAME = "number_override_map";

  private boolean _automatic = Boolean.FALSE;
  private String _dbName;
  private String _dburl = InteractiveSchemaGenerator.DEFAULT_DATABASE;
  private List<String> _fields;
  private String _password;
  private List<String> _primaryKeys;
  private String _schemaRegPath = InteractiveSchemaGenerator.DEFAULT_SCHEMA_REGISTRY_LOCATION;
  private String _table;
  private String _user;
  private HashMap<String,String> _dbFieldToAvroDataType;

  public InteractiveSchemaGeneratorCli()
  {
    super(createDefaultUsageString(InteractiveSchemaGenerator.class), null);
  }

  @SuppressWarnings("static-access")
  @Override
  protected void constructCommandLineOptions()
  {
    super.constructCommandLineOptions();

    Option automaticOption =
        OptionBuilder.withLongOpt(AUTOMATIC_OPT_NAME)
                     .withDescription("Answer all questions proved through cli automatically and interactive for the rest of the options.")
                     .create(AUTOMATIC_OPT_CHAR);
    Option dbnameOption =
        OptionBuilder.withLongOpt(DBNAME_OPT_NAME)
                     .hasArg()
                     .withArgName("db_name")
                     .withDescription("The name of database")
                     .create(DBNAME_OPT_CHAR);
    Option dburlOption =
        OptionBuilder.withLongOpt(DBURL_OPT_NAME)
                     .hasArg()
                     .withArgName("url")
                     .withDescription("DB URL to read view/table definitions")
                     .create(DBURL_OPT_CHAR);
    Option fieldsOption =
        OptionBuilder.withLongOpt(FIELDS_OPT_NAME)
                     .hasArg()
                     .withArgName("fields")
                     .withDescription("Comma-separated list of table/view fields to include in the schema")
                     .create(FIELDS_OPT_CHAR);
    Option passwordOption =
        OptionBuilder.withLongOpt(PASSWORD_OPT_NAME)
                     .hasArg()
                     .withArgName("password")
                     .withDescription("DB password")
                     .create(PASSWORD_OPT_CHAR);
    Option pkOption =
        OptionBuilder.withLongOpt(PK_OPT_NAME)
                     .hasArg()
                     .withArgName("keys")
                     .withDescription("Comma-separated list of primary keys of view/table")
                     .create(PK_OPT_CHAR);
    Option tableOption =
        OptionBuilder.withLongOpt(TABLE_OPT_NAME)
                     .hasArg()
                     .withArgName("table_name")
                     .withDescription("The name of the table/view whose schema to generate ")
                     .create(TABLE_OPT_CHAR);
    Option userOption =
        OptionBuilder.withLongOpt(USER_OPT_NAME)
                     .hasArg()
                     .withArgName("user")
                     .withDescription("DB username")
                     .create(USER_OPT_CHAR);
    Option schemaRegPathOption =
        OptionBuilder.withLongOpt(SCHEMA_REGISTRY_PATH_OPT_NAME)
                     .hasArg()
                     .withArgName("path")
                     .withDescription("The path where to checkout the schema registry")
                     .create(SCHEMA_REGISTRY_PATH_OPT_CHAR);

    Option numberOverrideOption =
        OptionBuilder.withLongOpt(NUMBEROVERRIDE_OPT_NAME)
            .hasArg()
            .withArgName("path")
            .withDescription("Override number fields datatype with FLOAT, LONG, INTEGER, DOUBLE. Input as, DB_FIELD_NAME1=FLOAT,DB_FIELD_NAME2=DOUBLE")
            .create(NUMBEROVERRIDE_OPT_CHAR);

    _cliOptions.addOption(automaticOption);
    _cliOptions.addOption(dbnameOption);
    _cliOptions.addOption(dburlOption);
    _cliOptions.addOption(fieldsOption);
    _cliOptions.addOption(passwordOption);
    _cliOptions.addOption(pkOption);
    _cliOptions.addOption(tableOption);
    _cliOptions.addOption(userOption);
    _cliOptions.addOption(schemaRegPathOption);
    _cliOptions.addOption(numberOverrideOption);
  }

  @Override
  public boolean processCommandLineArgs(String[] cliArgs)
  {
    boolean success = super.processCommandLineArgs(cliArgs);
    if (!success)
    {
      return false;
    }

    _automatic = _cmd.hasOption(AUTOMATIC_OPT_CHAR);

    if (_cmd.hasOption(DBNAME_OPT_CHAR))
    {
      _dbName = _cmd.getOptionValue(DBNAME_OPT_CHAR).trim();
    }

    if (_cmd.hasOption(DBURL_OPT_CHAR))
    {
      _dburl = _cmd.getOptionValue(DBURL_OPT_CHAR).trim();
    }

    if (_cmd.hasOption(FIELDS_OPT_CHAR))
    {
      String fieldsStr = _cmd.getOptionValue(FIELDS_OPT_CHAR).trim();
      String[] fields = fieldsStr.split("[, ]+");
      _fields = Collections.unmodifiableList(Arrays.asList(fields));
    }

    if (_cmd.hasOption(PASSWORD_OPT_CHAR))
    {
      _password = _cmd.getOptionValue(PASSWORD_OPT_CHAR);
    }

    if (_cmd.hasOption(PK_OPT_CHAR))
    {
      String pkStr = _cmd.getOptionValue(PK_OPT_CHAR).trim();
      String[] pks = pkStr.split("[, ]+");
      _primaryKeys = Collections.unmodifiableList(Arrays.asList(pks));
    }

    if (_cmd.hasOption(TABLE_OPT_CHAR))
    {
      _table = _cmd.getOptionValue(TABLE_OPT_CHAR).trim();
    }

    if (_cmd.hasOption(USER_OPT_CHAR))
    {
      _user = _cmd.getOptionValue(USER_OPT_CHAR).trim();
    }

    if (_cmd.hasOption(SCHEMA_REGISTRY_PATH_OPT_CHAR))
    {
      _schemaRegPath = _cmd.getOptionValue(SCHEMA_REGISTRY_PATH_OPT_CHAR).trim();
    }

    if(_cmd.hasOption(NUMBEROVERRIDE_OPT_CHAR))
    {
      _dbFieldToAvroDataType = new HashMap<String, String>();
      String mapList = _cmd.getOptionValue(NUMBEROVERRIDE_OPT_CHAR).trim();
      String[] mapElements = mapList.split(",");
      for(String mapElement : mapElements)
      {
        String[] dbFieldToDatatype = mapElement.split("=");
        _dbFieldToAvroDataType.put(dbFieldToDatatype[0].trim(), dbFieldToDatatype[1].trim());
      }
    }

    return true;
  }

  /** For testing */
  public static void main(String[] args)
  {

    for(int i=0;i <args.length ; i++)
      System.out.println("Arg["+i+"] = " + args[i]);

    String[] finalArgs;
    if(args.length > 1 && args[0].equals("-c")) //Python drive is going to pass all args in a "-c"
    {
      finalArgs = args[1].split("\\s");
    }
    else
    {
      finalArgs = args;
    }

    InteractiveSchemaGeneratorCli cli = new InteractiveSchemaGeneratorCli();
    if(!cli.processCommandLineArgs(finalArgs))
        return;
    try
    {
      System.out.println("Starting the schema generation with cli options: "+ cli.toString());
      InteractiveSchemaGenerator automaticSchemaGeneration = new InteractiveSchemaGenerator(cli);
      automaticSchemaGeneration.runSchemaGenTool();
    }
    catch (Exception e)
    {
      System.out.println("Error running the schema generation tool");
      e.printStackTrace();
    }
  }

  /** The path to the schema registry */
  public String getSchemaRegistryPath()
  {
    return _schemaRegPath;
  }

  /** Whether all questions should be answered automatically */
  public boolean isAutomatic()
  {
    return _automatic;
  }

  /** The db url to read table/view definitions */
  public String getDburl()
  {
    return _dburl;
  }

  /** DB user name */
  public String getUser()
  {
    return _user;
  }

  /** DB password */
  public String getPassword()
  {
    return _password;
  }

  /** The database/Oracle schema name*/
  public String getDbName()
  {
    return _dbName;
  }

  /** Table/view name */
  public String getTableName()
  {
    return _table;
  }

  /** Primary key(s) of the table/view*/
  public List<String> getPrimaryKeys()
  {
    return _primaryKeys;
  }

  /** List of table/view fields to include in the generated schema */
  public List<String> getFields()
  {
    return _fields;
  }

  @Override
  public String toString()
  {
    return "InteractiveSchemaGeneratorCli{" +
        "_automatic=" + _automatic +
        ", _dbName='" + _dbName + '\'' +
        ", _dburl='" + _dburl + '\'' +
        ", _fields=" + _fields +
        ", _password='" + _password + '\'' +
        ", _primaryKeys=" + _primaryKeys +
        ", _schemaRegPath='" + _schemaRegPath + '\'' +
        ", _table='" + _table + '\'' +
        ", _user='" + _user + '\'' +
        ", _dbFieldToAvroDataType=" + _dbFieldToAvroDataType +
        '}';
  }

  public HashMap<String, String> getDbFieldToAvroDataType()
  {
    return _dbFieldToAvroDataType;
  }

}
