package com.linkedin.databus.util;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import jline.Completor;
import jline.ConsoleReader;
import jline.SimpleCompletor;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificCompiler;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;

import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.schemas.FileSystemSchemaRegistryService;
import com.linkedin.databus2.schemas.utils.SchemaHelper;
import com.linkedin.databus2.util.DBHelper;

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


public class InteractiveSchemaGenerator
{
  //The jdbc driver to use
  private static final String[] DEFAULT_JDBC_DRIVERS = {"oracle.jdbc.driver.OracleDriver"};
  //The default oracle jdbc uri
  protected static final String DEFAULT_DATABASE = "jdbc:oracle:thin:@devdb:1521:db";
  //The default location of the of the the schema registry
  protected static final String DEFAULT_SCHEMA_REGISTRY_LOCATION = "/export/content/data/databus2_events";
  //location of svn databus_events
  private static final String DEFAULT_SCHEMA_REGISTRY_SVN_LOCATION = "svn+ssh://svn.corp.linkedin.com/netrepo/databus-events/trunk/databus-events";
  //The default location of the of the the relay configs
  private static final String DEFAULT_RELAY_CONFIG_LOCATION = "/export/content/data/databus2_relay_sources/";
  //path to svn
  private static final String PATH_TO_SVN = "/export/apps/xtools/bin/svn";
  //Namespace prefix to the schemas
  private static final String NAMESPACE_PREFIX = "com.linkedin.events";
  //Suffix for databus-events avro specific schema
  private static final String DATABUS_EVENTS_SUFFIX = "/databus-events/src/main/java/";
  //Suffix for databus-events generic records
  private static final String DATABUS_SCHEMAS_SUFFIX = "/schemas_registry/";
  //Options and help for schema generator tools
  private static final String[] GEN_SCHEMA_OPTIONS = new String[]{"GENSCHEMA","CHANGETAB","RESET","GENRELAYCONFIG","EXIT"};
  private static final String[] GEN_SCHEMA_HELP = new String[]{
      "GENSCHEMA - This option is used to generate the avro schema (used by databus) for the table you have selected. The schemas can be generated for both existing table/view or new table/view using this option." +
          ". If you would like to generate the schema for a different view/table than what you have selected, change the table using CHANGETAB option",
      "CHANGETAB -  Use this option if you would like to change the database and table/view you have selected for schema generation",
      "RESET - This option clears all the uncommitted changes from the schema registry (****Please note this option deletes the schema registry and checks out a fresh copy, you might lose any schemas that you have generated, if you have not committed them*****)",
      "GENRELAYCONFIG - This option is used to generate the relay configs for the current selected table/view. This config generated can be used to deploy the relay in the dev environment for the current table/view.",
      "EXIT - Exit the schema registry tool"
  };
  //Index for the schema registry
  private final String SCHEMAREGISTRY_INDEX = "index.schemas_registry";
  //Schema registry service
  private FileSystemSchemaRegistryService _schemaRegistryService;
  //Are we filtering any fields while generating schema
  private boolean _areFieldsFiltered = true;

  //The current state of the console reader
  private enum State{
    INITIATECONN,
    TABLEINFO,
    SCHEMA_MANIPULATION,
    GENERATE_RELAY_SRC_CONFIGS,
    EXIT
  }

  //The current reader the console reader is at
  private State currentState;
  boolean _verbose = false;
  //The uri for the database
  String _database;
  //The schema for the current database
  String _schemaName;
  //Login username
  String _userName;
  //Login password
  String _password;
  //The table name to generate schema for
  String _tableName;
  //The location where the schema registry(databus_events) is located at.
  String _schemaRegistryLocation;
  //The connection to the database
  Connection _conn;
  //The primary keys for the table
  List<String> _primaryKeys = null;
  //The list of fields the user wants to generate the fields
  List<String> _userFields = null;
  //The autocompletor in the current context
  Completor _currentCompletor = null;
  //console reader
  ConsoleReader _reader = null;
  //Regex to remove the version from the end of the view name
  Pattern _versionRegexPattern = Pattern.compile("(.*)_\\d+");
  //Map holds field to avrodatatype (only use in cli)
  HashMap<String,String> _dbFieldToAvroDataType;
  //Is the schemagenerator on automatic mode ? True = no user questions, excepts everything from cli.
  boolean _automatic;

  public SchemaMetaDataManager _manager = null;

  public InteractiveSchemaGenerator()
  {
    currentState = State.INITIATECONN;
    loadJdbcDrivers();
    _verbose = false;
  }

  @Override
  public String toString()
  {
    return "InteractiveSchemaGenerator{" +
        "SCHEMAREGISTRY_INDEX='" + SCHEMAREGISTRY_INDEX + '\'' +
        ", _schemaRegistryService=" + _schemaRegistryService +
        ", _database='" + _database + '\'' +
        ", _schemaName='" + _schemaName + '\'' +
        ", _userName='" + _userName + '\'' +
        ", _password='" + _password + '\'' +
        ", _tableName='" + _tableName + '\'' +
        ", _schemaRegistryLocation='" + _schemaRegistryLocation + '\'' +
        ", _conn=" + _conn +
        ", _primaryKeys=" + _primaryKeys +
        ", _userFields=" + _userFields +
        ", _dbFieldToAvroDataType=" + _dbFieldToAvroDataType +
        ", _automatic=" + _automatic +
        ", _manager=" + _manager +
        ", currentState=" + currentState +
        '}';
  }

  public InteractiveSchemaGenerator(InteractiveSchemaGeneratorCli cli)
      throws IOException, SQLException, DatabusException
  {
    _database = cli.getDburl();
    _userName = cli.getUser();
    _password = cli.getPassword();
    _schemaName = cli.getDbName();
    _tableName = cli.getTableName();
    _schemaRegistryLocation = cli.getSchemaRegistryPath();
    verifyAndCheckoutSchemaRegistry(_schemaRegistryLocation);
    _manager = new SchemaMetaDataManager(_schemaRegistryLocation + "/schemas_registry");
    _userFields = cli.getFields();
    _primaryKeys = cli.getPrimaryKeys();
    _dbFieldToAvroDataType = cli.getDbFieldToAvroDataType();
    loadSchemaRegistry();
    _conn =  getConnection();
    _automatic = cli.isAutomatic();
    File tempFile = new File(_schemaRegistryLocation);
    if(!tempFile.exists())
    {
      System.out.println("The is no directory at " + _schemaRegistryLocation + ". Attempting to checkout a new copy at this location..");
      ProcessBuilder pb = new ProcessBuilder(PATH_TO_SVN,"checkout",DEFAULT_SCHEMA_REGISTRY_SVN_LOCATION,_schemaRegistryLocation);
      if(executeProcessBuilder(pb) != 0)
        throw new DatabusException("Unable to checkout the code in the given directory");
    }
  }


  public void run()
      throws Exception
  {
    printWelcomeMessage();
    _reader = new ConsoleReader();
    _reader.setDefaultPrompt("SchemaGen>");
    _reader.setBellEnabled(false);
    processInput();

  }

  /**
   * prints the welcome message
   */
  private void printWelcomeMessage(){
    System.out.println("Welcome to Databus for Oracle - schema generation tool.");
  }

  /**
   * Reads the user input from the console reader and processes.
   * @throws Exception
   */
  private void processInput()
      throws Exception
  {
    boolean done = false;
    while(!done)
    {
      switch(currentState)
      {
        case INITIATECONN:
        {
          System.out.println("Enter the location of the schema registry (Hit enter to checkout out a new copy at [" + DEFAULT_SCHEMA_REGISTRY_LOCATION + "] and use it):");
          String line = checkAndRead();
          if(line.equals(""))
          {
            _schemaRegistryLocation = DEFAULT_SCHEMA_REGISTRY_LOCATION;
          }
          else
          {
            _schemaRegistryLocation = line;
          }

          if(!verifyAndCheckoutSchemaRegistry(_schemaRegistryLocation))
          {
            System.out.println("Unable to checkout the schema registry at the ["+ _schemaRegistryLocation + "], please retry..");
            continue;
          }


          //Load the schema registry
          loadSchemaRegistry();

          _manager = new SchemaMetaDataManager(_schemaRegistryLocation + "/schemas_registry");

          System.out.println("Using Database url : [ " + DEFAULT_DATABASE +  " ] to connect to DB (Hit enter to use this, or enter a valid database string): ");
          _database = DEFAULT_DATABASE;
          line = checkAndRead();
          if(!line.isEmpty())
          {
            _database = line;
            System.out.println("Overriding default Database url with: [" + _database +  "]");
          }

          System.out.println("Enter the username for the database:");
          _userName = checkAndRead();
          System.out.println("Enter the password for the database:");
          _password = checkAndRead();
          System.out.println("Attempting to connect with the database..");
          try
          {
            _conn = getConnection();
          }
          catch (SQLException e)
          {
            System.out.println("ERROR: Unable to connect with the db with the given credentials, please try again: " + e.toString());
            continue;
          }
          currentState = State.TABLEINFO;
        }
        break;
        case TABLEINFO:
        {
          _schemaName = _userName.toUpperCase(Locale.ENGLISH);
          System.out.println("Enter the Oracle DB name (Hit enter to use [ " + _schemaName + " ] as the DB name):");
          String line = checkAndRead();
          if(!line.isEmpty())
            _schemaName = line;

          List <String> tablesList = null;
          tablesList = getTablesInDB();
          tablesList.addAll(getViewsInDB());
          addListToCompletor(tablesList);
          System.out.println("Enter the name of table/view you would like to generate the schema for (use tab to autocomplete table names)):");
          _tableName = checkAndRead();


          if(!isValidSchema())
          {
            System.out.println("The schema [" + _schemaName + "] doesn't appear to be a valid schema");
            continue;
          }

          if(!isValidTable() && !isValidView())
          {
            System.out.println("This table [" + _tableName +  "] doesn't appear to be valid table or view, please retry");
            continue;
          }


          System.out.println("Attempting to identify primary keys..");
          _primaryKeys = getPrimaryKeys();
          for (String key : _primaryKeys)
            System.out.println(key);

          if(_primaryKeys.size() > 0)
            System.out.println ("If you would like to use the above as primary key(s) hit enter, or enter a comma separated list of fields to use as primary keys:");
          else
            System.out.println("Couldn't identify primary keys, enter a comma separated list of fields to use as primary keys:");

          String pkList = checkAndRead();

          if(!pkList.isEmpty())
          {
            System.out.println("Overriding default primary keys with: ");
            _primaryKeys = fieldToList(pkList);
            if(_primaryKeys== null)
            {
              System.out.println("You input does not have valid primary keys, please retry");
              continue;
            }

            System.out.println(_primaryKeys);
          }


          System.out.println("Using " + _primaryKeys.toString() + " as primary key(s)");
          System.out.println("Enter a comma separated list of fields you would like to include in the avro schema for databus (Use tab to autocomplete), [Hit enter to use all fields in the table]: ");
          List<String> fieldsInTable = getFieldsInTable(_tableName);
          addListToCompletor(fieldsInTable);
          String fieldList = checkAndRead();
          if(fieldList.isEmpty())
          {
            _userFields = fieldsInTable;
            _areFieldsFiltered = false;
          }
          else
          {
            _userFields = fieldToList(fieldList);
            _areFieldsFiltered = true;
          }

          if(_userFields == null)
            continue;

          if(_userFields.size() == 0)
            System.out.println("You have not selected any fields! Please retry");
          else
          {
            System.out.println("These are the chosen fields: ");
            for(String field : _userFields)
              System.out.println(field);
          }

          removeCurrentCompletor();
          currentState = State.SCHEMA_MANIPULATION;
        }
        break;
        case SCHEMA_MANIPULATION:
        {

          printSchemaManipulationHelp();
          addArrayToCompletor(GEN_SCHEMA_OPTIONS);
          String line = checkAndRead();
          invokeSchemaFunctions(line);
        }
        break;
        case EXIT:
          System.out.println("Schema generation is complete");
          done = true;
          break;
        default:
          throw new Exception("Undefined state!");
      }

    }
  }

  /**
   * JLine helper methods
   * ===================================================================================
   *
   */

  /**
   * Read and return a valid string input
   * @return the current line read
   * @throws IOException
   */
  private String checkAndRead()
      throws IOException
  {
    String line;
    if((line = _reader.readLine()) == null)
    {
      System.out.println("Unable to read a valid input from the console");
      return null;
    }

    return line.trim();
  }


  /**
   * Add a list to autocomplete
   * @param completorList  The list to add to autocomplete
   */
  private void addListToCompletor(List<String> completorList){

    if(completorList == null || completorList.size() == 0)
      return;

    String completorArray[] = completorList.toArray(new String[completorList.size()]);
    addArrayToCompletor(completorArray);
  }

  /**
   * Add a array to autocomplete
   * @param completorArray The array to add to autocomplete
   */
  private void addArrayToCompletor(String[] completorArray)
  {
    removeCurrentCompletor();

    if(completorArray == null || completorArray.length ==0)
      return;

    _currentCompletor = new SimpleCompletor(completorArray);
    _reader.addCompletor(_currentCompletor);
  }

  /**
   * Remove the current auto complete list
   */
  private void removeCurrentCompletor()
  {
    if(_currentCompletor != null)
      _reader.removeCompletor(_currentCompletor);
    _currentCompletor = null;
  }



  /**
   * JDBC specific methods
   * ===================================================================================
   */

  /**
   * Load the jdbc driver
   */
  public void loadJdbcDrivers()
  {

    for(String driver : DEFAULT_JDBC_DRIVERS)
    {
      try
      {
        Class.forName(driver);
      }
      catch(ClassNotFoundException ex)
      {
        System.out.println("Could not load JDBC driver: " + driver);
      }
    }
  }

  /**
   * Connect to the database with the username/password.
   * @return Return the new connection object
   * @throws SQLException
   */
  public Connection getConnection()
      throws SQLException
  {
    try
    {
      if(_verbose)
      {
        System.out.println("Connecting to database: " + _database);
      }
      Connection con = DriverManager.getConnection(_database, _userName, _password);
      if(_verbose)
      {
        System.out.println("Connected successfully.");
      }

      return con;
    }
    catch(SQLException ex)
    {
      System.out.println("Could not connect to database: " + _database);
      System.out.println(ex.getMessage());
      if(_verbose)
      {
        ex.printStackTrace();
      }
      throw ex;
    }
  }




  /**
   * Table specific methods
   * ===================================================================================
   */

  /**
   * checks if the current table is a valid table in the given schema
   * @return true if valid table, false otherwise
   * @throws SQLException
   */
  private boolean isValidTable()
  {

    PreparedStatement preparedStatement = null;
    ResultSet rs = null;
    try{
      preparedStatement = _conn.prepareStatement("select table_name from user_tables where table_name=?");
      preparedStatement.setString(1,_tableName.toUpperCase(Locale.ENGLISH));
      rs =  preparedStatement.executeQuery();
      if(rs.next())
      {
        return true;
      }
    }
    catch(SQLException e)
    {
      System.out.println("ERROR: Unable to determine if it's a valid table: " + e.toString());
      return false;
    }
    finally
    {
      cleanUpConnMeta(rs,preparedStatement);
    }
    return false;
  }

  private boolean isValidView()
  {
    PreparedStatement preparedStatement = null;
    ResultSet rs = null;
    try{
    preparedStatement = _conn.prepareStatement("select view_name from user_views where view_name=?");
    preparedStatement.setString(1,_tableName.toUpperCase(Locale.ENGLISH));
    rs = preparedStatement.executeQuery();
    if(rs.next())
      return true;
    }
    catch (SQLException e)
    {
      System.out.println("ERROR: Unable to determine if it's a valid view: " + e.toString());
    }
    finally
    {
      cleanUpConnMeta(rs,preparedStatement);
    }

    return false;
  }


  /**
   * Checks if the schema is valid
   * @return true if valid schema, false otherwise
   */
  private boolean isValidSchema()
  {
    ResultSet rs = null;
    PreparedStatement statement = null;

    try
    {
      final String query = String.format("select count(*) as count from %s.%s", _schemaName, _tableName);
      statement = _conn.prepareStatement(query);
      rs = statement.executeQuery();
      if(rs.next() && rs.getInt("count") >=0)
      {
        return true;
      }
    }
    catch (SQLException e)
    {
      System.out.println("ERROR: Unable to determine if it's a valid schema : " + e.toString());
      return false;
    }
    finally
    {
      cleanUpConnMeta(rs,statement);
    }

    return true;
  }

  /**
   * Checks if the field is present in the table
   * @param field  The field to check if it's valid
   * @return
   */
  private boolean isValidField(String field)
  {
    PreparedStatement preparedStatement = null;
    ResultSet rs = null;
    try
    {
      preparedStatement = _conn.prepareStatement("select count(1) as count from all_tab_cols where OWNER=? AND TABLE_NAME=? AND COLUMN_NAME=?");
      preparedStatement.setString(1,_schemaName);
      preparedStatement.setString(2,_tableName);
      preparedStatement.setString(3,field);
      rs = preparedStatement.executeQuery();
      return (rs.next() && rs.getInt("count") >0);
    }
    catch (SQLException e)
    {
      System.out.println("ERROR: Unable to determine if it's a valid field ( " + field +  "): " +  e.toString());
      return false;
    }
    finally
    {
      cleanUpConnMeta(rs,preparedStatement);
    }
  }


  /**
   * Get the tables in the current database
   * @return list of tables in the current db
   * @throws SQLException
   */
  private List<String> getTablesInDB()
  {
    PreparedStatement preparedStatement = null;  //Prepared statements to shutup find bugs
    ResultSet rs = null;

    ArrayList<String> list = new ArrayList<String>();
    try
    {
      preparedStatement = _conn.prepareStatement("select table_name from user_tables");
      rs = preparedStatement.executeQuery();
      while(rs.next())
      {
       list.add(rs.getString("table_name"));
      }
    }
    catch (SQLException e)
    {
      System.out.println("ERROR: Unable to fetch the table names in the database specified: " + e.toString());
    }
    finally
    {
      cleanUpConnMeta(rs,preparedStatement);
    }

    return list;
  }

  private List<String> getViewsInDB()
  {
    PreparedStatement preparedStatement = null;  //Prepared statements to shutup find bugs
    ResultSet rs = null;
    ArrayList<String> list = new ArrayList<String>();

    try{
    preparedStatement = _conn.prepareStatement("select view_name from user_views");
    rs = preparedStatement.executeQuery();
    while(rs.next())
    {
      list.add(rs.getString("view_name"));
    }
    }
    catch (SQLException e)
    {
      System.out.println("ERROR: Unable to fetch the view names in the database specified: " + e.toString());
    }
    finally{
     cleanUpConnMeta(rs,preparedStatement);
    }
    return list;
  }

  /**
   * Gets the primary key in the given table
   * @return list of primary keys
   */
  private List<String> getPrimaryKeys()
  {
    ArrayList<String> list = new ArrayList<String>();
    String primaryKeyQuery = "SELECT cols.column_name as primary_key FROM all_constraints cons, " +
        "all_cons_columns cols WHERE cols.table_name = ? AND cons.constraint_type = 'P' " +
        "AND cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner ORDER BY cols.position";
    PreparedStatement preparedStatement = null;
    ResultSet rs = null;
    try
    {
      preparedStatement = _conn.prepareStatement(primaryKeyQuery);
      preparedStatement.setString(1,_tableName);
      rs =  preparedStatement.executeQuery();
      while(rs.next())
        list.add(rs.getString("primary_key"));
    }
    catch (SQLException e)
    {
      System.out.println("Unable to determined the primary keys for the given table, this is expected if input was a view");
    }
    finally
    {
      cleanUpConnMeta(rs,preparedStatement);
    }
    return list;
  }

  private List<String> getFieldsInTable(String table)
  {
    ArrayList<String> list = new ArrayList<String>();
    String fieldsFetchQuery = "select COLUMN_NAME from all_tab_cols where OWNER=? AND table_name=?";
    PreparedStatement preparedStatement = null;
    ResultSet rs = null;
    try
    {
      preparedStatement = _conn.prepareStatement(fieldsFetchQuery);
      preparedStatement.setString(1,_schemaName);
      preparedStatement.setString(2,_tableName);
      rs = preparedStatement.executeQuery();
      while(rs.next())
        list.add(rs.getString("COLUMN_NAME"));
    }
    catch (SQLException e)
    {
      System.out.println("Unable to determine the fields from the given table: " + e.toString());
    }
    finally
    {
      cleanUpConnMeta(rs,preparedStatement);
    }
    return list;
  }

  /**
   * Convert a list of comma separated of fields to list of fields.
   * @param fieldList  The comma separated list of fields
   * @return A list of primary keys, null if the fieldList has an invalid field
   */
  private List<String> fieldToList(String fieldList)
  {
    String[] fieldArray = fieldList.split(",");
    ArrayList<String> fieldArrayList = new ArrayList<String>();


    if(fieldList.length() == 0)
      return null;

    for (String field : fieldArray) {
      if (!isValidField(field.trim()))
      {
        System.out.println("The field " + field + " is not a valid field in the table, please retry");
        return null;
      }

      fieldArrayList.add(field.trim().toUpperCase(Locale.ENGLISH));
    }

    return fieldArrayList;
  }


  private void cleanUpConnMeta(ResultSet rs, PreparedStatement preparedStatement)
  {
    DBHelper.close(rs);
    DBHelper.close(preparedStatement);
  }

  /**
   * Schema generation specific methods
   * ===================================================================================
   */

  /**
   * The method checks if there is schema registry directory existing in the location, if not, it will try to checkout a copy at the location specified
   * @param schemaRegistryLocation
   * @return True if a schema registry was checked out successfully or already exists (The function *only* checks if the directory exists). False, if not able to checkout a copy of schema registry at the location.
   */
  private boolean verifyAndCheckoutSchemaRegistry(String schemaRegistryLocation)
  {
    File tempFile = new File(schemaRegistryLocation);
    if(!tempFile.exists())
      if(!tempFile.mkdirs())
        return false;

    ProcessBuilder pb = new ProcessBuilder(PATH_TO_SVN,"checkout",DEFAULT_SCHEMA_REGISTRY_SVN_LOCATION,schemaRegistryLocation);
    return executeProcessBuilder(pb) == 0;
  }

  /**
   * Delete and checkout a fresh copy of the schema registry
   * @param schemaRegistryLocation The location of the schema registry
   * @return true - if successful operation, false otherwise
   */
  private boolean deleteAndCheckoutSchemaRegistry(String schemaRegistryLocation)
  {
    ProcessBuilder pb = new ProcessBuilder("/bin/rm","-rf", schemaRegistryLocation);
    if(executeProcessBuilder(pb) ==0)
      return verifyAndCheckoutSchemaRegistry(schemaRegistryLocation);
    else
      return false;
  }

  /**
   * Execute the process builder and print the output to the console
   * @param pb The process builder to execute
   * @return 0 on success, failure otherwise
   */
  private int executeProcessBuilder(ProcessBuilder pb)
  {
    int rc;
    System.out.println("Checking out the schema registry using svn..");
    Process process;
    InputStreamReader isr = null;
    BufferedReader br = null;
    try {
      process = pb.start();

      isr = new  InputStreamReader(process.getInputStream(),"UTF8");
      br = new BufferedReader(isr);

      String lineRead;
      while ((lineRead = br.readLine()) != null) {
        System.out.println(lineRead);
      }

      rc = process.waitFor();
      br.close();
      isr.close();
    } catch (IOException e) {
      System.out.println("Error while manipulating the schema registry: " + e.toString());
      return 1;
    } catch (InterruptedException e) {
      System.out.println("Error while manipulating the schema registry: " + e.toString());
      return 1;
    }
    finally
    {
      if(isr != null) try
      {
        isr.close();
      }
      catch (IOException e)
      {
        System.out.println("Error while manipulating the schema registry: " + e.toString());
      }
      if(br!=null) try
      {
        br.close();
      }
      catch (IOException e)
      {
        System.out.println("Error while manipulating the schema registry: " + e.toString());
      }
    }

    return rc;
  }

  private void printSchemaManipulationHelp()
  {
    System.out.println("Choose one of the operations you want to perform (Tab to autocomplete)");
    for (String helpOption : GEN_SCHEMA_HELP) {
      System.out.println(helpOption);
    }
  }

  private void invokeSchemaFunctions(String operation) throws IOException
  {
    operation = operation.trim();
    if(operation.equals(GEN_SCHEMA_OPTIONS[0]))
    {
      System.out.println("\n\n\n\n A new databus avro schema needs to be created only :");
      System.out.println("\n\t a) For a brand new Table/View for which databus schema does not exist.");
      System.out.println("\n\t b) When table/View schema changed and the latest databus schema does not reflect the table/view schema changes.");
      System.out.println("\n\n For Schema Generation needed for Mult-Colo, please look at https://iwww.corp.linkedin.com/wiki/cf/display/ENGS/Databus+GoldenGate+Client+Launches to verify if the table/View is already databusified.");
      System.out.println("\n\n If the schema already exists, you can look at " + _schemaRegistryLocation + "/schemas_registry to verify if the latest version of schema matches with the table/view  schema you expected.");
      if ( promptForYesOrNoQuestion("Are you sure you want to generate new schema "))
        runSchemaGenTool();
      System.out.println("\n\n");
    }
    else if(operation.equals(GEN_SCHEMA_OPTIONS[1]))
    {
      currentState = State.INITIATECONN;
      return;
    }
    else if(operation.equals(GEN_SCHEMA_OPTIONS[2]))
    {
      deleteAndCheckoutSchemaRegistry(_schemaRegistryLocation);
    }
    else if(operation.equals(GEN_SCHEMA_OPTIONS[3]))
    {
      boolean success = false;
      try
      {
        buildRelayConfigs();

        if (currentState != State.EXIT)
          success = true;
      }
      catch (Exception e)
      {
        System.out.println("Error while generating the relay configs: " + e);
      }
      if ( success )
        System.out.println("Relay config generation is complete");
      System.out.println("******************************************************************************************************************************************");
    }
   else if(operation.equals(GEN_SCHEMA_OPTIONS[4]))
    {
      currentState = State.EXIT;
    }
   else
    {
      System.out.println("Unknown option, please retry");
    }
  }



  public boolean runSchemaGenTool()
  {
    //TODO Things left to do:
    // 1. Determine if we need to bump up the version of schema, if yes, then throw an error and ask the user to use update schema
    // 2. Check if there is already an schema for this table
    // DONE ---- 3. Get the primary key list and confirm with user
    // DONE ---- 4. Modify the schema gen tool to generate schema with the new composite keys
    // 5. If the input type is number, verify from user the precision and scale

    if(_automatic)
      System.out.println("\n\n Starting the schema generation with the following settings: " + toString());

    System.out.println("\n\n Generating schema for " + _tableName);

    //Convert the list to a comma separted list
    StringBuilder pKeyList = new StringBuilder();
    for(int i=0 ;i <_primaryKeys.size() ; i++)
    {
      String avroName = SchemaUtils.toCamelCase(_primaryKeys.get(i));
      pKeyList.append(avroName);
      if(i != _primaryKeys.size() - 1)
        pKeyList.append(",");
    }

    try{
      TableTypeInfo ti = (TableTypeInfo) new TypeInfoFactoryInteractive().getTypeInfo(_conn, _schemaName, _tableName, 0, 0, pKeyList.toString(), _reader, _dbFieldToAvroDataType);

      String namespace = NAMESPACE_PREFIX + "." + _schemaName.toLowerCase(Locale.ENGLISH);

      if(!_automatic)
      {
        System.out.println("********************************************************************************************************");
        System.out.println("Hit enter to use the default namespace[" + namespace + "] or type the namespace you would like to use: ");
        String line = checkAndRead();
        if(!line.isEmpty())
        {
          namespace = line;
          System.out.println("Overriding the default namespace with [" + namespace +"]");
        }
      }
      else
      {
        System.out.println("Using the default namespace [" + namespace +"]");
      }


      String topRecordDatabaseName = _tableName;
      File avroOutDir = new File(_schemaRegistryLocation + DATABUS_SCHEMAS_SUFFIX);
      File javaOutDir = new File(_schemaRegistryLocation + DATABUS_EVENTS_SUFFIX);
      String recordName = _tableName.toLowerCase(Locale.ENGLISH);

      //Clean up record name if it's a view (Strip the SY$ from prefix, and _version from suffix)
      if(recordName.toUpperCase(Locale.ENGLISH).startsWith("SY$"))
      {
        recordName = recordName.substring(3);
        Matcher m = _versionRegexPattern.matcher(recordName);
        if(m.find())
        {
          recordName = m.group(1);
        }
      }
      recordName = SchemaUtils.toCamelCase(recordName,true);



      if(!_automatic)
      {
        System.out.println("User input for field datatypes complete.");
        System.out.println("********************************************************************************************************");
        System.out.println("Hit enter to use the default record name [" + recordName  + "] or type the record name you would like to use: ");
        String line = checkAndRead();
        if(!line.isEmpty())
        {
          recordName = line;
          System.out.println("Overriding the default record name with [" + recordName +"]");
        }
      }
      else
      {
        System.out.println("Using the recordname:  [" + recordName + "]");
      }

      //Figure out the avro version
      Integer avroOutVersion = getLatestVersionNumber(namespace + "." + recordName);
      if(avroOutVersion == -1)
        System.out.println("Unable to determine the schema version, cannot proceed with schema generation");

      avroOutVersion++;


      if(!_automatic)
      {
        System.out.println("Hit enter to use the schema version : " + avroOutVersion + " or type the version you would like to use: ");
        String line = checkAndRead();
        if(!line.isEmpty())
        {
          try{
            avroOutVersion = Integer.parseInt(line);
          }
          catch (NumberFormatException e)
          {
            System.out.println("Invalid input, unable to parse the version specified "+ e);
          }
          System.out.println("Overriding the default version with [" + avroOutVersion +"]");
        }
      }
      else
      {
        System.out.println("Using the avro version:  [" + avroOutVersion + "]");
      }

      String topRecordAvroName = recordName + "_V" + avroOutVersion.toString();

      // Get SrcId
      if(avroOutVersion == 1) //The first time the schema is created, we need to update some meta information for srcid mapping
      {
        String srcName = namespace + "." + recordName;
        _manager.updateAndGetNewSrcId(_schemaName, srcName);
      }

      //Generate the avro record
      FieldToAvro fa = new FieldToAvro();
      String schema = fa.buildAvroSchema(namespace, topRecordAvroName, topRecordDatabaseName, null, ti);
      System.out.println("Generated Schema:\n" + schema);

      //TODO do an intersection of the schema generated and the fields the user want.
      schema = filterUserFields(schema);
      System.out.println("Changing schema to user required fields:\n" + schema);

      // We will write the schema out to a file. If an output directory was specified then we will
      // write it there.
      String avroFileName = namespace + "." + recordName + "." + avroOutVersion.toString() + ".avsc";
      File avroOutFile = new File(avroOutDir,avroFileName);
      System.out.println("Avro schema will be saved in the file: " + avroOutFile.getAbsolutePath());

      // Write the schema and java object to file.
      PrintWriter pw = new PrintWriter(avroOutFile, "UTF-8");
      pw.println(schema);
      pw.flush();
      pw.close();

      System.out.println("Generating Java files in the directory: " + javaOutDir.getAbsolutePath());
      SpecificCompiler.compileSchema(avroOutFile, javaOutDir);


      //Add the new file to the index.schemaregistry
      addNameSpaceToIndex(avroFileName);
      _manager.store();
    }
    catch(RuntimeException e)
    {
      throw e;
    }
    catch(Exception e)
    {
      System.out.println("Exception while generating schema: " + e.toString());
      return false;
    }

    System.out.println("Schema generated at " + _schemaRegistryLocation + ". Please submit a review request (using svin) to the Databus team.");
    return true;
  }


  private String filterUserFields(String schema)
      throws IOException, DatabusException
  {

    //No filtering is necessary, just return the same schema
    if(!_areFieldsFiltered)
    {
      return schema;
    }

    Schema avroSchema = Schema.parse(schema);
    ObjectMapper mapper = new ObjectMapper();
    JsonFactory factory = new JsonFactory();
    StringWriter writer = new StringWriter();
    JsonGenerator jgen = factory.createJsonGenerator(writer);
    jgen.useDefaultPrettyPrinter();

    @SuppressWarnings("checked")
    HashMap<String,Object> schemaMap = new ObjectMapper().readValue(schema, HashMap.class);

    @SuppressWarnings("checked")
    ArrayList<HashMap<String,String>> list = (ArrayList<HashMap<String, String>>) schemaMap.get("fields");

    int i=0;
    while(i < list.size())
    {
      Schema.Field field = avroSchema.getField(list.get(i).get("name"));
      String dbFieldName;

      if(field.schema().getType() == Schema.Type.ARRAY)
      {

        throw new DatabusException("Field not supported for filtering");
        //TODO fix this

        /*
        String innerSchema = field.getProp("items");
        if(innerSchema == null)
          throw new DatabusException("Unable to the inner schema type of the array");
        Schema innerSchemaParsed = Schema.parse(innerSchema);
        dbFieldName = SchemaHelper.getMetaField(innerSchemaParsed, "dbFieldName");
        */
      }
      else
        dbFieldName = SchemaHelper.getMetaField(field, "dbFieldName");

      if(dbFieldName == null)
        throw new DatabusException("Unable to determine the dbFieldName from the meta information");

      if(!_userFields.contains(dbFieldName.toUpperCase(Locale.ENGLISH)))
        list.remove(i);
      else
        i++;
    }

    mapper.writeValue(jgen, schemaMap);
    return writer.getBuffer().toString();
  }

  /**
   *  =============================
   *  Schema meta data/file manipulation functions
   *
   */


  /**
   * Reads the index file and inserts the avroFileName in the index
   * @param avroFileName The file name to be added to the index
   */
  private void addNameSpaceToIndex(String avroFileName)
      throws Exception
  {
    String indexLocation = _schemaRegistryLocation + DATABUS_SCHEMAS_SUFFIX + SCHEMAREGISTRY_INDEX;
    BufferedReader index = null;
    BufferedWriter writeIndex = null;

    try{
      index = new BufferedReader(new InputStreamReader(new FileInputStream(indexLocation),"UTF-8"));
      ArrayList<String> fileNameList = new ArrayList<String>();
      String fileName;
      while((fileName = index.readLine()) != null)
      {
        fileNameList.add(fileName);
      }

      System.out.println("Adding the new schema " + avroFileName + " to the schema registry index");
      fileNameList.add(avroFileName);
      Collections.sort(fileNameList);

      writeIndex = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(indexLocation),"UTF-8"));

      for(String fName : fileNameList)
      {
        writeIndex.write(fName);
        writeIndex.newLine();
      }
    }
    catch(RuntimeException e)
    {
      throw e;
    }
    catch(Exception e)
    {
      System.out.println("Error adding the generated schema to the schema registry index (meta information was not added)" + e.toString());
      throw e;
    }
      finally
    {
      if(writeIndex != null){
        try
        {
          writeIndex.close();
        }
        catch (IOException e)
        {
          System.out.println("Error while closing index.schema_registry: " + e);
        }
      }

      if(index != null) {
        try
        {
          index.close();
        }
        catch (IOException e)
        {
          System.out.println("Error while closing index.schema_registry: " + e);
        }
      }
    }
  }


  /**
   * Loads the schema registry to the _schemaRegistryService
   * @return true if loaded successfully, false otherwise
   */
  private boolean loadSchemaRegistry()
  {

    try {
      FileSystemSchemaRegistryService.Config configBuilder = new FileSystemSchemaRegistryService.Config();
      configBuilder.setFallbackToResources(true);
      configBuilder.setSchemaDir(_schemaRegistryLocation+DATABUS_SCHEMAS_SUFFIX);
      FileSystemSchemaRegistryService.StaticConfig config = configBuilder.build();
      _schemaRegistryService = FileSystemSchemaRegistryService.build(config);
    } catch (InvalidConfigException e) {
      System.out.println("Error while attempting to load the schema registry: " + e.toString());
      return false;
    }

    return true;
  }


  /**
   * The function returns the latest version number of the schema
   * @param nameSpace The logical source name to look for in the schema registry
   * @return The maximum version of the schema in the relay
   */
  private int getLatestVersionNumber(String nameSpace){
    System.out.println("Checking if there is an existing schema for: "+ nameSpace);
    try {
      Map<Short, String> _schemaVersionMap= _schemaRegistryService.fetchAllSchemaVersionsBySourceName(nameSpace);
      int maxVersion = -1;

      if(_schemaVersionMap.size() == 0)     //This schema does not exist in the schema registry
      {
        System.out.println("The schema does not exist in the schema registry, this will be the first version");
        return 0;
      }

      for(int version : _schemaVersionMap.keySet())
      {
        if(version > maxVersion)
          maxVersion = version;
      }

      System.out.println("There is already a avro schema already existing for this table with verion: " + maxVersion);
      return maxVersion;

    } catch (DatabusException e) {
      System.out.println("Unable to determine the version of the schema to be generated: " + e.toString());
      return -1;
    }
  }

  private boolean promptForYesOrNoQuestion(String msg) throws IOException
  {
    Set<String> choices = new HashSet<String>(Arrays.asList(new String[] { "y", "n" }));

    boolean answer;
    String answerStr = promptMultiChoiceQuestion(msg + "(y/n) ?", new MultipleChoiceChecker(choices));

    answer = answerStr.toLowerCase(Locale.ENGLISH).startsWith("y");

    return answer;
  }

  public static interface UserInputChecker
  {
    public boolean isValidInput(String inputStr);
  }

  public class DbUriChecker implements UserInputChecker
  {
    @Override
    public boolean isValidInput(String inputStr)
    {
      inputStr = inputStr.trim();
      Connection conn = null;
      try
      {
        conn = DriverManager.getConnection(inputStr);
      } catch (SQLException sqlEx) {
        String msg = "The DBUrl (" + inputStr +") does not seem to be a valid URL. Do you still want to use the Url";
        boolean proceed = false;
        try
        {
          proceed = promptForYesOrNoQuestion(msg);
        } catch (IOException e) {}
        if ( ! proceed)
          return false;
      } finally {
        DBHelper.close(conn);
      }
      return true;
    }
  }

  private static class DirChecker implements UserInputChecker
  {
    @Override
    public boolean isValidInput(String inputStr)
    {
      inputStr = inputStr.trim();
      File f  = new File(inputStr);

      if ( ! f.isDirectory())
      {
        System.out.println("Location (" + inputStr + ") does not appear to be a valid directory. Please specify one !!");
        return false;
      }
      return true;
    }
  }

  public static class MultipleChoiceChecker implements UserInputChecker
  {
    private final Set<String> _choicesInLowerCase;

    public MultipleChoiceChecker(Set<String> choicesInLowerCase)
    {
      _choicesInLowerCase = choicesInLowerCase;
    }

    @Override
    public boolean isValidInput(String inputStr)
    {
      if (!_choicesInLowerCase.contains(inputStr.trim().toLowerCase(Locale.ENGLISH)))
      {
        System.out.println("Invalid Input. Should be one of :" + _choicesInLowerCase + ", Please re-enter :");
        return false;
      }
      return true;
    }
  }

  private String promptMultiChoiceQuestion(String msg, UserInputChecker checker)
      throws IOException
  {
    boolean validInput = false;
    String answer = null;
    while ( !validInput)
    {
      System.out.println(msg);

      answer = checkAndRead();

      if (answer == null)
        answer = "";

      validInput = checker.isValidInput(answer);
    }
    return answer;
  }

  private void generateRelayConfig(String schemaRegistryLocation, String dbName, String uri, String outputDir)
      throws Exception
  {
    List<String> srcs = _manager.getManagedSourcesForDB(dbName.trim().toLowerCase(Locale.ENGLISH));

    if (null == srcs)
    {
      String error = "No Sources found for database (" + dbName + "). Available sources are :" + _manager.getDbToSrcMap();
      System.out.println(error);
      throw new DatabusException(error);
    }

    List<String> addSrcs = new ArrayList<String>();
    for (String src : srcs)
    {
      boolean add = promptForYesOrNoQuestion("Do you want to add the src (" + src + ") to the relay config ");
      if (add)
        addSrcs.add(src);
    }

    DevRelayConfigGenerator.generateRelayConfig(schemaRegistryLocation, dbName, uri, outputDir, addSrcs, _manager);
  }

  private void buildRelayConfigs() throws Exception
  {
    boolean doGenerateRelayConfigs = promptForYesOrNoQuestion("Do you want to generate relay configs for the relay running locally");

    if ( !doGenerateRelayConfigs)
      return;

    String location = DEFAULT_RELAY_CONFIG_LOCATION;
    boolean override = promptForYesOrNoQuestion("Relay Configs will be created in :" + location + ". This is the default location read by dev relay. If you need to change this, make sure the relay config is also updated for dev. Do you want to override ");

    if ( override)
      location = promptMultiChoiceQuestion("Please specify the directory for creating the relay configs ", new DirChecker());
    else
    {
      File f = new File(location);
      while ( (!f.exists())  || (!f .isDirectory()))
      {
        System.out.println("The location (" + location +") does not exist or is not a directory !! Please create the directory (" + location + ") and press <enter>");
        _reader.readLine();
      }
    }

    String changeSrcConfirmQn = " Databus Relay's change-capture mechanism for a source can either be trigger-based or through Golden-Gate trail files (gg). Do you know the source's change-capture type ";
    boolean isUserAwareOfCCType = promptForYesOrNoQuestion(changeSrcConfirmQn);
    if ( ! isUserAwareOfCCType )
    {
      System.out.println("If this source is part of Multi-Colo Cache invalidation use-case, please look at the type column at " +
                         " https://iwww.corp.linkedin.com/wiki/cf/display/ENGS/Databus+GoldenGate+Client+Launches. " +
                         "Otherwise, if you are not sure, please email @ask-databus");

      if ( ! promptForYesOrNoQuestion("Have you figured out the change-capture type "))
      {
        System.out.println(" Please run this script again after you found out the change capture type.  Exiting without generating the configs !!");
        currentState = State.EXIT;
        return;
      }
    }
    String changeSrcQuestion = "Does the relay going to capture changes from Oracle (ora) or through golden-gate trail files (gg)?";
    Set<String> changeSrcChoices = new HashSet<String>(Arrays.asList(new String[] { "ora", "gg" }));
    String changeSrc = promptMultiChoiceQuestion(changeSrcQuestion,new MultipleChoiceChecker(changeSrcChoices));
    String uri;
    if ( changeSrc.equals("ora"))
    {
      System.out.println("Using Oracle as change stream source.");
      String defaultUri = "jdbc:oracle:thin:" + _userName + "/" + _password + "@devdb:1521:DB";
      override = promptForYesOrNoQuestion("Using Uri (" + defaultUri +"). Do you want to override ?");
      uri = defaultUri;
      if ( override )
      {
        String msg = "Enter the new DB URI :";
        uri = promptMultiChoiceQuestion(msg, new DbUriChecker());
      }
    } else {
      System.out.println("Please follow steps in \"go/databus2-gg -> Client Launch Workflow -> Testing In Dev\" to install and setup Oracle VM with GG");
      System.out.println("Please run the command (ssh oracle@devdb \"cd app/oracle/product/goldengate/dirprm/ && grep -l \\\"SHORTLINKS\\\" dbext*\" | sed -e 's/dbext//' -e 's/.prm//' to get GoldenGate extract process number !! Press <enter> to continue");
      _reader.readLine();
      System.out.println("If you cannot get the extract process number, please contact @ask-databus !! Press <enter> to continue");
      _reader.readLine();
      String extract = promptMultiChoiceQuestion("Please enter the extract number", new MultipleChoiceChecker(new HashSet<String>(Arrays.asList(new String[] { "1", "2", "3", "4"}))));
      uri = "gg:///mnt/gg/extract/dbext" + extract + ":x" + extract;
    }
    generateRelayConfig(_schemaRegistryLocation,_schemaName,uri,location);
  }


  public static void main(String[] args)
      throws Exception
  {
    InteractiveSchemaGenerator gen = new InteractiveSchemaGenerator();
    gen.run();
  }
}


