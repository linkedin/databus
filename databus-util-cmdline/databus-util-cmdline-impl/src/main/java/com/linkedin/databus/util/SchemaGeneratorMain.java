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


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;

import org.apache.avro.specific.SpecificCompiler;

/**
 * This tools is replaced by interactive schema generator tool, check InteractiveSchemaGenerator class for more details.
 */
@Deprecated
public class SchemaGeneratorMain
{
  private static final String[] DEFAULT_JDBC_DRIVERS = {"oracle.jdbc.driver.OracleDriver"};
  private static final String DEFAULT_DATABASE = "jdbc:oracle:thin:@devdb:1521:devdb";
  private static final String DEFAULT_USERNAME = "system";
  private static final String DEFAULT_PASSWORD = "manager";

  private final String _database;
  private final String _userName;
  private final String _password;
  private final String _viewName;
  private final String _recordName;
  private final String _namespace;
  private final File _javaOutDir;
  private final File _avroOutDir;
  private final boolean _verbose;
  private final String _driver;
  private final int _avroOutVersion;
private String _primaryKey;

  /**
   * Command line: user/password@connect_string object_name [-driver foo] [-v]
   *
   * Like:
   * java -classpath bin:/Users/jwesterm/.ivy2/lin-cache/ivy-cache/com.oracle/ojdbc14/10.2.0.2.0/ojdbc14-10.2.0.2.0.jar com.linkedin.databus.util.SchemaGenerator system/manager@jdbc:oracle:thin:@devdb:1521:db MEMBER2.SY\$MEMBER_PROFILE
   * or
   * java -classpath bin:/Users/jwesterm/.ivy2/lin-cache/ivy-cache/com.oracle/ojdbc14/10.2.0.2.0/ojdbc14-10.2.0.2.0.jar com.linkedin.databus.util.SchemaGenerator system/manager@jdbc:oracle:thin:@devdb:1521:db MEMBER2.DATABUS_PROF_EDU_T
   */
  public static void main(String[] args)
  throws Exception
  {
    CommandLineHelper commandLineParser = new CommandLineHelper();
    commandLineParser.addArgument("database", false, "Database JDBC connection string (e.g. jdbc:oracle:thin:@devdb:1521:db).", CommandLineHelper.ArgumentType.STRING);
    commandLineParser.addArgument("userName", false, "User name used to connect to the database.", CommandLineHelper.ArgumentType.STRING);
    commandLineParser.addArgument("password", false, "Password used to connect to the database.", CommandLineHelper.ArgumentType.STRING);
    commandLineParser.addArgument("viewName", true, "Name of the databus view for which to generate a schema..", CommandLineHelper.ArgumentType.STRING);
    commandLineParser.addArgument("recordName", true, "Record name for the generated schema (will be the Java class name of the compiled schema).", CommandLineHelper.ArgumentType.STRING);
    commandLineParser.addArgument("namespace", true, "Namespace for the generated schema (will be the Java package of the compiled schema).", CommandLineHelper.ArgumentType.STRING);
    commandLineParser.addArgument("javaOutDir", false, "Directory for generated Java source files (if absent, the files will not be generated).", CommandLineHelper.ArgumentType.DIRECTORY);
    commandLineParser.addArgument("avroOutDir", false, "Directory for generated .avsc files (if absent, the files will not be generated).", CommandLineHelper.ArgumentType.DIRECTORY);
    commandLineParser.addArgument("avroOutVersion", false, "Schema version for the avro output file (required if avroOutDir is specified).", CommandLineHelper.ArgumentType.INTEGER);
    commandLineParser.addArgument("primaryKey", false, "Primary key name).",CommandLineHelper.ArgumentType.STRING);

    commandLineParser.addArgument("verbose", false, "true to enable verbose output.", CommandLineHelper.ArgumentType.BOOLEAN);
    commandLineParser.addArgument("driver", false, "JDBC driver (if absent, defaults to oracle.jdbc.driver.OracleDriver).", CommandLineHelper.ArgumentType.STRING);

    Map<String, Object> parsedArgs = commandLineParser.parseCommandLine(args);
    if(parsedArgs == null)
    {
      return;
    }

    if(parsedArgs.containsKey("avroOutDir") && !parsedArgs.containsKey("avroOutVersion"))
    {
      commandLineParser.showUsage("Bad command line. avroOutDir requires avroOutVersion to be specified as well.");
      return;
    }

    String database = null != parsedArgs.get("database") ? (String)parsedArgs.get("database")
                                                         : DEFAULT_DATABASE;
    String username = null != parsedArgs.get("userName") ? (String)parsedArgs.get("userName")
                                                         : DEFAULT_USERNAME;
    String password = null != (String)parsedArgs.get("password") ? (String)(String)parsedArgs.get("password")
                                                         : DEFAULT_PASSWORD;
    
    String primaryKey = null != (String) parsedArgs.get("primaryKey") ? (String) parsedArgs.get("primaryKey") : "";

    // Show the arguments we read from the command line (helpful when running in an IDE, where you
    // don't always see the command line)
    commandLineParser.showParsedArguments("Processed command line arguments:", parsedArgs);

    // Create the new SchemaGeneratorMain instance using the command line args
    SchemaGeneratorMain generator = new SchemaGeneratorMain(
          database,
          username,
          password,
          (String)parsedArgs.get("viewName"),
          (String)parsedArgs.get("recordName"),
          (String)parsedArgs.get("namespace"),
          (File)parsedArgs.get("javaOutDir"),
          (File)parsedArgs.get("avroOutDir"),
          (Boolean)parsedArgs.get("verbose"),
          (String)parsedArgs.get("driver"),
          (Integer)parsedArgs.get("avroOutVersion"),
          primaryKey);

    // Load JDBC drivers
    generator.loadJdbcDrivers();

    // Generate the schema
    generator.generateSchema();

    System.out.println("Done.");
  }

  public SchemaGeneratorMain(String database,
                             String userName,
                             String password,
                             String viewName,
                             String recordName,
                             String namespace,
                             File javaOutDir,
                             File avroOutDir,
                             Boolean verbose,
                             String driver,
                             Integer avroOutVersion,
                             String primaryKey)
  {
    _database = database;
    _userName = userName;
    _password = password;
    _viewName = viewName;
    _recordName = recordName;
    _namespace = namespace;
    _javaOutDir = javaOutDir;
    _avroOutDir = avroOutDir;
    _verbose = verbose != null && verbose;
    _driver = driver;
    _avroOutVersion = avroOutVersion != null ? avroOutVersion : 0;
    _primaryKey = primaryKey;
  }

  public void loadJdbcDrivers()
  {
    // If a JDBC driver was specified then try to load it, otherwise load Oracle and MySQL if available
    if(_driver != null)
    {
      try
      {
        if(_verbose)
        {
          System.out.println("Loading JDBC driver: " + _driver);
        }
        Class.forName(_driver);
        if(_verbose)
        {
          System.out.println("JDBC driver loaded successfully.");
        }
      }
      catch(ClassNotFoundException ex)
      {
        System.out.println("Could not load JDBC driver: " + _driver);
        return;
      }
    }
    else
    {
      boolean loadedAtLeastOneDriver = false;
      for(int i=0; i < DEFAULT_JDBC_DRIVERS.length; i++)
      {
        String driver = DEFAULT_JDBC_DRIVERS[i];
        try
        {
          if(_verbose)
          {
            System.out.println("Loading default JDBC driver: " + driver);
          }
          Class.forName(driver);
          loadedAtLeastOneDriver = true;
          if(_verbose)
          {
            System.out.println("JDBC driver loaded successfully.");
          }
        }
        catch(ClassNotFoundException ex)
        {
          System.out.println("Could not load JDBC driver: " + driver);
        }
      }
      if(!loadedAtLeastOneDriver)
      {
        System.out.println("Unable to load any of the default JDBC drivers. Please make sure one of the default drivers \n" +
                          "is available in the classpath, or specify a driver with [-driver com.foo.driver]. \n" +
                          "Default drivers are: " + Arrays.toString(DEFAULT_JDBC_DRIVERS));
      }
    }
  }

  public void generateSchema()
  throws IOException
  {
    Connection con = null;

    try
    {
      con = getConnection();

      String owner;
      String table;
      String[] nameParts = _viewName.split("\\.");
      if(nameParts.length == 1)
      {
        owner = _userName;
        table = nameParts[0];
      }
      else
      {
        owner = nameParts[0];
        table = nameParts[1];
      }

      System.out.println("Generating schema for " + _viewName);
      if(_avroOutDir == null)
      {
        System.out.println("Avro schema will not be saved (use -avroOutDir if you want to save it).");
      }
      if(_javaOutDir == null)
      {
        System.out.println("Java files will not be generated (use -javaOutDir if you want to generate them).");
      }

      TableTypeInfo ti = (TableTypeInfo) new TypeInfoFactory().getTypeInfo(con, owner, table, 0, 0,_primaryKey);

      String topRecordAvroName = _recordName + "_V" + _avroOutVersion;
      String namespace = _namespace;
      String topRecordDatabaseName = _viewName;

      FieldToAvro fa = new FieldToAvro();
      String schema = fa.buildAvroSchema(namespace, topRecordAvroName, topRecordDatabaseName, null, ti);
      System.out.println("Generated Schema:\n" + schema);

      if(_avroOutDir != null || _javaOutDir != null)
      {
        // We will write the schema out to a file. If an output directory was specified then we will
        // write it there. Otherwise we write to a temp file that is deleted on exit.
        File avroOutFile;
        if(_avroOutDir != null)
        {
          avroOutFile = new File(_avroOutDir, _namespace + "." + _recordName + "." + _avroOutVersion + ".avsc");
          System.out.println("Avro schema will be saved in the file: " + avroOutFile.getAbsolutePath());
        }
        else
        {
          avroOutFile = File.createTempFile(getClass().getName(), null);
          avroOutFile.deleteOnExit();
        }

        // Write the schema to a file.
        PrintWriter pw = new PrintWriter(new FileWriter(avroOutFile));
        pw.println(schema);
        pw.flush();
        pw.close();

        if(_javaOutDir != null)
        {
          System.out.println("Generating Java files in the directory: " + _javaOutDir.getAbsolutePath());
          SpecificCompiler.compileSchema(avroOutFile, _javaOutDir);
        }
      }
    }
    catch(SQLException ex)
    {
      System.out.println(ex);
      ex.printStackTrace();
    }
    finally
    {
      SchemaUtils.close(con);
    }
  }

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
  
}
