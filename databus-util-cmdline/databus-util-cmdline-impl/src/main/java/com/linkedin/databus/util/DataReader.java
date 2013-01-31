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



import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Struct;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * User: jwesterm Date: Oct 12, 2010 Time: 10:29:24 AM
 */
public class DataReader
{
  private static final String[] DEFAULT_JDBC_DRIVERS = {"oracle.jdbc.driver.OracleDriver"};

  private final String _userName;
  private final String _password;
  private final String _connectString;
  private final String _driver;
  private final String _table;
  private final boolean _verbose;

  /**
   * Command line: user/password@connect_string table_name [-driver foo] [-v]
   */
  public static void main(String[] args)
  {
    DataReader generator = new DataReader(args);
    generator.loadJdbcDrivers();
    generator.doIt();
  }

  private DataReader(String[] args)
  {
    String userName = null;
    String password = null;
    String connectString = null;
    String driver = null;
    String table = null;
    boolean verbose = false;

    Pattern connectPattern = Pattern.compile("(\\S+)/([^\\s@]+)@(\\S+)");
    for(int i=0; i < args.length; i++)
    {
      Matcher connectMatcher = connectPattern.matcher(args[i]);
      if(connectMatcher.matches())
      {
        userName = connectMatcher.group(1);
        password = connectMatcher.group(2);
        connectString = connectMatcher.group(3);
        // Table name should be the next argument after the connect string
        if(args[i+1].startsWith("-"))
        {
          throw new IllegalArgumentException("Table name should come after connect string.");
        }
        table = args[i+1];
        i++;
      }
      else if("-driver".equals(args[i]))
      {
        driver = args[i+1];
        i++;
      }
      else if("-v".equals(args[i]) || "-verbose".equals(args[i]))
      {
        verbose = true;
      }
    }

    if(userName == null || password == null || connectString == null || table == null)
    {
      showUsage();
      throw new IllegalArgumentException("Missing one or more required parameters.");
    }

    if(verbose)
    {
      System.out.println("url: " + connectString + "; user:" + userName + "; password: " + password + "; table=" + table);
    }

    _userName = userName;
    _password = password;
    _connectString = connectString;
    _table = table;
    _verbose = verbose;
    _driver = driver;
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
        showUsage();
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

  public void doIt()
  {
    Connection con = null;
    PreparedStatement stmt = null;
    ResultSet rs = null;

    try
    {
      con = getConnection();

      stmt = con.prepareStatement("SELECT * FROM " + _table + " WHERE ROWNUM < 2");
      rs = stmt.executeQuery();
      rs.next();

      ResultSetMetaData rsmd = rs.getMetaData();
      for(int column=1; column <= rsmd.getColumnCount(); column++)
      {
        System.out.println(column + ": " + rsmd.getColumnName(column) + " --> " + rs.getObject(column).getClass().getName());
        if(rs.getObject(column) instanceof Array)
        {
          Array arr = rs.getArray(column);
          System.out.println("\t" + arr.getBaseTypeName());
          ResultSet rs2 = arr.getResultSet();
          while(rs2.next())
          {
            System.out.println("\t" + rs2.getObject(1));
            Struct struct = (Struct) rs2.getObject(2);
            System.out.println(Arrays.toString(struct.getAttributes()));
          }
          rs2.close();
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
      SchemaUtils.close(rs);
      SchemaUtils.close(stmt);
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
        System.out.println("Connecting to database: " + _connectString);
      }
      Connection con = DriverManager.getConnection(_connectString, _userName, _password);
      if(_verbose)
      {
        System.out.println("Connected successfully.");
      }

      return con;
    }
    catch(SQLException ex)
    {
      System.out.println("Could not connect to database: " + _connectString);
      System.out.println(ex.getMessage());
      if(_verbose)
      {
        ex.printStackTrace();
      }
      throw ex;
    }
  }

  private static void showUsage()
  {
//    System.out.println(SchemaGenerator.class.getName() + " user/password@connect_string table_name [-driver foo] [-verbose]" );
    System.out.println(  "Oracle connection string looks like: user/password@jdbc:oracle:thin:@devdb:1521:db");
  }
}
