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



import com.linkedin.databus2.core.DatabusException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import jline.ConsoleReader;


/**
 * User: jwesterm Date: Oct 14, 2010 Time: 6:26:32 PM
 */
public class TypeInfoFactoryInteractive
{
  public TypeInfo getTypeInfo(Connection con,
                              String typeOwner,
                              String typeName,
                              int precision,
                              int scale,
                              String primaryKey,
                              ConsoleReader reader,
                              HashMap<String, String> dbFieldToAvroDataType)
      throws SQLException, IOException, DatabusException
  {
    if(isSimpleType(con, typeOwner, typeName))
    {
      return buildSimpleTypeInfo(con, typeOwner, typeName, precision, scale);
    }
    else if(isTableType(con, typeOwner, typeName))
    {
      return buildTableType(con, typeOwner, typeName,primaryKey, reader, dbFieldToAvroDataType);
    }
    else if(isCollectionType(con, typeOwner, typeName))
    {
      return buildCollectionTypeInfo(con, typeOwner, typeName, reader, dbFieldToAvroDataType);
    }
    else if(isUserType(con, typeOwner, typeName))
    {
      return buildUserTypeInfo(con, typeOwner, typeName, reader, dbFieldToAvroDataType);
    }
    else
    {
      throw new SQLException("Cannot determine type info for the attribute (" + typeOwner + "." + typeName + ").");
    }
  }

  // Table / View Types
  public boolean isTableType(Connection con, String tableOwner, String tableName)
  {
    PreparedStatement stmt = null;

    try
    {
      String fullTableName = tableOwner + "." + tableName;
      stmt = con.prepareStatement("SELECT * FROM " + fullTableName + " WHERE 0=1");
      stmt.executeQuery();
      return true;
    }
    catch(SQLException ex)
    {
      return false;
    }
    finally
    {
      SchemaUtils.close(stmt);
    }
  }

  public TableTypeInfo buildTableType(Connection con,
                                      String tableOwner,
                                      String tableName,
                                      String pk,
                                      ConsoleReader reader,
                                      HashMap<String, String> dbFieldToAvroDataType)
      throws SQLException, IOException, DatabusException
  {
    PreparedStatement stmt = null;
    ResultSet rs = null;

    try
    {
      String fullTableName = tableOwner + "." + tableName;
      stmt = con.prepareStatement("SELECT * FROM " + fullTableName + " WHERE 0=1");
      rs = stmt.executeQuery();
      ResultSetMetaData rsmd = rs.getMetaData();

      List<FieldInfo> fields = new ArrayList<FieldInfo>();

      int numColumns = rsmd.getColumnCount();
      for(int column=1; column <= numColumns; column++)
      {
        String columnName = rsmd.getColumnName(column);

        System.out.println("Processing column " + tableName + "." + columnName + ":" + rsmd.getColumnTypeName(column));

        int columnPrecision = rsmd.getPrecision(column);
        int columnScale = rsmd.getScale(column);



        String columnTypeName;
        String columnTypeOwner;

        String[] columnTypeParts = rsmd.getColumnTypeName(column).split("\\.");
        if(columnTypeParts.length == 1)
        {
          columnTypeOwner = null;
          columnTypeName = columnTypeParts[0];
        }
        else
        {
          columnTypeOwner = columnTypeParts[0];
          columnTypeName = columnTypeParts[1];
        }


        if(columnTypeName.equals("NUMBER"))
        {
          System.out.println("If you are not sure about the following question, please talk with your DBA or the database owner");
          System.out.println("The following datatypes will be used by the avro generator: ");
          System.out.println("If scale <= 6                                     ===> FLOAT (Irrespective of the precision)");
          System.out.println("If scale <= 17                                    ===> DOUBLE (Irrespective of the precision)");
          System.out.println("If (precision > 9 or precision = 0) and scale = 0 ===> LONG ");
          System.out.println("If precision <= 9 and scale = 0                   ===> INTEGER");
          SimpleTypeInfo typeInfoValidate = new SimpleTypeInfo(columnTypeName,columnPrecision,columnScale);
          if(columnPrecision == 0 && columnScale == 0)
            System.out.println("Unable to determine the scale and precision for this column, please manually verify the the scale/precision in the oracle table ALL_TAB_COLUMNS");

          System.out.println("The precision ["+ columnPrecision +"] and scale ["+ columnScale + "] will be used for the field " + columnName  + " which has oracle datatype " + columnTypeName + " and the avro datatype " +
                                 typeInfoValidate.getPrimitiveType() + " will be used. (yes - to use the printed values, no - to override the datatype with user input): ");

          //If the hashmap is present, this indicates that it's cli driven, we don't ask user input, we except it to passed through cli.
          if(dbFieldToAvroDataType == null)
          {
            String line = checkAndRead(reader);
            while(true)
            {

              if(line.equals("yes"))
              {
                System.out.println("Using the precision ["+ columnPrecision +"] and scale ["+ columnScale + "]");
                break;
              }
              else if(line.equals("no"))
              {
                System.out.println("Overriding the avro datatype..");
                System.out.println("Please enter the avro datatype you would like to use [FLOAT,DOUBLE,LONG,INTEGER]: ");
                String datatype = checkAndRead(reader);
                try
                {
                  ScalePrecision scalePrecision = getScaleAndPrecision(datatype);
                  columnPrecision = scalePrecision.getPrecision();
                  columnScale = scalePrecision.getScale();
                }
                catch (DatabusException e)
                {
                  continue; //Invalid input, retry.
                }
                typeInfoValidate = new SimpleTypeInfo(columnTypeName, columnPrecision,columnScale);
                System.out.println("Based on your input, the avro datatype " + typeInfoValidate.getPrimitiveType() + " will be used for the field " + columnName);
                break;
              }
              else
              {
                System.out.println("Invalid input, say 'yes' or 'no'");
                line = checkAndRead(reader);

              }
            }
          }
          else
          {
              if(dbFieldToAvroDataType.containsKey(columnName.trim()))
              {
                  String avroDataType = dbFieldToAvroDataType.get(columnName.trim());
                  ScalePrecision scalePrecision = getScaleAndPrecision(dbFieldToAvroDataType.get(columnName.trim()));
                  System.out.println("Using avro data type [" + avroDataType +"] for the column [" + columnName + "]");
                  columnPrecision = scalePrecision.getPrecision();
                  columnScale = scalePrecision.getScale();
              }
              else
              {
                System.out.println("The override for the column [" + columnName + "] is not present, this is expected from the user input in cli");
                throw new DatabusException("Number override not present");
              }
          }

        }

        TypeInfo typeInfo = getTypeInfo(con, columnTypeOwner, columnTypeName, columnPrecision, columnScale,"", reader,
                                        dbFieldToAvroDataType);
        FieldInfo field = new FieldInfo(columnName, typeInfo, column - 1);
        fields.add(field);
      }
      return new TableTypeInfo(tableOwner, tableName, fields,pk);
    }
    catch (IOException e)
    {
      System.out.println("Unable to process user input, please try again.");
      e.printStackTrace();
      throw e;
    }
    finally
    {
      SchemaUtils.close(rs);
      SchemaUtils.close(stmt);
    }
  }

  private static class ScalePrecision
  {

    private int getScale()
    {
      return scale;
    }

    private int getPrecision()
    {
      return precision;
    }

    private int scale;
    private int precision;

    private ScalePrecision(int scale, int precision)
    {
      this.scale = scale;
      this.precision = precision;
    }
  }

  private ScalePrecision getScaleAndPrecision(String dataType)
      throws DatabusException
  {
    dataType = dataType.trim();
    if(dataType.equalsIgnoreCase("FLOAT"))
    {
      return new ScalePrecision(5,0);
    }
    else if(dataType.equalsIgnoreCase("DOUBLE"))
    {
      return new ScalePrecision(16,0);
    }
    else if(dataType.equalsIgnoreCase("LONG"))
    {
      return new ScalePrecision(0,10);
    }
    else if(dataType.equalsIgnoreCase("INTEGER"))
    {
      return new ScalePrecision(0,8);
    }
    else
    {
      throw new DatabusException("Unknown datatype, valid datatypes/input are FLOAT/DOUBLE/LONG/INTEGER, please retry.");
    }
  }

  private String checkAndRead(ConsoleReader reader)
      throws IOException
  {
    String line;
    if((line = reader.readLine()) == null)
    {
      System.out.println("Please enter a valid input");
      return null;
    }

    return line.trim();
  }

  // User Types
  public boolean isUserType(Connection con, String ownerName, String typeName)
      throws SQLException
  {
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try
    {
      // Query to see if  user attribute information exists for this user type
      stmt = con.prepareStatement("SELECT 1 FROM ALL_TYPE_ATTRS " +
                                      "WHERE OWNER=? AND TYPE_NAME=? AND ROWNUM < 2");
      stmt.setString(1, ownerName);
      stmt.setString(2, typeName);
      rs = stmt.executeQuery();

      // If a row exists then this is a user type
      return rs.next();
    }
    finally
    {
      SchemaUtils.close(rs);
      SchemaUtils.close(stmt);
    }
  }

  public UserTypeInfo buildUserTypeInfo(Connection con,
                                        String typeOwner,
                                        String typeName,
                                        ConsoleReader reader,
                                        HashMap<String, String> dbFieldToAvroDataType)
      throws SQLException, IOException, DatabusException
  {
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try
    {
      // Query to select the user type information
      stmt = con.prepareStatement("SELECT ATTR_NAME, ATTR_TYPE_OWNER, ATTR_TYPE_NAME, PRECISION, SCALE FROM ALL_TYPE_ATTRS " +
                                      "WHERE OWNER=? AND TYPE_NAME=?" +
                                      "ORDER BY ATTR_NO");
      stmt.setString(1, typeOwner);
      stmt.setString(2, typeName);
      rs = stmt.executeQuery();

      // If there was no row then this is not a user type
      if(!rs.next())
      {
        throw new SQLException("Not a user type. (" + typeName + ")");
      }

      // Build up the list of attributes (fields) in this user type
      List<FieldInfo> fields = new ArrayList<FieldInfo>();
      do
      {
        String attrName = rs.getString(1);
        String attrTypeOwner = rs.getString(2);
        String attrTypeName = rs.getString(3);
        int attrPrecision = rs.getInt(4);
        int attrScale = rs.getInt(5);

        TypeInfo typeInfo = getTypeInfo(con, attrTypeOwner, attrTypeName, attrPrecision, attrScale,"", reader,
                                        dbFieldToAvroDataType);
        fields.add(new FieldInfo(attrName, typeInfo, fields.size()));
      } while(rs.next());

      return new UserTypeInfo(typeOwner, typeName, fields);
    }
    finally
    {
      SchemaUtils.close(rs);
      SchemaUtils.close(stmt);
    }
  }

  // Built in types
  public boolean isSimpleType(Connection con, String typeOwner, String typeName)
      throws SQLException
  {
    //For whatever reason, the JDBC driver does not return this as primitive type
    if (typeName.equalsIgnoreCase("NVARCHAR")
        || typeName.equalsIgnoreCase("NVARCHAR2")
        || typeName.contains("XML"))
      return true;
    ResultSet rs = null;
    try
    {
      // This returns a ResultSet with all the built in types like NUMBER, VARCHAR2, CLOB, etc.
      rs = con.getMetaData().getTypeInfo();
      while(rs.next())
      {
        //System.out.print(" " + rs.getString("TYPE_NAME") + " ");
        if(rs.getString("TYPE_NAME").equalsIgnoreCase(typeName))
          return true;
      }
      return false;
    }
    finally
    {
      SchemaUtils.close(rs);
    }
  }

  public SimpleTypeInfo buildSimpleTypeInfo(Connection con, String typeOwner, String typeName, int precision, int scale)
      throws SQLException
  {
    if(!isSimpleType(con, typeOwner, typeName))
    {
      throw new SQLException("Not a simple type. (" + typeName + ")");
    }
    return new SimpleTypeInfo(typeName, precision, scale);
  }

  // Collections
  public boolean isCollectionType(Connection con, String ownerName, String typeName)
      throws SQLException
  {
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try
    {
      // Query to see if  collection type information exists for this collection type
      stmt = con.prepareStatement("SELECT 1 FROM ALL_COLL_TYPES " +
                                      "WHERE OWNER=? AND TYPE_NAME=? AND ROWNUM < 2");
      stmt.setString(1, ownerName);
      stmt.setString(2, typeName);
      rs = stmt.executeQuery();

      // If collection information exists then it is a collection type
      return rs.next();
    }
    finally
    {
      SchemaUtils.close(rs);
      SchemaUtils.close(stmt);
    }
  }

  public CollectionTypeInfo buildCollectionTypeInfo(Connection con,
                                                    String ownerName,
                                                    String typeName,
                                                    ConsoleReader reader,
                                                    HashMap<String, String> dbFieldToAvroDataType)
      throws SQLException, IOException, DatabusException
  {
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try
    {
      // Query to select the collection type information
      stmt = con.prepareStatement("SELECT ELEM_TYPE_OWNER, ELEM_TYPE_NAME, PRECISION, SCALE FROM ALL_COLL_TYPES " +
                                      "WHERE OWNER=? AND TYPE_NAME=?");
      stmt.setString(1, ownerName);
      stmt.setString(2, typeName);
      rs = stmt.executeQuery();

      // If there was no row then this is not a collection type
      if(!rs.next())
      {
        throw new SQLException("Not a collection type. (" + typeName + ")");
      }

      // Get the columns
      String elementTypeOwner = rs.getString(1);
      String elementTypeName = rs.getString(2);
      int precision = rs.getInt(3);
      int scale = rs.getInt(4);

      // Build the type for the element type ("this is a collection of ...")
      TypeInfo elementTypeInfo = getTypeInfo(con, elementTypeOwner, elementTypeName, precision, scale,"", reader,
                                             dbFieldToAvroDataType);

      // Construct and return the new CollectionTypeInfo
      return new CollectionTypeInfo(ownerName, typeName, elementTypeInfo);
    }
    finally
    {
      SchemaUtils.close(rs);
      SchemaUtils.close(stmt);
    }
  }
}
