package com.linkedin.databus.util;


import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


/**
 * User: jwesterm Date: Oct 13, 2010 Time: 5:29:45 PM
 */
public class SchemaUtils
{
  private SchemaUtils() {}

  public static String buildJson(String key, String value)
  {
    return "\"" + key + "\" : \"" + value + "\"";
  }

  public static String buildJson(String key, Number value)
  {
    return "\"" + key + "\" : " + value;
  }

  public static String toCamelCase(String columnName)
  {
    return toCamelCase(columnName, false);
  }

  public static String toCamelCase(String columnName, boolean initialCap)
  {
    boolean afterUnderscore = false;
    StringBuilder sb = new StringBuilder(columnName.length());
    for(int i=0; i < columnName.length(); i++)
    {
      char ch = columnName.charAt(i);
      if(ch == '_')
      {
        afterUnderscore = true;
      }
      else if(afterUnderscore)
      {
        sb.append(Character.toUpperCase(ch));
        afterUnderscore = false;
      }
      else
      {
        sb.append(Character.toLowerCase(ch));
        afterUnderscore = false;
      }
    }

    if(initialCap && sb.length() > 0)
    {
      sb.replace(0, 1, sb.substring(0,1).toUpperCase());
    }

    return sb.toString();
  }

  public static boolean in(String needle, String... haystack)
  {
    for(String s : haystack)
    {
      if(s.equalsIgnoreCase(needle))
      {
        return true;
      }
    }
    return false;
  }


  /**
   * Takes a name like "owner.table" or just "table" and returns an array such that
   * ret[0] = owner and ret[1] = table. If the "owner" part is not provided then returns
   * ret[0] = null, ret[1] = table.
   */
  public static final String[] splitSchemaAndName(String name)
      throws SQLException
  {
    String[] parts = name.split("\\.");
    if(parts.length == 1)
    {
      return new String[] {null, parts[0]};
    }
    else if(parts.length == 2)
    {
      return parts;
    }
    else
    {
      throw new SQLException("Bad schema/type name.");
    }
  }

  public static final void close(ResultSet target)
  {
    try
    {
      if(target != null)
      {
        target.close();
      }
    }
    catch(SQLException ex)
    {
      // Ignore this
    }
  }

  public static final void close(Statement target)
  {
    try
    {
      if(target != null)
      {
        target.close();
      }
    }
    catch(SQLException ex)
    {
      // Ignore this
    }
  }


  public static final void close(Connection target)
  {
    try
    {
      if(target != null)
      {
        target.close();
      }
    }
    catch(SQLException ex)
    {
      // Ignore this
    }
  }

}
