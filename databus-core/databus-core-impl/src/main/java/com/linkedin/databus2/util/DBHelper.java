/*
 * $Id: DBHelper.java 153194 2010-12-02 02:53:45Z jwesterm $
 */
package com.linkedin.databus2.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Static helper methods for common database operations.
 *
 * @author Jemiah Westerman<jwesterman@linkedin.com>
 * @version $Revision: 153194 $
 */
public class DBHelper
{

  public static void close(ResultSet rs, Statement stmt, Connection con)
  {
    close(rs);
    close(stmt);
    close(con);
  }

  /**
   * Close the Connection passed, ignoring any SQLException thrown by the close() method.
   */
  public static void close(Connection obj)
  {
    if(obj != null)
    {
      try
      {
        obj.close();
      }
      catch(SQLException ex)
      {
      }
    }
  }

  /**
   * Close the Statement passed, ignoring any SQLException thrown by the close() method.
   */
  public static void close(Statement obj)
  {
    if(obj != null)
    {
      try
      {
        obj.close();
      }
      catch(SQLException ex)
      {
      }
    }
  }

  /**
   * Close the ResultSet passed, ignoring any SQLException thrown by the close() method.
   */
  public static void close(ResultSet obj)
  {
    if(obj != null)
    {
      try
      {
        obj.close();
      }
      catch(SQLException ex)
      {
      }
    }
  }
}
