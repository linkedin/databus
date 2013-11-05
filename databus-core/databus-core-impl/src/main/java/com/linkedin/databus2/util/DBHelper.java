/*
 * $Id: DBHelper.java 153194 2010-12-02 02:53:45Z jwesterm $
 */
package com.linkedin.databus2.util;
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


import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

/**
 * Static helper methods for common database operations.
 *
 * @author Jemiah Westerman<jwesterman@linkedin.com>
 * @version $Revision: 153194 $
 */
public class DBHelper
{
  public static final String MODULE = DBHelper.class.getName();
  public static final Logger LOG    = Logger.getLogger(MODULE);
	
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
  
  /**
   * Invokes a commit on the connection if auto-commit is turned off
   */
  public static void commit(Connection conn)
  throws SQLException
  {
	  if (conn != null && !conn.isClosed())
	  {
		  if (!conn.getAutoCommit())
			  conn.commit();
	  }
	  else
	  {
		  LOG.error("Invoking a commit after the connection closed ");
	  }
  }
  
  /**
   * Invokes a rollback on the connection if auto-commit is turned off
   */
  public static void rollback(Connection conn)
  throws SQLException
  {
	  if (conn != null && !conn.isClosed())
	  {
		  if (!conn.getAutoCommit())
			  conn.rollback();
	  }
	  else
	  {
		  LOG.error("Invoking a rollback after the connection is closed ");
	  }
  }

}
