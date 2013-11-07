/**
 * 
 */
package com.linkedin.databus.bootstrap.common;
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
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.linkedin.databus2.util.DBHelper;

/**
 * @author lgao
 *
 */
public class SourceInfo
{
  public static final String      MODULE               = SourceInfo.class.getName();
  public static final Logger      LOG                  = Logger.getLogger(MODULE);
  
  private int  _srcid;
  private int  _currlogid;
  private int  _maxRowId;
  private long _minwindowscn;
  private long _maxwindowscn;
  private int  _status;

  public SourceInfo(int srcid,
                    int currlogid,
                    int maxRowId,
                    long minwindowscn,
                    long maxwindowscn,
                    int status)
  {
    _srcid = srcid;
    _currlogid = currlogid;
    _maxRowId = maxRowId;
    _minwindowscn = minwindowscn;
    _maxwindowscn = maxwindowscn;
    _status = status;
  }

  public int getStatus()
  {
	  return _status;
  }
  
  public int getSrcId()
  {
    return _srcid;
  }

  public int getCurrLogId()
  {
    return _currlogid;
  }

  public int getNumRows()
  {
    return _maxRowId;
  }

  public long getMinWindowScn()
  {
    return _minwindowscn;
  }

  public long getMaxWindowScn()
  {
    return _maxwindowscn;
  }

  public void setStatus(int status)
  {
	  _status = status;
  }
  
  public void setMaxRowId(int maxRowId)
  {
    _maxRowId = maxRowId;
  }

  public void setWindowScn(long windowscn)
  {
    if (_minwindowscn == -1)
      _minwindowscn = windowscn;

    _maxwindowscn = windowscn;
  }

  public void saveToDB(Connection conn) throws SQLException
  {
	PreparedStatement stmt = null;	  
    try
    {
      stmt =
          conn.prepareStatement("update bootstrap_loginfo set minwindowscn = ? , maxwindowscn = ? , maxrid = ? where srcid = ? and logid = ?");

      stmt.setLong(1, _minwindowscn);
      stmt.setLong(2, _maxwindowscn);
      stmt.setInt(3, _maxRowId);
      stmt.setInt(4, _srcid);
      stmt.setInt(5, _currlogid);

      stmt.executeUpdate();
      conn.commit();
    }
    catch (SQLException e)
    {
      LOG.error("Error occurred when saving source info to database" + e);
      throw e;
    } finally {
    	DBHelper.close(stmt);
    }
  }

  public void switchLogFile(Connection conn) throws SQLException
  {
    _currlogid++;
    _maxRowId = 0;
    _minwindowscn = -1;
    _maxwindowscn = -1;

    PreparedStatement stmt = null;
    try
    {
       stmt =
        conn.prepareStatement("insert into bootstrap_loginfo(srcid, logid) values(?,?)");

      stmt.setInt(1, _srcid);
      stmt.setInt(2, _currlogid);
      stmt.executeUpdate();
    }
    catch (SQLException e)
    {
      LOG.error("Error occurred when saving source info to database" + e);
      throw e;
    } finally {
    	DBHelper.close(stmt);
    }
  }
  
  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append("srcid=" + _srcid);
    sb.append("; logid=" + _currlogid);
    sb.append("; maxRowId=" + _maxRowId);
    sb.append("; minwindowscn=" + _minwindowscn);
    sb.append("; maxwindowscn=" + _maxwindowscn);
    sb.append("; status=" + _status);

    return sb.toString();
  }
}
