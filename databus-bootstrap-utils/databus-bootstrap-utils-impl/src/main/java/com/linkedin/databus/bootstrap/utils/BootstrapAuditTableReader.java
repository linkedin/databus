package com.linkedin.databus.bootstrap.utils;
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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.log4j.Logger;

import com.linkedin.databus2.util.DBHelper;

public abstract class BootstrapAuditTableReader
{
  public static final String MODULE = BootstrapAuditTableReader.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);
  public static final boolean _sDebug = LOG.isDebugEnabled();

	protected final Connection         _conn;
	protected final String			   _tableName;
	protected final Field              _srcPkeyField;
	protected final Type               _pkeyType;
	protected final String             _pkeyName;
	protected final int                _interval;

	public BootstrapAuditTableReader(
	                    Connection conn,
						String tableName,
						Field  pkeyField,
						String pkeyName,
						Type   pkeyType,
						int    interval)
	{
		_conn = conn;
		_tableName = tableName;
		_srcPkeyField = pkeyField;
		_pkeyType  = pkeyType;
		_pkeyName = pkeyName;
		_interval = interval;
	}

	public void close()
	{
	}
/**
 * Used in one-off situation to find the number of rows in the given table
 * @param key
 * @return
 */
	public long getNumRecords(String key)
	{
		String sql = getNumRecordsStmt(key);
		PreparedStatement stmt = null;
		ResultSet rs = null;
		long numRecords = -1;
		try
		{
			stmt = _conn.prepareStatement(sql);
			rs = stmt.executeQuery();
			if (rs.next())
			{
				numRecords = rs.getLong(1);
			}
		} catch ( SQLException sqlEx) {
		    LOG.error("getNumRecords() error: " + sqlEx.getMessage(), sqlEx);
		} finally {
			DBHelper.close(rs,stmt,null);
		}
		return numRecords;
	}

  /**
   *
   * @param from : Defines lower bound of srckey  (> from)
   * @return resultSet: consists of rows sorted in ascending order of srckey
   * Note that stmt's lifetime is managed by subclass
   * @throws SQLException
   */
  public ResultSet getRecords(String from) throws SQLException
  {
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try
    {
      stmt = getFetchStmt(from);
      // stmt.setFetchSize(10000);
      // stmt.setMaxRows(10);
      rs = stmt.executeQuery();
    }
    catch (SQLException sqlEx)
    {
      DBHelper.close(rs, null, null);
      throw sqlEx;
    }
    return rs;
  }

	private String getNumRecordsStmt(String key)
	{
		StringBuilder sql = new StringBuilder();

		sql.append("select count(" + key + ") from ");
		sql.append(_tableName);
		return sql.toString();
	}

	/*
	 * This is the audit record containing both the source row
	 * and avro rows to be audited
	 */
	public static class ResultSetEntry
	{
	  public Long getId()
      {
        return _id;
      }

      public void setId(Long id)
      {
        this._id = id;
      }

      public byte[] getAvroRecord()
      {
        return _avroRecord;
      }

      public void setAvroRecord(byte[] avroRecord)
      {
        this._avroRecord = avroRecord;
      }

      public Map<String,Object> getSourceRecord()
      {
        return _sourceRecord;
      }

      public void setSourceRecord(Map<String,Object> sourceRecord)
      {
        this._sourceRecord = sourceRecord;
      }

      public ResultSetEntry(long id)
      {
        _id = id;
        _sourceRecord = new HashMap<String, Object>();
      }

      private Long   _id;
	  private byte[] _avroRecord;
	  private Map<String,Object> _sourceRecord;
	}

	/*
	 * @param from - The pKey to start streaming
	 * @return PreparedStatement of the query for streaming records
	 */
	public abstract PreparedStatement getFetchStmt(String from)
			throws SQLException;
}
