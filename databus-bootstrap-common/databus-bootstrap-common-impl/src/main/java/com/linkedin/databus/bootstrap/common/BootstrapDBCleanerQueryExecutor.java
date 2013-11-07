package com.linkedin.databus.bootstrap.common;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.linkedin.databus.bootstrap.common.BootstrapCleanerStaticConfig.BootstrapDBType;
import com.linkedin.databus.core.DatabusRuntimeException;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventFactory;
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

public class BootstrapDBCleanerQueryExecutor
{
  public static final String MODULE = BootstrapDBCleanerQueryExecutor.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private final Connection _conn;
  private final BootstrapDBCleanerQueryHelper _bootstrapDBCleanerQueryHelper;

  public BootstrapDBCleanerQueryExecutor(Connection conn, BootstrapDBCleanerQueryHelper bootstrapDBCleanerQueryHelper)
  {
    if (null != conn)
    {
      _conn = conn;
    }
    else
    {
      throw new DatabusRuntimeException("Invalid to pass in a null connection object");
    }

    if (null != bootstrapDBCleanerQueryHelper)
    {
      _bootstrapDBCleanerQueryHelper = bootstrapDBCleanerQueryHelper;
    }
    else
    {
      throw new DatabusRuntimeException("Invalid to pass in a null for bootstrapDBCleanerQueryHelper");
    }

  }

  /*
   * @return the LogInfo(srcId,logId,windowSCN) for the entry in
   * bootstrap_applier_state with minimum SCN
   */
  public BootstrapLogInfo getThresholdWindowSCN(BootstrapDBType type, int srcId)
      throws SQLException
  {
    String sql = null;

    switch (type)
    {
    case BOOTSTRAP_CATCHUP_APPLIER_NOT_RUNNING:
      sql = getMinProducerSCNLogStmt(srcId);
      break;
    case BOOTSTRAP_CATCHUP_APPLIER_RUNNING:
    case BOOTSTRAP_FULL:
      sql = getMinApplierSCNLogStmt(srcId);
      break;
    }

    LOG.info("Threshold WindowScn fetch Query :" + sql);
    BootstrapLogInfo logInfo = new BootstrapLogInfo();
    ResultSet rs = null;
    PreparedStatement stmt = null;
    try
    {
      stmt = _conn.prepareStatement(sql);
      rs = stmt.executeQuery();
      if (!rs.next())
      {
        LOG.error("SQL :(" + sql + ") returned empty records");
        return null;
      }

      logInfo.setLogId(rs.getInt(1));
      logInfo.setSrcId(rs.getShort(2));
      logInfo.setMinWindowSCN(rs.getLong(3));
      logInfo.setMaxWindowSCN(rs.getLong(3));

    } finally
    {
      DBHelper.close(rs, stmt, null);
    }

    return logInfo;
  }

  /*
   * @param srcId : SourceId
   *
   * @param windowSCN : windowSCN to be checked
   *
   * @return the LogInfo(srcId,logId,minWindowSCN) for the entry in
   * bootstrap_loginfo with minimum SCN > passed SCN
   */
  public BootstrapLogInfo getFirstLogTableWithGreaterSCN(short srcId, long windowSCN)
      throws SQLException
  {
    String sql = _bootstrapDBCleanerQueryHelper.getFirstLogTableWithGreaterSCNStmt();
    ResultSet rs = null;
    PreparedStatement stmt = null;
    BootstrapLogInfo logInfo = new BootstrapLogInfo();

    try
    {
      stmt = _conn.prepareStatement(sql);
      stmt.setShort(1, srcId);
      stmt.setLong(2, windowSCN);
      rs = stmt.executeQuery();

      if (rs.next())
      {
        logInfo.setSrcId(srcId);
        logInfo.setLogId(rs.getInt(1));
        logInfo.setMinWindowSCN(rs.getLong(2));
        logInfo.setMaxWindowSCN(rs.getLong(3));
      }
      else
      {
        return null;
      }
    } finally
    {
      DBHelper.close(rs, stmt, null);
    }
    return logInfo;
  }

  /*
   * Find the list of logTables for a source that are candidates for cleanup
   *
   * @param windowSCN : MinimumSCN in bootstrap_applier_state table
   *
   * @param srcid : SourceId
   *
   * @return list of LogInfo for log tables for the source that can be removed
   * in descending order of logIds
   */
  public List<BootstrapLogInfo> getCandidateLogsInfo(long windowSCN, short srcId)
      throws SQLException
  {
    List<BootstrapLogInfo> logsInfo = new ArrayList<BootstrapLogInfo>();
    String sql = _bootstrapDBCleanerQueryHelper.getCandidateLogIdsForSrcStmt();
    LOG.info("SQL statement for fetching candidate logIds :" + sql);

    ResultSet rs = null;
    PreparedStatement stmt = null;
    try
    {
      stmt = _conn.prepareStatement(sql);
      stmt.setShort(1, srcId);
      stmt.setLong(2, windowSCN);
      stmt.setShort(3, srcId);
      rs = stmt.executeQuery();

      while (rs.next())
      {
        BootstrapLogInfo logInfo = new BootstrapLogInfo();
        logInfo.setSrcId(srcId);
        logInfo.setLogId(rs.getInt(1));
        logInfo.setMinWindowSCN(rs.getLong(2));
        logInfo.setMaxWindowSCN(rs.getLong(3));
        logsInfo.add(logInfo);
      }
    } finally
    {
      DBHelper.close(rs, stmt, null);
    }
    return logsInfo;
  }

  public void updateSource(BootstrapLogInfo logInfo) throws SQLException
  {
    LOG.info("Updating logStartSCN to " + logInfo.getMinWindowSCN()
        + " for srcid :" + logInfo.getSrcId());
    String sql = _bootstrapDBCleanerQueryHelper.getUpdateLogStartSCNStmt();
    PreparedStatement stmt = null;
    try
    {
      stmt = _conn.prepareStatement(sql);
      stmt.setLong(1, logInfo.getMinWindowSCN());
      stmt.setShort(2, logInfo.getSrcId());
      stmt.executeUpdate();
    } finally
    {
      DBHelper.close(stmt);
    }
  }

  /*
   * Drop Tables corresponding to the list of LogInfo
   *
   * @param logsInfo : LogInfo of the tables needed to be deleted
   *
   * @return the list of LogInfo corresponding to tables that have been
   * successfully dropped.
   */
  public List<BootstrapLogInfo> dropTables(List<BootstrapLogInfo> logsInfo)
  {
    List<BootstrapLogInfo> deletedLogsInfo = new ArrayList<BootstrapLogInfo>();

    for (BootstrapLogInfo logInfo : logsInfo)
    {
      try
      {
        dropTable(logInfo);
        deletedLogsInfo.add(logInfo);
      } catch (SQLException ex)
      {
        LOG.error("Unable to delete log table :" + logInfo.getLogTable());
      }
    }
    return deletedLogsInfo;
  }

  /*
   * Mark Tables deleted corresponding to the list of LogInfo
   *
   * @param logsInfo : LogInfo of the tables needed to be marked
   *
   * @return the list of LogInfo corresponding to tables that have been
   * successfully marked.
   */
  public List<BootstrapLogInfo> markDeleted(List<BootstrapLogInfo> logsInfo) throws SQLException
  {
    Connection conn = null;
    PreparedStatement stmt = null;
    List<BootstrapLogInfo> deletedLogsInfo = new ArrayList<BootstrapLogInfo>();
    String sql = _bootstrapDBCleanerQueryHelper.getMarkDeletedStmt();
    try
    {
      stmt = _conn.prepareStatement(sql);
      for (BootstrapLogInfo logInfo : logsInfo)
      {
        try
        {
          LOG.info("Marking table : " + logInfo.getLogTable() + " deleted !!");
          stmt.setShort(1, logInfo.getSrcId());
          stmt.setInt(2, logInfo.getLogId());
          stmt.executeUpdate();
          deletedLogsInfo.add(logInfo);
        } catch (SQLException ex)
        {
          LOG.error("Unable to mark delete log table :" + logInfo.getLogTable()
              + " in bootstrap_loginfo");
          throw ex;
        }
      }
    } finally
    {
      DBHelper.close(stmt);
    }
    return deletedLogsInfo;
  }

  public long getNanoTimestampOfLastEventinLog(BootstrapLogInfo logInfo, DbusEventFactory eventFactory)
  throws SQLException
  {
    long nanoTimestamp = Long.MAX_VALUE;
    BootstrapDBRow r = getLastEventinLog(logInfo, eventFactory);

    if ((null != r) && (null != r.getEvent()))
    {
      nanoTimestamp = r.getEvent().timestampInNanos();
    }
    return nanoTimestamp;
  }

  public long getSCNOfLastEventinLog(BootstrapLogInfo logInfo, DbusEventFactory eventFactory)
  throws SQLException
  {
    long scn = -1;
    BootstrapDBRow r = getLastEventinLog(logInfo, eventFactory);

    if (null != r)
    {
      scn = r.getScn();
    }
    return scn;
  }

  private BootstrapDBRow getLastEventinLog(BootstrapLogInfo logInfo, DbusEventFactory eventFactory)
  throws SQLException
  {
    Statement stmt = null;
    ResultSet rs = null;
    long id = -1;
    long scn = -1;
    String key = null;
    DbusEvent event = null;
    try
    {
      String tableName = logInfo.getLogTable();
      StringBuilder sql = new StringBuilder();
      sql.append("select id, srckey, scn, val from ");
      sql.append(tableName);
      sql.append(" where id = ( select max(id) from ");
      sql.append(tableName);
      sql.append(" )");
      stmt = _conn.createStatement();
      rs = stmt.executeQuery(sql.toString());
      if (rs.next())
      {
        int i = 1;
        id = rs.getLong(i++);
        String srcKey = rs.getString(i++);
        scn = rs.getLong(i++);
        ByteBuffer tmpBuffer = ByteBuffer.wrap(rs.getBytes(i));
        LOG.info("BUFFER SIZE:" + tmpBuffer.limit());
        event = eventFactory.createReadOnlyDbusEventFromBuffer(tmpBuffer,
            tmpBuffer.position());
        LOG.info("Last Row for log (" + logInfo + ") - ID :" + id
            + ", srcKey :" + srcKey + ", SCN :" + scn + ", Event :"
            + event.toString());
      }
      else
      {
        LOG.error("No ResultSet for query :" + sql.toString());
      }

    } finally
    {
      DBHelper.close(rs, stmt, null);
    }
    return new BootstrapDBRow(id, key, scn, event);
  }

  /*
   * Drops a table corresponding to the passed logInfo
   *
   * @param logInfo : LogInfo of the table to be dropped.
   */
  private void dropTable(BootstrapLogInfo logInfo)
  throws SQLException
  {
    String tableName = logInfo.getLogTable();
    String cmd = "drop table if exists " + tableName;
    Statement stmt = null;
    try
    {
      LOG.info("Dropping table :" + tableName);
      stmt = _conn.createStatement();
      stmt.executeUpdate(cmd);
    } finally
    {
      DBHelper.close(stmt);
    }
  }

  public int optimizeTable(String tableName)
  throws SQLException
  {
    final String cmd = "optimize table " + tableName;
    PreparedStatement stmt = null;
    int ret = -1;
    try
    {
      LOG.info("Optimizing table :" + tableName + ", CMD :(" + cmd + ")");
      stmt = _conn.prepareStatement(cmd);
      ret = stmt.executeUpdate();
    } finally
    {
      DBHelper.close(stmt);
    }
    return ret;
  }

  public int deleteTable(String tableName, long scn)
  throws SQLException
  {
    final String cmd = "delete from " + tableName + " where scn <= ?";
    int ret = -1;
    if (scn <= 0)
    {
      LOG.info("Trying to delete rows whose scn <=" + scn + ", skipping !!");
      return ret;
    }

    PreparedStatement stmt = null;
    try
    {
      LOG.info("Deleting events from table :" + tableName + ", CMD :(" + cmd
          + "), SCN :" + scn);
      stmt = _conn.prepareStatement(cmd);
      stmt.setLong(1, scn);
      ret = stmt.executeUpdate();
    } finally
    {
      DBHelper.close(stmt);
    }
    return ret;
  }

  private String getMinApplierSCNLogStmt(int srcId)
  {
    StringBuilder sql = new StringBuilder();
    sql.append("select logid,srcid,windowscn from bootstrap_applier_state ");
    sql.append("where srcid=");
    sql.append(srcId);
    sql.append(" order by windowscn asc limit 1");
    return sql.toString();
  }

  private String getMinProducerSCNLogStmt(int srcId)
  {
    StringBuilder sql = new StringBuilder();
    sql.append("select logid,srcid,windowscn from bootstrap_producer_state ");
    sql.append("where srcid=");
    sql.append(srcId);
    sql.append(" order by windowscn asc limit 1");
    return sql.toString();
  }
}
