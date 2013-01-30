/**
 *
 */
package com.linkedin.databus.bootstrap.server;
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
import java.util.ArrayList;

import org.apache.log4j.Logger;
import com.linkedin.databus.bootstrap.api.BootstrapEventCallback;
import com.linkedin.databus.bootstrap.api.BootstrapEventProcessResult;
import com.linkedin.databus.bootstrap.api.BootstrapProcessingException;
import com.linkedin.databus.bootstrap.common.BootstrapConn;
import com.linkedin.databus.bootstrap.common.BootstrapDBMetaDataDAO;
import com.linkedin.databus.bootstrap.common.BootstrapReadOnlyConfig;
import com.linkedin.databus2.core.container.request.BootstrapDatabaseTooOldException;
import com.linkedin.databus2.core.filter.DbusFilter;
import com.linkedin.databus2.core.filter.DbusKeyFilter;
import com.linkedin.databus2.core.filter.FilterToSQL;
import com.linkedin.databus2.util.DBHelper;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DbusClientMode;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;

/**
 * @author ksurlake
 * 
 */
public class BootstrapProcessor
{
  public static final String            MODULE                      =
      BootstrapProcessor.class.getName();
  public static final Logger            LOG                         =
      Logger.getLogger(MODULE);

  public final static String            EVENT_COLUMNS               = "val";
  public final static String            PHASE_COMPLETED_HEADER_NAME = "PhaseCompleted";
  public final static String            EMPTY_STRING = "";
  public final static boolean          DEFAULT_PREDICATE_OPTIMIZATION = true;

  private final long                    _maxRowsPerFetch;
  private BootstrapDBMetaDataDAO                _dbDao;
  private DbusEventsStatisticsCollector _curStatsCollector;
  private DbusKeyFilter                 keyFilter;
  private boolean predicatePushDownOptimization = DEFAULT_PREDICATE_OPTIMIZATION;
  
  public BootstrapProcessor(BootstrapReadOnlyConfig config,
                            DbusEventsStatisticsCollector curStatsCollector) throws InstantiationException,
                            IllegalAccessException,
                            ClassNotFoundException,
                            SQLException
                            {
    _curStatsCollector = curStatsCollector;
     BootstrapConn dbConn = new BootstrapConn();
    dbConn.initBootstrapConn(true,
                              config.getBootstrapDBUsername(),
                              config.getBootstrapDBPassword(),
                              config.getBootstrapDBHostname(),
                              config.getBootstrapDBName());
    _dbDao = new BootstrapDBMetaDataDAO(dbConn,
    							config.getBootstrapDBHostname(),
    							config.getBootstrapDBUsername(),
    							config.getBootstrapDBPassword(),
    							config.getBootstrapDBName(),
    							false);
    
    _maxRowsPerFetch = config.getBootstrapBatchSize();
    if (LOG.isDebugEnabled())
    {
    	LOG.debug("BootstrapProcessor: config=" + config + ", dbConn=" + dbConn);
    }
                            }

  public DbusKeyFilter getKeyFilter()
  {
    return keyFilter;
  }

  public void setKeyFilter(DbusKeyFilter keyFilter)
  {
    this.keyFilter = keyFilter;
  }

  public boolean getPredicatePushDownOptimization()
  {
    return predicatePushDownOptimization;
  }

  public void setPredicatePushDownOptimization(boolean predicatePushDownOptimization)
  {
    this.predicatePushDownOptimization = predicatePushDownOptimization;
  }

  // Get specified number of catchup rows
  public boolean streamCatchupRows(Checkpoint currState, BootstrapEventCallback callBack) throws SQLException,
  BootstrapProcessingException,
  BootstrapDatabaseTooOldException
  {
    assert (currState.getConsumptionMode() == DbusClientMode.BOOTSTRAP_CATCHUP);
    boolean foundRows = false;

    BootstrapDBMetaDataDAO.SourceStatusInfo srcIdStatusPair = _dbDao.getSrcIdStatusFromDB(currState.getCatchupSource(),true);
    
    if ( !srcIdStatusPair.isValidSource())
    	throw new BootstrapProcessingException("Bootstrap DB not servicing source :" + currState.getCatchupSource());
    	
    int curSrcId = srcIdStatusPair.getSrcId();

    int curLogId = _dbDao.getLogIdToCatchup(curSrcId, currState.getWindowScn());
    int targetLogId = _dbDao.getLogIdToCatchup(curSrcId, currState.getBootstrapTargetScn());

    boolean phaseCompleted = false;
    PreparedStatement stmt = null;

    try
    {
      ResultSet rs = null;
      while (!foundRows && curLogId <= targetLogId)
      {
        stmt = createCatchupStatement(curSrcId, curLogId, currState);
        rs = stmt.executeQuery();

        foundRows = rs.isBeforeFirst();
        if (!foundRows)
        {
          curLogId++; // move to next log
          currState.setCatchupOffset(0); // reset rid to 0 for the next log
          LOG.info("Moving to next log table log_" + curSrcId + "_" + curLogId
                   + " because current log table exhausted!");
        }
      }

      phaseCompleted = streamOutRows(currState, rs, callBack);
    }
    catch (SQLException e)
    {
      LOG.error("Exception occured during fetching catchup rows" + e);
      throw e;
    }
    finally
    {
      if (stmt != null)
      {
        stmt.close();
        stmt = null;
      }
      mergeAndResetStats();
    }

    return phaseCompleted;
  }


  private String getFilterSQL()
  {
    if(keyFilter == null) // No filter is defined.
      return EMPTY_STRING;

    ArrayList<DbusFilter> filters = keyFilter.getFilters();
    ArrayList<String> filterStrings = new ArrayList<String>(filters.size());
    for (int i = 0; i < filters.size(); i++)
    {
      String filterStringTemp = FilterToSQL.convertToSQL(filters.get(i));
      if( filterStringTemp != EMPTY_STRING)
        filterStrings.add(filterStringTemp);
    }
    
    //check for none partitions - do we have any filters to apply ?
    if(filterStrings.size() == 0)
      return EMPTY_STRING;

    //build the filter string
    StringBuilder filterSqlBuilder = new StringBuilder();
    filterSqlBuilder.append(" ( ");
    for (int i = 0; i < filterStrings.size(); i++)
    {
      filterSqlBuilder.append(filterStrings.get(i));
      if(i!=filterStrings.size()-1)
        filterSqlBuilder.append(" OR ");
    }
    filterSqlBuilder.append(" ) ");

    return filterSqlBuilder.toString();
  }

  public String getCatchupSQLString(String catchupTab)
  {
    StringBuilder sql = new StringBuilder();
    String filterSql = getFilterSQL();
    sql.append("Select ");
    sql.append("id, ");
    sql.append("scn, ");
    sql.append("windowscn, ");
    sql.append(EVENT_COLUMNS);
    if (getPredicatePushDownOptimization() && !filterSql.isEmpty() && filterSql != null)
      sql.append(", CAST(srckey as SIGNED) as srckey");
    sql.append(" from ");
    sql.append(catchupTab);
    sql.append(" where ");
    sql.append(" id > ? ");
 	sql.append(" and windowscn >= ? and windowscn <= ? ");
    sql.append(" and windowscn >= ? ");    
    if (getPredicatePushDownOptimization() && !filterSql.isEmpty() && filterSql !=null )
      sql.append("AND " + filterSql);
    sql.append(" order by id limit ?");
    
    
    return sql.toString();
  }

  public String getSnapshotSQLString(String snapShotTable)
  {
    StringBuilder sql = new StringBuilder();    
    String filterSql = getFilterSQL();
    sql.append("Select ");
    sql.append("id, ");
    sql.append("scn, ");
    if (getPredicatePushDownOptimization() && !filterSql.isEmpty() && filterSql != null)
      sql.append(" CAST(srckey as SIGNED) as srckey, ");
    else
      sql.append("srckey, ");
    sql.append(EVENT_COLUMNS);
    sql.append(" from ");
    sql.append(snapShotTable);
    sql.append(" where ");
    sql.append(" id > ? ");
    sql.append(" and scn < ? ");
    sql.append(" and scn >= ? ");
    if (getPredicatePushDownOptimization() && !filterSql.isEmpty() && filterSql != null)
      sql.append("AND " + filterSql);
    sql.append(" order by id limit ?");
    return sql.toString();
  }

  // TODO: DDSDBUS-345 : Bootstrap Serving might be incorrect when multiple bootstrap
  // servers are serving
  private PreparedStatement createCatchupStatement(int srcId,
                                                   int logId,
                                                   Checkpoint currState) throws SQLException                
  {
    Connection conn = _dbDao.getBootstrapConn().getDBConn();
    String catchupTab = "log_" + srcId + "_" + logId;
    PreparedStatement stmt = null;
    String catchUpString = getCatchupSQLString(catchupTab);
    long offset = -1;
    try
    {
    	stmt = conn.prepareStatement(catchUpString);
    	offset = currState.getWindowOffset();
    	int i = 1;
    	stmt.setLong(i++, offset);
    	stmt.setLong(i++, currState.getBootstrapStartScn());
    	stmt.setLong(i++, currState.getBootstrapTargetScn());
    	stmt.setLong(i++, currState.getBootstrapSinceScn());
    	stmt.setLong(i++, _maxRowsPerFetch);
    } catch (SQLException ex) {
    	DBHelper.close(stmt);
    }
    
    LOG.info("Catchup SQL String: "
                       + catchUpString 
                       + ", " + offset 
                       + ", " +  currState.getBootstrapStartScn() 
                       + " , " + currState.getBootstrapTargetScn()
                       + " , " + currState.getBootstrapSinceScn() 
                       + " , " + _maxRowsPerFetch);
    return stmt;
  }

  // Get specificed number of snapshot rows
  public boolean streamSnapShotRows(Checkpoint currState, BootstrapEventCallback callBack) 
		  throws SQLException, BootstrapProcessingException,BootstrapDatabaseTooOldException
  {
    assert (currState.getConsumptionMode() == DbusClientMode.BOOTSTRAP_SNAPSHOT);

    boolean phaseCompleted = false;

    long startSCN = currState.getBootstrapStartScn();
    long sinceSCN = currState.getBootstrapSinceScn();

    if (startSCN <= sinceSCN)
    {
      LOG.info("StartSCN is less than or equal to sinceSCN. Bypassing snapshot phase !! startSCN:"
          + startSCN + ",sinceSCN:" + sinceSCN);
      return true;
    }

    Connection conn = _dbDao.getBootstrapConn().getDBConn();
    BootstrapDBMetaDataDAO.SourceStatusInfo srcIdStatusPair = _dbDao.getSrcIdStatusFromDB(currState.getSnapshotSource(), true);

    if (!srcIdStatusPair.isValidSource())
      throw new BootstrapProcessingException("Bootstrap DB not servicing source :"
          + currState.getCatchupSource());

    PreparedStatement stmt = null;
    ResultSet rs = null;
    try
    {
      String snapshotSQL = getSnapshotSQLString(_dbDao.getBootstrapConn().getSrcTableName(srcIdStatusPair.getSrcId()));
      stmt =
          conn.prepareStatement(snapshotSQL);
      long offset = currState.getSnapshotOffset();
      int i = 1;
      stmt.setLong(i++, offset);
      // stmt.setLong(i++, offset + numRows);
      stmt.setLong(i++, currState.getBootstrapStartScn());
      stmt.setLong(i++, currState.getBootstrapSinceScn());
      stmt.setLong(i++, _maxRowsPerFetch);
     
      LOG.info("SnapshotSQL string: " 
               + snapshotSQL
               + ", " + offset
               + ", " + currState.getBootstrapStartScn()
               + ", " + currState.getBootstrapSinceScn()
               + ", "  + _maxRowsPerFetch);
      
      rs = stmt.executeQuery();
      phaseCompleted = streamOutRows(currState, rs, callBack);
    }
    catch (SQLException e)
    {
      DBHelper.close(rs, stmt, null);	
      LOG.error("Exception occurred when getting snapshot rows" + e);
      throw e;
    }
    finally
    {
      if (stmt != null)
      {
        stmt.close();
        stmt = null;
      }
      mergeAndResetStats();
    }

    return phaseCompleted;
  }

  private boolean streamOutRows(Checkpoint ckpt,
                                ResultSet rs,
                                BootstrapEventCallback callback) throws SQLException,
                                BootstrapProcessingException
                                {
    BootstrapEventProcessResult result = null;
    long windowScn = Long.MIN_VALUE;

    while (rs.next())
    {
      long rid = rs.getLong(1);

      result = callback.onEvent(rs, _curStatsCollector);
      if (result.isClientBufferLimitExceeded())
      { // This break is important because we don't want to checkpoint the current
        // row if it is not sent due to client buffer space limitation.
        break;
      }

      if (DbusClientMode.BOOTSTRAP_SNAPSHOT == ckpt.getConsumptionMode())
      {
        ckpt.onSnapshotEvent(rid);
      }
      else if (DbusClientMode.BOOTSTRAP_CATCHUP == ckpt.getConsumptionMode())
      {
        windowScn = rs.getLong(3);
        ckpt.onCatchupEvent(windowScn, rid);
      }
    }

    if (null != result && result.getProcessedRowCount() > 0)
    { // don't send checkpoint if nothing has been sent
      callback.onCheckpointEvent(ckpt, _curStatsCollector);
    }

    LOG.info("Terminating batch with result: " + result);

    boolean isPhaseCompleted = false;
    if (null == result
        || !result.isClientBufferLimitExceeded()
        && result.getProcessedRowCount() < _maxRowsPerFetch
        && (DbusClientMode.BOOTSTRAP_SNAPSHOT == ckpt.getConsumptionMode() || DbusClientMode.BOOTSTRAP_CATCHUP == ckpt.getConsumptionMode()
        && ckpt.getBootstrapTargetScn() == windowScn))
    { // if we get here w/o having filled client buffer and the total
      // rows processed is less than the max rows per fetch in snapshot mode;
      // or in catchup mode we are up to the targetScn, we have
      // finished the current phase (snapshot or catchup)
      isPhaseCompleted = true;
    }

    if (DbusClientMode.BOOTSTRAP_CATCHUP == ckpt.getConsumptionMode()
        && ckpt.getBootstrapTargetScn() < windowScn)
    {
      throw new RuntimeException("Events with higher windowscn delivered: bootstrapTargetScn="
          + ckpt.getBootstrapTargetScn() + " event windowscn=" + windowScn);
    }

    return isPhaseCompleted;
                                }

  private void mergeAndResetStats()
  {
    // TODO DDS-302 merge into the global stats collector
    /*
     * StatisticsCollector statsColl =
     * (StatisticsCollector)_configManager.getStaticConfig(
     * ).getStatisticsCollectorMBean(); statsColl.merge(_curStatsCollector);
     * LOG.info("Events streamed out = " +
     * statsColl.getOutboundTrafficTotalStats().getNumDataEvents());
     * _curStatsCollector.reset();
     */
  }

  public void shutdown()
  {
	if (null != _dbDao)
	{
		_dbDao.getBootstrapConn().close();
		_dbDao = null;
	}
  }
}
