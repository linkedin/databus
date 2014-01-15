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
import com.linkedin.databus.bootstrap.common.BootstrapDBTimedQuery;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DbusClientMode;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus2.core.container.request.BootstrapDatabaseTooOldException;
import com.linkedin.databus2.core.container.request.BootstrapDatabaseTooYoungException;
import com.linkedin.databus2.core.filter.DbusFilter;
import com.linkedin.databus2.core.filter.DbusKeyFilter;
import com.linkedin.databus2.core.filter.FilterToSQL;
import com.linkedin.databus2.util.DBHelper;


public class BootstrapProcessor
{
  public static final String            MODULE                      =
      BootstrapProcessor.class.getName();
  public static final Logger            LOG                         =
      Logger.getLogger(MODULE);

  public final static String            EVENT_COLUMNS               = "val";
  public final static String            PHASE_COMPLETED_HEADER_NAME = "PhaseCompleted";
  public final static String            PHASE_COMPLETED_HEADER_TRUE = "TRUE";
  public final static String            EMPTY_STRING = "";
  private final long                    _maxSnapshotRowsPerFetch;
  private final long                    _maxCatchupRowsPerFetch;
  private final int                     _queryTimeInSec;
  private BootstrapDBMetaDataDAO                _dbDao;
  private final DbusEventsStatisticsCollector _curStatsCollector;
  private DbusKeyFilter                 keyFilter;
  //Bootstrap server config
  BootstrapServerStaticConfig config;

  public BootstrapProcessor(BootstrapServerStaticConfig config,
                            DbusEventsStatisticsCollector curStatsCollector) throws InstantiationException,
                            IllegalAccessException,
                            ClassNotFoundException,
                            SQLException
  {
    _curStatsCollector = curStatsCollector;
    BootstrapConn dbConn = new BootstrapConn();
    this.config = config;
    final boolean autoCommit = true;
    dbConn.initBootstrapConn(autoCommit,
                              config.getDb().getBootstrapDBUsername(),
                              config.getDb().getBootstrapDBPassword(),
                              config.getDb().getBootstrapDBHostname(),
                              config.getDb().getBootstrapDBName());
    _dbDao = new BootstrapDBMetaDataDAO(dbConn,
    							config.getDb().getBootstrapDBHostname(),
    							config.getDb().getBootstrapDBUsername(),
    							config.getDb().getBootstrapDBPassword(),
    							config.getDb().getBootstrapDBName(),
    							autoCommit);

    _maxSnapshotRowsPerFetch = config.getDb().getBootstrapSnapshotBatchSize();
    _maxCatchupRowsPerFetch = config.getDb().getBootstrapCatchupBatchSize();
    _queryTimeInSec = config.getQueryTimeoutInSec();
    if (LOG.isDebugEnabled())
    {
    	LOG.debug("BootstrapProcessor: config=" + config + ", dbConn=" + dbConn);
    }
  }

  /**
   * Used for unit-testing only
   */
  protected BootstrapProcessor()
  {
    _curStatsCollector = null;
    _maxSnapshotRowsPerFetch = -1;
    _maxCatchupRowsPerFetch = -1;
    _queryTimeInSec = -1;
  }

  public DbusKeyFilter getKeyFilter()
  {
    return keyFilter;
  }

  public void setKeyFilter(DbusKeyFilter keyFilter)
  {
    this.keyFilter = keyFilter;
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
        rs = new BootstrapDBTimedQuery(stmt,_queryTimeInSec).executeQuery();

        foundRows = rs.isBeforeFirst();
        if (!foundRows)
        {
          curLogId++; // move to next log
          currState.setCatchupOffset(0); // reset rid to 0 for the next log
          LOG.info("Moving to next log table log_" + curSrcId + "_" + curLogId
                   + " because current log table exhausted!");
        }
      }

      phaseCompleted = streamOutRows(currState, rs, callBack,_maxCatchupRowsPerFetch);
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
      return getCatchupSQLString(catchupTab, null);
  }

  public String getCatchupSQLString(String catchupTab, String source)
  {
    StringBuilder sql = new StringBuilder();
    String filterSql = getFilterSQL();
    boolean predicatePushDown = config.isPredicatePushDownEnabled(source) && !filterSql.isEmpty() && filterSql != null;
    sql.append("Select ");
    sql.append("id, ");
    sql.append("scn, ");
    sql.append("windowscn, ");
    sql.append(EVENT_COLUMNS);
    if (predicatePushDown)
      sql.append(", CAST(srckey as SIGNED) as srckey");
    sql.append(" from ");
    sql.append(catchupTab);
    sql.append(" where ");
    sql.append(" id > ? ");
 	sql.append(" and windowscn >= ? and windowscn <= ? ");
    sql.append(" and windowscn >= ? ");
    if (predicatePushDown)
      sql.append("AND " + filterSql);
    sql.append(" order by id limit ?");


    return sql.toString();
  }

  public String getSnapshotSQLString(String snapShotTable)
  {
       return getSnapshotSQLString(snapShotTable, null);
  }

    public String getSnapshotSQLString(String snapShotTable, String source)
  {
    StringBuilder sql = new StringBuilder();
    String filterSql = getFilterSQL();
    boolean predicatePushDown = config.isPredicatePushDownEnabled(source) && !filterSql.isEmpty() && filterSql != null;
    sql.append("Select ");
    sql.append("id, ");
    sql.append("scn, ");
    if (predicatePushDown)
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
    if (predicatePushDown)
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
    String catchUpString = getCatchupSQLString(catchupTab, currState.getCatchupSource());
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
    	stmt.setLong(i++, _maxCatchupRowsPerFetch);
    } catch (SQLException ex) {
    	DBHelper.close(stmt);
    }

    LOG.info("Catchup SQL String: "
                       + catchUpString
                       + ", " + offset
                       + ", " +  currState.getBootstrapStartScn()
                       + " , " + currState.getBootstrapTargetScn()
                       + " , " + currState.getBootstrapSinceScn()
                       + " , " + _maxCatchupRowsPerFetch);
    return stmt;
  }

  // Get specificed number of snapshot rows
  public boolean streamSnapShotRows(Checkpoint currState, BootstrapEventCallback callBack)
		  throws SQLException, BootstrapProcessingException,BootstrapDatabaseTooOldException,BootstrapDatabaseTooYoungException
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
      if (config.isEnableMinScnCheck())
      {
        long minScn = _dbDao.getMinScnOfSnapshots(srcIdStatusPair.getSrcId());
        LOG.info("Min scn for tab tables is: " + minScn);
        if (minScn == BootstrapDBMetaDataDAO.DEFAULT_WINDOWSCN)
        {
          throw new BootstrapDatabaseTooYoungException("BootstrapDB has no minScn for these sources, but minScn check is enabled! minScn=" + minScn);
        }
        //Note: The cleaner deletes rows less than or equal to scn. Rows with scn=minScn are not available
        //sinceSCN should be greater than minScn, except when sinceSCN == minScn == 0.
        if ((sinceSCN <= minScn) && !(sinceSCN==0 && minScn==0))
        {
          LOG.error("Bootstrap Snapshot doesn't have requested data . sinceScn too old! sinceScn is " + sinceSCN +  " but minScn available is " + minScn);
          throw new BootstrapDatabaseTooYoungException("Min scn=" + minScn + " Since scn=" + sinceSCN);
        }
      }
      else
      {
        LOG.debug("Bypassing minScn check!");
      }
      String snapshotSQL = getSnapshotSQLString(_dbDao.getBootstrapConn().getSrcTableName(srcIdStatusPair.getSrcId()), currState.getSnapshotSource());
      stmt =
          conn.prepareStatement(snapshotSQL);
      long offset = currState.getSnapshotOffset();
      int i = 1;
      stmt.setLong(i++, offset);
      stmt.setLong(i++, currState.getBootstrapStartScn());
      stmt.setLong(i++, currState.getBootstrapSinceScn());
      stmt.setLong(i++, _maxSnapshotRowsPerFetch);
      LOG.info("SnapshotSQL string: "
               + snapshotSQL
               + ", " + offset
               + ", " + currState.getBootstrapStartScn()
               + ", " + currState.getBootstrapSinceScn()
               + ", "  + _maxSnapshotRowsPerFetch);

      rs = new BootstrapDBTimedQuery(stmt,_queryTimeInSec).executeQuery();
      phaseCompleted = streamOutRows(currState, rs, callBack,_maxSnapshotRowsPerFetch);
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
                                BootstrapEventCallback callback,
                                long maxRowsPerFetch) throws SQLException,
                                BootstrapProcessingException
  {
    BootstrapEventProcessResult result = null;
    long windowScn = Long.MIN_VALUE;
    long numRowsReadFromDb = 0;  // Number or rows returned in the result-set so far
    while (rs.next())
    {
      numRowsReadFromDb++;
      long rid = rs.getLong(1);

      result = callback.onEvent(rs, _curStatsCollector);
      if ((result.isClientBufferLimitExceeded()) || (result.isError()))
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
      else
      {
        String errMsg = "The checkpoint received by bootstrap server is neither SNAPSHOT nor CATCHUP" + ckpt;
        LOG.error(errMsg);
        throw new RuntimeException(errMsg);
      }
    }

    if(numRowsReadFromDb > maxRowsPerFetch)
    {
      String errMsg = "Number of rows read from DB = " + numRowsReadFromDb +
                      " are greater than sepcfied maxRowsPerFetch = " + maxRowsPerFetch;
      LOG.error(errMsg);
      throw new RuntimeException(errMsg);
    }

    // Sends checkpoint to client if prescribed conditions are met
    writeCkptIfAppropriate(result, callback, numRowsReadFromDb, ckpt, rs.getStatement().toString());

    // Computes whether or not "a bootstrap phase" has completed
    boolean isPhaseCompleted = computeIsPhaseCompleted(result, ckpt, numRowsReadFromDb, maxRowsPerFetch, windowScn);

    // Detect an error about having streamed higher SCNs than were requested.
    // TBD : Isn't this too late to have the check here ?
    if (DbusClientMode.BOOTSTRAP_CATCHUP == ckpt.getConsumptionMode()
        && ckpt.getBootstrapTargetScn() < windowScn)
    {
      throw new RuntimeException("Events with higher windowscn delivered: bootstrapTargetScn="
          + ckpt.getBootstrapTargetScn() + " event windowscn=" + windowScn);
    }

    return isPhaseCompleted;
  }

  /**
   * A checkpoint is sent if there are no errors (and)
   * 1. There are non-zero rows written on the channel to the client
   * 2. Or, if all the results in the chunked query response are filtered out.
   *    The filtering may be due to a user-level filter or a predicate push down filter
   */
  protected void writeCkptIfAppropriate(BootstrapEventProcessResult result,
                                      BootstrapEventCallback callback,
                                      long numRowsReadFromDb,
                                      Checkpoint ckpt,
                                      String resultSetStmtStr)
  throws SQLException
  {
    assert (null != callback);
    assert (numRowsReadFromDb >= 0);
    assert (null != ckpt);
    if (null != result && !result.isError())
    {
      assert (numRowsReadFromDb >= result.getNumRowsWritten());
      if (result.getNumRowsWritten() > 0)
      {
        callback.onCheckpointEvent(ckpt, _curStatsCollector);
      }
      else if ((result.getNumRowsWritten() == 0) && (numRowsReadFromDb > 0))
      {
        if (!result.isClientBufferLimitExceeded())
        {
          // The first sentence in the log message below is used for an integration test. Please do not change.
          LOG.info("All the rows read from DB have been filtered out by user-level filter. numRowsReadFromDb = "
              + numRowsReadFromDb + " sending checkpoint = " + ckpt);
          callback.onCheckpointEvent(ckpt, _curStatsCollector);
        }
        else
        {
          // pendingEvent header will be set
          LOG.info("There have been rowsReadFromDb that could not be written as the clientBufferLimit has been exceeded. " +
                   "A checkpoint will not be sent, but pendingEvent header will be set. numRowsReadFromDb = " +
                    numRowsReadFromDb + " checkpoint = " + ckpt);
        }
      }
      else
      {
        // For the case of predicatePushDownFilter=true, where no events have been read / none written on channel,
        // result == null and we will not enter this loop. That is, it is not possible that numRowsReadFromDb == 0
        // and result.getNumRowsWritten() == 0
        String errMsg = "This is an error-case that should not happen. First, there were no rows in the resultSet " +
            " Second, this is not a case where all the events have been filtered out. Both of these cannot happen simulateneously. " +
            " Debug information is logged below. " +
            " numRowsReadFromDb = " + numRowsReadFromDb +
            " result = " + result +
            " resultSet statement " + resultSetStmtStr;
        LOG.error(errMsg);
        throw new RuntimeException(errMsg);
      }
    }
  }

  /**
   * Phase Completed logic :
   *
   * Snapshot Mode: ( All 3 cases : No Filtering, Push-Down Filter, User-Level Filter)
   * Phase is said to be completed if (a) result == null (implies no rows were selected
   * in the last chunked-select query in tab table) or (b) Number of rows read from the
   * last select is less than limit and client buffer size is exceeded.
   *
   * Please note there could be error cases where numRowsReadFromDB is less than is less
   * than maxRowsPerFetch ( and client buffer not exceeded). In this case, phase is not
   * set to be completed.
   *
   * Catchup Mode:
   *
   * A single Select query will NOT span tables.So Client buffer not being exceeded and
   * numRowsReadFromDB being less than the limit are not sufficient conditions for phase
   * completed as this condition could be true when we are streaming out log tables that
   * are older than the one containing targetSCN.
   *
   * There are 2 cases w.r.t to whether we saw events (from the select query) with
   * windowSCN that matches targetSCN
   *
   * 1. Events will be seen (a) For the No filtering and User-level filtering case when
   * there are atleast one event in the targetSCN window for the source (b) For the
   * Push-Down Filter case, only if the window contains events that match the filter 2.
   * Events will not be seen Otherwise
   *
   * Also, Please note a single window will not span multiple log tables.
   *
   * Hence, For (1), we will be able to detect the phase completion by also ensuring
   * targetSCN matches windowSCN of the last event read. For (2), after streaming out
   * the last chunk, phase completion will not be set immediately. It will be set only
   * in next client bootstrap request when no more data has to be read and the case
   * (null == result) will be true
   */
  protected boolean computeIsPhaseCompleted(BootstrapEventProcessResult result,
                                            Checkpoint ckpt,
                                            long numRowsReadFromDb,
                                            long maxRowsPerFetch,
                                            long windowScn)
  {
    assert(numRowsReadFromDb <= maxRowsPerFetch);
    boolean isPhaseCompleted = false;
    if (null == result)
    {
      /**
       * There are no rows read from the DB in the resultSet.
       * In this case, it definitely means that "isPhaseCompleted" is true, i.e., bootstrap has finished
       */
      assert(numRowsReadFromDb == 0);
      isPhaseCompleted = true;
    }
    else
    {
      /**
       * Note that 0<= numRowsReadFromDb <= maxRowsPerFetch in this case
       * i.e., numRowsReadFromDb == 0 is possible
       * in the case when all events are filtered out
       */
      if (
          (numRowsReadFromDb < maxRowsPerFetch) &&
          (DbusClientMode.BOOTSTRAP_SNAPSHOT == ckpt.getConsumptionMode())
          )
      {
        /**
         * 1. The total rows processed is less than the max rows per fetch
         * 2. The client is in snapshot mode
         */
        isPhaseCompleted = true;
      }
      else if (
          (numRowsReadFromDb < maxRowsPerFetch) &&
          (DbusClientMode.BOOTSTRAP_CATCHUP == ckpt.getConsumptionMode()) &&
          (ckpt.getBootstrapTargetScn() == windowScn)
          )
      {
        /**
         * 1. If the total rows processed is less than the max rows per fetch
         * 2. The client is in catchup mode
         * 3. We are caught up to targetScn
         */
        isPhaseCompleted = true;
      }

      LOG.info("Terminating batch with result: " + result);

      if (result.isError() || result.isClientBufferLimitExceeded())
      {
        /**
         * Don't write either checkpoint or phaseCompleted when
         * 1. There was an error, or
         * 2. ClientBufferLimit was exceeded
         *
         * Note that even if isPhaseCompleted was set earlier, it is overruled
         */
        isPhaseCompleted = false;
      }
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
