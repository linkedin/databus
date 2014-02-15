package com.linkedin.databus.bootstrap.producer;

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

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.log4j.Logger;

import com.linkedin.databus.bootstrap.api.BootstrapProducerStatus;
import com.linkedin.databus.bootstrap.common.BootstrapConn;
import com.linkedin.databus.bootstrap.common.BootstrapDBMetaDataDAO;
import com.linkedin.databus.bootstrap.common.BootstrapProducerStatsCollector;
import com.linkedin.databus.bootstrap.common.BootstrapReadOnlyConfig;
import com.linkedin.databus.bootstrap.common.SourceInfo;
import com.linkedin.databus.client.consumer.AbstractDatabusStreamConsumer;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.client.pub.SCN;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventInternalWritable;
import com.linkedin.databus.core.DbusPrettyLogUtils;
import com.linkedin.databus.core.ScnNotFoundException;
import com.linkedin.databus.core.util.RateMonitor;
import com.linkedin.databus.core.util.StringUtils;
import com.linkedin.databus2.core.BackoffTimer;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.util.DBHelper;

public class BootstrapProducerCallback extends AbstractDatabusStreamConsumer
{
  public static final String MODULE = BootstrapProducerCallback.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private BootstrapDBMetaDataDAO _bootstrapDao = null;
  private PreparedStatement _stmt = null;
  private PreparedStatement _logScnStmt = null;
  private int _numEvents = 0;
  private int _totalNumEvents = 0;
  private long _seedCatchupScn = -1;
  private int _state = BootstrapProducerStatus.ACTIVE;
  private long _oldWindowScn = -1;
  private long _newWindowScn = -1;
  // max(scn) of log at producer startup
  private long _producerStartScn = -1;
  private Map<String, SourceInfo> _trackedSources = null;
  private Map<Integer, String> _trackedSrcIdsToNames = null;
  private BootstrapReadOnlyConfig _config = null;
  private String _currentSource = null;
  private BackoffTimer _retryTimer = null;
  private List<String> _logicalSources = null;

  /* Stats Specific */
  private BootstrapProducerStatsCollector _statsCollector = null;
  private final RateMonitor _srcRm = new RateMonitor(
      "ProducerSourceRateMonitor");
  private final RateMonitor _totalRm = new RateMonitor(
      "ProducerTotalRateMonitor");
  private int _currentLogId;
  private int _currentRowId;

  private final int _maxRowsInLog;
  private boolean _errorRetriesExceeded;

  private ErrorCaseHandler _errorHandler = null;

  public BootstrapProducerCallback(BootstrapReadOnlyConfig config,
      List<String> logicalSources) throws Exception
  {
    this(config, null, null, logicalSources);
  }

  public BootstrapProducerCallback(BootstrapReadOnlyConfig config,
      BootstrapProducerStatsCollector statsCollector,
      ErrorCaseHandler errorHandler, List<String> logicalSources)
      throws SQLException, DatabusException
  {
    _config = config;
    _logicalSources = logicalSources;
    _statsCollector = statsCollector;
    _maxRowsInLog = _config.getBootstrapLogSize();
    _retryTimer = new BackoffTimer("BootstrapProducer", config.getRetryConfig());
    _errorRetriesExceeded = false;
    _errorHandler = errorHandler;
    getConnection();
    init();
  }

  public void init()
      throws SQLException, DatabusException
  {
    Set<String> configedSources = new HashSet<String>(_logicalSources);
    _trackedSources = _bootstrapDao.getDBTrackedSources(configedSources);

    _trackedSrcIdsToNames = new HashMap<Integer, String>();
    for (Entry<String, SourceInfo> entry : _trackedSources.entrySet())
    {
      _trackedSrcIdsToNames.put(entry.getValue().getSrcId(), entry.getKey());
    }

    LOG.info("maxRowsInLog=" + _maxRowsInLog);
    LOG.info("trackedSources: ");
    int lastState = BootstrapProducerStatus.UNKNOWN;
    int curr = 0;
    for (SourceInfo info : _trackedSources.values())
    {
      if (0 == curr)
      {
        lastState = info.getStatus();
      }
      else
      {
        if (info.getStatus() != lastState)
        {
          String msg = "Bootstrap Source state does not seem to be consistent for all the sources that this producer listens to. "
              + " Found atleast 2 different states :"
              + lastState
              + ","
              + info.getStatus();
          LOG.error(msg);
          throw new RuntimeException(msg);
        }
      }
      curr++;
      LOG.info(info.toString());
    }

    _state = lastState;

    initWindowScn();
  }

  @Override
  public ConsumerCallbackResult onStartDataEventSequence(SCN startScn)
  {
    _srcRm.start();
    _totalNumEvents = 0;
    ConsumerCallbackResult success = ConsumerCallbackResult.SUCCESS;
    try
    {
      if (_oldWindowScn == -1)
      {
        initWindowScn();
      }
    } catch (SQLException e)
    {
      if (null != _statsCollector)
        _statsCollector.registerSQLException();
      LOG.error(
          "Got SQLException in startDataEventSequence Hanlder!! Connections will be reset !!",
          e);
      try
      {
        reset();
      }
      catch (DatabusException e2)
      {
        DbusPrettyLogUtils.logExceptionAtError("Unable to reset connection", e2, LOG);
      }
      catch (SQLException sqlEx)
      {
        DbusPrettyLogUtils.logExceptionAtError("Got exception while resetting connections. Stopping Client !!", sqlEx, LOG);
        return ConsumerCallbackResult.ERROR_FATAL;
      }
      success = ConsumerCallbackResult.ERROR;
    }
    return success;
  }

  @Override
  public ConsumerCallbackResult onEndDataEventSequence(SCN endScn)
  {
    try
    {
      // Update the metadata for all sources
      updateAllProducerSourcesMetaData();
      _oldWindowScn = _newWindowScn;

      // Update all the sources info in the database.
      // If we need to create new log file for a source, create one.
      updateSourcesInDB();

      boolean markActive = false;
      if (_state == BootstrapProducerStatus.SEEDING_CATCHUP)
      {
        if (_newWindowScn > _seedCatchupScn)
        {
          LOG.info("Bootstrap DB for sources ("
              + _trackedSources.values()
              + ") has completed the seeding catchup phase. Marking them active in bootstrap_sources table !! SeedCatchupSCN was :"
              + _seedCatchupScn);
          markActive = true;
        }
      }
      else if (_state == BootstrapProducerStatus.FELL_OFF_RELAY)
      {
        if (_newWindowScn > _producerStartScn)
        {
          LOG.info("Bootstrap DB for sources ("
              + _trackedSources.values()
              + ") has started getting events since last fell-off relay !! Marking them active !!");
          markActive = true;
        }
      }

      if (markActive)
        _bootstrapDao.updateSourcesStatus(_trackedSources.keySet(),
            BootstrapProducerStatus.ACTIVE);

      Connection conn = getConnection();
      try
      {
        DBHelper.commit(conn);
      } catch (SQLException s)
      {
        DBHelper.rollback(conn);
        throw s;
      }

      if (markActive)
      {
        _state = BootstrapProducerStatus.ACTIVE;
        for (SourceInfo info : _trackedSources.values())
        {
          info.setStatus(BootstrapProducerStatus.ACTIVE);
        }
      }

      LOG.info("bootstrap producer upto scn " + _newWindowScn);

    } catch (SQLException e)
    {

      if (null != _statsCollector)
        _statsCollector.registerSQLException();

      LOG.error(
          "Got SQLException in endDataEventSequence Handler !! Connections will be reset !!",
          e);
      try
      {
        reset();
      }
      catch (DatabusException e2)
      {
        DbusPrettyLogUtils.logExceptionAtError("Unable to reset connection", e2, LOG);
      }
      catch (SQLException sqlEx)
      {
        LOG.error(
            "Got exception while resetting connections. Stopping Client !!",
            sqlEx);
        return ConsumerCallbackResult.ERROR_FATAL;
      }
      return ConsumerCallbackResult.ERROR;
    } finally
    {
      _totalRm.stop();
      long latency = _totalRm.getDuration() / 1000000L;
      if (null != _statsCollector)
      {
        _statsCollector.registerEndWindow(latency, _totalNumEvents,
            _newWindowScn);
      }
    }

    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onRollback(SCN startScn)
  {
    return _errorRetriesExceeded ? ConsumerCallbackResult.ERROR_FATAL
        : ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onStartSource(String source, Schema sourceSchema)
  {
    _numEvents = 0;
    boolean ret = false;
    SourceInfo srcInfo = null;
    _currentSource = source;
    _srcRm.start();
    try
    {
      srcInfo = _trackedSources.get(source);
      if (null == srcInfo)
      {
        LOG.error("Source :"
            + source
            + " not managed in this bootstrap DB instance !! Managed Sources : ("
            + _trackedSources + ")");
        return ConsumerCallbackResult.ERROR;
      }
      ret = prepareStatement(srcInfo.getSrcId());
    } catch (SQLException e)
    {

      if (null != _statsCollector)
        _statsCollector.registerSQLException();

      LOG.error(
          "Got SQLException in startSource Hanlder!! Connections will be reset !!",
          e);
      try
      {
        reset();
      }
      catch (DatabusException e2)
      {
        DbusPrettyLogUtils.logExceptionAtError("Unable to reset connection", e2, LOG);
      }
      catch (SQLException sqlEx)
      {
        LOG.error(
            "Got exception while resetting connections. Stopping Client !!",
            sqlEx);
        return ConsumerCallbackResult.ERROR_FATAL;
      }
      return ConsumerCallbackResult.ERROR;
    }
    
    return ret ? ConsumerCallbackResult.SUCCESS : ConsumerCallbackResult.ERROR;
  }

  @Override
  public ConsumerCallbackResult onEndSource(String source, Schema sourceSchema)
  {

    try
    {
      // Update the metadata for this source
      updateProducerSourceMetaData(source);

      if (_stmt != null)
      {
        _stmt.close();
        _stmt = null;
      }
    } catch (SQLException e)
    {
      if (null != _statsCollector)
        _statsCollector.registerSQLException();

      LOG.error(
          "Got SQLException in endSource Hanlder!! Connections will be reset !!",
          e);
      try
      {
        reset();
      }
      catch (DatabusException e2)
      {
        DbusPrettyLogUtils.logExceptionAtError("Unable to reset connection", e2, LOG);
      }
      catch (SQLException sqlEx)
      {
        LOG.error(
            "Got exception while resetting connections. Stopping Client !!",
            sqlEx);
        return ConsumerCallbackResult.ERROR_FATAL;
      }
      return ConsumerCallbackResult.ERROR;
    } finally
    {
      _srcRm.stop();
      long latency = _srcRm.getDuration() / 1000000L;

      if (null != _statsCollector)
        _statsCollector.registerBatch(_currentSource, latency, _numEvents,
            _newWindowScn, _currentLogId, _currentRowId);

      _totalNumEvents += _numEvents;
      _numEvents = 0;
    }

    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onDataEvent(DbusEvent e,
      DbusEventDecoder eventDecoder)
  {
    if (e.sequence() < _newWindowScn)
    {
      LOG.warn("Seeing an Old event. Dropping it !! Current SCN : "
          + _newWindowScn + ". Event :" + e.toString());
      return ConsumerCallbackResult.SUCCESS;
    }
    _numEvents++;
    _newWindowScn = e.sequence();

    try
    {
      // TODO (DDSDBUS-776) : remove erstwhile windowscn column
      _stmt.setLong(1, _newWindowScn);
      _stmt.setLong(2, _newWindowScn);

      String keyStr = null;
      if (e.isKeyNumber())
      {
        keyStr = Long.toString(e.key());
      }
      else if (e.isKeyString())
      {
        keyStr = StringUtils.bytesToString(e.keyBytes());
      }
      else if (e.isKeySchema()) {
        LOG.error("schema key type not supported: " + e);
        return ConsumerCallbackResult.ERROR;
      }
      else {
        LOG.error("unknown event key type: " + e);
        return ConsumerCallbackResult.ERROR;
      }
      _stmt.setString(3, keyStr);
      if (!(e instanceof DbusEventInternalWritable))
      {
        throw new UnsupportedClassVersionError(
            "Cannot get raw bytes out of DbusEvent");
      }
      ByteBuffer bytebuff = ((DbusEventInternalWritable) e).getRawBytes();

      byte val[] = new byte[bytebuff.remaining()];
      bytebuff.get(val);

      _stmt.setBytes(4, val);

      _stmt.executeUpdate();
    } catch (SQLException e1)
    {
      if (null != _statsCollector)
        _statsCollector.registerSQLException();

      LOG.error(
          "Got SQLException in dataEvent Hanlder!! Connections will be reset !!",
          e1);
      try
      {
        reset();
      }
      catch (DatabusException e2)
      {
        DbusPrettyLogUtils.logExceptionAtError("Unable to reset connection", e2, LOG);
      }
      catch (SQLException sqlEx)
      {
        LOG.error(
            "Got exception while resetting connections. Stopping Client !!",
            sqlEx);
        return ConsumerCallbackResult.ERROR_FATAL;
      }
      return ConsumerCallbackResult.ERROR;
    }

    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onCheckpoint(SCN checkpointScn)
  {
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onError(Throwable err)
  {
    ConsumerCallbackResult success = ConsumerCallbackResult.ERROR;
    try
    {
      if (err instanceof ScnNotFoundException)
      {
        try
        {
          // Producer fell-off the relay. It could be in an active transaction
          // (last valid event was not EOP) in
          // which case, we should roll-back. This is safe since the checkpoint
          // is consistent with the
          // roll-back state
          getConnection().rollback();
        } catch (SQLException sqlEx)
        {
          if (null != _statsCollector)
            _statsCollector.registerSQLException();

          LOG.error("Got exception while rolling back transaction !!", sqlEx);
        }

        _bootstrapDao.updateSourcesStatus(_trackedSources.keySet(),
            BootstrapProducerStatus.FELL_OFF_RELAY);

        if (null != _statsCollector)
          _statsCollector.registerFellOffRelay();
      }
      success = ConsumerCallbackResult.SUCCESS;
    } catch (Exception e)
    {
      LOG.error("Got exception onError() ", e);
      success = ConsumerCallbackResult.ERROR;
    }
    return success;
  }

  /*
   * Reset the Bootstrap Connection and in memory state of Producer
   */
  private void reset()
      throws SQLException, DatabusException
  {
    boolean success = false;
    /*
     * Retry Connections with exponential backoff upto 1 min.
     */
    _retryTimer.reset();
    while (!success)
    {
      try
      {
        // Close automatically rollbacks the transaction
        DBHelper.close(_stmt);
        _stmt = null;

        DBHelper.close(_logScnStmt);
        _logScnStmt = null;

        _bootstrapDao.getBootstrapConn().close();

        _bootstrapDao.getBootstrapConn().getDBConn(); // recreate the Connection

        _bootstrapDao.getBootstrapConn().executeDummyBootstrapDBQuery();

        // Initialize in memory state
        init();

        success = true;
      } catch (SQLException sqlEx)
      {
        if (null != _statsCollector)
          _statsCollector.registerSQLException();

        LOG.error("Unable to reset the Bootstrap DB connection !!", sqlEx);

        if (_retryTimer.getRemainingRetriesNum() <= 0)
        {
          String message = "Producer Thread reached max retries trying to reset the MySQL Connections. Stopping !!";
          LOG.fatal(message);
          _errorRetriesExceeded = true;
          // throw sqlEx;
          /*
           * ERROR_FATAL is not implemented yet, Hence using callback to let
           * HttpClientImpl stop
           */
          _errorHandler.onErrorRetryLimitExceeded(message, sqlEx);
        }
        _retryTimer.backoffAndSleep();
      }
    }
  }

  private void updateAllProducerSourcesMetaData() throws SQLException
  {
    for (Map.Entry<String, SourceInfo> entry : _trackedSources.entrySet())
    {
      String src = entry.getKey();
      updateProducerSourceMetaData(src);
    }
  }

  private void updateProducerSourceMetaData(String source) throws SQLException
  {
    // Update the metadata for this source
    SourceInfo srcinfo = _trackedSources.get(source);
    _currentRowId = getLastLogEntry(source);
    _currentLogId = srcinfo.getCurrLogId();

    setLogPosition(_currentLogId, _currentRowId, _newWindowScn, source);

    // Update the source info for this source
    srcinfo.setMaxRowId(_currentRowId);
    srcinfo.setWindowScn(_newWindowScn);
  }

  private boolean prepareStatement(int srcId) throws SQLException
  {
    Connection conn = null;

    try
    {
      conn = getConnection();
      StringBuilder sql = new StringBuilder();

      sql.append("insert into ");
      sql.append(getTableName(srcId));
      sql.append("(scn, windowscn, srckey, val) ");
      sql.append(" values(?,?,?,?)");
      // sql.append("(scn, windowscn, srckey) ");
      // sql.append(" values(?, ?,?)");

      _stmt = conn.prepareStatement(sql.toString());
    } catch (SQLException e)
    {
      LOG.error("Got SQLException in prepareStatement!! ", e);
      throw e;
    }

    return true;
  }

  private void initWindowScn() throws SQLException
  {
    ResultSet rs = null;
    StringBuilder sql = new StringBuilder();

    Statement stmt = null;
    try
    {
      sql.append("select max(p.windowscn), max(s.endscn) from bootstrap_producer_state p, bootstrap_seeder_state s ");
      sql.append("where p.srcid = s.srcid and p.srcid in (");
      int count = _trackedSources.size();
      for (SourceInfo srcInfo : _trackedSources.values())
      {
        count--;
        sql.append(srcInfo.getSrcId());
        if (count > 0)
          sql.append(",");
      }
      sql.append(")");
      stmt = getConnection().createStatement();
      LOG.info("sql query = " + sql.toString());
      rs = stmt.executeQuery(sql.toString());
      if (rs.next())
      {
        _producerStartScn = _oldWindowScn = _newWindowScn = rs.getLong(1);
        _seedCatchupScn = rs.getLong(2);
      }
    } catch (SQLException e)
    {
      if (null != _statsCollector)
        _statsCollector.registerSQLException();
      LOG.error(
          "Unable to select producer's max windowscn. Setting windowscn to -1",
          e);
      _oldWindowScn = -1;
      _newWindowScn = -1;
      _producerStartScn = -1;
      throw e;
    } finally
    {
      DBHelper.close(rs, stmt, null);

    }
  }

  private int getLastLogEntry(String source) throws SQLException
  {
    int rid = 0;
    Statement stmt = null;
    ResultSet rs = null;
    int srcId = _trackedSources.get(source).getSrcId();
    try
    {
      stmt = getConnection().createStatement();
      rs = stmt.executeQuery("select max(id) from " + getTableName(srcId));
      if (rs.next())
      {
        rid = rs.getInt(1);
      }
    } catch (SQLException e)
    {
      LOG.error("Unable to find max. rid. Setting current rid to -1", e);
      rid = -1;
      throw e;
    } finally
    {
      if (null != stmt)
      {
        stmt.close();
        stmt = null;
      }
      if (null != rs)
      {
        rs.close();
        rs = null;
      }
    }
    return rid;
  }

  private void setLogPosition(int logid, int logrid, long windowscn,
      String source) throws SQLException
  {
    PreparedStatement stmt = getLogPositionStmt();

    stmt.setInt(1, logid);
    stmt.setInt(2, logrid);
    stmt.setLong(3, windowscn);
    stmt.setString(4, source);

    stmt.executeUpdate();
  }

  private PreparedStatement getLogPositionStmt() throws SQLException
  {
    if (_logScnStmt != null)
    {
      return _logScnStmt;
    }

    Connection conn = null;

    try
    {
      conn = getConnection();
      StringBuilder sql = new StringBuilder();

      sql.append("update bootstrap_producer_state set logid = ?, rid = ? , windowscn = ? where srcid = (select id from bootstrap_sources where src = ?)");
      _logScnStmt = conn.prepareStatement(sql.toString());
    } catch (SQLException e)
    {
      if (null != _statsCollector)
        _statsCollector.registerSQLException();

      LOG.error(
          "Exception occurred while getting the bootstrap_producer statement",
          e);
      throw e;
    }

    return _logScnStmt;
  }

  private void updateSourcesInDB() throws SQLException
  {
    for (Map.Entry<String, SourceInfo> entry : _trackedSources.entrySet())
    {
      SourceInfo srcinfo = entry.getValue();
      srcinfo.saveToDB(getConnection());
    }

    for (Map.Entry<String, SourceInfo> entry : _trackedSources.entrySet())
    {
      SourceInfo srcinfo = entry.getValue();
      if (srcinfo.getNumRows() >= _maxRowsInLog)
      {
        srcinfo.switchLogFile(getConnection());
        setLogPosition(srcinfo.getCurrLogId(), 0, _newWindowScn, entry.getKey());
        // getConnection().commit();
        _bootstrapDao.createNewLogTable(srcinfo.getSrcId());
      }
    }
  }

  private Connection getConnection() throws SQLException
  {
    Connection conn = null;

    if (_bootstrapDao == null)
    {
      BootstrapConn dbConn = new BootstrapConn();
      try
      {
        final boolean autoCommit = false;
        dbConn.initBootstrapConn(autoCommit, _config.getBootstrapDBUsername(),
            _config.getBootstrapDBPassword(), _config.getBootstrapDBHostname(),
            _config.getBootstrapDBName());
        _bootstrapDao = new BootstrapDBMetaDataDAO(dbConn,
            _config.getBootstrapDBHostname(), _config.getBootstrapDBUsername(),
            _config.getBootstrapDBPassword(), _config.getBootstrapDBName(),
            autoCommit);
      } catch (Exception e)
      {
        LOG.fatal("Unable to open BootstrapDB Connection !!", e);
        return null;
      }
    }

    try
    {
      conn = _bootstrapDao.getBootstrapConn().getDBConn();
    } catch (SQLException e)
    {
      if (null != _statsCollector)
        _statsCollector.registerSQLException();
      LOG.fatal("Not able to open BootstrapDB Connection !!", e);
      throw e;
    }
    return conn;
  }

  private String getTableName(int srcId) throws SQLException
  {
    return _bootstrapDao.getBootstrapConn().getLogTableNameToProduce(srcId);
  }

  public interface ErrorCaseHandler
  {
    /* Callback for handling case when error retries limit reached */
    void onErrorRetryLimitExceeded(String message, Throwable exception);
  }

}
