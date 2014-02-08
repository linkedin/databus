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

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.databus.bootstrap.api.BootstrapProducerStatus;
import com.linkedin.databus.bootstrap.common.BootstrapConn;
import com.linkedin.databus.bootstrap.common.BootstrapDBMetaDataDAO;
import com.linkedin.databus.bootstrap.common.BootstrapReadOnlyConfig;
import com.linkedin.databus.core.DbusEventBufferAppendable;
import com.linkedin.databus.core.DbusEventBufferStreamAppendable;
import com.linkedin.databus.core.DbusEventFactory;
import com.linkedin.databus.core.DbusEventInfo;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.DbusEventKey.KeyType;
import com.linkedin.databus.core.DbusOpcode;
import com.linkedin.databus.core.InternalDatabusEventsListener;
import com.linkedin.databus.core.InvalidEventException;
import com.linkedin.databus.core.KeyTypeNotImplementedException;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus2.producers.db.OracleTriggerMonitoredSourceInfo;
import com.linkedin.databus2.util.DBHelper;

public class BootstrapDBSeeder
implements DbusEventBufferAppendable, DbusEventBufferStreamAppendable
{
  private static final int FIFTY_MB_IN_BYTES =     50000000;
  public static final String      MODULE               = BootstrapDBSeeder.class.getName();
  public static final Logger      LOG                  = Logger.getLogger(MODULE);

  private BootstrapDBMetaDataDAO           _bootstrapDao       = null;
  private BootstrapReadOnlyConfig _config              = null;
  private ByteBuffer              _buf                  = null;
  private ByteArrayInputStream    _bufStream           = null;
  private ByteArrayInputStream    _bufStream2           = null;
  private final Map<Short, PreparedStatement> _statementMap;
  private short                   _currSrcId    		 = -1;
  private long                    _totLatency          = 0;
  private long                    _scn                 = 0;
  private List<OracleTriggerMonitoredSourceInfo> _sources           = null;
  private Map<String, Long>         _lastRows          = null;
  private Map<String, String>       _lastKeys          = null;
  private String                    _lastSeenKey       = null;
  private long                      _startSCN          = -1;


  public BootstrapDBSeeder(BootstrapReadOnlyConfig config,
                           List<OracleTriggerMonitoredSourceInfo> sources)
  throws Exception
  {
    _config = config;
    _statementMap = new HashMap<Short, PreparedStatement>();
    byte[] b = new byte[FIFTY_MB_IN_BYTES];
    _buf = ByteBuffer.wrap(b);
    _bufStream = new ByteArrayInputStream(b);
    _bufStream2 = new ByteArrayInputStream(b);
    getConnection();
    _sources = sources;
    initSources();
  }


  public void initSources()
  throws SQLException
  {
    LOG.info("MySQL JDBC Version :" + getConnection().getMetaData().getDriverVersion());
    _lastRows = new HashMap<String, Long>();
    _lastKeys = new HashMap<String, String>();
    for ( OracleTriggerMonitoredSourceInfo sourceInfo : _sources)
    {
      createBootStrapSourceDB(sourceInfo);
      _lastRows.put(sourceInfo.getEventView(), getRowIdFromCheckpoint(sourceInfo));
      String k = geSrcKeyFromCheckpoint(sourceInfo);
      _lastKeys.put(sourceInfo.getEventView(), k);
      if ( -1 == _currSrcId)
        _currSrcId = sourceInfo.getSourceId();

      if ( null == _lastSeenKey)
        _lastSeenKey = k;
    }
  }

  public void startSeeding()
  throws SQLException
  {
    setBootstrapSourceStatus(BootstrapProducerStatus.SEEDING);
  }

  public Map<String, Long> getLastRows()
  {
    return _lastRows;
  }

  public Map<String, String> getLastKeys()
  {
    return _lastKeys;
  }

  private PreparedStatement prepareInsertStatement(short srcId) throws SQLException
  {
    Connection conn = null;
    PreparedStatement stmt = null;

    StringBuilder sql = new StringBuilder();

    try
    {
      conn = getConnection();
      sql.append("insert into ");
      sql.append(getTableName(srcId));
      sql.append("(scn, srckey, val) ");
      sql.append(" values(?,?,?) ");
      sql.append("on duplicate key update scn = ?, ");
      sql.append("val = ?");
      // sql.append("(scn, windowscn, srckey) ");
      // sql.append(" values(?, ?,?)");

      stmt = conn.prepareStatement(sql.toString());
    } catch (SQLException e) {
      LOG.fatal("Unable to create insert statement for Statement : (" + sql + ")", e);
      throw e;
    }

    return stmt;
  }


  /*
   * Returns a connection object.
   * Note: The connection object is still owned by BootstrapConn. SO dont close it
   */
  public Connection getConnection()
  {
    Connection conn = null;

    if (_bootstrapDao == null)
    {
      LOG.info("<<<< Creating Bootstrap Connection!! >>>>");
      BootstrapConn dbConn = new BootstrapConn();
      try
      {
        dbConn.initBootstrapConn(false,
                                 java.sql.Connection.TRANSACTION_READ_UNCOMMITTED,
                                 _config.getBootstrapDBUsername(),
                                 _config.getBootstrapDBPassword(),
                                 _config.getBootstrapDBHostname(),
                                 _config.getBootstrapDBName());
        _bootstrapDao = new BootstrapDBMetaDataDAO(dbConn,
                                                   _config.getBootstrapDBHostname(),
                                                   _config.getBootstrapDBUsername(),
                                                   _config.getBootstrapDBPassword(),
                                                   _config.getBootstrapDBName(),
                                                   false);
      } catch (Exception e) {
        LOG.fatal("Unable to open BootstrapDB Connection !!", e);
        throw new RuntimeException("Got exception when getting bootstrap DB Connection.",e);
      }
    }

    try
    {
      conn = _bootstrapDao.getBootstrapConn().getDBConn();
    } catch ( SQLException e) {
      LOG.fatal("Not able to open BootstrapDB Connection !!", e);
      throw new RuntimeException("Got exception when getting bootstrap DB Connection.",e);
    }
    return conn;
  }

  /*
   * Initialize Bootstrap Sources entry
   */
  public void setBootstrapSourceStatus(int status)
      throws SQLException
      {
    Connection conn = null;
    PreparedStatement stmt = null;
    try
    {
      String sql = "insert into bootstrap_sources (id, src, status) values (?, ?, ?) on duplicate key update id = ?, src= ?, status=?";
      conn = getConnection();
      stmt = conn.prepareStatement(sql);

      for ( OracleTriggerMonitoredSourceInfo sourceInfo : _sources)
      {
        stmt.setInt(1, sourceInfo.getSourceId());
        stmt.setString(2, sourceInfo.getSourceName());
        stmt.setInt(3,status);
        stmt.setInt(4, sourceInfo.getSourceId());
        stmt.setString(5, sourceInfo.getSourceName());
        stmt.setInt(6,status );
        stmt.executeUpdate();
      }
      conn.commit();
    } catch (SQLException sqlEx) {
      LOG.error("Got Exception while initializing Bootstrap Sources !!", sqlEx);
      if ( conn != null)
        conn.rollback();
      throw sqlEx;
    } finally {
      DBHelper.close(stmt);
    }

      }

  /*
   * Creates Bootstrap source DB only if it is not already available
   */
  public void createBootStrapSourceDB(OracleTriggerMonitoredSourceInfo source ) throws SQLException
  {
    _bootstrapDao.createNewSrcTable(source.getSourceId());
  }


  public String getTableName(int srcId) throws SQLException
  {
    return _bootstrapDao.getBootstrapConn().getSrcTableName(srcId);
  }

  @Override
  public void start(long startSCN)
  {
    _scn = startSCN;
  }

  @Override
  public void startEvents() {
  }

  @Override
  public boolean appendEvent(DbusEventKey key,
                             short pPartitionId,
                             short lPartitionId,
                             long timeStamp,
                             short srcId,
                             byte[] schemaId,
                             byte[] value,
                             boolean enableTracing,
                             DbusEventsStatisticsCollector statsCollector)
  {
    return appendEvent(key, _scn, pPartitionId, lPartitionId, timeStamp, srcId,
                       schemaId, value, enableTracing, statsCollector);
  }

  @Override
  @Deprecated
  public boolean appendEvent(DbusEventKey key,
                             short pPartitionId,
                             short lPartitionId,
                             long timeStamp,
                             short srcId,
                             byte[] schemaId,
                             byte[] value,
                             boolean enableTracing) {
    throw new RuntimeException("Not implemented!!!");
  }

  public boolean appendEvent(DbusEventKey key,
                             DbusEventKey seederChunkKey,
                             long sequenceId,
                             short pPartitionId,
                             short lPartitionId,
                             long timeStamp,
                             short srcId,
                             byte[] schemaId,
                             byte[] value,
                             boolean enableTracing,
                             DbusEventsStatisticsCollector statsCollector)
  {
    DbusEventInfo eventInfo = new DbusEventInfo(DbusOpcode.UPSERT, sequenceId, pPartitionId, lPartitionId,
                                                timeStamp, srcId, schemaId, value, enableTracing,
                                                false);
    return appendEvent(key, seederChunkKey, eventInfo, statsCollector);
  }

  @Override
  public boolean appendEvent(DbusEventKey key,
                             short pPartitionId,
                             short lPartitionId,
                             long timeStamp,
                             short srcId,
                             byte[] schemaId,
                             byte[] value,
                             boolean enableTracing,
                             boolean isReplicated,
                             DbusEventsStatisticsCollector statsCollector)
  {
    throw new RuntimeException("This API not expected to be called on BootstrapDBSeeder !!");
  }

  public boolean appendEvent(DbusEventKey key,
                             DbusEventKey seederChunkKey,
                             DbusEventInfo eventInfo,
                             DbusEventsStatisticsCollector statsCollector)
  {
    boolean isDebugEnabled = LOG.isDebugEnabled();

    boolean ret = false;
    PreparedStatement stmt = null;

    /*
     * Track the startSCN
     */
    if ( _startSCN == -1 )
      _startSCN = eventInfo.getSequenceId();

    short srcId = eventInfo.getSrcId();
    long sequenceId = eventInfo.getSequenceId();

    try
    {
      //long start = System.nanoTime();
      if ( seederChunkKey.getKeyType() == KeyType.LONG )
      {
        _lastSeenKey = "" + seederChunkKey.getLongKey();
      } else {
        _lastSeenKey = seederChunkKey.getStringKey();
      }

      if (isDebugEnabled)
        LOG.debug("Seeder Chunk Key is: " + seederChunkKey + ",EventInfo :" + eventInfo + ", Key is :" + key);

      eventInfo.setOpCode(null);
      eventInfo.setAutocommit(true);

      if (eventInfo.getValueLength() >= FIFTY_MB_IN_BYTES)
      {
        LOG.fatal("Event Size larger than 50 MB. For Key :" + key + ", avro record size is : "+ eventInfo.getValueLength());
      }

      DbusEventFactory.serializeEvent(key, _buf, eventInfo);
      long end = System.nanoTime();

      stmt = _statementMap.get(srcId);

      if ( null == stmt)
      {
        stmt = prepareInsertStatement(srcId);
        _statementMap.put(srcId, stmt);
      }

      if ( isDebugEnabled)
      {
        LOG.debug("Number of Bytes in serialized format:" + _buf.position());
        LOG.debug("Key is :" + ( (key.getKeyType() == KeyType.LONG) ? key.getLongKey() : key.getStringKey()));
      }

      stmt.setLong(1,sequenceId);
      String keyStr = null;

      if (key.getKeyType() == DbusEventKey.KeyType.LONG)
      {
        keyStr = key.getLongKey().toString();
      } else {
        keyStr = key.getStringKey();
      }
      stmt.setString(2, keyStr);

      // Reuse the iStream to set the blob
      _bufStream.reset();
      stmt.setBlob(3, _bufStream, _buf.position());
      //stmt.setBytes(3, _buf.array());

      stmt.setLong(4,sequenceId);

      // Reuse the iStream to se the blob
      _bufStream2.reset();
      stmt.setBlob(5, _bufStream2, _buf.position());
      //stmt.setBytes(5, _buf.array());

      stmt.executeUpdate();

      long end2   = System.nanoTime();

      _totLatency += (end2 - end);

      _currSrcId = srcId;
      ret = true;
    } catch (SQLException sqlEx) {
      LOG.error("Error occured while inserting record for key:" +
          key.getStringKey() + "(" + key.getLongKey() + ") with sequenceId:" + sequenceId, sqlEx);
      throw new RuntimeException(sqlEx);
    } catch (KeyTypeNotImplementedException ex) {
      LOG.error("KeyNotImplemented error while inserting record for key:" +
          key.getStringKey() + "(" + key.getLongKey() + ") with sequenceId:" + sequenceId, ex);
      throw new RuntimeException(ex);
    }
    finally {
      _buf.clear();
    }

    return ret;
  }


  @Override
  public void rollbackEvents() {
    Connection conn = getConnection();
    try
    {
      LOG.error("Rolling back uncommitted bootstrap events");
      conn.rollback();
    }  catch (SQLException e) {
      LOG.error("Unable to rollback while seeding bootstrap.", e);
    }
  }

  @Override
  public void endEvents(boolean updateWindowScn, long windowScn,
                        boolean updateIndex, boolean callListener, DbusEventsStatisticsCollector statsCollector) {

  }

  /*
   * Callback for processing end of source
   */
  public void endSource(long scn)
  {
    String seederSql = "update bootstrap_seeder_state set endscn = ? where srcid = ? ";
    Connection conn = null;
    PreparedStatement stmt = null;
    try
    {
      conn = getConnection();
      stmt = conn.prepareStatement(seederSql);
      stmt.setLong(1,scn);
      stmt.setInt(2,_currSrcId);
      stmt.executeUpdate();
      conn.commit();
    } catch (SQLException sqlEx) {
      throw new RuntimeException(sqlEx);
    } finally {
      DBHelper.close(stmt);
    }
  }

  /*
   * Callback for processing end of Seeding
   */
  public void endSeeding()
  {
    //Copy seeder state to producer and applier state
    Connection conn = null;
    Statement stmt = null;
    ResultSet rs = null;
    long minScn  = -1;
    try
    {
      String sql = getMinSCNSeederQuery();
      conn = getConnection();
      stmt = conn.createStatement();
      LOG.info("Executing Min startSCN Query in Bootstrap DB:" + sql);
      rs = stmt.executeQuery(sql);
      rs.next();
      minScn = rs.getLong(1);
      LOG.info("Minimum startSCN of all the sources which were seeded was :" + minScn);
      updateBootstrapStateTable(minScn,"bootstrap_applier_state");
      updateBootstrapStateTable(minScn,"bootstrap_producer_state");
      createLogTableRows();

      // Go to the next bootstrap state for this Source
      setBootstrapSourceStatus(BootstrapProducerStatus.SEEDING_CATCHUP);
      conn.commit();
    } catch (SQLException sqlEx) {
      LOG.error("Got Exception while updating bootstrap state tables",sqlEx);
      try
      {
        conn.rollback();
      }catch(Exception ex) {
        LOG.error("Error while rolling back ...",ex);
      }
      throw new RuntimeException(sqlEx);
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      DBHelper.close(rs,stmt,null);
    }
  }

  private void updateBootstrapStateTable(long scn, String table)
      throws SQLException
      {
    String upsertSQL = getBootstrapStateInsertStmt(table);
    Connection conn = null;
    PreparedStatement stmt = null;
    try
    {
      conn = getConnection();
      stmt = conn.prepareStatement(upsertSQL);

      for (OracleTriggerMonitoredSourceInfo srcInfo : _sources)
      {
        stmt.setInt(1, srcInfo.getSourceId());
        stmt.setInt(2,0);
        stmt.setLong(3,scn);
        stmt.setLong(4, 0);
        stmt.setInt(5,0);
        stmt.setLong(6, scn);
        stmt.setLong(7,0);
        stmt.executeUpdate();
      }
    } finally {
      DBHelper.close(stmt);
    }
      }

  private void createLogTableRows()
      throws SQLException
      {
    String sql = "insert into bootstrap_loginfo (srcid, logid, minwindowscn, maxwindowscn, maxrid) values ( ?, 0, -1, -1, 0)";
    Connection conn = null;
    PreparedStatement stmt = null;
    try
    {
      conn = getConnection();
      stmt = conn.prepareStatement(sql);

      for (OracleTriggerMonitoredSourceInfo srcInfo : _sources)
      {
        try
        {
          stmt.setInt(1,srcInfo.getSourceId());
          stmt.executeUpdate();
          _bootstrapDao.createNewLogTable(srcInfo.getSourceId());
        } catch ( SQLException sqlEx) {
          LOG.error("Got Error inserting entry into bootstrap_loginfo but proceeding !!", sqlEx);
        }
      }
    } catch ( SQLException sqlEx) {
      LOG.error("Got Error inserting entry into bootstrap_loginfo !!", sqlEx);
      throw sqlEx;
    } finally {
      DBHelper.close(stmt);
    }
      }

  private String getBootstrapStateInsertStmt(String table)
  {
    StringBuilder sql = new StringBuilder();
    sql.append("insert into ").append(table);
    sql.append(" (srcid, logid, windowscn, rid) values ( ?, ?, ?, ?) ");
    sql.append(" on duplicate key update logid = ?, windowscn = ?, rid = ?");
    return sql.toString();
  }

  private String getMinSCNSeederQuery()
  {
    StringBuilder buf = new StringBuilder();
    buf.append("select min(startscn) from bootstrap_seeder_state where srcid IN (");

    for (int i = 0 ; i < _sources.size() - 1; i++ )
    {
      buf.append(_sources.get(i).getSourceId()).append(", ");
    }

    buf.append(_sources.get(_sources.size() -1).getSourceId());
    buf.append(")");
    return buf.toString();
  }

  @Override
  public void endEvents(long rowId, DbusEventsStatisticsCollector statsCollector)
  {

    Connection conn = null;
    PreparedStatement stmt = null;

    try
    {
      StringBuilder sql = new StringBuilder();
      //sql.append("insert into bootstrap_applier_state ");
      //sql.append("values (?,?,?,?) ");
      //sql.append("on duplicate key update rid = ?");
      sql.append("insert into bootstrap_seeder_state ");
      sql.append("values ( ?, ?, -1, ?, ?)");
      sql.append("on duplicate key update rid = ?, srckey = ?"); //startscn set only at the first insert

      conn = getConnection();
      stmt = conn.prepareStatement(sql.toString());
      stmt.setInt(1,_currSrcId);
      stmt.setLong(2, _startSCN);
      stmt.setLong(3,rowId);
      stmt.setString(4,_lastSeenKey);
      stmt.setLong(5,rowId);
      stmt.setString(6, _lastSeenKey);

      LOG.info("\t Total Latency before commit :" + (_totLatency/1000000000));
      stmt.executeUpdate();
      long start = System.nanoTime();
      conn.commit();
      long end = System.nanoTime();
      long lat = (end - start)/1000000;
      LOG.info("\t Comit time (msec) :" + lat);
    } catch (Exception e) {
      LOG.fatal("Got Exception in endEvents !!", e);
      throw new RuntimeException("Got Exception in endEvents !!", e);
    } finally {
      // Dont close conn as it is being reused.
      DBHelper.close(stmt);
    }
  }


  @Override
  public boolean empty() {
    return false;
  }

  @Override
  public int readEvents(ReadableByteChannel readChannel,
                        Iterable<InternalDatabusEventsListener> eventListeners,
                        DbusEventsStatisticsCollector statsCollector)
                            throws InvalidEventException {
    return 0;
  }


  public long getRowIdFromCheckpoint(OracleTriggerMonitoredSourceInfo sourceInfo)
  {
    Connection conn = null;
    PreparedStatement stmt = null;
    ResultSet row = null;
    long rowId = -1;
    try
    {
      StringBuilder sql = new StringBuilder();
      sql.append("select rid from bootstrap_seeder_state ");
      sql.append("where srcid = ?");

      conn = getConnection();
      stmt = conn.prepareStatement(sql.toString());

      stmt.setInt(1, sourceInfo.getSourceId());
      row = stmt.executeQuery();

      if (row.next())
      {
        rowId = row.getLong(1);
      }

    } catch (Exception e) {
      LOG.error("Got exception while trying to fetch the rowid from bootstrap_seeder_state !!", e);
    } finally {
      DBHelper.close(row, stmt, null);
    }

    return rowId;
  }

  /*
   * TODO: Refactor to do one query to get rid and srckey from bootstrap_seeder_state
   */
  public String geSrcKeyFromCheckpoint(OracleTriggerMonitoredSourceInfo sourceInfo)
  {
    Connection conn = null;
    PreparedStatement stmt = null;
    ResultSet row = null;
    String srcKey = null;
    try
    {
      StringBuilder sql = new StringBuilder();
      sql.append("select srckey from bootstrap_seeder_state ");
      sql.append("where srcid = ?");

      conn = getConnection();
      stmt = conn.prepareStatement(sql.toString());

      stmt.setInt(1, sourceInfo.getSourceId());
      row = stmt.executeQuery();

      if (row.next())
      {
        srcKey = row.getString(1);
      }

    } catch (Exception e) {
      LOG.error("Got exception while trying to fetch the srckey from bootstrap_seeder_state !!", e);
    } finally {
      DBHelper.close(row, stmt, null);
    }

    return srcKey;
  }

  @Override
  public long getMinScn()
  {
    return 0;
  }

  @Override
  public long lastWrittenScn()
  {
    return 0;
  }


  @Override
  public void setStartSCN(long sinceSCN)  { }


  @Override
  public long getPrevScn()
  {
    return 0;
  }


  @Override
  public boolean appendEvent(DbusEventKey key, long sequenceId,
                             short pPartitionId, short lPartitionId, long timeStamp, short srcId,
                             byte[] schemaId, byte[] value, boolean enableTracing,
                             DbusEventsStatisticsCollector statsCollector) {
    return false;
  }


  @Override
  public boolean appendEvent(DbusEventKey key, DbusEventInfo eventInfo,
                             DbusEventsStatisticsCollector statsCollector) {
    return false;
  }
}
