package com.linkedin.databus.bootstrap.utils;

/*
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
 */


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.linkedin.databus.bootstrap.utils.BootstrapAuditTableReader.ResultSetEntry;
import com.linkedin.databus.bootstrap.utils.BootstrapSrcDBEventReader.PrimaryKeyTxn;
import com.linkedin.databus.client.DbusEventAvroDecoder;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus2.producers.db.OracleTriggerMonitoredSourceInfo;
import com.linkedin.databus2.relay.OracleJarUtils;
import com.linkedin.databus2.schemas.FileSystemSchemaRegistryService;
import com.linkedin.databus2.schemas.SchemaRegistryService;
import com.linkedin.databus2.schemas.VersionedSchema;
import com.linkedin.databus2.schemas.VersionedSchemaSet;
import com.linkedin.databus2.schemas.utils.SchemaHelper;
import com.linkedin.databus2.util.DBHelper;

public class BootstrapAuditMain
{
  public static final String MODULE = BootstrapAuditMain.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  static
  {
    BasicConfigurator.configure();
    Logger.getRootLogger().setLevel(Level.INFO);
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception
  {
    BootstrapSeederMain.init(args);

    BootstrapSeederMain.StaticConfig staticConfig = BootstrapSeederMain
        .getStaticConfig();

    int interval = staticConfig.getController().getCommitInterval();

    int sourceChunkSize = staticConfig.getController().getNumRowsPerQuery();

    List<OracleTriggerMonitoredSourceInfo> sources = BootstrapSeederMain
        .getSources();
    BootstrapDBSeeder seeder = BootstrapSeederMain.getSeeder();
    BootstrapSrcDBEventReader seedController = BootstrapSeederMain.getReader();

    Map<String, String> pKeyNameMap = seedController.getpKeyNameMap();
    Map<String, DbusEventKey.KeyType> pKeyTypeMap = seedController
        .getpKeyTypeMap();

    for (OracleTriggerMonitoredSourceInfo source : sources)
    {
      short srcId = source.getSourceId();

      new ConcurrentHashMap<Long, ResultSetEntry>();
      OracleTableReader oracleReader = null;
      MySQLTableReader mySQLReader = null;

      try
      {

        SchemaRegistryService schemaRegistry = FileSystemSchemaRegistryService
            .build(staticConfig.getSchemaRegistry().getFileSystem());
        Map<Short, String> schemaSet = schemaRegistry
            .fetchAllSchemaVersionsBySourceName(source.getSourceName());
        VersionedSchemaSet vSchemaSet = new VersionedSchemaSet();
        Iterator<Map.Entry<Short, String>> it = schemaSet.entrySet().iterator();
        while (it.hasNext())
        {
          Map.Entry<Short, String> pairs = it.next();
          Schema s = Schema.parse(pairs.getValue());
          VersionedSchema vs = new VersionedSchema(s.getFullName(),
              pairs.getKey(), s, null);
          vSchemaSet.add(vs);
        }

        /* Try and identify the schema key */
        VersionedSchema vschema = schemaRegistry
            .fetchLatestVersionedSchemaBySourceName(source.getSourceName());
        Schema schema = Schema.parse(vschema.getSchema().toString());
        LOG.info("Schema =" + vschema.getSchema() + "version="
            + vschema.getVersion() + " name=" + vschema.getSchemaBaseName());

        /* Determine type of field txn */
        Field txnFieldType = schema.getField("txn");
        if (txnFieldType == null)
        {
          throw new Exception(
              "Unable to find field called 'txn'. Cannot proceeed\n");
        }
        Type txnType = SchemaHelper.getAnyType(txnFieldType);

        /*
         * Determine primary key of schema. This is assumed to be invariant
         * across versions
         */
        String keyOverrideName = SchemaHelper.getMetaField(schema, "pk");
        String keyColumnName = "key";
        if (null != keyOverrideName)
        {
          keyColumnName = keyOverrideName;
        }
        Field pkeyField = schema.getField(keyColumnName);
        if (null == pkeyField)
        {
          keyColumnName = "id";
          pkeyField = schema.getField("id");
        }
        if (null == pkeyField)
        {
          throw new Exception(
              "Unable to get the primary key for schema. Schema is :" + schema);
        }

        DbusEventAvroDecoder decoder = new DbusEventAvroDecoder(vSchemaSet);

        BootstrapAuditTester auditor = new BootstrapAuditTester(schema, BootstrapSrcDBEventReader.getTableName(source));
        List<BootstrapAuditTester> auditors = new ArrayList<BootstrapAuditTester>();
        auditors.add(auditor);

        oracleReader = new OracleTableReader(BootstrapSeederMain.getDataStore()
            .getConnection(), BootstrapSrcDBEventReader.getTableName(source),
            pkeyField, SchemaHelper.getMetaField(pkeyField, "dbFieldName"),
            SchemaHelper.getAnyType(pkeyField), sourceChunkSize,
            seedController.getPKIndex(source),
            seedController.getQueryHint(source));

        mySQLReader = new MySQLTableReader(seeder.getConnection(),
            seeder.getTableName(srcId), pkeyField, "id", // THis is the primary
                                                         // key always for
                                                         // bootstrapDB
            SchemaHelper.getAnyType(pkeyField), interval);

        double samplePct = BootstrapSeederMain.getValidationSamplePct();

        TableComparator comparator = new TableComparator(oracleReader,
            mySQLReader, auditor, decoder, interval, pKeyNameMap.get(source
                .getEventView()), pKeyTypeMap.get(source.getEventView()),
            txnType, samplePct);

        boolean success = false;
        if (BootstrapSeederMain.getValidationType().equals("point"))
        {
          success = comparator.compareRecordsPoint();
        }
        else if (BootstrapSeederMain.getValidationType().equals("pointBs"))
        {
          success = comparator.compareRecordsPointBs();
        }
        else
        {
          success = comparator.compareRecordsNew();
        }

        if (success)
          LOG.info("Audit completed successfully");
        else
          LOG.error("Audit FAILED !!! ");
      }
      catch (Exception ex)
      {
        LOG.error("Caught an exception ex", ex);
        throw ex;
      }
      finally
      {
        if (null != oracleReader)
          oracleReader.close();
      }
    }
    DBHelper.close(seeder.getConnection());
  }

  public static class AuditStats implements Runnable
  {

    private long _numProcessed = 0;
    private long _numKeyEqual = 0;
    private long _numKeyAbsentInBootstrap = 0;
    private long _numKeyAbsentInOracle = 0;
    private long _numDataEqual = 0;
    private long _numOlderTxnInOracle = 0;
    private long _numOlderTxnInBootstrap = 0;

    private boolean _shutdown = false;
    final private int _intervalInSec;
    private long _numError = 0;

    public AuditStats()
    {
      this(30);
    }

    public AuditStats(int intervalSec)
    {
      _intervalInSec = intervalSec;
    }

    @Override
    public void run()
    {
      while (!_shutdown)
      {
        print();
        try
        {
          Thread.sleep(_intervalInSec * 1000);
        }
        catch (InterruptedException e)
        {
          _shutdown = true;
        }
      }
      print();
    }

    public void print()
    {
      LOG.info("AuditStats\n" + this);
    }

    @Override
    public String toString()
    {
      StringBuilder s = new StringBuilder(512);
      s.append(" numProcessed=").append(_numProcessed).append(" numKeyEqual=")
          .append(_numKeyEqual).append(" numDataEqual=").append(_numDataEqual)
          .append(" numKeyAbsentInOracle=").append(_numKeyAbsentInOracle)
          .append(" numKeyAbsentInBootstrap=").append(_numKeyAbsentInBootstrap)
          .append(" numOlderTxnInOracle=").append(_numOlderTxnInOracle)
          .append(" numOlderTxnInBootstrap=").append(_numOlderTxnInBootstrap)
          .append(" numError=").append(_numError);
      return s.toString();
    }

    public synchronized void shutdown()
    {
      _shutdown = true;
    }

    public long getNumProcessed()
    {
      return _numProcessed;
    }

    public long getNumKeyEqual()
    {
      return _numKeyEqual;
    }

    public long getNumKeyAbsentInBootstrap()
    {
      return _numKeyAbsentInBootstrap;
    }

    public long getNumKeyAbsentInOracle()
    {
      return _numKeyAbsentInOracle;
    }

    public long getNumDataEqual()
    {
      return _numDataEqual;
    }

    public long getNumOlderTxnInOracle()
    {
      return _numOlderTxnInOracle;
    }

    public long getNumOlderTxnInBootstrap()
    {
      return _numOlderTxnInBootstrap;
    }

    public boolean isShutdown()
    {
      return _shutdown;
    }

    public int getIntervalInSec()
    {
      return _intervalInSec;
    }

    public long getNumError()
    {
      return _numError;
    }

    public synchronized void incNumProcessed()
    {
      _numProcessed++;
    }

    public synchronized void incNumnKeyAbsentInOracle()
    {
      _numKeyAbsentInOracle++;
    }

    public synchronized void incNumKeyAbsentInBootstrap()
    {
      _numKeyAbsentInBootstrap++;
    }

    public synchronized void incNumOlderTxnInBootstrap()
    {
      _numOlderTxnInBootstrap++;
    }

    public synchronized void incNumOlderTxnInOracle()
    {
      _numOlderTxnInOracle++;
    }

    public synchronized void incNumDataEqual()
    {
      _numDataEqual++;
    }

    public synchronized void incNumKeyEqual()
    {
      _numKeyEqual++;
    }

    public synchronized void incNumError()
    {
      _numError++;
    }

  }

  public static class TableComparator
  {
    private final OracleTableReader _srcReader;
    private final MySQLTableReader _destReader;
    private final DbusEventAvroDecoder _decoder;
    private final BootstrapAuditTester _auditor;
    private final int _interval;
    private final String _pKey;
    private final DbusEventKey.KeyType _pKeyType;
    private final Type _txnType;
    private final double _samplePct;
    private final Random _rand = new Random();

    public TableComparator(OracleTableReader srcReader,
        MySQLTableReader destReader, BootstrapAuditTester auditor,
        DbusEventAvroDecoder decoder, int interval, String pKey,
        DbusEventKey.KeyType pKeyType, Type txnType, double samplePct)
    {
      _srcReader = srcReader;
      _destReader = destReader;
      _decoder = decoder;
      _interval = interval;
      _auditor = auditor;
      _pKey = pKey;
      _pKeyType = pKeyType;
      _txnType = txnType;
      _samplePct = samplePct;
      LOG.info("Comparator: interval=" + _interval + " samplePct=" + _samplePct);
    }

    // called before deciding to compare
    private boolean shouldCompare()
    {
      if (_samplePct == 0)
      {
        return false;
      }
      else if (_samplePct == 100)
      {
        return true;
      }
      return (_samplePct > _rand.nextDouble() * 100.0);
    }

    public boolean compareRecordsPointBs() throws SQLException
    {
      AuditStats stats = new AuditStats();
      Thread statsThread = new Thread(stats);
      try
      {
        ResultSet srcRs = null;
        ResultSet destRs = null;
        /*
         * long srcNumRows = _srcReader.getNumRecords(_pKey); long destNumRows =
         * _destReader.getNumRecords("id");
         *
         * LOG.info("Current Number of Rows in Source DB :" + srcNumRows);
         * LOG.info("Current Number of Rows in Destination DB :" + destNumRows);
         */
        statsThread.start();

        String lastSrcKey = "0";
        boolean oracleDone = true;
        boolean done = false;
        while (!done)
        {

          // Read the next chunks from src and dest db's
          if (oracleDone)
          {
            done = true;
            DBHelper.close(srcRs);
            srcRs = _srcReader.getRecords(lastSrcKey);
          }

          oracleDone = !srcRs.next();
          done = done && oracleDone;
          if (!oracleDone)
          {
            /* For getting next row in boostrap db and oracle */
            /* The keys on which the query result set is ordered */
            lastSrcKey = getKey(srcRs);
            // read one record from BS

            if (!shouldCompare())
              continue;
            stats.incNumProcessed();
            DBHelper.close(destRs);
            destRs = _destReader.getRecord(lastSrcKey);

            boolean found = destRs.next();
            // Compare txn_id, key of both oracle and bs sources;
            // src_txn (oracle) , dst_txn(bootstrap) : destKey is the key field
            // in bs and srcKey is corresponding one in oracle
            if (found)
            {
              stats.incNumKeyEqual();
              /* For reading txnid, key which will form the basis of comparison */
              long srcTxnId = srcRs.getLong(2);
              long dstTxnId = getDestTxnId(destRs);

              // assumption: txnids are monotonically increasing
              if (srcTxnId == dstTxnId)
              {
                boolean result = _auditor
                    .compareRecord(srcRs, destRs, _decoder);
                if (result)
                {
                  stats.incNumDataEqual();
                }
                else
                {
                  LOG.error("Compare error: Key=" + lastSrcKey);
                }
              }
              else if (srcTxnId < dstTxnId)
              {
                stats.incNumOlderTxnInOracle();
              }
              else
              {
                // older txn in bootstrap;
                stats.incNumOlderTxnInBootstrap();
              }
            }
            else
            {
              LOG.info("Absent in bootstrap: " + lastSrcKey);
              // key present in oracle but not in bootstrap;
              stats.incNumKeyAbsentInBootstrap();
            }
          }

        }
        LOG.info("Done with audit- end of stream reached\n");
        stats.shutdown();
        statsThread.interrupt();
        statsThread.join();
        return stats.getNumProcessed() == stats.getNumDataEqual();
      }
      catch (SQLException e)
      {
        stats.shutdown();
        statsThread.interrupt();
        throw e;
      }
      catch (InterruptedException e)
      {

      }
      LOG.info("Done with audit- end of stream reached\n");
      return stats.getNumProcessed() == stats.getNumDataEqual();

    }

    public boolean compareRecordsPoint() throws SQLException
    {
      AuditStats stats = new AuditStats();
      Thread statsThread = new Thread(stats);
      try
      {
        ResultSet srcRs = null;
        ResultSet destRs = null;

        statsThread.start();

        long lastDestId = 0;
        boolean bootstrapDone = true;
        boolean done = false;
        while (!done)
        {

          if (bootstrapDone)
          {
            done = true;
            // batch sample; for uniform sampling set batch size (_interval) to
            // 1 .
            if (shouldCompare())
            {
              DBHelper.close(destRs);
              destRs = _destReader.getRecordsSequential(lastDestId);
            }
            else
            {
              // skip this batch
              done = false;
              lastDestId += _interval;
              continue;
            }
          }

          bootstrapDone = !destRs.next();

          done = done && bootstrapDone;
          if (!bootstrapDone)
          {
            // get next rowId
            lastDestId = destRs.getLong(1);
            /* For getting next row in boostrap db and oracle */
            String lastDestKey = destRs.getString(3);

            stats.incNumProcessed();

            // read that one record from Oracle
            DBHelper.close(srcRs);
            srcRs = _srcReader.getRecord(lastDestKey);
            boolean found = srcRs.next();

            if (found)
            {
              stats.incNumKeyEqual();
              /* For reading txnid, key which will form the basis of comparison */
              long srcTxnId = srcRs.getLong(2);
              long dstTxnId = getDestTxnId(destRs);
              if (dstTxnId != 0)
              {
                // assumption: txnids are monotonically increasing
                if (srcTxnId == dstTxnId)
                {
                  boolean result = _auditor.compareRecord(srcRs, destRs,
                      _decoder);
                  if (result)
                  {
                    stats.incNumDataEqual();
                  }
                  else
                  {
                    LOG.error("Compare error: Key=" + lastDestKey);
                  }
                }
                else if (srcTxnId < dstTxnId)
                {
                  // older txn in oracle - this cannot happen
                  stats.incNumOlderTxnInOracle();
                }
                else
                {
                  // older txn in bootstrap;
                  stats.incNumOlderTxnInBootstrap();
                }
              }
              else
              {
                stats.incNumError();
              }
            }
            else
            {
              // key present in bootstrap but not in oracle;
              stats.incNumnKeyAbsentInOracle();
            }
          }
        }
        LOG.info("Done with audit- end of stream reached\n");
        stats.shutdown();
        statsThread.interrupt();
        statsThread.join();
        return stats.getNumProcessed() == stats.getNumDataEqual();
      }
      catch (SQLException e)
      {
        stats.shutdown();
        statsThread.interrupt();
        throw e;
      }
      catch (InterruptedException e)
      {

      }
      LOG.info("Done with audit- end of stream reached\n");
      return stats.getNumProcessed() == stats.getNumDataEqual();
    }

    public boolean compareRecordsNew() throws SQLException
    {
      AuditStats stats = new AuditStats();
      Thread statsThread = new Thread(stats);
      try
      {
        ResultSet srcRs = null;
        ResultSet destRs = null;

        statsThread.start();

        String lastSrcKey = "0";
        String lastDestKey = "0";
        int compareResult = 0;
        boolean oracleDone = true;
        boolean bootstrapDone = true;
        boolean done = false;
        while (!done)
        {

          // Read the next chunks from src and dest db's
          if (oracleDone)
          {
            done = true;
            DBHelper.close(srcRs);
            srcRs = _srcReader.getRecords(lastSrcKey);
          }

          if (bootstrapDone)
          {
            done = true;
            DBHelper.close(destRs);
            destRs = _destReader.getRecords(lastDestKey);
          }

          // Iterate over two streams in 'key' order
          if (compareResult <= 0)
          {
            oracleDone = !srcRs.next();
          }
          if (compareResult >= 0)
          {
            bootstrapDone = !destRs.next();
          }

          done = done && (bootstrapDone || oracleDone);
          if (!bootstrapDone && !oracleDone)
          {
            // TODO: sampling
            stats.incNumProcessed();
            /* The keys on which the query result set is ordered */
            lastSrcKey = getKey(srcRs);
            lastDestKey = destRs.getString(3);
            compareResult = keyCompare(lastSrcKey, lastDestKey);
            // Compare txn_id, key of both oracle and bs sources;
            // src_txn (oracle) , dst_txn(bootstrap) : destKey is the key field
            // in bs and srcKey is corresponding one in oracle
            if (compareResult == 0)
            {
              stats.incNumKeyEqual();
              /* For reading txnid, key which will form the basis of comparison */
              long srcTxnId = srcRs.getLong(2);
              long dstTxnId = getDestTxnId(destRs);

              // assumption: txnids are monotonically increasing
              if (srcTxnId == dstTxnId)
              {
                boolean result = _auditor
                    .compareRecord(srcRs, destRs, _decoder);
                if (result)
                {
                  stats.incNumDataEqual();
                }
                else
                {
                  LOG.error("Compare error: Key=" + lastSrcKey);
                }
              }
              else if (srcTxnId < dstTxnId)
              {
                // older txn in oracle - this cannot happen
                stats.incNumOlderTxnInOracle();
              }
              else
              {
                // older txn in bootstrap;
                stats.incNumOlderTxnInBootstrap();
              }
            }
            else if (compareResult < 0)
            {
              LOG.info("Absent in bootstrap: " + lastSrcKey);
              // key present in oracle but not in bootstrap;
              stats.incNumKeyAbsentInBootstrap();
            }
            else
            {
              LOG.info("Absent in oracle: " + lastDestKey);
              // key present in bootstrap but not in oracle;
              stats.incNumnKeyAbsentInOracle();
            }
          }
          else if (!bootstrapDone && oracleDone)
          {
            compareResult = -1;
          }
          else if (!oracleDone && bootstrapDone)
          {
            compareResult = 1;
          }
          else
          {
            compareResult = 0;
          }
        }
        LOG.info("Done with audit- end of stream reached\n");
        stats.shutdown();
        statsThread.interrupt();
        statsThread.join();
        return stats.getNumProcessed() == stats.getNumDataEqual();
      }
      catch (SQLException e)
      {
        stats.shutdown();
        statsThread.interrupt();
        throw e;
      }
      catch (InterruptedException e)
      {

      }
      LOG.info("Done with audit- end of stream reached\n");
      return stats.getNumProcessed() == stats.getNumDataEqual();
    }

    protected String getKey(ResultSet srcRs) throws SQLException
    {
      if (_pKeyType == DbusEventKey.KeyType.LONG)
      {
        Long key = srcRs.getLong(_pKey);
        return key.toString();
      }
      return srcRs.getString(_pKey);
    }

    protected int keyCompare(String keySrc, String keyDst)
    {

      if (_pKeyType == DbusEventKey.KeyType.LONG)
      {
        long srcKeyLong = Long.parseLong(keySrc);
        long destKeyLong = Long.parseLong(keyDst);
        if (srcKeyLong == destKeyLong)
        {
          return 0;
        }
        return (srcKeyLong < destKeyLong) ? -1 : 1;
      }
      return keySrc.compareTo(keyDst);
    }

    private long getDestTxnId(ResultSet bsRes) throws SQLException
    {
      GenericRecord avroRec = _auditor.getGenericRecord(bsRes, _decoder);
      if (avroRec == null)
      {
        LOG.error("No avro record skipping");
        return 0;
      }
      Object txnId = avroRec.get("txn");
      if (txnId == null)
      {
        LOG.error("Could not find a field called 'txn' in avro event in bootstrap db");
        return 0;
      }
      switch (_txnType)
      {
      case LONG:
        if (txnId instanceof Integer)
        {
          Integer i = (Integer) txnId;
          return i.longValue();
        }
        else if (txnId instanceof Long)
        {
          return (Long) txnId;
        }
      case INT:
        Integer i = (Integer) txnId;
        return i.longValue();
      default:
        return 0;
      }
    }
  }

  public static class MySQLTableReader extends BootstrapAuditTableReader
  {
    private PreparedStatement _pointRecordStmt = null;
    private PreparedStatement _rangeRecordStmt = null;
    private PreparedStatement _sequentialRangeRecordStmt = null;

    public MySQLTableReader(Connection conn, String tableName, Field pkeyField,
        String pkeyName, Type pkeyType, int interval) throws SQLException
    {
      super(conn, tableName, pkeyField, pkeyName, pkeyType, interval);
      StringBuilder sql = new StringBuilder();
      sql.append("select id,scn,srckey,val from ").append(_tableName);
      sql.append(" where srckey= ?");

      _pointRecordStmt = _conn.prepareStatement(sql.toString());

      StringBuilder sqlSequence = new StringBuilder();
      sqlSequence.append("select id,scn,srckey,val from ").append(_tableName);
      sqlSequence.append(" where id > ? limit ? ");
      _sequentialRangeRecordStmt = _conn.prepareStatement(sqlSequence
          .toString());
    }

    public ResultSet getRecordsSequential(long lastDestId) throws SQLException
    {
      ResultSet rs = null;
      try
      {
        _sequentialRangeRecordStmt.setLong(1, lastDestId);
        _sequentialRangeRecordStmt.setLong(2, _interval);
        rs = _sequentialRangeRecordStmt.executeQuery();
      }
      catch (SQLException sqlEx)
      {
        DBHelper.close(rs, _sequentialRangeRecordStmt, null);
        throw sqlEx;
      }
      return rs;
    }

    @Override
    public void close()
    {
      DBHelper.close(_pointRecordStmt);
      DBHelper.close(_rangeRecordStmt);
      DBHelper.close(_sequentialRangeRecordStmt);
      super.close();
    }

    /**
     *
     * @param lastSrcKey
     *          : a srckey ;
     * @return resultSet that may contain a row whose srckey=lastSrckey;
     * @throws SQLException
     */
    public ResultSet getRecord(String lastSrcKey) throws SQLException
    {
      ResultSet rs = null;
      try
      {
        _pointRecordStmt.setString(1, lastSrcKey);
        // stmt.setFetchSize(10000);
        // stmt.setMaxRows(10);
        rs = _pointRecordStmt.executeQuery();
      }
      catch (SQLException sqlEx)
      {
        DBHelper.close(rs, _pointRecordStmt, null);
        throw sqlEx;
      }
      return rs;
    }


    @Override
    public PreparedStatement getFetchStmt(String from) throws SQLException
    {
      try
      {
        if (_rangeRecordStmt == null || _rangeRecordStmt.isClosed())
        {

          StringBuilder sql = new StringBuilder();
          /**
           * Ordering should be same as the other stream: srckey is a string
           * (type: varchar)
           */
          sql.append(" select id, scn, srckey, val from ").append(_tableName);
          if (_pkeyType == Type.LONG || _pkeyType == Type.INT)
          {
            sql.append(" where cast(srckey as decimal(40)) > ?  order by ");
            sql.append(" cast(srckey as decimal(40)) asc");
          }
          else
          {
            sql.append(" where srckey > ?  order by ");
            sql.append(" srckey asc");
          }
          sql.append(" limit ? ");
          String stmtStr = sql.toString();
          LOG.info("MySQL Query=" + stmtStr);
          _rangeRecordStmt = _conn.prepareStatement(stmtStr);
        }
        LOG.info("MySQL Query params: FromSrcKey= " + from + ",Limit="
            + (_interval));
        _rangeRecordStmt.setString(1, from);
        _rangeRecordStmt.setLong(2, _interval);
        return _rangeRecordStmt;
      }
      catch (SQLException e)
      {
        DBHelper.close(null, _rangeRecordStmt, null);
        _rangeRecordStmt = null;
        throw e;
      }
    }

  }

  public static class OracleTableReader extends BootstrapAuditTableReader
  {
    private final DbusEventKey.KeyType _dbusKeyType;
    private final String _pkIndex;
    private final String _queryHint;
    private PreparedStatement _pointRecordStmt = null;
    private PreparedStatement _rangeRecordStmt = null;

    Method _setLobPrefetchSizeMethod = null;
    Class _oraclePreparedStatementClass = null;

    public OracleTableReader(Connection conn, String tableName,
        Field pkeyField, String pkeyName, Type pkeyType, int interval,
        String pkIndex, String queryHint) throws SQLException
    {
      super(conn, tableName, pkeyField, pkeyName, pkeyType, interval);

      if ((pkeyType == Type.INT) || (pkeyType == Type.LONG))
      {
        _dbusKeyType = DbusEventKey.KeyType.LONG;
      }
      else
      {
        _dbusKeyType = DbusEventKey.KeyType.STRING;
      }

      _pkIndex = pkIndex;
      _queryHint = queryHint;

      String pointRecordsql = generatePointQuery(_tableName, _pkeyName,
          _pkIndex, _queryHint);
      _pointRecordStmt = _conn.prepareStatement(pointRecordsql);

      LOG.info("Tablename=" + tableName);
      LOG.info("Point Oracle Query =" + pointRecordsql);

      try
      {
        _oraclePreparedStatementClass = OracleJarUtils
            .loadClass("oracle.jdbc.OraclePreparedStatement");
        _setLobPrefetchSizeMethod = _oraclePreparedStatementClass.getMethod(
            "setLobPrefetchSize", int.class);
      }
      catch (Exception e)
      {
        LOG.error("Exception raised while trying to get oracle methods", e);
        throw new SQLException(e.getMessage());
      }
    }

    public String generatePointQuery(String table, String keyName,
        String pkIndex, String queryHint)
    {
      StringBuilder sql = new StringBuilder();

      if ((null == queryHint) || (queryHint.isEmpty()))
        sql.append("select /*+ INDEX(src ").append(pkIndex).append(") */ ");
      else
        sql.append("select /*+ " + queryHint + " */ ");

      sql.append(keyName).append(" keyn,");
      sql.append(" txn txnid, src.* ").append(" from ");
      sql.append(table);
      sql.append(" src");
      sql.append(" where src." + keyName + " = ?");
      return sql.toString();
    }

    @Override
    public void close()
    {
      DBHelper.close(_pointRecordStmt);
      DBHelper.close(_rangeRecordStmt);
      super.close();
    }

    /**
     *
     * @param destKey
     *          : the srckey
     * @return ResultSet
     * @throws SQLException
     */
    public ResultSet getRecord(String destKey) throws SQLException
    {
      ResultSet rs = null;
      try
      {
        // Reuse prepared statement
        _pointRecordStmt.setString(1, destKey);
        // stmt.setFetchSize(10000);
        // stmt.setMaxRows(10);
        rs = _pointRecordStmt.executeQuery();
      }
      catch (SQLException sqlEx)
      {
        DBHelper.close(rs, _pointRecordStmt, null);
        throw sqlEx;
      }
      return rs;
    }

    @Override
    public PreparedStatement getFetchStmt(String from) throws SQLException

    {
      try
      {
        if (_rangeRecordStmt == null || _rangeRecordStmt.isClosed())
        {

          String sql = BootstrapSrcDBEventReader.generateEventQueryAudit(
              _tableName, _pkeyName, _dbusKeyType, _pkIndex, _queryHint);
          LOG.info("Oracle Query =" + sql);

          _rangeRecordStmt = _conn.prepareStatement(sql);
          Object ds = _oraclePreparedStatementClass.cast(_rangeRecordStmt);
          try
          {
            _setLobPrefetchSizeMethod.invoke(ds, 1000);
          }
          catch (Exception e)
          {
            LOG.error("Could not set LobPreFetchSize: " + e);
            throw new SQLException(e.getMessage());
          }
        }
        LOG.info("Oracle Query: From=" + from + " interval=" + _interval);
        _rangeRecordStmt.setString(1, from);
        _rangeRecordStmt.setLong(2, _interval);
        return _rangeRecordStmt;
      }
      catch (SQLException e)
      {
        DBHelper.close(_rangeRecordStmt);
        throw e;
      }
    }
  }

  public static class KeyTxnReader
  {
    private final File _file;
    private final BufferedReader _reader;

    public KeyTxnReader(File file)
    {
      try
      {
        _file = file;
        _reader = new BufferedReader(new FileReader(file));
      }
      catch (IOException ioe)
      {
        LOG.error("KeyTxnReader error: " + ioe.getMessage(), ioe);
        throw new RuntimeException(ioe);
      }
    }

    public boolean readNextEntry(PrimaryKeyTxn entry) throws IOException
    {
      String line = _reader.readLine();

      if (null == line)
        return false;

      entry.readFrom(line);
      return true;
    }

    public void close()
    {
      try
      {
        _reader.close();
      }
      catch (IOException ioe)
      {
        LOG.error("KeyTxnReader error: " + ioe.getMessage(), ioe);
      }
    }
  }

}
