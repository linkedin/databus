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
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.databus.bootstrap.common.BootstrapCleanerStaticConfig.BootstrapDBType;
import com.linkedin.databus.bootstrap.common.BootstrapCleanerStaticConfig.RetentionStaticConfig;
import com.linkedin.databus.bootstrap.common.BootstrapDBMetaDataDAO.SourceStatusInfo;
import com.linkedin.databus.core.DatabusThreadBase;
import com.linkedin.databus.core.DbusEventFactory;
import com.linkedin.databus.core.DbusEventV1Factory;
import com.linkedin.databus2.core.container.request.BootstrapDatabaseTooOldException;

public class BootstrapDBCleaner
{
  public static final long MILLISEC_IN_SECONDS = 1000;
  public static final long NANOSEC_IN_MILLISECONDS = 1000000L;
  public static final long MILLISEC_IN_MINUTES = 60 * MILLISEC_IN_SECONDS;
  public static final long MILLISEC_IN_HOUR = 60 * MILLISEC_IN_MINUTES;
  public static final long MILLISEC_IN_DAY = 24 * MILLISEC_IN_HOUR;

  public static final String MODULE = BootstrapDBCleaner.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  public BootstrapCleanerStaticConfig _cleanerConfig;
  public BootstrapReadOnlyConfig _bootstrapConfig;
  public List<SourceStatusInfo> _sources;
  private Map<String, DatabusThreadBase> _appliers;
  private BootstrapDBMetaDataDAO _bootstrapDao = null;
  private final Map<Short, BootstrapLogInfo> _lastValidLogMap = new HashMap<Short, BootstrapLogInfo>();
  private volatile boolean isCleaning = false;
  private final DbusEventFactory _eventFactory;
  private final BootstrapDBCleanerQueryHelper _bootstrapDBCleanerQueryHelper;
  private final BootstrapDBCleanerQueryExecutor _bootstrapDBCleanerQueryExecutor;

  public boolean isCleanerRunning()
  {
    return isCleaning;
  }

  public List<SourceStatusInfo> getSources()
  {
    return _sources;
  }

  public void setSources(List<SourceStatusInfo> sources)
  {
    this._sources = sources;
  }

  public BootstrapDBMetaDataDAO getBootstrapDao()
  {
    return _bootstrapDao;
  }

  public BootstrapDBCleaner(String name, BootstrapCleanerStaticConfig config,
      BootstrapReadOnlyConfig bootstrapConfig, Map<String, DatabusThreadBase> appliers,
      List<String> sources) throws SQLException
  {
    _cleanerConfig = config;
    _bootstrapConfig = bootstrapConfig;
    if (null != appliers)
    {
      _appliers = appliers;
    }
    else
    {
      _appliers = new HashMap<String, DatabusThreadBase>();
    }
    _eventFactory = new DbusEventV1Factory();
    Connection conn = getOrCreateConnection();
    if (null != sources)
    {
      try
      {
        _sources = _bootstrapDao.getSourceIdAndStatusFromName(sources, false);
      } catch (BootstrapDatabaseTooOldException bto)
      {
        LOG.error(
            "Not expected to receive this exception as activeCheck is turned-off",
            bto);
        throw new RuntimeException(bto);
      }
    }
    _bootstrapDBCleanerQueryHelper = BootstrapDBCleanerQueryHelper.getInstance();
    _bootstrapDBCleanerQueryExecutor = new BootstrapDBCleanerQueryExecutor(conn, _bootstrapDBCleanerQueryHelper);
    LOG.info("Cleaner Config is :" + _cleanerConfig);
  }

  /*
   * @return a bootstrapDB connection object. Note: The connection object is
   * still owned by BootstrapConn. SO dont close it
   */
  public Connection getOrCreateConnection() throws SQLException
  {
    Connection conn = null;

    if (_bootstrapDao == null)
    {
      LOG.info("<<<< Creating Bootstrap Connection!! >>>>");
      BootstrapConn dbConn = new BootstrapConn();
      final boolean autoCommit = true;
      _bootstrapDao = new BootstrapDBMetaDataDAO(dbConn,
          _bootstrapConfig.getBootstrapDBHostname(),
          _bootstrapConfig.getBootstrapDBUsername(),
          _bootstrapConfig.getBootstrapDBPassword(),
          _bootstrapConfig.getBootstrapDBName(), autoCommit);
      try
      {
        dbConn.initBootstrapConn(autoCommit,
            _bootstrapConfig.getBootstrapDBUsername(),
            _bootstrapConfig.getBootstrapDBPassword(),
            _bootstrapConfig.getBootstrapDBHostname(),
            _bootstrapConfig.getBootstrapDBName());
      } catch (Exception e)
      {
        LOG.fatal("Unable to open BootstrapDB Connection !!", e);
        throw new RuntimeException(
            "Got exception when getting bootstrap DB Connection.", e);
      }
    }

    try
    {
      conn = _bootstrapDao.getBootstrapConn().getDBConn();
    } catch (SQLException e)
    {
      LOG.fatal("Not able to open BootstrapDB Connection !!", e);
      throw new RuntimeException(
          "Got exception when getting bootstrap DB Connection.", e);
    }
    return conn;
  }

  /**
   * Return the milli-second threshold for delete criteria.
   *
   * @param config
   *          RetentionConfig
   * @return milliSecThreshold
   */
  private long getMilliSecTime(RetentionStaticConfig config)
  {
    long qty = config.getRetentionQuantity();
    long milliSecQty = -1;

    switch (config.getRetentiontype())
    {
    case RETENTION_SECONDS:
      milliSecQty = qty * MILLISEC_IN_SECONDS;
      break;

    default:
      throw new RuntimeException("Retention Config (" + config
          + ") expected to be time based but is not !!");

    }
    return milliSecQty;
  }

  public long filterCandidateLogInfo(short srcId,
      List<BootstrapLogInfo> candidateLogsInfo, RetentionStaticConfig config)
      throws SQLException
  {
    switch (config.getRetentiontype())
    {
    case NO_CLEANUP:
      return -1;
    case RETENTION_LOGS:
    {
      Iterator<BootstrapLogInfo> itr = candidateLogsInfo.iterator();
      BootstrapLogInfo lastValidLog = null;
      int i = 0;
      while (i < config.getRetentionQuantity() && itr.hasNext())
      {
        BootstrapLogInfo log = itr.next();
        LOG.info("Removing the log table :" + log.getLogTable()
            + " from the delete List as it is too recent. Retaining :"
            + config.getRetentionQuantity() + " logs");
        itr.remove();
        lastValidLog = log;
        i++;
      }
      _lastValidLogMap.put(srcId, lastValidLog);
      break;
    }

    case RETENTION_SECONDS:
    {
      long quantity = config.getRetentionQuantity();
      LOG.info("Retaining tables which could contain events which is less than "
          + quantity + " seconds old !!");
      long currTs = System.currentTimeMillis() * NANOSEC_IN_MILLISECONDS;
      long nanoSecQty = getMilliSecTime(config) * NANOSEC_IN_MILLISECONDS;
      long threshold = (currTs - nanoSecQty);

      LOG.info("Removing tables from the delete-list whose last row has timestamp newer than :"
          + threshold + " nanosecs");

      Iterator<BootstrapLogInfo> itr = candidateLogsInfo.iterator();
      BootstrapLogInfo lastValidLog = null;
      LOG.info("Timestamp Threshold for src id :" + srcId + " is :" + threshold
          + ", Retention Config " + config + "(" + nanoSecQty + " nanosecs)");

      while (itr.hasNext())
      {
        BootstrapLogInfo log = itr.next();

        long timestamp = _bootstrapDBCleanerQueryExecutor.getNanoTimestampOfLastEventinLog(log, _eventFactory);

        if (timestamp < threshold)
        {
          LOG.info("Reached the log table whose timestamp (" + timestamp
              + ") is less than the threshold (" + threshold + ").");
          break;
        }
        else
        {
          LOG.info("Removing the log table :"
              + log.getLogTable()
              + " from the delete List as it is too recent. Last Event Timestamp :"
              + timestamp + ", threshold :" + threshold);
          lastValidLog = log;
          itr.remove();
        }
      }
      _lastValidLogMap.put(srcId, lastValidLog);
    }
      break;
    }

    long scn = -1;

    if (!candidateLogsInfo.isEmpty())
      scn = _bootstrapDBCleanerQueryExecutor.getSCNOfLastEventinLog(candidateLogsInfo.get(0), _eventFactory);

    return scn;
  }

  public synchronized void doClean()
  {
    DatabusThreadBase applier = null;
    try
    {
      isCleaning = true;

      for (SourceStatusInfo s : _sources)
      {
        BootstrapDBType type = _cleanerConfig.getBootstrapType(s.getSrcName());

        LOG.info("Cleaner running for source :" + s.getSrcName() + "("
            + s.getSrcId() + ") with bootstrapDB type :" + type);

        BootstrapLogInfo logInfo = _bootstrapDBCleanerQueryExecutor.getThresholdWindowSCN(type, s.getSrcId());

        if (null == logInfo)
        {
          LOG.info("No WindowSCN. Nothing to cleanup for source : "
              + s.getSrcName());
          continue;
        }

        LOG.info("LOG info with lowest windowSCN :" + logInfo);

        LOG.info("Begin phase 1 : Gather candidate loginfo :");
        List<BootstrapLogInfo> candidateLogsInfo = _bootstrapDBCleanerQueryExecutor.getCandidateLogsInfo(
            logInfo.getMinWindowSCN(), (short) (s.getSrcId()));
        if ((null == candidateLogsInfo) || (candidateLogsInfo.isEmpty()))
        {
          LOG.info("No logs to cleanup for source :" + s.getSrcName() + "("
              + s.getSrcId() + ")");
          continue;
        }
        LOG.info("End phase 1 : Gather candidate loginfo :");

        LOG.info("Initial Candidate Set for Source :" + s.getSrcName()
            + " is :" + candidateLogsInfo);
        RetentionStaticConfig rConf = _cleanerConfig.getRetentionConfig(s
            .getSrcName());
        LOG.info("Retention Config for source :" + s.getSrcName() + " is :"
            + rConf);

        LOG.info("Begin phase 2 : Filter based on retention config :");
        long scn = filterCandidateLogInfo((short) s.getSrcId(),
            candidateLogsInfo,
            _cleanerConfig.getRetentionConfig(s.getSrcName()));

        LOG.info("Log tables to be deleted for source :" + s.getSrcName() + "("
            + s.getSrcId() + ") are :" + candidateLogsInfo
            + ", Max SCN of deleted logs:" + scn);
        LOG.info("End phase 2 : Filter based on retention config :");

        if ((scn <= 0) || (candidateLogsInfo.isEmpty()))
        {
          LOG.info("Source :" + s.getSrcName() + "(" + s.getSrcId()
              + ") No log tables to be deleted !! MaxSCN : " + scn
              + ", candidateLogs :" + candidateLogsInfo);
          continue;
        }

        LOG.info("Begin phase 3 : Updating Meta Info :");
        BootstrapLogInfo firstValidLog = _bootstrapDBCleanerQueryExecutor.getFirstLogTableWithGreaterSCN(
            (short) s.getSrcId(), scn);
        _bootstrapDBCleanerQueryExecutor.updateSource(firstValidLog);
        LOG.info("End phase 3 : Updating Meta Info :");

        LOG.info("Begin phase 4 : Deleting Log tables :");
        // marking logs as done; if any failures; there is a chance that the
        // logs have to be cleaned up later
        _bootstrapDBCleanerQueryExecutor.markDeleted(candidateLogsInfo);
        _bootstrapDBCleanerQueryExecutor.dropTables(candidateLogsInfo);
        LOG.info("End phase 4 : Deleting Log tables :");

        if ((_cleanerConfig.getBootstrapType(s.getSrcName()) == BootstrapDBType.BOOTSTRAP_CATCHUP_APPLIER_RUNNING)
            && ((_appliers.size() != 0) || _cleanerConfig.forceTabTableCleanup(s
                .getSrcName())))
        {
          LOG.info("Source :" + s.getSrcName() + "(" + s.getSrcId()
              + ") is running in catchup_applier_running mode. "
              + "Will delete all rows whose scn is less than or equal to "
              + scn);
          applier = _appliers.get(s.getSrcName());
          if ((null != applier) && (applier.isAlive()))
          {
            LOG.info("Begin phase 5 : Pausing Applier and deleting Rows from tab table :");

            LOG.info("Requesting applier to pause !!");
            applier.pause();
            LOG.info("Applier paused !!");
          }

          try
          {
            // mark ahead of time; if this doesn't work this time; it will next
            // cycle
            _bootstrapDao.updateMinScnOfSnapshot(s.getSrcId(), scn);
            String srcTable = _bootstrapDBCleanerQueryHelper.getSrcTable(s.getSrcId());
            int numRowsDeleted = _bootstrapDBCleanerQueryExecutor.deleteTable(srcTable, scn);
            LOG.info("Number of Rows deleted for source  :" + s.getSrcName()
                + "(" + s.getSrcId() + ") :" + numRowsDeleted);
            if (numRowsDeleted > 0
                && _cleanerConfig.isOptimizeTableEnabled(s.getSrcName()))
            {
              LOG.info("Optimizing table to reclaim space for source :"
                  + s.getSrcName() + "(" + s.getSrcId() + ")");
              _bootstrapDBCleanerQueryExecutor.optimizeTable(srcTable);
            }
          } finally
          {
            if ((null != applier) && (applier.isAlive()))
            {
              LOG.info("Requesting applier to resume !!");
              applier.unpause();
              LOG.info("Applier resumed !!");
            }
          }

          LOG.info("End phase 5 : Deleting Rows from tab table :");
        }

        LOG.info("Cleaner done for source :" + s.getSrcName() + "("
            + s.getSrcId() + ")");
      }
    } catch (SQLException ex)
    {
      LOG.error("Got SQL exception while cleaning bootstrapDB !!", ex);
    } catch (InterruptedException ie)
    {
      LOG.error("Got interrupted exception while cleaning bootstrapDB !!", ie);
    } finally
    {
      isCleaning = false;
    }
  }

  public void close()
  {
    _bootstrapDao.close();
  }

}
