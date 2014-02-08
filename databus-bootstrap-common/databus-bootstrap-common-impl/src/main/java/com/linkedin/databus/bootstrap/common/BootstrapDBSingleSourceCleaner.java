package com.linkedin.databus.bootstrap.common;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;




import org.apache.log4j.Logger;

import com.linkedin.databus.bootstrap.common.BootstrapCleanerStaticConfig.BootstrapDBType;
import com.linkedin.databus.bootstrap.common.BootstrapCleanerStaticConfig.RetentionStaticConfig;
import com.linkedin.databus.bootstrap.common.BootstrapDBMetaDataDAO.SourceStatusInfo;
import com.linkedin.databus.core.DatabusThreadBase;
import com.linkedin.databus.core.DbusConstants;
import com.linkedin.databus.core.DbusEventFactory;
import com.linkedin.databus.core.DbusEventV1Factory;
import com.linkedin.databus2.core.container.request.BootstrapDatabaseTooOldException;

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

public class BootstrapDBSingleSourceCleaner implements Runnable
{
  public static final String MODULE = BootstrapDBSingleSourceCleaner.class.getName();
  public final Logger LOG;

  private final String _name;
  private final String _source;
  private final DatabusThreadBase _applier;
  private final BootstrapCleanerStaticConfig _bootstrapCleanerStaticConfig;
  private final BootstrapReadOnlyConfig _bootstrapReadOnlyConfig;

  private BootstrapDBMetaDataDAO _bootstrapDao = null;
  private SourceStatusInfo _sourceStatusInfo = null;
  private final BootstrapDBCleanerQueryHelper _bootstrapDBCleanerQueryHelper;
  private final BootstrapDBCleanerQueryExecutor _bootstrapDBCleanerQueryExecutor;
  private final DbusEventFactory _eventFactory;
  private BootstrapLogInfo _lastValidLog;
  private volatile boolean _isCleaning = false;

  private static final AtomicInteger _numCleanersRunning = new AtomicInteger(0);
  private static final AtomicInteger _numCleanersRunningHWM = new AtomicInteger(0);

  public BootstrapDBSingleSourceCleaner(String name,
                                        String source,
                                        DatabusThreadBase applier,
                                        BootstrapCleanerStaticConfig bootstrapCleanerStaticConfig,
                                        BootstrapReadOnlyConfig bootstrapReadOnlyConfig)
  throws SQLException
  {
    _name = name;
    _source = source;
    _applier = applier;
    _bootstrapCleanerStaticConfig = bootstrapCleanerStaticConfig;
    _bootstrapReadOnlyConfig = bootstrapReadOnlyConfig;
    LOG = Logger.getLogger(name);

    Connection conn = getOrCreateConnection();
    if (null != source)
    {
      try
      {
        List<SourceStatusInfo> ssil = _bootstrapDao.getSourceIdAndStatusFromName(Arrays.asList(source), false);
        assert(ssil.size() == 1);
        _sourceStatusInfo = ssil.get(0);
      } catch (BootstrapDatabaseTooOldException bto)
      {
        LOG.error(
            "Not expected to receive this exception as activeCheck is turned-off",
            bto);
        throw new RuntimeException(bto);
      }
    }
    _bootstrapDBCleanerQueryHelper = BootstrapDBCleanerQueryHelper.getInstance();
    _bootstrapDBCleanerQueryExecutor = new BootstrapDBCleanerQueryExecutor(_name, conn, _bootstrapDBCleanerQueryHelper);
    _eventFactory = new DbusEventV1Factory();
  }

  /*
   * @return a bootstrapDB connection object. Note: The connection object is
   * still owned by BootstrapConn. SO dont close it
   */
  private Connection getOrCreateConnection() throws SQLException
  {
    Connection conn = null;

    if (_bootstrapDao == null)
    {
      LOG.info("<<<< Creating Bootstrap Connection!! >>>>");
      BootstrapConn dbConn = new BootstrapConn();
      final boolean autoCommit = true;
      try
      {
      _bootstrapDao = new BootstrapDBMetaDataDAO(dbConn,
          _bootstrapReadOnlyConfig.getBootstrapDBHostname(),
          _bootstrapReadOnlyConfig.getBootstrapDBUsername(),
          _bootstrapReadOnlyConfig.getBootstrapDBPassword(),
          _bootstrapReadOnlyConfig.getBootstrapDBName(), autoCommit);
        dbConn.initBootstrapConn(autoCommit,
            _bootstrapReadOnlyConfig.getBootstrapDBUsername(),
            _bootstrapReadOnlyConfig.getBootstrapDBPassword(),
            _bootstrapReadOnlyConfig.getBootstrapDBHostname(),
            _bootstrapReadOnlyConfig.getBootstrapDBName());
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

  @Override
  public void run()
  {
    doClean();
  }

  private void doClean()
  {
    try
    {
      incCleanerStats();

      SourceStatusInfo s = _sourceStatusInfo;
      {
        assert(s.getSrcName().equals(_source));
        BootstrapDBType type = _bootstrapCleanerStaticConfig.getBootstrapType(s.getSrcName());

        LOG.info("Cleaner running for source :" + s.getSrcName() + "("
            + s.getSrcId() + ") with bootstrapDB type :" + type);

        BootstrapLogInfo logInfo = _bootstrapDBCleanerQueryExecutor.getThresholdWindowSCN(type, s.getSrcId());

        if (null == logInfo)
        {
          LOG.info("No WindowSCN. Nothing to cleanup for source : "
              + s.getSrcName());
          return;
        }

        LOG.info("LOG info with lowest windowSCN :" + logInfo);

        LOG.info("Begin phase 1 : Gather candidate loginfo :");
        List<BootstrapLogInfo> candidateLogsInfo = _bootstrapDBCleanerQueryExecutor.getCandidateLogsInfo(
            logInfo.getMinWindowSCN(), (short) (s.getSrcId()));
        if ((null == candidateLogsInfo) || (candidateLogsInfo.isEmpty()))
        {
          LOG.info("No logs to cleanup for source :" + s.getSrcName() + "("
              + s.getSrcId() + ")");
          return;
        }
        LOG.info("End phase 1 : Gather candidate loginfo :");

        LOG.info("Initial Candidate Set for Source :" + s.getSrcName()
            + " is :" + candidateLogsInfo);
        RetentionStaticConfig rConf = _bootstrapCleanerStaticConfig.getRetentionConfig(s
            .getSrcName());
        LOG.info("Retention Config for source :" + s.getSrcName() + " is :"
            + rConf);

        LOG.info("Begin phase 2 : Filter based on retention config :");
        long scn = filterCandidateLogInfo((short) s.getSrcId(),
            candidateLogsInfo,
            _bootstrapCleanerStaticConfig.getRetentionConfig(s.getSrcName()));

        LOG.info("Log tables to be deleted for source :" + s.getSrcName() + "("
            + s.getSrcId() + ") are :" + candidateLogsInfo
            + ", Max SCN of deleted logs:" + scn);
        LOG.info("End phase 2 : Filter based on retention config :");

        if ((scn <= 0) || (candidateLogsInfo.isEmpty()))
        {
          LOG.info("Source :" + s.getSrcName() + "(" + s.getSrcId()
              + ") No log tables to be deleted !! MaxSCN : " + scn
              + ", candidateLogs :" + candidateLogsInfo);
          return;
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

        if ((_bootstrapCleanerStaticConfig.getBootstrapType(s.getSrcName()) == BootstrapDBType.BOOTSTRAP_CATCHUP_APPLIER_RUNNING)
            && ((_applier != null) || _bootstrapCleanerStaticConfig.forceTabTableCleanup(s
                .getSrcName())))
        {
          LOG.info("Source :" + s.getSrcName() + "(" + s.getSrcId()
              + ") is running in catchup_applier_running mode. "
              + "Will delete all rows whose scn is less than or equal to "
              + scn);
          if ((null != _applier) && (_applier.isAlive()))
          {
            LOG.info("Begin phase 5 : Pausing Applier and deleting Rows from tab table :");

            LOG.info("Requesting applier to pause !!");
            _applier.pause();
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
                && _bootstrapCleanerStaticConfig.isOptimizeTableEnabled(s.getSrcName()))
            {
              LOG.info("Optimizing table to reclaim space for source :"
                  + s.getSrcName() + "(" + s.getSrcId() + ")");
              _bootstrapDBCleanerQueryExecutor.optimizeTable(srcTable);
            }
          } finally
          {
            if ((null != _applier) && (_applier.isAlive()))
            {
              LOG.info("Requesting applier to resume !!");
              _applier.unpause();
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
      decCleanerStats();
    }
  }

  public BootstrapDBMetaDataDAO getBootstrapDao()
  {
    return _bootstrapDao;
  }

  public boolean isCleanerRunning()
  {
    return _isCleaning;
  }

  public String getName()
  {
    return _name;
  }

  public void close()
  {
    if (_bootstrapDao != null)
    {
      _bootstrapDao.close();
      _bootstrapDao = null;
    }
  }

  /**
   * A diagnotic to expose the number of cleaners running at a given moment
   */
  public static int getNumCleanersRunningHWM()
  {
    return _numCleanersRunningHWM.get();
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
      milliSecQty = qty * DbusConstants.NUM_MSECS_IN_SEC;
      break;

    default:
      throw new RuntimeException("Retention Config (" + config
          + ") expected to be time based but is not !!");

    }
    return milliSecQty;
  }

  private long filterCandidateLogInfo(short srcId,
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
      _lastValidLog = lastValidLog;
      break;
    }

    case RETENTION_SECONDS:
    {
      long quantity = config.getRetentionQuantity();
      LOG.info("Retaining tables which could contain events which is less than "
          + quantity + " seconds old !!");
      long currTs = System.currentTimeMillis() * DbusConstants.NUM_NSECS_IN_MSEC;
      long nanoSecQty = getMilliSecTime(config) * DbusConstants.NUM_NSECS_IN_MSEC;
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
      _lastValidLog = lastValidLog;
    }
      break;
    }

    long scn = -1;

    if (!candidateLogsInfo.isEmpty())
      scn = _bootstrapDBCleanerQueryExecutor.getSCNOfLastEventinLog(candidateLogsInfo.get(0), _eventFactory);

    return scn;
  }

  private void incCleanerStats()
  {
    _isCleaning = true;

    // Update HWM
    int curCleanersHwm = _numCleanersRunningHWM.get();
    // Increment internal metrics used for measuring parallelism
    int curCleaners = _numCleanersRunning.incrementAndGet();
    if (curCleanersHwm < curCleaners)
    {
      _numCleanersRunningHWM.set(curCleaners);
    }
  }

  private void decCleanerStats()
  {
    _isCleaning = false;

    // Decrement internal metrics used for measuring parallelism
    _numCleanersRunning.decrementAndGet();
  }
}
