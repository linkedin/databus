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

import org.apache.log4j.Logger;

import com.linkedin.databus.bootstrap.common.BootstrapCleanerStaticConfig.PeriodicTriggerConfig;
import com.linkedin.databus.bootstrap.common.BootstrapDBCleaner;
import com.linkedin.databus.core.DatabusThreadBase;

public class BootstrapDBPeriodicTriggerThread extends DatabusThreadBase
{
  public static final String MODULE = BootstrapDBPeriodicTriggerThread.class
      .getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  public static final long MILLISEC_IN_SECONDS = 1000;

  private final BootstrapDBCleaner _cleaner;
  private final PeriodicTriggerConfig _config;

  public BootstrapDBPeriodicTriggerThread(BootstrapDBCleaner cleaner,
      PeriodicTriggerConfig config)
  {
    super("BootstrapDBPeriodicTriggerThread");
    this._cleaner = cleaner;
    this._config = config;
  }

  @Override
  public void run()
  {
    LOG.info("PeriodicTrigger Config :" + _config);

    long runNumber = 0;
    long roundBeginTime = System.currentTimeMillis();
    long roundEndTime = roundBeginTime;
    long timeToSleep = _config.getRunIntervalSeconds() * 1000;

    if (!_config.isRunOnStart())
    {
      LOG.info("Sleeping for :" + (timeToSleep / MILLISEC_IN_SECONDS)
          + " seconds !!");
      try
      {
        Thread.sleep(timeToSleep);
      } catch (InterruptedException ie)
      {
        LOG.info("Got interrupted while sleeping for :" + timeToSleep);
      }
    }

    while (!isShutdownRequested())
    {
      runNumber++;
      LOG.info("Run : " + runNumber);

      synchronized (_cleaner)
      {
        roundBeginTime = System.currentTimeMillis();
        _cleaner.doClean();
        roundEndTime = System.currentTimeMillis();
      }

      long timeTakenMs = roundEndTime - roundBeginTime;
      LOG.info("Round (" + runNumber + ") complete. Took around "
          + (timeTakenMs / 1000) + " seconds !!");
      timeToSleep = (_config.getRunIntervalSeconds() * MILLISEC_IN_SECONDS)
          - timeTakenMs;
      LOG.info("Sleeping for :" + (timeToSleep / MILLISEC_IN_SECONDS)
          + " seconds !!");

      try
      {
        Thread.sleep(timeToSleep);
      } catch (InterruptedException ie)
      {
        LOG.info("Got interrupted while sleeping for :" + timeToSleep + " ms");
      }
    }
    doShutdownNotify();
  }
}
