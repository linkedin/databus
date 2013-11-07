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

import java.io.File;

import org.apache.log4j.Logger;

import com.linkedin.databus.bootstrap.common.BootstrapCleanerStaticConfig.DiskSpaceTriggerConfig;
import com.linkedin.databus.bootstrap.common.BootstrapDBCleaner;
import com.linkedin.databus.core.DatabusThreadBase;

public class BootstrapDBDiskSpaceTriggerThread extends DatabusThreadBase
{
  public static final String MODULE = BootstrapDBDiskSpaceTriggerThread.class
      .getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private final BootstrapDBCleaner _cleaner;
  private final DiskSpaceTriggerConfig _config;

  public BootstrapDBDiskSpaceTriggerThread(BootstrapDBCleaner cleaner,
      DiskSpaceTriggerConfig config)
  {
    super("DiskSpaceTrigger");
    _cleaner = cleaner;
    _config = config;
  }

  @Override
  public void run()
  {
    LOG.info("DiskSpaceTrigger Config :" + _config);

    File bootstrapDBDrive = new File(_config.getBootstrapDBDrive());

    if (!bootstrapDBDrive.exists())
    {
      LOG.error("Bootstrap Drive (" + bootstrapDBDrive.getAbsolutePath()
          + ") not found. Disabling DiskSpaceTrigger !!");
      return;
    }

    long run = 0;
    while (!isShutdownRequested())
    {
      run++;
      LOG.info("DiskSpace Trigger run :" + run);
      long total = bootstrapDBDrive.getTotalSpace();
      long available = bootstrapDBDrive.getUsableSpace();
      double percentAvailable = ((available * 100.0) / total);

      LOG.info("BootstrapDB Drive(" + bootstrapDBDrive.getAbsolutePath()
          + ") : Total bytes :" + total + ", Avalable bytes :" + available
          + ", Percent Available :" + percentAvailable);

      if (percentAvailable < _config.getAvailableThresholdPercent())
      {
        LOG.info("Available Space (" + percentAvailable
            + ") less than threshold (" + _config.getAvailableThresholdPercent()
            + "). Triggering cleaner !!");

        synchronized (_cleaner)
        {
          if (!_cleaner.isCleanerRunning())
          {
            _cleaner.doClean();
          }
          else
          {
            LOG.info("Skipping as cleaner is already running !!");
          }
        }
      }

      LOG.info("Sleeping for :" + _config.getRunIntervalSeconds()
          + " seconds !!");
      try
      {
        Thread.sleep(_config.getRunIntervalSeconds() * 1000);
      } catch (InterruptedException ie)
      {
        LOG.info("Got interrupted while sleeping for :"
            + _config.getRunIntervalSeconds() + " seconds !!");
      }
    }
    doShutdownNotify();
  }
}
