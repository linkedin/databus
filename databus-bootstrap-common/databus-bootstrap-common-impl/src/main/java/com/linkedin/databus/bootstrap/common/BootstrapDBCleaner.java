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

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.linkedin.databus.core.DatabusThreadBase;
import com.linkedin.databus.core.util.NamedThreadFactory;
import com.linkedin.databus2.core.DatabusException;

public class BootstrapDBCleaner
{
  public static final String MODULE = BootstrapDBCleaner.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);
  private final long TERMINATION_TIMEOUT_IN_MS = 10000;

  public BootstrapCleanerStaticConfig _cleanerConfig;
  public BootstrapReadOnlyConfig _bootstrapConfig;
  private Map<String, DatabusThreadBase> _appliers;
  private final Map<String, BootstrapDBSingleSourceCleaner> _cleaners;

  private final ExecutorService _cleanerThreadPoolService;
  private final Map<String, Future<?>> _cleanerFutures;

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
    _cleaners = new HashMap<String, BootstrapDBSingleSourceCleaner>();
    if (null != sources)
    {
      for (String source: sources)
      {
        String perSourceName = name + "_" + source;
        DatabusThreadBase perSourceApplier = _appliers.get(source);
        BootstrapDBSingleSourceCleaner cleaner =
            new BootstrapDBSingleSourceCleaner(perSourceName,
                source,
                perSourceApplier,
                config,
                bootstrapConfig);
        _cleaners.put(source,cleaner);
      }
    }
    ThreadFactory tf = new NamedThreadFactory(name);
    _cleanerThreadPoolService = Executors.newCachedThreadPool(tf);
    _cleanerFutures =  new HashMap<String, Future<?>>();
    LOG.info("Cleaner Config is :" + _cleanerConfig);
  }

  public synchronized void doClean()
  {
    // Invoke doClean on each of the individual single source cleaners
    for (Map.Entry<String, BootstrapDBSingleSourceCleaner> entry : _cleaners.entrySet())
    {
      String source = entry.getKey();
      BootstrapDBSingleSourceCleaner singleSourceCleaner = entry.getValue();
      Future<?> c = _cleanerFutures.get(source);
      if (c != null && !c.isDone())
      {
        LOG.info("Skipping running cleaner as it is already running for source = " + source);
      }
      else
      {
        LOG.info("Submitting a cleaner task for source = " + source);
        Future<?> cleaner = _cleanerThreadPoolService.submit(singleSourceCleaner);
        _cleanerFutures.put(source, cleaner);
      }
    }
  }

  public synchronized boolean isAnyCleanerRunning()
  {
    for (Map.Entry<String, Future<?>> entry : _cleanerFutures.entrySet())
    {
      Future<?> cleanerFuture = entry.getValue();
      if (!cleanerFuture.isDone())
      {
        LOG.debug("Cleaner process is running for source = " + entry.getKey());
        return true;
      }
    }
    LOG.info("There are no cleaner processes running");
    return false;
  }

  public synchronized void sleepTillNoCleanerIsRunning()
  throws DatabusException, InterruptedException
  {
    final long maxWaitTime = TERMINATION_TIMEOUT_IN_MS;
    long waitTime = 0;
    while (isAnyCleanerRunning())
    {
      if (waitTime >= TERMINATION_TIMEOUT_IN_MS)
      {
        throw new DatabusException("The cleaners have not terminated within " + maxWaitTime + " ms");
      }
      final long sleepIntervalInMs = 100;
      Thread.sleep(sleepIntervalInMs);
      waitTime += sleepIntervalInMs;
    }
  }

  public void close()
  {
    List<Runnable> incompleteCleaners = _cleanerThreadPoolService.shutdownNow();
    if (incompleteCleaners.size() > 0)
    {
      // The cleaners that have not started as of initiating a shutdown, will not be started
      // Not an error, hence logging for informational purpose
      LOG.info("Number of cleaners that have not completed = " + incompleteCleaners.size());
      LOG.info("Printing out sources for which cleaners what not completed ");
      for (Runnable r: incompleteCleaners)
      {
        BootstrapDBSingleSourceCleaner bsc = (BootstrapDBSingleSourceCleaner) r;
        LOG.error(bsc.getName());
      }
    }

    try
    {
      boolean hasTerminated = _cleanerThreadPoolService.awaitTermination(TERMINATION_TIMEOUT_IN_MS, TimeUnit.MILLISECONDS);
      LOG.info("Result of terminating cleaner thread pool service: " + (hasTerminated ? "success" : "failure"));
    } catch (InterruptedException e)
    {
      LOG.error("Cleaner thread pool service termination has been interrupted", e);
    } finally
    {
      for (Map.Entry<String, BootstrapDBSingleSourceCleaner> entry : _cleaners.entrySet())
      {
        String source = entry.getKey();
        BootstrapDBSingleSourceCleaner singleSourceCleaner = entry.getValue();
        LOG.info("Invoking close on cleaner for source = " + source);
        singleSourceCleaner.close();
      }
    }
  }

}
