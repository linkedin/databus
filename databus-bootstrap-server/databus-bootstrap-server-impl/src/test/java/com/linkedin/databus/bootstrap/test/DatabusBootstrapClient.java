package com.linkedin.databus.bootstrap.test;

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

import java.io.IOException;
import java.sql.SQLException;

import com.linkedin.databus.bootstrap.api.BootstrapEventCallback;
import com.linkedin.databus.bootstrap.api.BootstrapProcessingException;
import com.linkedin.databus.bootstrap.server.BootstrapProcessor;
import com.linkedin.databus.bootstrap.server.BootstrapServerConfig;
import com.linkedin.databus.bootstrap.server.BootstrapServerStaticConfig;
import com.linkedin.databus.core.BootstrapCheckpointHandler;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DatabusRuntimeException;
import com.linkedin.databus.core.DbusClientMode;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.container.request.BootstrapDBException;
import com.linkedin.databus2.core.container.request.BootstrapDatabaseTooOldException;

/**
 * A helper class for testing bootstrap
 */
//TODO rename to DatabusBootstrapClientForTesting
public class DatabusBootstrapClient
{

  private Checkpoint                       _initState;
  private Checkpoint                       _currState;
  private BootstrapProcessor               _dbProcessor;
  private String                           sources[];
  private final BootstrapCheckpointHandler _bstCheckpointHandler;
  final long _targetScn ;

  public DatabusBootstrapClient(Checkpoint initCheckpoint, String bootstrapSources[]) throws InstantiationException,
      IllegalAccessException,
      ClassNotFoundException,
      SQLException,
      IOException,
      InvalidConfigException
  {
    _targetScn = -1;
    _initState = initCheckpoint;
    _currState = _initState;
    sources = bootstrapSources;
    _bstCheckpointHandler = new BootstrapCheckpointHandler(sources);
    init();
  }

  public DatabusBootstrapClient(String bootstrapSources[]) throws InstantiationException,
      IllegalAccessException,
      ClassNotFoundException,
      SQLException,
      IOException,
      InvalidConfigException
  {
    init();

    long startScn = 1; /* getProcessor().getCurrentScn(); */
    _targetScn = startScn;

    sources = bootstrapSources;
    _bstCheckpointHandler = new BootstrapCheckpointHandler(sources);

    _initState = _bstCheckpointHandler.createInitialBootstrapCheckpoint(null, 12345L);
    _initState.setBootstrapStartScn(startScn);

    _currState = _initState;
  }

  private void init() throws InstantiationException,
      IllegalAccessException,
      ClassNotFoundException,
      SQLException,
      IOException,
      InvalidConfigException
  {

    BootstrapServerConfig configBuilder = new BootstrapServerConfig();
    BootstrapServerStaticConfig staticConfig = configBuilder.build();
    _dbProcessor = new BootstrapProcessor(staticConfig, null);
  }

  public void setSources(String bootstrapSources[])
  {
    sources = bootstrapSources;
  }

  public Checkpoint getNextBatch(int batchSize, BootstrapEventCallback callBack) throws SQLException,
      BootstrapProcessingException,
      BootstrapDatabaseTooOldException,
      BootstrapDBException
  {
    boolean phaseCompleted = false;

    if (_currState.getConsumptionMode() == DbusClientMode.ONLINE_CONSUMPTION)
    {
      return _currState;
    }

    if (_currState.getConsumptionMode() == DbusClientMode.BOOTSTRAP_SNAPSHOT)
    {
      phaseCompleted = getProcessor().streamSnapShotRows(_currState, callBack);
      _currState.bootstrapCheckPoint();

      // If there are no more rows to be fetched from the current source, move to the
      // catchup
      // phase.
      if (phaseCompleted)
      {
       _bstCheckpointHandler.finalizeSnapshotPhase(_currState);
       _bstCheckpointHandler.advanceAfterSnapshotPhase(_currState);
       _currState.setBootstrapTargetScn(_targetScn);
       _bstCheckpointHandler.advanceAfterTargetScn(_currState);
      }
    }
    else if (_currState.getConsumptionMode() == DbusClientMode.BOOTSTRAP_CATCHUP)
    {
      phaseCompleted = getProcessor().streamCatchupRows(_currState, callBack);
      _currState.bootstrapCheckPoint();

      // If there are no more rows to be fetched from the current source, go the next
      // source
      if (phaseCompleted)
      {
        _bstCheckpointHandler.finalizeCatchupPhase(_currState);
        _bstCheckpointHandler.advanceAfterCatchupPhase(_currState);
        if (!_bstCheckpointHandler.needsMoreSnapshot(_currState))
        {
          _currState.setConsumptionMode(DbusClientMode.ONLINE_CONSUMPTION);
        }
      }
    }
    else
    {
      throw new DatabusRuntimeException("Unknown checkpoint type:" + _currState);
    }

    return _currState;
  }

  private BootstrapProcessor getProcessor()
  {
    return _dbProcessor;
  }
}
