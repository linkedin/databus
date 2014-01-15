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


import junit.framework.Assert;

import org.easymock.EasyMock;
import org.testng.annotations.Test;

import com.linkedin.databus.bootstrap.api.BootstrapEventCallback;
import com.linkedin.databus.bootstrap.api.BootstrapEventProcessResult;
import com.linkedin.databus.core.Checkpoint;

public class TestBootstrapProcessor
{

  @Test
  public void testIsPhaseCompletedFlag()
  throws Exception
  {
    BootstrapProcessor bp = new BootstrapProcessor();

    long processedRowCount = 1;
    // order of args : processedRowCount, isLimitExceeded, isDropped, isError
    BootstrapEventProcessResult result_ok = new BootstrapEventProcessResult(processedRowCount, false, false, false);
    BootstrapEventProcessResult result_err = new BootstrapEventProcessResult(processedRowCount, false, false, true);
    BootstrapEventProcessResult result_ile = new BootstrapEventProcessResult(processedRowCount, true, false, false);

    Checkpoint ckpt_ss = new Checkpoint("{\"consumption_mode\":\"BOOTSTRAP_SNAPSHOT\", \"bootstrap_since_scn\":0," +
        "\"bootstrap_start_scn\":1000,\"bootstrap_target_scn\":2000,\"bootstrap_catchup_source_index\":0," +
        "\"bootstrap_snapshot_source_index\":1}");
    ckpt_ss.assertCheckpoint();

    Checkpoint ckpt_cu = new Checkpoint("{\"consumption_mode\":\"BOOTSTRAP_CATCHUP\", \"bootstrap_since_scn\":0," +
            "\"bootstrap_start_scn\":1000,\"bootstrap_target_scn\":2000,\"bootstrap_catchup_source_index\":1," +
            "\"bootstrap_snapshot_source_index\":1}");
    ckpt_cu.assertCheckpoint();
    long numRowsReadFromDb = 1;
    long maxRowsPerFetch = 2;
    long windowScn = 1;

    // result is null, phaseCompleted == false ( irrespective of snapshot / catchup phase )
    numRowsReadFromDb = 0;
    boolean pc = bp.computeIsPhaseCompleted(null, ckpt_ss, numRowsReadFromDb, maxRowsPerFetch, windowScn);
    Assert.assertTrue(pc);
    pc = bp.computeIsPhaseCompleted(null, ckpt_cu, numRowsReadFromDb, maxRowsPerFetch, windowScn);
    Assert.assertTrue(pc);

    numRowsReadFromDb = 1;
    // numRowsReadFromDb < maxRowsPerFetch, in SNAPSHOT mode
    pc = bp.computeIsPhaseCompleted(result_ok, ckpt_ss, numRowsReadFromDb, maxRowsPerFetch, windowScn);
    Assert.assertTrue(pc);
    // Same as above, but result.isError == true ( overrides all other conditions )
    pc = bp.computeIsPhaseCompleted(result_err, ckpt_ss, numRowsReadFromDb, maxRowsPerFetch, windowScn);
    Assert.assertFalse(pc);
    // Same as above, but result.isLimitExceeded == true ( overrides all other conditions )
    pc = bp.computeIsPhaseCompleted(result_ile, ckpt_ss, numRowsReadFromDb, maxRowsPerFetch, windowScn);
    Assert.assertFalse(pc);

    // numRowsReadFromDb == maxRowsPerFetch, in SNAPSHOT mode
    numRowsReadFromDb = 2;
    pc = bp.computeIsPhaseCompleted(result_ok, ckpt_ss, numRowsReadFromDb, maxRowsPerFetch, windowScn);
    Assert.assertFalse(pc);
    // Same as above, but result.isError == true
    pc = bp.computeIsPhaseCompleted(result_err, ckpt_ss, numRowsReadFromDb, maxRowsPerFetch, windowScn);
    Assert.assertFalse(pc);
    // Same as above, but result.isLimitExceeded == true
    pc = bp.computeIsPhaseCompleted(result_ile, ckpt_ss, numRowsReadFromDb, maxRowsPerFetch, windowScn);
    Assert.assertFalse(pc);

    numRowsReadFromDb = 1;
    // numRowsReadFromDb < maxRowsPerFetch, in CATCHUP mode, windowScn != bootstrap_target_scn
    pc = bp.computeIsPhaseCompleted(result_ok, ckpt_ss, numRowsReadFromDb, maxRowsPerFetch, windowScn);
    Assert.assertTrue(pc);

    windowScn = ckpt_cu.getBootstrapTargetScn();
    pc = bp.computeIsPhaseCompleted(result_ok, ckpt_cu, numRowsReadFromDb, maxRowsPerFetch, windowScn);
    Assert.assertTrue(pc);

    // Same as above, but result.isError == true ( overrides all other conditions )
    pc = bp.computeIsPhaseCompleted(result_err, ckpt_ss, numRowsReadFromDb, maxRowsPerFetch, windowScn);
    Assert.assertFalse(pc);
    // Same as above, but result.isLimitExceeded == true ( overrides result being null )
    pc = bp.computeIsPhaseCompleted(result_ile, ckpt_ss, numRowsReadFromDb, maxRowsPerFetch, windowScn);
    Assert.assertFalse(pc);

    // numRowsReadFromDb == maxRowsPerFetch, in CATCHUP mode
    numRowsReadFromDb = 2;
    windowScn = 10; // not equal to bootstrap_target_scn
    pc = bp.computeIsPhaseCompleted(result_ok, ckpt_cu, numRowsReadFromDb, maxRowsPerFetch, windowScn);
    Assert.assertFalse(pc);
    windowScn = ckpt_cu.getBootstrapTargetScn(); // equal to bootstrap_target_scn, but does not matter still
    pc = bp.computeIsPhaseCompleted(result_ok, ckpt_cu, numRowsReadFromDb, maxRowsPerFetch, windowScn);
    Assert.assertFalse(pc);

    // Same as above, but result.isError == true
    pc = bp.computeIsPhaseCompleted(result_err, ckpt_cu, numRowsReadFromDb, maxRowsPerFetch, windowScn);
    Assert.assertFalse(pc);
    // Same as above, but result.isLimitExceeded == true
    pc = bp.computeIsPhaseCompleted(result_ile, ckpt_cu, numRowsReadFromDb, maxRowsPerFetch, windowScn);
    Assert.assertFalse(pc);
  }

  @Test
  public void testCkptWriteLogic()
  throws Exception
  {
    BootstrapProcessor bp = new BootstrapProcessor();

    long processedRowCount = 1;
    // order of args : processedRowCount, isLimitExceeded, isDropped, isError
    BootstrapEventProcessResult result_ok = new BootstrapEventProcessResult(processedRowCount, false, false, false);
    BootstrapEventProcessResult result_zero_ok = new BootstrapEventProcessResult(0, false, false, false);
    BootstrapEventProcessResult result_err = new BootstrapEventProcessResult(processedRowCount, false, false, true);
    BootstrapEventProcessResult result_zero_ile = new BootstrapEventProcessResult(0, true, false, false);

    Checkpoint ckpt_ss = new Checkpoint("{\"consumption_mode\":\"BOOTSTRAP_SNAPSHOT\", \"bootstrap_since_scn\":0," +
        "\"bootstrap_start_scn\":1000,\"bootstrap_target_scn\":2000,\"bootstrap_catchup_source_index\":0," +
        "\"bootstrap_snapshot_source_index\":1}");
    ckpt_ss.assertCheckpoint();

    long numRowsReadFromDb = 1;

    BootstrapEventCallback callback1 = EasyMock.createMock(BootstrapEventCallback.class);
    EasyMock.replay(callback1);

    BootstrapEventCallback callback2 = EasyMock.createMock(BootstrapEventCallback.class);
    callback2.onCheckpointEvent(ckpt_ss, null);
    EasyMock.replay(callback2);

    BootstrapEventCallback callback3 = EasyMock.createMock(BootstrapEventCallback.class);
    callback3.onCheckpointEvent(ckpt_ss, null);
    EasyMock.replay(callback3);

    // No checkpoint when result is null
    bp.writeCkptIfAppropriate(null, callback1, numRowsReadFromDb, ckpt_ss, "test");

    // No checkpoint when result is error
    bp.writeCkptIfAppropriate(result_err, callback1, numRowsReadFromDb, ckpt_ss, "test");

    // numRowsWritten == 1, must checkpoint
    bp.writeCkptIfAppropriate(result_ok, callback2, numRowsReadFromDb, ckpt_ss, "test");

    // numRowsWritten == 0, must checkpoint as numRowsReadFromDb > 0
    bp.writeCkptIfAppropriate(result_zero_ok, callback3, numRowsReadFromDb, ckpt_ss, "test");

    // numRowsWritten == 0, must have checkpointed as numRowsReadFromDb > 0. However result is client buffer exceeded
    // So .. sorry to disappoint but no checkpoint as we want that pending_event_header
    bp.writeCkptIfAppropriate(result_zero_ile, callback1, numRowsReadFromDb, ckpt_ss, "test");

    // result != null, numRowsWritten == 0, numRowsReadFromDb == 0. We expect a RuntimeException here
    try
    {
      bp.writeCkptIfAppropriate(result_zero_ok, callback2, 0, ckpt_ss, "test");
      Assert.fail();
    } catch (RuntimeException e)
    {
    }

  }

}
