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
package com.linkedin.databus.core;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.databus2.test.TestUtil;

/**
 * Unit tests for {@link BootstrapCheckpointHandler}
 */
public class TestBootstrapCheckpointHandler
{
  private static final String[] ONE_SOURCE = {"source1"};
  private static final List<String> TWO_SOURCES = Arrays.asList("source1", "source2");
  private static final String[] THREE_SOURCES = {"Source1", "Source2", "Source3"};
  private static final String[] MANY_SOURCES = {"A", "B", "C", "D", "E", "F", "G", "H"};

  @BeforeTest
  public void setupClass()
  {
    TestUtil.setupLoggingWithTimestampedFile(true, "/tmp/TestBootstrapCheckpointHandler_", ".log", Level.ERROR);
  }

  @Test
  /** Tests the logic for creating an initial checkpoint for bootstrapping */
  public void testStartBootstrap()
  {
    //one source
    BootstrapCheckpointHandler handler = new BootstrapCheckpointHandler(ONE_SOURCE);
    Checkpoint cp1 = handler.createInitialBootstrapCheckpoint(null, 0L);
    assertStartBootstrapCheckpoint(handler, cp1, ONE_SOURCE[0], 0L);

    //two sources
    handler = new BootstrapCheckpointHandler(TWO_SOURCES);
    Checkpoint cp2 = handler.createInitialBootstrapCheckpoint(null, (long)Integer.MAX_VALUE + 5);
    assertStartBootstrapCheckpoint(handler, cp2, TWO_SOURCES.get(0), (long)Integer.MAX_VALUE + 5);

    //three sources
    handler = new BootstrapCheckpointHandler(THREE_SOURCES);
    Checkpoint cp3 = handler.createInitialBootstrapCheckpoint(null, Long.MAX_VALUE);
    assertStartBootstrapCheckpoint(handler, cp3, THREE_SOURCES[0], Long.MAX_VALUE);

    //invalid sinceScn
    try
    {
      handler.createInitialBootstrapCheckpoint(null, -1L);
      Assert.fail("DatabusRuntimeException expected");
    }
    catch (DatabusRuntimeException e)
    {
      //OK
    }
  }

  @Test
  /** Tests the logic for transitioning from a snapshot to a catchup phase with one source */
  public void testAdvanceAfterSnapshotPhaseOneSource()
  {
    BootstrapCheckpointHandler handler = new BootstrapCheckpointHandler(ONE_SOURCE);
    Checkpoint cp1 = handler.createInitialBootstrapCheckpoint(null, 1L);

    try
    {
      //we should not be able to advance the checkpoint without startScn
      cp1.setSnapshotOffset(100L);
      Assert.fail("InvalidCheckpointException expected");
    }
    catch (InvalidCheckpointException e)
    {
      //OK -- reset the checkpoint
      cp1.setSnapshotOffset(0);
    }

    //once bootstrap_start_scn is set, we should be able to change the snapshot offset
    cp1.setBootstrapStartScn(10L);
    cp1.setSnapshotOffset(100L);
    handler.assertBootstrapCheckpoint(cp1);
    Assert.assertTrue(!cp1.isSnapShotSourceCompleted());

    //now finalize the snapshot phase
    handler.finalizeSnapshotPhase(cp1);
    assertSnapshotCompleteCheckpoint(handler, cp1, 0, ONE_SOURCE[0]);
    Assert.assertTrue(cp1.isSnapShotSourceCompleted());

    cp1.setBootstrapTargetScn(100L);

    //now advance to next phase
    handler.advanceAfterSnapshotPhase(cp1);
    assertAfterSnapshotCheckpoint(cp1, 1, Checkpoint.NO_SOURCE_NAME);
    Assert.assertTrue(cp1.isSnapShotSourceCompleted());
    Assert.assertTrue(! handler.needsMoreSnapshot(cp1));
  }

  @Test
  /** Tests the logic for transitioning from a snapshot to a catchup phase with multiple sources*/
  public void testAdvanceAfterSnapshotPhaseManySources()
  {
    BootstrapCheckpointHandler handler = new BootstrapCheckpointHandler(THREE_SOURCES);
    Checkpoint cp1 = handler.createInitialBootstrapCheckpoint(null, 1L);

    //once bootstrap_start_scn is set, we should be able to change the snapshot offset
    cp1.setBootstrapStartScn(10L);
    cp1.setSnapshotOffset(100L);
    handler.assertBootstrapCheckpoint(cp1);
    Assert.assertTrue(!cp1.isSnapShotSourceCompleted());

    //now finalize the snapshot phase
    handler.finalizeSnapshotPhase(cp1);
    assertSnapshotCompleteCheckpoint(handler, cp1, 0, THREE_SOURCES[0]);
    Assert.assertTrue(cp1.isSnapShotSourceCompleted());

    cp1.setBootstrapTargetScn(110L);

    //now advance to next phase
    handler.advanceAfterSnapshotPhase(cp1);
    assertAfterSnapshotCheckpoint(cp1, 1, THREE_SOURCES[1]);
    Assert.assertTrue(cp1.isSnapShotSourceCompleted());
    Assert.assertTrue(handler.needsMoreSnapshot(cp1));
  }

  @Test
  /** Tests the logic of advancing the checkpoint after completing a Catchup phase with one source */
  public void testAdvanceAfterCatchupPhaseOneSource()
  {
    //start bootstrap
    BootstrapCheckpointHandler handler = new BootstrapCheckpointHandler(ONE_SOURCE);
    Checkpoint cp1 = handler.createInitialBootstrapCheckpoint(null, 100L);
    assertStartBootstrapCheckpoint(handler, cp1, ONE_SOURCE[0], 100L);
    cp1.setBootstrapStartScn(200L);

    //now finalize the snapshot phase
    handler.finalizeSnapshotPhase(cp1);
    assertSnapshotCompleteCheckpoint(handler, cp1, 0, ONE_SOURCE[0]);
    Assert.assertTrue(cp1.isSnapShotSourceCompleted());

    //try to set an invalid targetSCN (< startSCN)
    try
    {
      cp1.setBootstrapTargetScn(100L);
      Assert.fail("InvalidCheckpointException expected");
    }
    catch (InvalidCheckpointException e)
    {
      //OK
    }

    //now advance to next phase -- catchup
    handler.advanceAfterSnapshotPhase(cp1);
    assertAfterSnapshotCheckpoint(cp1, 1, Checkpoint.NO_SOURCE_NAME);
    Assert.assertTrue(cp1.isSnapShotSourceCompleted());
    Assert.assertTrue(!handler.needsMoreSnapshot(cp1));

    //set up a valid targetScn
    cp1.setBootstrapTargetScn(200L);
    handler.advanceAfterTargetScn(cp1);

    //finalize the catchup phase
    handler.finalizeCatchupPhase(cp1);
    assertCatchupCompleteCheckpoint(handler, cp1, 0, ONE_SOURCE[0]);

    //advance after catchup
    handler.advanceAfterCatchupPhase(cp1);
    assertAfterFinalCatchupCheckpoint(handler, cp1);
  }

  @Test
  /**
   * Tests the logic of advancing the checkpoint after completing a Catchup phase with
   * many sources */
  public void testAdvanceAfterCatchupPhaseManySources()
  {
    //start bootstrap
    BootstrapCheckpointHandler handler = new BootstrapCheckpointHandler(TWO_SOURCES);
    Checkpoint cp1 = handler.createInitialBootstrapCheckpoint(null, 50L);
    assertStartBootstrapCheckpoint(handler, cp1, TWO_SOURCES.get(0), 50L);
    cp1.setBootstrapStartScn(200L);

    //now finalize the snapshot phase
    handler.finalizeSnapshotPhase(cp1);
    assertSnapshotCompleteCheckpoint(handler, cp1, 0, TWO_SOURCES.get(0));
    Assert.assertTrue(cp1.isSnapShotSourceCompleted());


    //now advance to next phase -- catchup for source1
    handler.advanceAfterSnapshotPhase(cp1);
    assertAfterSnapshotCheckpoint(cp1, 1, TWO_SOURCES.get(1));
    Assert.assertTrue(cp1.isSnapShotSourceCompleted());
    Assert.assertTrue(handler.needsMoreSnapshot(cp1));

    //set up a valid targetScn
    cp1.setBootstrapTargetScn(250L);
    handler.advanceAfterTargetScn(cp1);

    //finalize the catchup phase
    handler.finalizeCatchupPhase(cp1);
    assertCatchupCompleteCheckpoint(handler, cp1, 0, TWO_SOURCES.get(0));

    //advance after catchup for source1 -- switch to snapshot for source2
    handler.advanceAfterCatchupPhase(cp1);
    assertSnapshotAfterCatchupCheckpoint(handler, cp1, 1, TWO_SOURCES.get(1));
  }

  @Test
  public void testSanity()
  {
    BootstrapCheckpointHandler handler = new BootstrapCheckpointHandler(ONE_SOURCE);
    Assert.assertTrue(handler.needsMoreCowbell(null));
  }

  @Test
  /**
   * Simulates a scenario of a full bootstrap with two sources
   */
  public void testFullBootstrapTwoSources()
  {
    //start bootstrap
    BootstrapCheckpointHandler handler = new BootstrapCheckpointHandler(TWO_SOURCES);
    Checkpoint cp1 = handler.createInitialBootstrapCheckpoint(null, 50L);
    assertStartBootstrapCheckpoint(handler, cp1, TWO_SOURCES.get(0), 50L);
    cp1.setBootstrapStartScn(200L);

    //now finalize the snapshot phase
    handler.finalizeSnapshotPhase(cp1);
    assertSnapshotCompleteCheckpoint(handler, cp1, 0, TWO_SOURCES.get(0));

    //now advance to next phase -- catchup for source1
    handler.advanceAfterSnapshotPhase(cp1);
    assertAfterSnapshotCheckpoint(cp1, 1, TWO_SOURCES.get(1));
    Assert.assertTrue(handler.needsMoreSnapshot(cp1));

    //set up a valid targetScn
    cp1.setBootstrapTargetScn(250L);
    handler.advanceAfterTargetScn(cp1);

    //finalize the catchup phase
    handler.finalizeCatchupPhase(cp1);
    assertCatchupCompleteCheckpoint(handler, cp1, 0, TWO_SOURCES.get(0));

    //advance after catchup for source1 -- switch to snapshot for source2
    handler.advanceAfterCatchupPhase(cp1);
    assertSnapshotAfterCatchupCheckpoint(handler, cp1, 1, TWO_SOURCES.get(1));

    //finalize snapshot for source2
    handler.finalizeSnapshotPhase(cp1);
    assertSnapshotCompleteCheckpoint(handler, cp1, 1, TWO_SOURCES.get(1));

    //advance to next phase -- catchup for source1
    handler.advanceAfterSnapshotPhase(cp1);
    assertAfterSnapshotCheckpoint(cp1, 2, Checkpoint.NO_SOURCE_NAME);

    //set up a valid targetScn
    cp1.setBootstrapTargetScn(250L);
    handler.advanceAfterTargetScn(cp1);

    //finalize the catchup phase for source 1
    handler.finalizeCatchupPhase(cp1);
    assertCatchupCompleteCheckpoint(handler, cp1, 0, TWO_SOURCES.get(0));

    //advance after catchup for source1 -- switch to catchup for source2
    handler.advanceAfterCatchupPhase(cp1);
    assertAfterNonfinalCatchupCheckpoint(handler, cp1, 1, TWO_SOURCES.get(1));

    //finalize the catchup phase for source 2
    handler.finalizeCatchupPhase(cp1);
    assertCatchupCompleteCheckpoint(handler, cp1, 1, TWO_SOURCES.get(1));

    //advance after catchup for source2 -- done
    handler.advanceAfterCatchupPhase(cp1);
    assertAfterFinalCatchupCheckpoint(handler, cp1);
  }

  @Test
  /**
   * Simulates a scenario of a full bootstrap with many sources
   */
  public void testFullBootstrapManySources()
  {
    final Logger log = Logger.getLogger("TestBootstrapCheckpointHandler.testFullBootstrapManySources");
    //log.setLevel(Level.INFO);
    log.info("START SNAPSHOT: " + MANY_SOURCES[0]);
    BootstrapCheckpointHandler handler = new BootstrapCheckpointHandler(MANY_SOURCES);
    Checkpoint cp1 = handler.createInitialBootstrapCheckpoint(null, 0L);
    assertStartBootstrapCheckpoint(handler, cp1, MANY_SOURCES[0], 0L);
    cp1.setBootstrapStartScn(1000L);


    for (int snapshotSourceIndex = 0; snapshotSourceIndex < MANY_SOURCES.length; ++snapshotSourceIndex)
    {
      log.info("FINISH SNAPSHOT: " + MANY_SOURCES[snapshotSourceIndex]);
      handler.finalizeSnapshotPhase(cp1);
      assertSnapshotCompleteCheckpoint(handler, cp1, snapshotSourceIndex, MANY_SOURCES[snapshotSourceIndex]);

      log.info("    START  CATCHUP: " + MANY_SOURCES[0]);
      handler.advanceAfterSnapshotPhase(cp1);
      assertAfterSnapshotCheckpoint(cp1, snapshotSourceIndex + 1,
                                    snapshotSourceIndex < MANY_SOURCES.length - 1 ? MANY_SOURCES[snapshotSourceIndex + 1]
                                                                                  : Checkpoint.NO_SOURCE_NAME);
      Assert.assertTrue(snapshotSourceIndex < MANY_SOURCES.length -1 || !handler.needsMoreSnapshot(cp1));

      //set up a valid targetScn
      cp1.setBootstrapTargetScn(1000L + snapshotSourceIndex * 10);
      handler.advanceAfterTargetScn(cp1);

      for (int catchupSourceIndex = 0; catchupSourceIndex <= snapshotSourceIndex; ++catchupSourceIndex)
      {
        log.info("    FINISH CATCHUP: " + MANY_SOURCES[catchupSourceIndex]);
        handler.finalizeCatchupPhase(cp1);
        assertCatchupCompleteCheckpoint(handler, cp1, catchupSourceIndex, MANY_SOURCES[catchupSourceIndex]);

        log.info("    ADVANCE: " + MANY_SOURCES[catchupSourceIndex]);
        handler.advanceAfterCatchupPhase(cp1);
        if (catchupSourceIndex < snapshotSourceIndex)
        {
          log.info("    START  CATCHUP: " + MANY_SOURCES[catchupSourceIndex + 1]);
          assertAfterNonfinalCatchupCheckpoint(handler, cp1, catchupSourceIndex + 1,
                                               MANY_SOURCES[catchupSourceIndex + 1]);
        }
      }
      if (snapshotSourceIndex < MANY_SOURCES.length - 1)
      {
        log.info("START  SNAPSHOT: " + MANY_SOURCES[snapshotSourceIndex + 1] );
        assertSnapshotAfterCatchupCheckpoint(handler, cp1, snapshotSourceIndex + 1,
                                             MANY_SOURCES[snapshotSourceIndex + 1]);
      }
    }

    log.info("bootstrap should be done");
    assertAfterFinalCatchupCheckpoint(handler, cp1);
  }

  @Test
  public void testAdvanceAfterTargetScn()
  {
    BootstrapCheckpointHandler handler = new BootstrapCheckpointHandler(ONE_SOURCE);
    Checkpoint cp1 = handler.createInitialBootstrapCheckpoint(null, 100L);

    assertStartBootstrapCheckpoint(handler, cp1, ONE_SOURCE[0], 100L);
    cp1.setBootstrapStartScn(200L);

    //now finalize the snapshot phase
    handler.finalizeSnapshotPhase(cp1);
    assertSnapshotCompleteCheckpoint(handler, cp1, 0, ONE_SOURCE[0]);
    Assert.assertTrue(cp1.isSnapShotSourceCompleted());

    //now advance to next phase -- catchup
    handler.advanceAfterSnapshotPhase(cp1);
    assertAfterSnapshotCheckpoint(cp1, 1, Checkpoint.NO_SOURCE_NAME);
    Assert.assertTrue(cp1.isSnapShotSourceCompleted());
    Assert.assertTrue(!handler.needsMoreSnapshot(cp1));

    //try to set an invalid targetSCN (< startSCN)
    try
    {
      cp1.setBootstrapTargetScn(100L);
      Assert.fail("InvalidCheckpointException expected");
    }
    catch (InvalidCheckpointException e)
    {
      //OK
    }

    //try to setup -1 as targetScn
    try
    {
      cp1.setBootstrapTargetScn(Checkpoint.UNSET_BOOTSTRAP_TARGET_SCN);
      handler.advanceAfterTargetScn(cp1);
      Assert.fail("InvalidCheckpointException expected");
    }
    catch (InvalidCheckpointException e)
    {
      //OK
    }

    cp1.setBootstrapTargetScn(200L);
    handler.advanceAfterTargetScn(cp1);
    Assert.assertEquals(DbusClientMode.BOOTSTRAP_CATCHUP, cp1.getConsumptionMode());
    Assert.assertEquals(cp1.getWindowOffset().longValue(), 0L);
    Assert.assertEquals(cp1.getWindowScn(), cp1.getBootstrapStartScn().longValue());
  }

  /**
   * Verify that a change of servers does not leave the checkpoint in an inconsistent
   * state relative to the checkpoint handler.
   */
  @Test
  public void testBootstrapResetNewServers()  // borrowed from similar V3 test
  throws Exception
  {
    List<String> sourceNames = new ArrayList<String>();
    final String firstSourceName = "com.linkedin.events.example.Person";
    final int firstSourceIndex = 0;
    sourceNames.add(firstSourceName);
    final String catchupSourceName = "com.linkedin.events.example.Place";
    final int catchupSourceIndex = 1;
    sourceNames.add(catchupSourceName);
    final String snapshotSourceName = "com.linkedin.events.example.Thing";
    final int snapshotSourceIndex = 2;
    sourceNames.add(snapshotSourceName);
    BootstrapCheckpointHandler ckptHandler = new BootstrapCheckpointHandler(sourceNames);

    final long targetScn = 129971;
    final long sinceScn = 0;
    final long windowOffset = 0;
    final long prevScn = -1;
    final long windowScn = 123456;
    final long startScn = 123456;
    final long snapshotOffset = -1;

    Checkpoint ckpt = new Checkpoint();
    ckpt.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
    ckpt.setBootstrapStartScn(startScn);
    ckpt.setConsumptionMode(DbusClientMode.BOOTSTRAP_CATCHUP);
    ckpt.setBootstrapTargetScn(targetScn);
    ckpt.setBootstrapSinceScn(sinceScn);
    ckpt.setWindowOffset(windowOffset);  // must set after targetScn if CATCHUP
    ckpt.setPrevScn(prevScn);
    ckpt.setWindowScn(windowScn);
    ckpt.setSnapshotOffset(snapshotOffset);

    // no public setter for source names, so use introspection instead
    Class[] params = new Class[2];
    params[0] = Integer.TYPE;
    params[1] = String.class;
    Method setSnapshotSourceMethod = Checkpoint.class.getDeclaredMethod("setSnapshotSource", params);
    Method setCatchupSourceMethod  = Checkpoint.class.getDeclaredMethod("setCatchupSource", params);
    setSnapshotSourceMethod.setAccessible(true);
    setCatchupSourceMethod.setAccessible(true);
    setSnapshotSourceMethod.invoke(ckpt, snapshotSourceIndex, snapshotSourceName);
    setCatchupSourceMethod.invoke(ckpt, catchupSourceIndex, catchupSourceName);

    ckptHandler.resetForServerChange(ckpt);

    Assert.assertEquals(ckpt.getConsumptionMode(), DbusClientMode.BOOTSTRAP_SNAPSHOT);
    Assert.assertEquals(ckpt.getBootstrapSinceScn().longValue(), sinceScn);
    // TODO/FIXME:  for next three, check for exact values or just assertNotEquals against original values?
    Assert.assertEquals(ckpt.getBootstrapStartScn().longValue(), -1);
    Assert.assertEquals(ckpt.getBootstrapTargetScn().longValue(), -1);
    Assert.assertEquals(ckpt.getSnapshotOffset().longValue(), 0);
    // expect indices both reset to 0, and therefore, for consistency, names both reset to corresponding source name:
    Assert.assertEquals(ckpt.getBootstrapSnapshotSourceIndex().intValue(), firstSourceIndex);
    Assert.assertEquals(ckpt.getBootstrapCatchupSourceIndex().intValue(), firstSourceIndex);
    Assert.assertEquals(ckpt.getSnapshotSource(), firstSourceName);
    Assert.assertEquals(ckpt.getCatchupSource(), firstSourceName);
  }

  private void assertAfterSnapshotCheckpoint(Checkpoint cp1, int expectedSnapshotSourceIndex,
                                             String expectedSnapshotSourceName)
  {
    //intentional pointer comparison
    Assert.assertEquals(cp1.getConsumptionMode(), DbusClientMode.BOOTSTRAP_SNAPSHOT);
    Assert.assertEquals(cp1.getBootstrapSnapshotSourceIndex().intValue(), expectedSnapshotSourceIndex);
    Assert.assertEquals(cp1.getSnapshotSource(), expectedSnapshotSourceName);
  }

  private void assertSnapshotCompleteCheckpoint(BootstrapCheckpointHandler handler, Checkpoint cp,
                                                int snapshotSourceIndex, String snapshortSourceName)
  {
    Assert.assertTrue(handler.assertBootstrapCheckpoint(cp));
    Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.BOOTSTRAP_SNAPSHOT);
    Assert.assertTrue(cp.isSnapShotSourceCompleted());
    Assert.assertEquals(cp.getBootstrapSnapshotSourceIndex().intValue(), snapshotSourceIndex);//still same source
    Assert.assertEquals(cp.getSnapshotSource(), snapshortSourceName);//still same source
    Assert.assertEquals(cp.getSnapshotOffset().longValue(), -1L);
  }

  private void assertAfterNonfinalCatchupCheckpoint(BootstrapCheckpointHandler handler,
                                               Checkpoint cp,
                                               int expectedCatchupSourceIndex,
                                               String expectedCatchupSourceName)
  {
    Assert.assertTrue(handler.assertBootstrapCheckpoint(cp));
    Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.BOOTSTRAP_CATCHUP);
    Assert.assertTrue(handler.needsMoreCatchup(cp));
    Assert.assertEquals(cp.getWindowOffset().longValue(), 0L);
    Assert.assertEquals(cp.getBootstrapCatchupSourceIndex().intValue(),
                        expectedCatchupSourceIndex);
    Assert.assertEquals(cp.getCatchupSource(), expectedCatchupSourceName);
  }

  private void assertSnapshotAfterCatchupCheckpoint(BootstrapCheckpointHandler handler,
                                                    Checkpoint cp,
                                                    int expectedCatchupSourceIndex,
                                                    String expectedCatchupSourceName)
  {
    Assert.assertTrue(handler.assertBootstrapCheckpoint(cp));
    Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.BOOTSTRAP_SNAPSHOT);
    Assert.assertTrue(cp.isCatchupSourceCompleted());
    Assert.assertEquals(cp.getBootstrapCatchupSourceIndex().intValue(), 0);
    Assert.assertTrue(handler.needsMoreCatchup(cp));
    Assert.assertEquals(cp.getBootstrapSnapshotSourceIndex().intValue(), expectedCatchupSourceIndex);
    Assert.assertEquals(cp.getSnapshotSource(), expectedCatchupSourceName);
  }

  private void assertAfterFinalCatchupCheckpoint(BootstrapCheckpointHandler handler,
                                                Checkpoint cp)
  {
    Assert.assertTrue(handler.assertBootstrapCheckpoint(cp));
    if (handler.needsMoreSnapshot(cp))
    {
      Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.BOOTSTRAP_SNAPSHOT);
    }
    Assert.assertTrue(cp.isCatchupSourceCompleted());
    Assert.assertEquals(cp.getBootstrapCatchupSourceIndex().intValue(),
                        cp.getBootstrapSnapshotSourceIndex().intValue());
    Assert.assertFalse(handler.needsMoreCatchup(cp));
    Assert.assertFalse(handler.needsMoreSnapshot(cp));
  }

  private void assertCatchupCompleteCheckpoint(BootstrapCheckpointHandler handler,
                                               Checkpoint cp1,
                                               int expectedCatchupSourceIndex,
                                               String expectedCatchupSourceName)
  {
    Assert.assertTrue(handler.assertBootstrapCheckpoint(cp1));
    Assert.assertEquals(cp1.getConsumptionMode(), DbusClientMode.BOOTSTRAP_CATCHUP);
    Assert.assertTrue(cp1.isCatchupSourceCompleted());
    Assert.assertEquals(cp1.getBootstrapCatchupSourceIndex().intValue(), expectedCatchupSourceIndex);
    Assert.assertEquals(cp1.getCatchupSource(), expectedCatchupSourceName);
    Assert.assertEquals(cp1.getWindowOffset().longValue(), -1L);
  }

  private void assertStartBootstrapCheckpoint(BootstrapCheckpointHandler handler,
                                              Checkpoint cp,
                                              String sourceName,
                                              long sinceScn)
  {
    Assert.assertNotNull(cp);
    Assert.assertTrue(handler.assertBootstrapCheckpoint(cp));
    Assert.assertEquals(cp.getConsumptionMode(), DbusClientMode.BOOTSTRAP_SNAPSHOT);
    Assert.assertTrue(cp.isBootstrapSinceScnSet());
    Assert.assertEquals(cp.getBootstrapSinceScn().longValue(), sinceScn);
    Assert.assertFalse(cp.isBootstrapStartScnSet());
    Assert.assertFalse(cp.isBootstrapTargetScnSet());
    Assert.assertEquals(cp.getSnapshotOffset().longValue(), 0L);
    Assert.assertEquals(cp.getBootstrapCatchupSourceIndex().intValue(), 0);
    Assert.assertEquals(cp.getSnapshotSource(), sourceName);
    Assert.assertTrue(handler.needsMoreSnapshot(cp));
  }

}
