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
package com.linkedin.databus.core;

import java.util.Arrays;
import java.util.List;

/**
 * Helper class for management of bootstrap checkpoints. This is to get around the fact that the
 * {@link Checkpoint} does not store the list of bootstrap sources.
 *
 * <p>If we have three sources S1, S2, S3, then the bootstrap proceeds as follows:
 * <ol>
 *  <li>Snapshot(S1)
 *  <li>Catchup(S1)
 *  <li>Snapshot(S2)
 *  <li>Catchup(S1)
 *  <li>Catchup(S2)
 *  <li>Snapshot(S3)
 *  <li>Catchup(S1)
 *  <li>Catchup(S2)
 *  <li>Catchup(S3)
 * </ol>
 */
public class BootstrapCheckpointHandler
{
  // TODO: We need to add the list of bootstrap sources (or at least a hash) to the checkpoint
  // to make sure that nothing funky happens on restart or server changes (e.g., the list of
  // sources or their order changing).  See also the _sourceNames comment below.

  /**
   * The list of source names being bootstrapped.  BootstrapCheckpointHandler both owns and is
   * authoritative for this list; the Checkpoint class knows about only two sources (at most)
   * and may reset their index values in a manner inconsistent with _sourceNames.  In such
   * cases the handler is responsible for resetting the Checkpoint's source names to be
   * consistent with the Checkpoint's source indices and with _sourceNames.
   */
  private String[] _sourceNames;

  public BootstrapCheckpointHandler(List<String> sourceNames)
  {
    assert null != sourceNames;
    assert sourceNames.size() > 0;

    _sourceNames = sourceNames.toArray(new String[sourceNames.size()]);
  }

  public BootstrapCheckpointHandler(String... sourceNames)
  {
    assert null != sourceNames;
    assert sourceNames.length > 0;

    _sourceNames = Arrays.copyOf(sourceNames, sourceNames.length);
  }

  /**
   * sourceNames are dynamically updated based on SourcesResponse from relay.
   * Specifically, this is used in V3 as the sources hosted on a relay can
   * change over time.
   */
  public void setSourceNames(List<String> sourceNames)
  {
    _sourceNames = sourceNames.toArray(new String[sourceNames.size()]);
  }
  /** Check if there are any sources left to catchup before the next snapshot */
  public boolean needsMoreCatchup(Checkpoint ckpt)
  {
    final int snapshotSourceIndex = ckpt.getBootstrapSnapshotSourceIndex();
    final int catchupSourceIndex = ckpt.getBootstrapCatchupSourceIndex();
    return catchupSourceIndex < snapshotSourceIndex;
  }

  /** Check if there are any sources left to snapshot */
  public boolean needsMoreSnapshot(Checkpoint ckpt)
  {
    final int snapshotSourceIndex = ckpt.getBootstrapSnapshotSourceIndex();
    return snapshotSourceIndex < _sourceNames.length;
  }

  /**
   * Sets up a checkpoint for bootstrap snapshot from a given SCN
   * @param  ckpt       the checkpoint to modify; if null, a new checkpoint will be created
   * @param  sinceScn   the SCN to start the bootstrap from
   * @return the modified checkpoint
   * @note Method overridden for V3
   */
  public Checkpoint createInitialBootstrapCheckpoint(Checkpoint ckpt, Long sinceScn)
  {
    if (sinceScn < 0)
    {
      throw new DatabusRuntimeException("sinceScn must be non-negative:" + sinceScn);
    }

    if (null == ckpt)
    {
      // purely for test cases to work.
      ckpt = new Checkpoint();
    }
    // TODO (DDSDBUS-85): For now, we use the same startScn, min(windowscn) from bootstrap_applier_state,
    // for snapshot all sources. It makes catchup inefficient. We need to optimize it later.
    ckpt.resetBootstrap();

    ckpt.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);

    // set since scn
    ckpt.setBootstrapSinceScn(sinceScn);
    setSnapshotSource(ckpt, 0);
    setCatchupSource(ckpt, 0);
    ckpt.startSnapShotSource();

    return ckpt;
  }

  /** Ask Bruce Dickinson */
  public boolean needsMoreCowbell(Checkpoint ckpt)
  {
    return true;
  }

  /**
   * Finish the current snapshot phase
   * @param  ckpt       the checkpoint to modify
   */
  public void finalizeSnapshotPhase(Checkpoint ckpt)
  {
    assertBootstrapCheckpoint(ckpt);
    ckpt.endSnapShotSource();
  }

  /**
   * Finish then current catchup phase
   * @param  ckpt       the checkpoint to modify
   */
  public void finalizeCatchupPhase(Checkpoint ckpt)
  {
    assertBootstrapCheckpoint(ckpt);
    ckpt.endCatchupSource();
  }

  /**
   * Mark the completion of the processing of the current bootstrap snapshot source
   * and start the next catchup phase.
   * @param  ckpt       the checkpoint to modify
   */
  public void advanceAfterSnapshotPhase(Checkpoint ckpt)
  {
    assertBootstrapCheckpoint(ckpt);
    if (! needsMoreSnapshot(ckpt))
    {
      throw new InvalidCheckpointException("unexpected endSnapshotSource", ckpt);
    }

    final int snapshotSourceIndex = ckpt.nextBootstrapSnapshotSourceIndex();
    setSnapshotSource(ckpt, snapshotSourceIndex);
    setCatchupSource(ckpt, 0);
  }

  /**
   * Mark the completion of the processing of the current bootstrap catchup source
   * @param  ckpt       the checkpoint to modify
   */
  public void advanceAfterCatchupPhase(Checkpoint ckpt)
  {
    assertBootstrapCheckpoint(ckpt);
    if (! needsMoreCatchup(ckpt))
    {
      throw new InvalidCheckpointException("unexpected endCatchupSource", ckpt);
    }

    final int catchupSourceIndex = ckpt.nextBootstrapCatchupSourceIndex();
    setCatchupSource(ckpt, catchupSourceIndex);

    if (needsMoreCatchup(ckpt))
    {
      // move to next source for catchup source
      startCatchupSource(ckpt);
    }
    else if (needsMoreSnapshot(ckpt))
    {
      // move to next snapshot
      startNextSnapshotSource(ckpt);
      //cp = initCheckpointForSnapshot(cp, cp.getBootstrapSinceScn());
      //cp.setBootstrapCatchupSourceIndex(0);
    }
  }

  public void advanceAfterTargetScn(Checkpoint ckpt)
  {
    assertBootstrapCheckpoint(ckpt);
    if (! ckpt.isBootstrapTargetScnSet())
    {
      throw new InvalidCheckpointException("bootstrap_target_scn must be set", ckpt);
    }
    if (0 != ckpt.getBootstrapCatchupSourceIndex())
    {
      throw new InvalidCheckpointException("bootstrap_catchup_source_index must be 0", ckpt);
    }

    startCatchupSource(ckpt);
  }

  /**
   * Checks invariants for a bootstrap checkpoint.
   * @param ckpt    the checkpoint to validate
   * @return true; this is so one can write "assert assertBootstrapCheckpoint(ckpt)" if they want
   *         control if the assert is to be run
   * @throws InvalidCheckpointException if the validation fails
   */
  public boolean assertBootstrapCheckpoint(Checkpoint ckpt)
  {
    assert null != ckpt;
    if (! ckpt.assertCheckpoint()) return false;

    switch (ckpt.getConsumptionMode())
    {
    case BOOTSTRAP_SNAPSHOT: return assertSnapshotCheckpoint(ckpt);
    case BOOTSTRAP_CATCHUP: return assertCatchupCheckpoint(ckpt);
    default:
      throw new InvalidCheckpointException("not a bootstrap checkpoint", ckpt);
    }
  }

  // Overridden for V3.
  public void resetForServerChange(Checkpoint ckpt)
  {
    ckpt.resetForServerChange();

    // The Checkpoint class doesn't know about _sourceNames, so it may have set SNAPSHOT_SOURCE
    // and BOOTSTRAP_SNAPSHOT_SOURCE_INDEX (and/or the CATCHUP equivalents) inconsistently with
    // _sourceNames[].  We're authoritative, so fix that by fixing the checkpoint's source names.
    // (Also see TODO near the top of this class.)
    final int snapshotSourceIndex = ckpt.getBootstrapSnapshotSourceIndex();
    final int catchupSourceIndex = ckpt.getBootstrapCatchupSourceIndex();
    setSnapshotSource(ckpt, snapshotSourceIndex);
    setCatchupSource(ckpt, catchupSourceIndex);
  }

  /**
   * Starts the consumption of a new bootstrap catchup source.
   * @param  ckpt       the checkpoint to modify
   */
  private void startCatchupSource(Checkpoint ckpt)
  {
    // TODO (DDSDBUS-86): For catchup of sources already made consistent prior to snapshotting the current
    // source, we could have used the scn on which they are consistent of. But for simplicity
    // for now, we are using the startScn. Optimization will be needed later for more efficient
    // catchup
    if (!ckpt.isBootstrapStartScnSet())
    {
      throw new InvalidCheckpointException("startScn not set for catchup", ckpt);
    }

    String source = _sourceNames[ckpt.getBootstrapCatchupSourceIndex()];
    if (null == source)
    {
      throw new InvalidCheckpointException("no sources available for catchup", ckpt);
    }

    ckpt.setConsumptionMode(DbusClientMode.BOOTSTRAP_CATCHUP);
    ckpt.startCatchupSource();
  }

  /**
   * Move the checkpoint to the next snapshot source.
   * @param  ckpt       the checkpoint to modify
   */
  private void startNextSnapshotSource(Checkpoint ckpt)
  {
    // TODO (DDSDBUS-85): For now, we use the same startScn, min(windowscn) from bootstrap_applier_state,
    // for snapshot all sources. It makes catchup inefficient. We need to optimize it later.
    String source = _sourceNames[ckpt.getBootstrapSnapshotSourceIndex()];
    if (null == source)
    {
      throw new RuntimeException("no sources available for snapshot");
    }

    ckpt.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
    ckpt.startSnapShotSource();
    // need to reset _catchupSource to 0 because we need to do catchup
    // for all sources from the first source to the current snapshot source
    setCatchupSource(ckpt, 0);
  }

  /**
   * Validate a BOOTSTRAP_SNAPSHOT checkpoint.
   * @param ckpt    the checkpoint to validate
   * @throws InvalidCheckpointException if the validation fails
   */
  private boolean assertSnapshotCheckpoint(Checkpoint ckpt)
  {
    assertSnapshotSourceIndex(ckpt);
    return true;
  }

  private void validateSnapshotSourceIndex(int snapshotSourceIndex, Checkpoint ckpt)
  {
    if (0 > snapshotSourceIndex || snapshotSourceIndex > _sourceNames.length)
    {
      throw new InvalidCheckpointException("invalid snapshot source index " + snapshotSourceIndex +
                                           " for source names " + Arrays.toString(_sourceNames), ckpt);
    }
    return;
  }

  private void assertSnapshotSourceIndex(Checkpoint ckpt)
  {
    final int snapshotSourceIndex = ckpt.getBootstrapSnapshotSourceIndex();
    validateSnapshotSourceIndex(snapshotSourceIndex, ckpt);
    if (_sourceNames.length > snapshotSourceIndex
        &&
        ! _sourceNames[snapshotSourceIndex].equals(ckpt.getSnapshotSource()))
    {
      throw new InvalidCheckpointException("snapshot index/source name mismatch for source names " +
                                           Arrays.toString(_sourceNames), ckpt);
    }
  }

  /**
   * Validate a BOOTSTRAP_CATCHUP checkpoint
   * @param ckpt    the checkpoint to validate
   * @throws InvalidCheckpointException if the validation fails
   */
  private boolean assertCatchupCheckpoint(Checkpoint ckpt)
  {
    assertSnapshotSourceIndex(ckpt);
    assertCatchupSourceIndex(ckpt);

    return true;
  }

  private void assertCatchupSourceIndex(Checkpoint ckpt)
  {
    final int catchupSourceIndex = ckpt.getBootstrapCatchupSourceIndex();
    if (_sourceNames.length > catchupSourceIndex &&
        ! _sourceNames[catchupSourceIndex].equals(ckpt.getCatchupSource()))
    {
      throw new InvalidCheckpointException("catchup index/source name mismatch for source names " +
                                           Arrays.toString(_sourceNames), ckpt);
    }
  }

  private void setCatchupSource(Checkpoint ckpt, int catchupSourceIndex)
  {
    if (catchupSourceIndex < getSourcesNamesListLength())
    {
          ckpt.setCatchupSource(catchupSourceIndex, _sourceNames[catchupSourceIndex]);
    }
    else if (catchupSourceIndex == getSourcesNamesListLength())
    {
      ckpt.setCatchupSource(catchupSourceIndex, Checkpoint.NO_SOURCE_NAME);
    }
    else
    {
      throw new InvalidCheckpointException("invalid catchup source index " + catchupSourceIndex, ckpt);
    }
  }

  private void setSnapshotSource(Checkpoint ckpt, int snapshotSourceIndex)
  {
    if (snapshotSourceIndex < getSourcesNamesListLength())
    {
       ckpt.setSnapshotSource(snapshotSourceIndex, _sourceNames[snapshotSourceIndex]);
    }
    else if (snapshotSourceIndex == getSourcesNamesListLength())
    {
      ckpt.setSnapshotSource(snapshotSourceIndex, Checkpoint.NO_SOURCE_NAME);
    }
    else
    {
      throw new InvalidCheckpointException("invalid snapshot source index " + snapshotSourceIndex, ckpt);
    }
  }

  private int getSourcesNamesListLength()
  {
    return _sourceNames.length;
  }

}
