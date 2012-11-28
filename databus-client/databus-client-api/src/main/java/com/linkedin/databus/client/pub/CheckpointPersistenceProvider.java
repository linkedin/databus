package com.linkedin.databus.client.pub;

import java.io.IOException;
import java.util.List;

import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.data_model.DatabusSubscription;

/**
 * The interfaces for classes that can persistent checkpoints during streaming of databus data events.
 * Each provider can maintain the checkpoints for multiple streams. Each stream is identified by the
 * list of databus streams for that stream.
 *
 * The provider is required to store only the last successful checkpoint. In essesnce, the provider
 * implements a map: List<String> ==> Checkpoint
 *
 * @author cbotev
 *
 */
public interface CheckpointPersistenceProvider
{

  /**
   * Stores a new checkpoint for the stream identified by the list of databus source names. If
   * successful, it will overwrite any previous checkpoints stored.
   * @param  sourceNames        the list of source names for the stream
   * @param  checkpoint         the new checkpoint
   * @throws IOException        if something went wrong while persisting the checkpoint.
   */
  void storeCheckpoint(List<String> sourceNames, Checkpoint checkpoint) throws IOException;
  void storeCheckpointV3(List<DatabusSubscription> sourceNames, Checkpoint checkpoint, RegistrationId registrationId) throws IOException;

  /**
   * Loads the last successful checkpoint for the stream identified by the list of databus source
   * names.
   * @param  sourceNames        the list of databus source names for the stream
   * @return the last successful checkpoint or null if none exists
   */
  Checkpoint loadCheckpoint(List<String> sourceNames);
  Checkpoint loadCheckpointV3(List<DatabusSubscription> sourceNames, RegistrationId registrationId);

  /**
   * Removes the currently persisted checkpoint
   * @param  sourceNames        the list of databus source names for the stream
   */
  void removeCheckpoint(List<String> sourceNames);
  void removeCheckpointV3(List<DatabusSubscription> sourceNames, RegistrationId registrationId);
}
