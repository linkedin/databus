package com.linkedin.databus.client.pub;
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
