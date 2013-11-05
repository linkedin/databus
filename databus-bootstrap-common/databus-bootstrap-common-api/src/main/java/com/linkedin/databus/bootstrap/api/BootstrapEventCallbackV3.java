package com.linkedin.databus.bootstrap.api;
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

import org.apache.avro.generic.GenericRecord;

import com.linkedin.databus.core.Checkpoint;

public interface BootstrapEventCallbackV3
{
  /**
   * This method is implemented by an "event writer" object on the Databus V3 bootstrap server.
   * It is invoked for every event returned to the client
   *
   * @param rowRecord A generic record for the event data
   * @return @see BootstrapEventProcessResult
   * @throws BootstrapProcessingException
   * @throws IOException 
   */
  BootstrapEventProcessResult onEvent(GenericRecord rowRecord)
          throws BootstrapProcessingException, IOException;

  /**
   * Invoked when there is a checkpoint event being sent from the server to the client
   *
   * @param currentCheckpoint Checkpoint that is sent
   * @param curStatsCollector Collects statistics for the events processed on server
   */
  void onCheckpointEvent(Checkpoint currentCheckpoint);
}
