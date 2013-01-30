package com.linkedin.databus.client;
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


import com.linkedin.databus.core.Checkpoint;

/**
 * Message type for controlling checkpoints.
 *
 * <p>Available types:
 * <ul>
 *   <li>SET_CHECKPOINT - changes the current checkpoint</li>
 * </ul>
 *
 * @author cbotev
 *
 */
public class CheckpointMessage
{
  public enum TypeId
  {
    SET_CHECKPOINT
  }

  private TypeId _typeId;
  private Checkpoint _checkpoint;

  private CheckpointMessage(TypeId typeId, Checkpoint checkpoint)
  {
    _typeId = typeId;
    _checkpoint = checkpoint;
  }

  public static CheckpointMessage createSetCheckpointMessage(Checkpoint checkpoint)
  {
    return new CheckpointMessage(TypeId.SET_CHECKPOINT, checkpoint);
  }

  public CheckpointMessage switchToSetCheckpoint(Checkpoint checkpoint)
  {
    _typeId = TypeId.SET_CHECKPOINT;
    _checkpoint = checkpoint;

    return this;
  }

  public TypeId getTypeId()
  {
    return _typeId;
  }

  public Checkpoint getCheckpoint()
  {
    return _checkpoint;
  }

  @Override
  public String toString()
  {
    return _typeId.toString();
  }

}
