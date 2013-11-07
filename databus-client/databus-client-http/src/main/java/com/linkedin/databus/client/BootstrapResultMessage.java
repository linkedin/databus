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
 * A message class that describes the result of a bootstrap operator
 *
 * <p>Possible results:
 * <ul>
 *   <li>BOOTSTRAP_COMPLETE - the bootstrap completed successfully.</li>
 *   <li>BOOTSTRAP_FAILED - the bootstrap failed </li>
 * </ul>
 * */
public class BootstrapResultMessage
{

  public enum TypeId
  {
    BOOTSTRAP_COMPLETE,
    BOOTSTRAP_FAILED
  }

  private TypeId _typeId;
  /** The checkpoint after the bootstrap is complete */
  private Checkpoint _bootstrapCheckpoint;
  /** The reason the bootstrap failed */
  private Throwable _failureReason;

  private BootstrapResultMessage(TypeId typeId,
                                 Checkpoint bootstrapCheckpoint,
                                 Throwable failureReason)
  {
    super();
    _typeId = typeId;
    _bootstrapCheckpoint = bootstrapCheckpoint;
    _failureReason = failureReason;
  }

  /**
   * Creates a new BOOTSTRAP_COMPLETE message
   * @param  bootstrapCheckpoint        the checkpoint after the bootstrap completion
   * @return the new message
   */
  public static BootstrapResultMessage createBootstrapCompleteMessage(Checkpoint bootstrapCheckpoint)
  {
    return new BootstrapResultMessage(TypeId.BOOTSTRAP_COMPLETE, bootstrapCheckpoint, null);
  }

  /**
   * Creates a new BOOTSTRAP_FAILED message
   * @param  failureReason        the reason for the failure
   * @return the new message
   */
  public static BootstrapResultMessage createBootstrapFailedMessage(Throwable failureReason)
  {
    return new BootstrapResultMessage(TypeId.BOOTSTRAP_FAILED, null, failureReason);
  }

  /**
   * Switches an existing message to BOOTSTRAP_COMPLETE
   * @param  bootstrapCheckpoint        the checkpoint after the bootstrap completion
   * @return the this message
   **/
  public BootstrapResultMessage switchToBootstrapComplete(Checkpoint bootstrapCheckpoint)
  {
    _typeId = TypeId.BOOTSTRAP_COMPLETE;
    _bootstrapCheckpoint = bootstrapCheckpoint;
    _failureReason = null;

    return this;
  }

  /**
   * Switches an existing message to BOOTSTRAP_FAILED
   * @param  failureReason        the reason for the failure
   * @return this message
   **/
  public BootstrapResultMessage switchToBootstrapFailed(Throwable failureReason)
  {
    _typeId = TypeId.BOOTSTRAP_COMPLETE;
    _bootstrapCheckpoint = null;
    _failureReason = failureReason;

    return this;
  }

  /** Returns the type of the message */
  public TypeId getTypeId()
  {
    return _typeId;
  }

  /** Returns the checkpoint after th end of the bootstrap; meaningful only for BOOTSTRAP_COMPLETE */
  public Checkpoint getBootstrapCheckpoint()
  {
    return _bootstrapCheckpoint;
  }

  /** Returns the bootstrap failure reason; meaningful only for BOOTSTRAP_FAILED */
  public Throwable getFailureReason()
  {
    return _failureReason;
  }


  @Override
  public String toString() {
	return _typeId.toString();
  }
}
