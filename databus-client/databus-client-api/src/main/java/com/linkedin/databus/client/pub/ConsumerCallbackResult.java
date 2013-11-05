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


/**
 * The result code from the execution of a consumer callback.
 *
 * <ul>
 *  <li>SUCCESS - callback finished successfully</li>
 *  <li>CHECKPOINT - callback finished successfully and consumer is ready for a checkpoint</li>
 *  <li>SKIP_CHECKPOINT - onCheckpoint callback finished without error but consumer hasn't saved interim work and doesn't want current checkpoint to be persisted </li>
 *  <li>ERROR - callback finished unsuccessfully; the Databus library should retry the call</li>
 *  <li>ERROR_FATAL - callback finished unsuccessfully with an unrecoverable error</li>
 * </ul>
 */
public enum ConsumerCallbackResult
{
  SUCCESS(0),
  CHECKPOINT(100),
  SKIP_CHECKPOINT(150),
  ERROR(200),
  ERROR_FATAL(300);

  private final int _level;

  private ConsumerCallbackResult(int level)
  {
    _level = level;
  }

  public int getLevel()
  {
    return _level;
  }

  public static boolean isSuccess(ConsumerCallbackResult resultCode)
  {
    return SUCCESS == resultCode || CHECKPOINT == resultCode || SKIP_CHECKPOINT==resultCode;
  }

  public static boolean isFailure(ConsumerCallbackResult resultCode)
  {
    return ERROR == resultCode || ERROR_FATAL == resultCode;
  }

  public static boolean isSkipCheckpoint(ConsumerCallbackResult resultCode)
  {
    return SKIP_CHECKPOINT == resultCode;
  }

  /** Returns the more severe result code */
  public static ConsumerCallbackResult max(ConsumerCallbackResult r1, ConsumerCallbackResult r2)
  {
    return (r1._level < r2._level) ? r2 : r1;
  }
}
