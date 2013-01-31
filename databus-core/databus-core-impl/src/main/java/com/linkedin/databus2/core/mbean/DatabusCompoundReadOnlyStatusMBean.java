package com.linkedin.databus2.core.mbean;
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


import java.util.List;

public interface DatabusCompoundReadOnlyStatusMBean extends DatabusReadOnlyStatusMBean
{
  /** The list of names of children components not started yet */
  public List<String> getInitializing();

  /** The number of children components not started yet */
  public int getNumInitializing();

  /** The list of names of children components that are currently suspended because of an error */
  public List<String> getSuspended();

  /** The number of children components that are currently suspended because of an error */
  public int getNumSuspended();

  /** The list of names of children components that are currently manually paused */
  public List<String> getPaused();

  /** The number of children components that are currently manually paused */
  public int getNumPaused();

  /** The list of names of children components that are shutdown */
  public List<String> getShutdown();

  /** The number of children components that are shutdown */
  public int getNumShutdown();

  /** The list of names of children components that are currently retrying because of an error */
  public List<String> getRetryingOnError();

  /** The number of children components that are currently retrying because of an error */
  public int getNumRetryingOnError();
}
