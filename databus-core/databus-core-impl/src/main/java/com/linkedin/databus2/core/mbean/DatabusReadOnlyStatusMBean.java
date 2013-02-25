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


public interface DatabusReadOnlyStatusMBean
{
  /**** =============== getters ================ ******/

  /**
   * The status of the dbus component: running, paused, suspendedOnError")
   * @return status string
   */
  String getStatus();

  /**
   * The detailed message about the status
   * @return detailed status message string
   */
  String getStatusMessage();

  /**
   * The dbus component name
   * @return component name
   */
  String getComponentName();

  /** A number representation of of the status for graphing/alerts. Larger value means worse
   * */
  int getStatusCode();

  /** The number of successive errors */
  int getRetriesNum();

  /** The number of remaining retries */
  int getRemainingRetriesNum();

  /** The current retry duration */
  long getCurrentRetryLatency();

  /** Time in ms since last first retry */
  long getTotalRetryTime();

  /** The current uptime: milliseconds since last start/resume or 0 if in error state */
  long getUptimeMs();
}
