/**
 *
 */
package com.linkedin.databus2.core.container.monitoring.mbean;
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
 * Commong interface for all databus admin mbeans
 * @author lgao
 *
 */
public interface DatabusComponentAdminMBean
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
   * The container ID of the dbus component
   * @return container ID
   */
  long getContainerId();

  /**
   * The HTTP port on which the component is listening
   * @return http port number
   */
  long getHttpPort();

  /**
   * The dbus component name
   * @return component name
   */
  String getComponentName();

  /**
   * A number representation of of the status for graphing/alerts. Larger value means worse
   * */
  int getStatusCode();

  /**** =============== setters ================ ******/

  /**
   * Pause the component temporarily
   */
  void pause();

  /**
   * Resume the component paused previously
   */
  void resume();

  /**
   * Shutdown the component permanently
   */
  void shutdown();
}
