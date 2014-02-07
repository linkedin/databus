package com.linkedin.databus.core.monitoring.mbean;
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
import java.io.OutputStream;

import org.apache.avro.io.JsonEncoder;

/**
 * Common interface for all monitoring mbeans
 * @author cbotev
 *
 */
public interface DatabusMonitoringMBean<T>
{

  // ************** GETTERS *********************

  /**Obtains the Unix timestamp of the last {@link #reset()} call. */
  long getTimestampLastResetMs();

  /** Obtains the time in milliseconds since the last {@link #reset()} call. */
  long getTimeSinceLastResetMs();

  /** Checks if the stats collector is enabled */
  boolean isEnabled();

  /**
   * Creates a reusable JsonEncoder
   * @param  out                the output stream to write to
   * @return the json encoder
   * @throws IOException
   */
  JsonEncoder createJsonEncoder(OutputStream out) throws IOException;

  /**
   * Converts the statistics to JSON representation. The data will be extracted and stored in the
   * passed event object to decrease contention in the bean and to avoid deadlocks (e.g. if the
   * jsonEncoder tries to write to the network which can lead to attempts to update some of the
   * statistics).
   *
   * @param  jsonEncoder         use an existing encoder; must exist
   * @param  eventReuse          the event data object to store the data in; if null, it will be
   *                             allocated
   * @return the event data object
   */
  T toJson(JsonEncoder jsonEncoder, T eventReuse) throws IOException;

  /**
   * Converts the statistics to JSON representation
   * @return the json string
   */
  String toJson();

  // ****************** MUTATORS *********************

  /** Resets the statistics. */
  void reset();

  /**
   * Enables/disables the stats collector
   * @param enabled        true to enable, false to disable
   */
  void setEnabled(boolean enabled);

  /**
   * Obtains a copy of the current statistics
   * @param   reuse             the statistics event object to fill in; if null, a new one will be
   *                            allocated
   * @return  the statistics event object
   */
  T getStatistics(T reuse);

  /** Merges all statistics accumulated in another bean */
  void mergeStats(DatabusMonitoringMBean<T> other);

}
