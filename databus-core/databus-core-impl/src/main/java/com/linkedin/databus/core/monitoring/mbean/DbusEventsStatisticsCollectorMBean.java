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


import java.util.List;

/**
 * MBean interface for the Container for all monitoring mbeans
 */
public interface DbusEventsStatisticsCollectorMBean
{

  // ************** GETTERS *********************

  /** Obtains the mbean used to collect statistics about total databus events streamed */
  DbusEventsTotalStats getTotalStats();

  /** Checks if the stats collector is enabled */
  boolean isEnabled();

  /** Obtains the list of outbound source ids for which there are stats accumulated */
  List<Integer> getSources();

  /**
   * Obtains the stats monitoring bean for a given outbound physical source id
   * @return the stats mbean or null
   * */
  DbusEventsTotalStats getSourceStats(int physicalSrcId);


  /** Obtains the list of peer ids for which there are stats accumulated */
  List<String> getPeers();

  /** Obtains the stats monitoring bean for a given peer id */
  DbusEventsTotalStats getPeerStats(String peer);

  // ****************** MUTATORS *********************

  /** Resets the statistics. */
  void reset();

  /**
   * Enables/disables the stats collector
   * @param enabled        true to enable, false to disable
   */
  void setEnabled(boolean enabled);

}
