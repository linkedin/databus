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


import java.util.List;



/**
 * MBean interface for the Container for all monitoring mbeans
 */
public interface ContainerStatisticsCollectorMBean
{

  // ************** GETTERS *********************

  /**
   * Obtains the mbean used to collect statistics about the total traffic originating from
   * remote clients.
   */
  ContainerTrafficTotalStatsMBean getOutboundTrafficTotalStats();

  /**
   * Obtains the mbean used to collect statistics about the total traffic originating from the
   * container.
   */
  ContainerTrafficTotalStatsMBean getInboundTrafficTotalStats();

  /** Checks if the stats collector is enabled */
  boolean isEnabled();

  /** Obtains the list of client ids for which there are stats accumulated */
  List<String> getOutboundClients();

  /** Obtains the stats monitoring bean for a given client id */
  ContainerTrafficTotalStats getOutboundClientStats(String client);

  // ****************** MUTATORS *********************

  /** Resets the statistics. */
  void reset();

  /**
   * Enables/disables the stats collector
   * @param enabled        true to enable, false to disable
   */
  void setEnabled(boolean enabled);
}
