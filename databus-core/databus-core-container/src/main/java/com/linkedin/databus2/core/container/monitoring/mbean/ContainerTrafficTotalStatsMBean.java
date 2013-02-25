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


import com.linkedin.databus.core.monitoring.mbean.DatabusMonitoringMBean;
import com.linkedin.databus2.core.container.monitoring.events.ContainerTrafficTotalStatsEvent;

/**
 * Collector/accessor for total outbound traffic statistics.
 * @author cbotev
 *
 */
public interface ContainerTrafficTotalStatsMBean
       extends DatabusMonitoringMBean<ContainerTrafficTotalStatsEvent>
{

  // ************** GETTERS *********************

  /** Obtains the number of bytes sent */
  long getNumBytes();

  /** Obtains the number of connections open */
  long getNumOpenConns();

  /** Obtains the number of connections closed */
  long getNumClosedConns();

  /** Obtains the number of distinct clients that have connected */
  int getNumClients();

  /** Obtains total lifespan of close connections */
  long getTimeClosedConnLifeMs();

  /** Obtains total lifespan of currently open connections */
  long getTimeOpenConnLifeMs();

  /** total number of networking errors */
  long getErrorTotalCount();

  /** connection refused networking errors */
  long getErrorConnectCount();

  /** connection timeout networking errors */
  long getErrorTimeoutCount();

  // ****************** MIGRATED GETTERS *********************

  /** Obtains the number of connections open */
  long getOpenConnsRate();

  /** Obtains the number of connections closed */
  long getClosedConnsRate();

  /** Obtains the number of distinct clients that have connected */
  int getClientsRate();

  /** Obtains average lifespan of currently open connections in ms */
  long getLatencyOpenConn();

  /** Obtains average lifespan of closed connections */
  long getLatencyClosedConn();

  // ****************** MUTATORS *********************

  /** Resets the statistics. */
  @Override
  void reset();

}
