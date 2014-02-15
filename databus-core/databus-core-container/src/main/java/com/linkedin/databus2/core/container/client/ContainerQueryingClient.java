package com.linkedin.databus2.core.container.client;

/*
 *
 * Copyright 2014 LinkedIn Corp. All rights reserved
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

import java.net.InetSocketAddress;

import com.linkedin.databus.core.monitoring.events.DbusEventsTotalStatsEvent;


/**
 * For databus internal use only
 */
/*
 * A class to query the Container via HTTP and gather responses.
 */
public class ContainerQueryingClient
{
  private final InetSocketAddress _address;

  /**
   * For databus internal use only
   */
  public ContainerQueryingClient(InetSocketAddress address)
  {
    _address = address;
  }

  /**
   * For databus internal use only
   */
  public DbusEventsTotalStatsEvent getDbusStats(String dbName, int partId)
  {
    // TODO fetch and return the statistics for this partition
    return null;
  }
}
