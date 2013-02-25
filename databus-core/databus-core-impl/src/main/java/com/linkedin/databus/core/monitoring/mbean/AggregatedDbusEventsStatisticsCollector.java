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



import com.linkedin.databus.core.monitoring.events.DbusEventsTotalStatsEvent;
import javax.management.MBeanServer;


/**
 * This class exists only so that we can make a different kind of DbusEventsTotalStats
 * (AggregatedDbusEventsTotalStats) that knows how to merge statistics from DbusEventsTotalStats
 * into itself.
 */
public class AggregatedDbusEventsStatisticsCollector extends DbusEventsStatisticsCollector
{
  public AggregatedDbusEventsStatisticsCollector(int relayId,
                                                String name,
                                                boolean enabled,
                                                boolean threadSafe,
                                                MBeanServer mbeanServer)
  {
    super(relayId, name, enabled, threadSafe, mbeanServer);
  }

  @Override
  protected DbusEventsTotalStats makeDbusEventsTotalStats(int ownerId,
                                                          String dimension,
                                                          boolean enabled,
                                                          boolean threadSafe,
                                                          DbusEventsTotalStatsEvent initData)
  {
    return new AggregatedDbusEventsTotalStats(ownerId,
                                              dimension,
                                              enabled,
                                              threadSafe,
                                              initData);
  }
}
