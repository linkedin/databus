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
import javax.management.MBeanServer;

import com.linkedin.databus.core.data_model.PhysicalPartition;

/**
 * DbusEventStats Merger that merges all inbound/outbound stats belonging to different
 * physical partitions for the same database. Physical partitions belonging to different
 * databases are grouped in different buckets and an dbusEventStatsMerger provided to
 * merge all those belonging to the same database.
 */
public class DbusEventStatsCollectorsPartitioner extends
    AbstractStatsCollectorsPartitioner<DbusEventsStatisticsCollector>
{
  /**
   *
   * @param ownerId
   *          Owner Id
   * @param suffix
   *          Suffix to be added to mbean name for bucket collectors
   * @param mbeanServer
   *          MBeanServer to register/deregister
   */
  public DbusEventStatsCollectorsPartitioner(int ownerId,
                                             String suffix,
                                             MBeanServer mbeanServer)
  {
    super(ownerId, suffix, mbeanServer);
  }

  /**
   * Add the statsCollector for the physical partition. If this is the first stats
   * collector added for the DB, then a stats collector merger object "StatsCollectors" is
   * instantiated and the passed stats collector added to the newly instantiated db-level
   * stats-collector.
   *
   * @param p
   *          : Physical partition corresponding to the stats collector
   * @param collector
   *          : Stats collector to be added
   */
  public void addStatsCollector(PhysicalPartition p,
                                DbusEventsStatisticsCollector collector)
  {
    String bucketKey =
        (null == _mbeanNameSuffix) ? p.getName() : p.getName() + _mbeanNameSuffix;
    addStatsCollector(bucketKey, collector.getName(), collector);
  }

  @Override
  protected StatsCollectors<DbusEventsStatisticsCollector> createStatsCollector(int ownerId,
                                                                                String key,
                                                                                MBeanServer server)
  {
    return new StatsCollectors<DbusEventsStatisticsCollector>(new AggregatedDbusEventsStatisticsCollector(ownerId,
                                                                                                          key,
                                                                                                          true,
                                                                                                          false,
                                                                                                          server));
  }

  @Override
  protected void unregisterStatsCollector(StatsCollectors<DbusEventsStatisticsCollector> collector)
  {
    collector.getStatsCollector().unregisterMBeans();
  }

  /**
   * Get Stats collector for the database
   *
   * @param dbName
   *          DB Name
   */
  public StatsCollectors<DbusEventsStatisticsCollector> getDBStatsCollector(String dbName)
  {
    String bucketKey =
        (null == _mbeanNameSuffix) ? dbName : dbName + _mbeanNameSuffix;
    return super.getBucketStatsCollector(bucketKey);
  }
}
