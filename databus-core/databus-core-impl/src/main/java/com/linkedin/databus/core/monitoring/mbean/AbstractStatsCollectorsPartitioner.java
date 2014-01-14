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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import javax.management.MBeanServer;

import com.linkedin.databus.core.monitoring.StatsCollectorCallback;

/**
 * This class divides the StatsCollectorMergeable instances that are added to this class
 * into "buckets". It uses one "StatsCollectors" instance per bucket to merge all
 * StatsCollectorMergeable instances falling to same buckets.
 *
 * It also provides registration API for registering callback to listen to bucket
 * collector's add/remove events
 *
 * A typical use of this is where we want to merge all relay stats belonging to one DB. A
 * single relay can host multiple physical partitions belonging to different databases. In
 * this case, all the partition specific StatsCollectorMergeable instances belonging to
 * one database are considered one "bucket" and merged using StatsCollectors instance.
 *
 * This class is not thread-safe w.r.t addition/removal of
 * StatsCollector/StatsCollectorCallback.
 */
public abstract class AbstractStatsCollectorsPartitioner<T extends StatsCollectorMergeable<T>>
{
  /**
   * Map between bucket name to StatsCollectors
   */
  private final Map<String, StatsCollectors<T>>      _statCollectorsMap;

  /**
   * MBeanServer
   */
  private final MBeanServer                          _mbeanServer;

  /**
   * Callback for addition/removal of statsCollectors objects
   */
  private StatsCollectorCallback<StatsCollectors<T>> _statsCallback;

  // Mbean Owner id
  private final int                                  _ownerId;

  // Suffix to the MbeanName ( ".inbound", ".outbound", etc)
  protected final String                             _mbeanNameSuffix;

  /**
   * @param ownerId
   *          : Owner Id
   * @param suffix
   *          : Suffix to be added to mbean name for bucket collectors
   * @param mbeanServer
   *          : MBeanServer to register/deregister
   */
  public AbstractStatsCollectorsPartitioner(int ownerId,
                                            String suffix,
                                            MBeanServer mbeanServer)
  {
    _statCollectorsMap = new HashMap<String, StatsCollectors<T>>();
    _mbeanServer = mbeanServer;
    _ownerId = ownerId;
    _mbeanNameSuffix = suffix;
  }

  /**
   * Stats Collector Merger for an individual bucket
   *
   * @param bucketName
   *          bucket Name
   * @return
   */
  protected StatsCollectors<T> getBucketStatsCollector(String bucketName)
  {
    return _statCollectorsMap.get(bucketName);
  }

  /**
   * Add the individual StatsCollectorMergeable instance.
   *
   * @param bucketKey
   *          : Key used to locate the StatsCollectors instance which merges all
   *          "StatsCollectorMergeable" instances belonging to one bucket
   * @param key
   *          : Key used to identify "StatsCollectorMergeable" instance within
   *          the bucket
   * @param collector
   *          : StatsCollectorMergeable to be added to the partition
   */
  protected void addStatsCollector(String bucketKey, String key, T collector)
  {
    StatsCollectors<T> c = _statCollectorsMap.get(bucketKey);

    if (null == c)
    {
      c = createStatsCollector(_ownerId, bucketKey, _mbeanServer);
      _statCollectorsMap.put(bucketKey, c);
      notifyStatsAdd(c);
    }
    c.addStatsCollector(key, collector);
  }

  /**
   * De-register all stats collectors and remove from MBeanServer.
   */
  public void removeAllStatsCollector()
  {
    Iterator<Entry<String, StatsCollectors<T>>> itr =
        _statCollectorsMap.entrySet().iterator();
    while (itr.hasNext())
    {
      StatsCollectors<T> c = itr.next().getValue();
      unregisterStatsCollector(c);
      notifyStatsRemove(c);
      itr.remove();
    }
  }

  /**
   * Notify the stats callback when bucket (merger) collector is added.
   *
   * @param collector
   */
  private void notifyStatsAdd(StatsCollectors<T> collector)
  {
    if (null != _statsCallback)
    {
      _statsCallback.addedStats(collector);
    }
  }

  /**
   * Notify the stats callback when bucket (merger) collector is removed.
   *
   * @param collector
   */
  private void notifyStatsRemove(StatsCollectors<T> collector)
  {
    if (null != _statsCallback)
    {
      _statsCallback.removedStats(collector);
    }
  }

  /**
   * Register the stats callback to get notification when bucket (merger) collector is
   * added/removed.
   *
   * @param collector
   */
  public void registerStatsCallback(StatsCollectorCallback<StatsCollectors<T>> statsCallback)
  {
    _statsCallback = statsCallback;

    // Trigger callback for those collectors that were already added
    for (Entry<String, StatsCollectors<T>> e : _statCollectorsMap.entrySet())
    {
      notifyStatsAdd(e.getValue());
    }
  }

  /**
   * Factory to instantiate the bucket collector. This is invoked the first time an
   * StatsCollectorMergeable gets added to a bucket.
   *
   * @param ownerId
   *          Owner Id
   * @param suffix
   *          Suffix name to add to the name of the bucket collector
   * @param server
   *          Mbean Server
   * @return
   */
  protected abstract StatsCollectors<T> createStatsCollector(int ownerId,
                                                             String suffix,
                                                             MBeanServer server);

  /**
   * Unregister the bucket stats collector from mbean server.
   */
  protected abstract void unregisterStatsCollector(StatsCollectors<T> collector);
}
