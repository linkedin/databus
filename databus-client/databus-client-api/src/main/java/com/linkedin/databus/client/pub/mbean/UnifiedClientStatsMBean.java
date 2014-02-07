/*
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
 */
package com.linkedin.databus.client.pub.mbean;


/**
 * Simplified/unified metrics for Databus consumers.  Both inbound (from the relay or
 * bootstrap server) and outbound (to the consumer via callbacks) statistics are included.
 *
 * This interface is used both for per-partition (single source), non-exposed statistics
 * and for two kinds of exposed aggregates:  single-source (single connection) and multi-
 * source (multiple connections).
 */
public interface UnifiedClientStatsMBean
{
  /** GETTERS */

  // TODO:  track changes in canonical config fingerprint (e.g., checksum of key/value
  //        pairs in sorted order)?  (ideally would persist previous value, but could
  //        also just expose CRC-32 as [unsigned?] int metric)

  /**
   * Number of partitions or tables currently bootstrapping.  At the per-partition or
   * per-table (lowest) level, this is equivalent to a boolean:  0 if pulling from the
   * relay, 1 if pulling from the bootstrap server.  For Espresso ("v3") bootstraps, which
   * limit concurrent bootstraps to one, the aggregate level will show the total number
   * of partitions/tables either bootstrapping or waiting to bootstrap; only one will
   * be making progress at any given time.  (Oracle bootstraps may run in parallel.)
   */
  // TODO:  expand description to describe v2, v3, and client-load-balancing nuances
  public int getCurBootstrappingPartitions();

  /**
   * Number of connections (per-table or per-partition) currently suspended, i.e., dead.
   * Though it is possible to manually resurrect a suspended connection via the JMX
   * console, by default they remain dead.  The normal fix is to bounce the application.
   */
  public int getCurDeadConnections();

  /**
   * Number of errors returned by consumer callbacks (i.e., by application code).
   * All callbacks are considered, not just onDataEvent() ones.
   */
  public long getNumConsumerErrors();

  /**
   * Number of data events received by the client library from the relay and/or
   * bootstrap server.
   */
  // [NOTE:  consumers can trivially do their own callback counts if they care]
  public long getNumDataEvents();

  /**
   * Median time interval, in milliseconds, between the time recently received
   * events were committed at the source database and the time they were received
   * by the Databus client library.  This is a sampled approximation, weighted most
   * heavily toward events received in the past five minutes.  When aggregated, this
   * is the median interval across all subscribed partitions.
   */
  public double getTimeLagSourceToReceiptMs_HistPct_50();

  /**
   * 90th-percentile time interval, in milliseconds, between the time recently received
   * events were committed at the source database and the time they were received
   * by the Databus client library.  This is a sampled approximation, weighted most
   * heavily toward events received in the past five minutes.  When aggregated, this
   * is the 90th-percentile interval across all subscribed partitions.
   */
  public double getTimeLagSourceToReceiptMs_HistPct_90();

  /**
   * 95th-percentile time interval, in milliseconds, between the time recently received
   * events were committed at the source database and the time they were received
   * by the Databus client library.  This is a sampled approximation, weighted most
   * heavily toward events received in the past five minutes.  When aggregated, this
   * is the 95th-percentile interval across all subscribed partitions.
   */
  public double getTimeLagSourceToReceiptMs_HistPct_95();

  /**
   * 99th-percentile time interval, in milliseconds, between the time recently received
   * events were committed at the source database and the time they were received
   * by the Databus client library.  This is a sampled approximation, weighted most
   * heavily toward events received in the past five minutes.  When aggregated, this
   * is the 99th-percentile interval across all subscribed partitions.
   */
  public double getTimeLagSourceToReceiptMs_HistPct_99();

  /**
   * Time interval, in milliseconds, between the receipt of the most recent event
   * and the current time.  When aggregated, this is the max interval across all
   * subscribed partitions.
   */
  public long getTimeLagLastReceivedToNowMs();

  /**
   * Max time interval, in milliseconds, for the consumer application to process
   * callbacks.  This is a sampled approximation, weighted most heavily toward events
   * received in the past five minutes.  When aggregated, this is the corresponding
   * max across all subscribed partitions.  Note that all callbacks are considered,
   * not just onDataEvent() ones.
   */
  public double getTimeLagConsumerCallbacksMs_Max();

  /**
   * Median time interval, in milliseconds, for the consumer application to process
   * callbacks.  This is a sampled approximation, weighted most heavily toward events
   * received in the past five minutes.  When aggregated, this is the corresponding
   * median across all subscribed partitions.  Note that all callbacks are considered,
   * not just onDataEvent() ones.
   */
  public double getTimeLagConsumerCallbacksMs_HistPct_50();

  /**
   * 90th-percentile time interval, in milliseconds, for the consumer application to
   * process callbacks.  This is a sampled approximation, weighted most heavily toward
   * events received in the past five minutes.  When aggregated, this is the corresponding
   * 90th-percentile interval across all subscribed partitions.  Note that all callbacks
   * are considered, not just onDataEvent() ones.
   */
  public double getTimeLagConsumerCallbacksMs_HistPct_90();

  /**
   * 95th-percentile time interval, in milliseconds, for the consumer application to
   * process callbacks.  This is a sampled approximation, weighted most heavily toward
   * events received in the past five minutes.  When aggregated, this is the corresponding
   * 95th-percentile interval across all subscribed partitions.  Note that all callbacks
   * are considered, not just onDataEvent() ones.
   */
  public double getTimeLagConsumerCallbacksMs_HistPct_95();

  /**
   * 99th-percentile time interval, in milliseconds, for the consumer application to
   * process callbacks.  This is a sampled approximation, weighted most heavily toward
   * events received in the past five minutes.  When aggregated, this is the corresponding
   * 99th-percentile interval across all subscribed partitions.  Note that all callbacks
   * are considered, not just onDataEvent() ones.
   */
  public double getTimeLagConsumerCallbacksMs_HistPct_99();


  /** MUTATORS */

  void reset();
}
