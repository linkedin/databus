/*
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
 */
package com.linkedin.databus.client.pub;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;

import org.apache.avro.Schema;
//import org.apache.commons.math3.stat.StatUtils;
import org.testng.annotations.Test;

import com.codahale.metrics.MergeableExponentiallyDecayingReservoir;

import com.linkedin.databus.client.pub.mbean.UnifiedClientStats;
import com.linkedin.databus.core.DbusConstants;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventFactory;
import com.linkedin.databus.core.DbusEventInfo;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.DbusEventV2Factory;
import com.linkedin.databus.core.DbusOpcode;
import com.linkedin.databus.core.KeyTypeNotImplementedException;
import com.linkedin.databus2.schemas.utils.SchemaHelper;

/**
 * Mainly tests the histogram/percentile metrics of UnifiedClientStats, including multi-connection
 * aggregation (merging).
 *
 * See also TestGenericDispatcher (unit test; tests numConsumerErrors) and TestRelayBootstrapSwitch
 * (integration test; tests all remaining UnifiedClientStats metrics:  curBootstrappingPartitions,
 * curDeadConnections, numDataEvents, timeLagLastReceivedToNowMs).
 */
public class TestUnifiedClientStats
{
  private static final Schema SOURCE1_SCHEMA =
      Schema.parse("{\"name\":\"source1\",\"type\":\"record\",\"fields\":[{\"name\":\"s\",\"type\":\"string\"}]}");
  private static final String SOURCE1_SCHEMA_STR = SOURCE1_SCHEMA.toString();
  private static final byte[] SOURCE1_SCHEMAID = SchemaHelper.getSchemaId(SOURCE1_SCHEMA_STR);
  private static final byte[] SOURCE1_PAYLOAD = new byte[] { 0x67, 0x72, 0x6f, 0x6e, 0x6b };
  // alternatively:    byte[] SOURCE1_PAYLOAD = javax.xml.bind.DatatypeConverter.parseHexBinary("67726f6e6b");

  private DbusEventFactory _eventFactory = new DbusEventV2Factory();


  private DbusEvent createEvent(long timestampNs)
  {
    DbusEventInfo eventInfo = new DbusEventInfo(DbusOpcode.UPSERT,
                                                6004L,             // SCN
                                                (short) 1,         // physical partition ID
                                                (short) 1,         // logical partition ID
                                                timestampNs,
                                                (short) 1,         // srcId
                                                SOURCE1_SCHEMAID,  // payloadSchemaMd5
                                                SOURCE1_PAYLOAD,   // payload
                                                false,             // enableTracing
                                                true);             // autocommit
    DbusEventKey key = new DbusEventKey("myKey".getBytes(Charset.forName("UTF-8")));
    ByteBuffer buf = ByteBuffer.allocate(1000).order(ByteOrder.BIG_ENDIAN);
    try
    {
      DbusEventFactory.serializeEvent(key, buf, eventInfo);
    }
    catch (KeyTypeNotImplementedException ex)
    {
      fail("string key type not supported by DbusEventV2Factory?!? " + ex.getLocalizedMessage());
    }
    return _eventFactory.createReadOnlyDbusEventFromBuffer(buf, 0);
  }


  /**
   * Tests the basic (non-aggregated) functionality of the histogram/percentile metrics
   * (timeLagSourceToReceiptMs and timeLagConsumerCallbacksMs).
   */
  @Test
  public void testBasicHistogramMetrics()
  {
    // (1) create stats object
    UnifiedClientStats unifiedClientStats = new UnifiedClientStats(3 /* ownerId */, "stats_name", "stats_dim");

    for (int i = 0; i < 200; ++i)
    {
      // Without the ability to override System.currentTimeMillis() (or hacking UnifiedClientStats to use an
      // overridable method to provide the time, and then overriding it here), there's a small chance that
      // our System.currentTimeMillis() call and that in registerDataEventReceived() will return values that
      // differ by a non-constant amount (i.e., jitter).  But we can manage that with inequalities in our
      // assertions.
      // Expected histogram values for timeLagSourceToReceiptMs range from 0 to 1990 ms (approximately).
      long sourceTimestampNs = (System.currentTimeMillis() - 10*i) * DbusConstants.NUM_NSECS_IN_MSEC;

      // We have perfect control over the values for timeLagConsumerCallbacksMs.  Make calculations trivial:
      // histogram values will be 0 through 199 ms (exactly).
      long callbackTimeElapsedNs = (long)i * DbusConstants.NUM_NSECS_IN_MSEC;

      // (2) create 200 fake DbusEvents
      DbusEvent dbusEvent = createEvent(sourceTimestampNs);

      // (3) call registerDataEventReceived() and registerCallbacksProcessed() for each event
      // (normally there are more of the latter since there are more callback types than just onDataEvent(),
      // but it doesn't really matter, and it simplifies things if we keep a fixed ratio--here just 1:1)
      unifiedClientStats.registerDataEventReceived(dbusEvent);
      unifiedClientStats.registerCallbacksProcessed(callbackTimeElapsedNs);
    }

    // (4) verify histogram values are as expected

    // Both metrics-core and Apache Commons Math use the "R-6" quantile-estimation method, as described
    // at http://en.wikipedia.org/wiki/Quantile .
    //
    // N = 200
    // p = 0.5, 0.9, 0.95, 0.99
    // h = (N+1)*p = 100.5, 180.9, 190.95, 198.99
    //
    // Q[50th]  =  x[100-1] + (100.5  - 100)*(x[100-1+1] - x[100-1])  =   99.0 + 0.5 *(100.0 -  99.0)  =   99.5
    // Q[90th]  =  x[180-1] + (180.9  - 180)*(x[180-1+1] - x[180-1])  =  179.0 + 0.9 *(180.0 - 179.0)  =  179.9
    // Q[95th]  =  x[190-1] + (190.95 - 190)*(x[190-1+1] - x[190-1])  =  189.0 + 0.95*(190.0 - 189.0)  =  189.95
    // Q[99th]  =  x[198-1] + (198.99 - 198)*(x[198-1+1] - x[198-1])  =  197.0 + 0.99*(198.0 - 197.0)  =  197.99
    assertEquals("unexpected timeLagConsumerCallbacksMs 50th percentile",
                 99.5,
                 unifiedClientStats.getTimeLagConsumerCallbacksMs_HistPct_50());
    assertEquals("unexpected timeLagConsumerCallbacksMs 90th percentile",
                 179.9,
                 unifiedClientStats.getTimeLagConsumerCallbacksMs_HistPct_90());
    assertEquals("unexpected timeLagConsumerCallbacksMs 95th percentile",
                 189.95,
                 unifiedClientStats.getTimeLagConsumerCallbacksMs_HistPct_95());
    assertEquals("unexpected timeLagConsumerCallbacksMs 99th percentile",
                 197.99,
                 unifiedClientStats.getTimeLagConsumerCallbacksMs_HistPct_99());
    assertEquals("unexpected timeLagConsumerCallbacksMs max value",
                 199.0,
                 unifiedClientStats.getTimeLagConsumerCallbacksMs_Max());

    // See sourceTimestampNs comment above.  Approximately:
    // Q[50th]  =  x[100-1] + (100.5  - 100)*(x[100-1+1] - x[100-1])  =   990.0 + 0.5 *(1000.0 -  990.0)  =   995.0
    // Q[90th]  =  x[180-1] + (180.9  - 180)*(x[180-1+1] - x[180-1])  =  1790.0 + 0.9 *(1800.0 - 1790.0)  =  1799.0
    // Q[95th]  =  x[190-1] + (190.95 - 190)*(x[190-1+1] - x[190-1])  =  1890.0 + 0.95*(1900.0 - 1890.0)  =  1899.5
    // Q[99th]  =  x[198-1] + (198.99 - 198)*(x[198-1+1] - x[198-1])  =  1970.0 + 0.99*(1980.0 - 1970.0)  =  1979.9
    // ...but allow +/-1 for jitter
    double percentile = unifiedClientStats.getTimeLagSourceToReceiptMs_HistPct_50();
    assertTrue("unexpected timeLagSourceToReceiptMs 50th percentile: " + percentile,
               994.0 <= percentile && percentile <= 996.0);    // nominal value is 995.0

    percentile = unifiedClientStats.getTimeLagSourceToReceiptMs_HistPct_90();
    assertTrue("unexpected timeLagSourceToReceiptMs 90th percentile: " + percentile,
               1798.0 <= percentile && percentile <= 1800.0);  // nominal value is 1799.0

    percentile = unifiedClientStats.getTimeLagSourceToReceiptMs_HistPct_95();
    assertTrue("unexpected timeLagSourceToReceiptMs 95th percentile: " + percentile,
               1898.5 <= percentile && percentile <= 1900.5);  // nominal value is 1899.5, but saw 1900.45 once

    percentile = unifiedClientStats.getTimeLagSourceToReceiptMs_HistPct_99();
    assertTrue("unexpected timeLagSourceToReceiptMs 99th percentile: " + percentile,
               1978.9 <= percentile && percentile <= 1980.9);  // nominal value is 1979.9
  }


  /**
   * Tests aggregation (merging) of the timeLagSourceToReceiptMs histogram/percentile metric in the case
   * that one of the connections is bootstrapping.
   *
   * Blast out 1000 data values for stats #1 and #2, but with the latter in bootstrap mode:
   * timestampLastDataEventWasReceivedMs will be zero for stats #2 (and its reservoir empty), so
   * merging it won't affect the aggregate value for timeLagSourceToReceiptMs; all such aggregate
   * stats should be identical to those for stats #1.  Also, all values for stats #2 should be -1.0,
   * per our design spec.  (This is similar to testHistogramMetricsAggregationDeadSourcesConnection().)
   */
  @Test
  public void testHistogramMetricsAggregationBootstrapMode()
  {
    // create stats objects:  two low-level (per-connection) ones and one aggregator
    UnifiedClientStats unifiedClientStats1 = new UnifiedClientStats(1 /* ownerId */, "test1", "dim1");
    UnifiedClientStats unifiedClientStats2 = new UnifiedClientStats(2 /* ownerId */, "test2", "dim2");
    UnifiedClientStats unifiedClientStatsAgg = new UnifiedClientStats(99 /* ownerId */, "testAgg", "dimAgg");

    unifiedClientStats2.setBootstrappingState(true);

    for (int i = 0; i < MergeableExponentiallyDecayingReservoir.DEFAULT_SIZE; ++i)  // 1028
    {
      long now = System.currentTimeMillis();
      long sourceTimestampNs1 = (now - 1000L - i) * DbusConstants.NUM_NSECS_IN_MSEC;
      long sourceTimestampNs2 = (now - 5000L - i) * DbusConstants.NUM_NSECS_IN_MSEC;

      unifiedClientStats1.registerDataEventReceived(createEvent(sourceTimestampNs1));
      unifiedClientStats2.registerDataEventReceived(createEvent(sourceTimestampNs2));
    }

    unifiedClientStatsAgg.merge(unifiedClientStats1);
    unifiedClientStatsAgg.merge(unifiedClientStats2);

    assertEquals("unexpected timeLagSourceToReceiptMs 50th percentile for aggregated stats",
                 unifiedClientStats1.getTimeLagSourceToReceiptMs_HistPct_50(),
                 unifiedClientStatsAgg.getTimeLagSourceToReceiptMs_HistPct_50());
    assertEquals("unexpected timeLagSourceToReceiptMs 90th percentile for aggregated stats",
                 unifiedClientStats1.getTimeLagSourceToReceiptMs_HistPct_90(),
                 unifiedClientStatsAgg.getTimeLagSourceToReceiptMs_HistPct_90());
    assertEquals("unexpected timeLagSourceToReceiptMs 95th percentile for aggregated stats",
                 unifiedClientStats1.getTimeLagSourceToReceiptMs_HistPct_95(),
                 unifiedClientStatsAgg.getTimeLagSourceToReceiptMs_HistPct_95());
    assertEquals("unexpected timeLagSourceToReceiptMs 99th percentile for aggregated stats",
                 unifiedClientStats1.getTimeLagSourceToReceiptMs_HistPct_99(),
                 unifiedClientStatsAgg.getTimeLagSourceToReceiptMs_HistPct_99());

    // bootstrap mode => should return -1.0 for all percentiles
    assertEquals("unexpected timeLagSourceToReceiptMs 50th percentile for connection #2",
                 -1.0,
                 unifiedClientStats2.getTimeLagSourceToReceiptMs_HistPct_50());
    assertEquals("unexpected timeLagSourceToReceiptMs 90th percentile for connection #2",
                 -1.0,
                 unifiedClientStats2.getTimeLagSourceToReceiptMs_HistPct_90());
    assertEquals("unexpected timeLagSourceToReceiptMs 95th percentile for connection #2",
                 -1.0,
                 unifiedClientStats2.getTimeLagSourceToReceiptMs_HistPct_95());
    assertEquals("unexpected timeLagSourceToReceiptMs 99th percentile for connection #2",
                 -1.0,
                 unifiedClientStats2.getTimeLagSourceToReceiptMs_HistPct_99());
  }


  /**
   * Tests aggregation (merging) of the timeLagSourceToReceiptMs histogram/percentile metric in the case
   * that one of the connections is dead (i.e., no data events received).
   *
   * Blast out 1000 data values for stats #1 but none for stats #2 (in particular, no registerDataEventReceived()
   * calls):  timestampLastDataEventWasReceivedMs will be zero for stats #2 (and its reservoir empty), so
   * merging it won't affect the aggregate value for timeLagSourceToReceiptMs; all such aggregate stats should
   * be identical to those for stats #1.  Also, all values for stats #2 should be -1.0, per our design spec.
   * (This is similar to testHistogramMetricsAggregationBootstrapMode().)
   */
  @Test
  public void testHistogramMetricsAggregationDeadSourcesConnection()
  {
    // create stats objects:  two low-level (per-connection) ones and one aggregator
    UnifiedClientStats unifiedClientStats1 = new UnifiedClientStats(1 /* ownerId */, "test1", "dim1");
    UnifiedClientStats unifiedClientStats2 = new UnifiedClientStats(2 /* ownerId */, "test2", "dim2");
    UnifiedClientStats unifiedClientStatsAgg = new UnifiedClientStats(99 /* ownerId */, "testAgg", "dimAgg");

    // could break this into two (or more) parts and do multiple merges, but not clear there's any point...
    for (int i = 0; i < 2*MergeableExponentiallyDecayingReservoir.DEFAULT_SIZE; ++i)  // 2*1028
    {
      long sourceTimestampNs1 = (System.currentTimeMillis() - 1000L - i) * DbusConstants.NUM_NSECS_IN_MSEC;
      // no data events for connection #2 => no need for sourceTimestampNs2

      unifiedClientStats1.registerDataEventReceived(createEvent(sourceTimestampNs1));
    }

    unifiedClientStatsAgg.merge(unifiedClientStats1);
    unifiedClientStatsAgg.merge(unifiedClientStats2);

    assertEquals("unexpected timeLagSourceToReceiptMs 50th percentile for aggregated stats",
                 unifiedClientStats1.getTimeLagSourceToReceiptMs_HistPct_50(),
                 unifiedClientStatsAgg.getTimeLagSourceToReceiptMs_HistPct_50());
    assertEquals("unexpected timeLagSourceToReceiptMs 90th percentile for aggregated stats",
                 unifiedClientStats1.getTimeLagSourceToReceiptMs_HistPct_90(),
                 unifiedClientStatsAgg.getTimeLagSourceToReceiptMs_HistPct_90());
    assertEquals("unexpected timeLagSourceToReceiptMs 95th percentile for aggregated stats",
                 unifiedClientStats1.getTimeLagSourceToReceiptMs_HistPct_95(),
                 unifiedClientStatsAgg.getTimeLagSourceToReceiptMs_HistPct_95());
    assertEquals("unexpected timeLagSourceToReceiptMs 99th percentile for aggregated stats",
                 unifiedClientStats1.getTimeLagSourceToReceiptMs_HistPct_99(),
                 unifiedClientStatsAgg.getTimeLagSourceToReceiptMs_HistPct_99());

    // no data values => should return -1.0 for all percentiles
    assertEquals("unexpected timeLagSourceToReceiptMs 50th percentile for connection #2",
                 -1.0,
                 unifiedClientStats2.getTimeLagSourceToReceiptMs_HistPct_50());
    assertEquals("unexpected timeLagSourceToReceiptMs 90th percentile for connection #2",
                 -1.0,
                 unifiedClientStats2.getTimeLagSourceToReceiptMs_HistPct_90());
    assertEquals("unexpected timeLagSourceToReceiptMs 95th percentile for connection #2",
                 -1.0,
                 unifiedClientStats2.getTimeLagSourceToReceiptMs_HistPct_95());
    assertEquals("unexpected timeLagSourceToReceiptMs 99th percentile for connection #2",
                 -1.0,
                 unifiedClientStats2.getTimeLagSourceToReceiptMs_HistPct_99());
  }


  /**
   * Tests aggregation (merging) of the histogram/percentile metrics (timeLagSourceToReceiptMs and
   * timeLagConsumerCallbacksMs) in the case that there have been no data events or callbacks, i.e.,
   * there are no data points in the histogram reservoirs.  All timeLagSourceToReceiptMs metrics
   * and timeLagConsumerCallbacksMs metrics should be -1.0, per the design spec.
   */
  @Test
  public void testHistogramMetricsAggregationNoData()
  {
    UnifiedClientStats unifiedClientStats1 = new UnifiedClientStats(1 /* ownerId */, "test1", "dim1");
    UnifiedClientStats unifiedClientStats2 = new UnifiedClientStats(2 /* ownerId */, "test2", "dim2");
    UnifiedClientStats unifiedClientStatsAgg = new UnifiedClientStats(99 /* ownerId */, "testAgg", "dimAgg");

    assertEquals("unexpected timeLagSourceToReceiptMs 50th percentile for connection #1",
                 -1.0,
                 unifiedClientStats1.getTimeLagSourceToReceiptMs_HistPct_50());
    assertEquals("unexpected timeLagSourceToReceiptMs 90th percentile for connection #1",
                 -1.0,
                 unifiedClientStats1.getTimeLagSourceToReceiptMs_HistPct_90());
    assertEquals("unexpected timeLagSourceToReceiptMs 95th percentile for connection #1",
                 -1.0,
                 unifiedClientStats1.getTimeLagSourceToReceiptMs_HistPct_95());
    assertEquals("unexpected timeLagSourceToReceiptMs 99th percentile for connection #1",
                 -1.0,
                 unifiedClientStats1.getTimeLagSourceToReceiptMs_HistPct_99());

    assertEquals("unexpected timeLagSourceToReceiptMs 50th percentile for connection #2",
                 -1.0,
                 unifiedClientStats2.getTimeLagSourceToReceiptMs_HistPct_50());
    assertEquals("unexpected timeLagSourceToReceiptMs 90th percentile for connection #2",
                 -1.0,
                 unifiedClientStats2.getTimeLagSourceToReceiptMs_HistPct_90());
    assertEquals("unexpected timeLagSourceToReceiptMs 95th percentile for connection #2",
                 -1.0,
                 unifiedClientStats2.getTimeLagSourceToReceiptMs_HistPct_95());
    assertEquals("unexpected timeLagSourceToReceiptMs 99th percentile for connection #2",
                 -1.0,
                 unifiedClientStats2.getTimeLagSourceToReceiptMs_HistPct_99());

    assertEquals("unexpected timeLagSourceToReceiptMs 50th percentile for aggregated stats",
                 -1.0,
                 unifiedClientStatsAgg.getTimeLagSourceToReceiptMs_HistPct_50());
    assertEquals("unexpected timeLagSourceToReceiptMs 90th percentile for aggregated stats",
                 -1.0,
                 unifiedClientStatsAgg.getTimeLagSourceToReceiptMs_HistPct_90());
    assertEquals("unexpected timeLagSourceToReceiptMs 95th percentile for aggregated stats",
                 -1.0,
                 unifiedClientStatsAgg.getTimeLagSourceToReceiptMs_HistPct_95());
    assertEquals("unexpected timeLagSourceToReceiptMs 99th percentile for aggregated stats",
                 -1.0,
                 unifiedClientStatsAgg.getTimeLagSourceToReceiptMs_HistPct_99());


    assertEquals("unexpected timeLagConsumerCallbacksMs 50th percentile for connection #1",
                 -1.0,
                 unifiedClientStats1.getTimeLagConsumerCallbacksMs_HistPct_50());
    assertEquals("unexpected timeLagConsumerCallbacksMs 90th percentile for connection #1",
                 -1.0,
                 unifiedClientStats1.getTimeLagConsumerCallbacksMs_HistPct_90());
    assertEquals("unexpected timeLagConsumerCallbacksMs 95th percentile for connection #1",
                 -1.0,
                 unifiedClientStats1.getTimeLagConsumerCallbacksMs_HistPct_95());
    assertEquals("unexpected timeLagConsumerCallbacksMs 99th percentile for connection #1",
                 -1.0,
                 unifiedClientStats1.getTimeLagConsumerCallbacksMs_HistPct_99());
    assertEquals("unexpected timeLagConsumerCallbacksMs max for connection #1",
                 -1.0,
                 unifiedClientStats1.getTimeLagConsumerCallbacksMs_Max());

    assertEquals("unexpected timeLagConsumerCallbacksMs 50th percentile for connection #2",
                 -1.0,
                 unifiedClientStats2.getTimeLagConsumerCallbacksMs_HistPct_50());
    assertEquals("unexpected timeLagConsumerCallbacksMs 90th percentile for connection #2",
                 -1.0,
                 unifiedClientStats2.getTimeLagConsumerCallbacksMs_HistPct_90());
    assertEquals("unexpected timeLagConsumerCallbacksMs 95th percentile for connection #2",
                 -1.0,
                 unifiedClientStats2.getTimeLagConsumerCallbacksMs_HistPct_95());
    assertEquals("unexpected timeLagConsumerCallbacksMs 99th percentile for connection #2",
                 -1.0,
                 unifiedClientStats2.getTimeLagConsumerCallbacksMs_HistPct_99());
    assertEquals("unexpected timeLagConsumerCallbacksMs max for connection #2",
                 -1.0,
                 unifiedClientStats2.getTimeLagConsumerCallbacksMs_Max());

    assertEquals("unexpected timeLagConsumerCallbacksMs 50th percentile for aggregated stats",
                 -1.0,
                 unifiedClientStatsAgg.getTimeLagConsumerCallbacksMs_HistPct_50());
    assertEquals("unexpected timeLagConsumerCallbacksMs 90th percentile for aggregated stats",
                 -1.0,
                 unifiedClientStatsAgg.getTimeLagConsumerCallbacksMs_HistPct_90());
    assertEquals("unexpected timeLagConsumerCallbacksMs 95th percentile for aggregated stats",
                 -1.0,
                 unifiedClientStatsAgg.getTimeLagConsumerCallbacksMs_HistPct_95());
    assertEquals("unexpected timeLagConsumerCallbacksMs 99th percentile for aggregated stats",
                 -1.0,
                 unifiedClientStatsAgg.getTimeLagConsumerCallbacksMs_HistPct_99());
    assertEquals("unexpected timeLagConsumerCallbacksMs max for aggregated stats",
                 -1.0,
                 unifiedClientStatsAgg.getTimeLagConsumerCallbacksMs_Max());
  }


  /**
   * Tests aggregation (merging) of the histogram/percentile metrics (timeLagSourceToReceiptMs and
   * timeLagConsumerCallbacksMs).  This is basically the "happy path" case.
   *
   * Blast out 1000 low data values for stats #1 and 1000 high data values for stats #2 (interleaved so
   * timestamps [and therefore priorities] are comparable), then merge and verify that max is within #2's
   * range and that median falls between the two ranges.  (There's no guarantee that #1's minimum or #2's
   * maximum will survive, but roughly half of each one's values should, so the min and max are guaranteed
   * to fall within #1's and #2's range, respectively.)
   */
  @Test
  public void testHistogramMetricsAggregationNonOverlappingRanges()
  {
    // create stats objects:  two low-level (per-connection) ones and one aggregator
    UnifiedClientStats unifiedClientStats1 = new UnifiedClientStats(1 /* ownerId */, "test1", "dim1");
    UnifiedClientStats unifiedClientStats2 = new UnifiedClientStats(2 /* ownerId */, "test2", "dim2");
    UnifiedClientStats unifiedClientStatsAgg = new UnifiedClientStats(99 /* ownerId */, "testAgg", "dimAgg");

    for (int i = 0; i < MergeableExponentiallyDecayingReservoir.DEFAULT_SIZE; ++i)  // 1028
    {
      // As noted in testBasicHistogramMetrics(), our dependence on System.currentTimeMillis() may lead
      // to some jitter in the data values for timeLagSourceToReceiptMs.
      long now = System.currentTimeMillis();
      long sourceTimestampNs1 = (now - 1000L - i) * DbusConstants.NUM_NSECS_IN_MSEC;
      long sourceTimestampNs2 = (now - 5000L - i) * DbusConstants.NUM_NSECS_IN_MSEC;

      long callbackTimeElapsedNs1 = (long)i * DbusConstants.NUM_NSECS_IN_MSEC;
      long callbackTimeElapsedNs2 = ((long)i + 2000L) * DbusConstants.NUM_NSECS_IN_MSEC;

      DbusEvent dbusEvent1 = createEvent(sourceTimestampNs1);
      DbusEvent dbusEvent2 = createEvent(sourceTimestampNs2);

      unifiedClientStats1.registerDataEventReceived(dbusEvent1);
      unifiedClientStats2.registerDataEventReceived(dbusEvent2);
      unifiedClientStats1.registerCallbacksProcessed(callbackTimeElapsedNs1);
      unifiedClientStats2.registerCallbacksProcessed(callbackTimeElapsedNs2);
    }

    unifiedClientStatsAgg.merge(unifiedClientStats1);
    unifiedClientStatsAgg.merge(unifiedClientStats2);

    // Expected timeLagConsumerCallbacksMs histogram values (exact):
    //   unifiedClientStats1:  0 to 1027 ms
    //   unifiedClientStats2:  2000 to 3027 ms

    assertEquals("unexpected timeLagConsumerCallbacksMs 50th percentile for connection #1",
                 513.5,
                 unifiedClientStats1.getTimeLagConsumerCallbacksMs_HistPct_50());
    assertEquals("unexpected timeLagConsumerCallbacksMs 50th percentile for connection #2",
                 2513.5,
                 unifiedClientStats2.getTimeLagConsumerCallbacksMs_HistPct_50());

    // The exact value depends on the relative fraction of '1' and '2' values that are retained in the
    // aggregate.  If equal, the value should be near 1513.5, but even then the exact value depends on
    // whether the 1027 and 2000 values get bumped out of the aggregate.  In the more common case that
    // the fractions retained are unequal, the median will fall between two values near the top end of
    // unifiedClientStats1 or near the bottom end of unifiedClientStats2.  An allowance of 100 either
    // way should be safe.
    double percentile = unifiedClientStatsAgg.getTimeLagConsumerCallbacksMs_HistPct_50();
    assertTrue("unexpected timeLagConsumerCallbacksMs 50th percentile for aggregated stats: " + percentile,
               927.0 <= percentile && percentile <= 2100.0);

    assertEquals("unexpected timeLagConsumerCallbacksMs max value for connection #1",
                 1027.0,
                 unifiedClientStats1.getTimeLagConsumerCallbacksMs_Max());
    assertEquals("unexpected timeLagConsumerCallbacksMs max value for connection #2",
                 3027.0,
                 unifiedClientStats2.getTimeLagConsumerCallbacksMs_Max());

    double max = unifiedClientStatsAgg.getTimeLagConsumerCallbacksMs_Max();
    assertTrue("unexpected timeLagConsumerCallbacksMs max value for aggregated stats: " + max,
               2000.0 <= max && max <= 3027.0);  // nominal value is 3027.0

    // Expected timeLagSourceToReceiptMs histogram values (approximate):
    //   unifiedClientStats1:  1000 to 2027 ms
    //   unifiedClientStats2:  5000 to 6027 ms

    percentile = unifiedClientStats1.getTimeLagSourceToReceiptMs_HistPct_50();
    assertTrue("unexpected timeLagSourceToReceiptMs 50th percentile for connection #1: " + percentile,
               1512.5 <= percentile && percentile <= 1514.5);    // nominal value is 1513.5

    percentile = unifiedClientStats2.getTimeLagSourceToReceiptMs_HistPct_50();
    assertTrue("unexpected timeLagSourceToReceiptMs 50th percentile for connection #2: " + percentile,
               5512.5 <= percentile && percentile <= 5514.5);    // nominal value is 5513.5

    // same caveat as above:  the median depends strongly on the relative proportion of unifiedClientStats1
    // and unifiedClientStats2 data points retained in the aggregate, so the inequality is quite loose
    percentile = unifiedClientStatsAgg.getTimeLagSourceToReceiptMs_HistPct_50();
    assertTrue("unexpected timeLagSourceToReceiptMs 50th percentile for aggregated stats: " + percentile,
               1927.0 <= percentile && percentile <= 5100.0);
  }

}
