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
package com.codahale.metrics;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.Double;
import java.lang.InterruptedException;

import org.apache.commons.math3.stat.StatUtils;
import org.testng.annotations.Test;

import com.codahale.metrics.MergeableExponentiallyDecayingReservoir;

/**
 * This class actually tests the combination of MergeableExponentiallyDecayingReservoir and StatUtils.
 * It includes one test of merge(), but merging/aggregation is mostly tested in TestUnifiedClientStats.
 */
public class TestMergeableExponentiallyDecayingReservoir
{
  // borrowed from com.linkedin.databus.core.DbusConstants:
  private static final long NUM_MSECS_IN_SEC = 1000L;
  private static final long NUM_NSECS_IN_MSEC = 1000000L;

  @Test
  public void testEmptyReservoir()
  {
    MergeableExponentiallyDecayingReservoir res = new MergeableExponentiallyDecayingReservoir(10, 0.015);

    // add NO data

    double[] dataValues = res.getUnsortedValues();
    assertEquals("expected empty dataValues array", 0, dataValues.length);
    double result = StatUtils.percentile(dataValues, 50.0);
    assertEquals("expected NaN for 50th percentile of empty array", Double.NaN, result);
    result = StatUtils.max(dataValues);
    assertEquals("expected NaN for max of empty array", Double.NaN, result);
  }

  /**
   * Tests aggregation (merging) of two "low-level" reservoirs into a third.  The reservoirs are created
   * with different landmark values; the test verifies that the landmark values are the same after merging.
   *
   * In particular, create the aggregator first; wait 1 sec, then create the first low-level reservoir (res1);
   * wait another second, then create the second low-level reservoir (res2).  Initially three landmark values
   * should all differ.  (Landmarks are stored only at 1-second granularity.)  After merging res1 into the
   * aggregate, the latter's landmark should equal res1's; res1's should not have changed.  After merging res2
   * into the aggregate, the latter's landmark should now equal res2's, but res2's similarly should not have
   * changed.  After generating more data and merging res1 into the aggregate again, res1's landmark should
   * now equal res2's and the aggregate's, i.e., all three values are synchronized.
   */
  @Test
  public void testReservoirMergeAndLandmarkSynch() throws InterruptedException
  {
    // two low-level reservoirs and one aggregator
    MergeableExponentiallyDecayingReservoir res1;
    MergeableExponentiallyDecayingReservoir res2;
    MergeableExponentiallyDecayingReservoir aggr;

    aggr = new MergeableExponentiallyDecayingReservoir(10, 0.015);
    Thread.sleep(1000L);
    res1 = new MergeableExponentiallyDecayingReservoir(10, 0.015);
    Thread.sleep(1000L);
    res2 = new MergeableExponentiallyDecayingReservoir(10, 0.015);

    //long origLandmarkAggr = aggr.getLandmark();
    long origLandmarkRes1 = res1.getLandmark();
    long origLandmarkRes2 = res2.getLandmark();
    assertTrue("expected aggregator to have older landmark value than res1", aggr.getLandmark() < origLandmarkRes1);
    assertTrue("expected res1 to have older landmark value than res2", origLandmarkRes1 < origLandmarkRes2);

    // generate some data for both low-level reservoirs, then make sure their landmarks don't change
    for (int i = 0; i < 10; ++i)
    {
      long nowSecs = System.currentTimeMillis() / NUM_MSECS_IN_SEC;
      long timestamp1 = nowSecs - i - 3L;
      long timestamp2 = nowSecs - i - 5L;

      res1.update((double)i, timestamp1);
      res2.update((double)(i+100), timestamp2);
    }
    assertEquals("expected res1 landmark value to be unchanged", origLandmarkRes1, res1.getLandmark());
    assertEquals("expected res2 landmark value to be unchanged", origLandmarkRes2, res2.getLandmark());

    aggr.merge(res1);
    assertEquals("expected res1 landmark value to be unchanged", origLandmarkRes1, res1.getLandmark());
    assertEquals("expected aggregator landmark value to match res1", origLandmarkRes1, aggr.getLandmark());

    aggr.merge(res2);
    assertEquals("expected res2 landmark value to be unchanged", origLandmarkRes2, res2.getLandmark());
    assertEquals("expected aggregator landmark value to match res2", origLandmarkRes2, aggr.getLandmark());

    // generate some more data for both low-level reservoirs; their landmarks still should not have changed
    for (int i = 0; i < 10; ++i)
    {
      long nowSecs = System.currentTimeMillis() / NUM_MSECS_IN_SEC;
      long timestamp1 = nowSecs - i - 1L;
      long timestamp2 = nowSecs - i - 2L;

      res1.update((double)(i+200), timestamp1);
      res2.update((double)(i+300), timestamp2);
    }
    assertEquals("expected res1 landmark value to be unchanged", origLandmarkRes1, res1.getLandmark());
    assertEquals("expected res2 landmark value to be unchanged", origLandmarkRes2, res2.getLandmark());

    aggr.merge(res1);
    assertEquals("expected aggregator landmark value to be unchanged", origLandmarkRes2, aggr.getLandmark());
    assertEquals("expected res1 landmark value to match res2", origLandmarkRes2, res1.getLandmark());
  }

  /**
   * Using an artificial clock, pass in new data values after "half an hour," and verify that
   * they replace some of the older values.
   */
  // Both metrics-core and Apache Commons Math use the "R-6" quantile-estimation method, as described
  // at http://en.wikipedia.org/wiki/Quantile .
  //
  // N = 10
  // p = 0.5, 0.9, 0.95, 0.99
  // h = 5.5, 9.9, 10.45, 10.89
  // (assume x[n] for n >= dataValues.length equals x[dataValues.length - 1] == max value)
  //
  // Q[50th]  =  x[5-1] + (5.5 - 5)*(x[5-1+1] - x[5-1])        =  5.0 + 0.5*(6.0 - 5.0)      =  5.5
  // Q[90th]  =  x[9-1] + (9.9 - 9)*(x[9-1+1] - x[9-1])        =  9.0 + 0.9*(10.0 - 9.0)     =  9.9
  // Q[95th]  =  x[10-1] + (10.45 - 10)*(x[10-1+1] - x[10-1])  =  10.0 + 0.45*(10.0 - 10.0)  =  10.0
  // Q[99th]  =  x[10-1] + (10.89 - 10)*(x[10-1+1] - x[10-1])  =  10.0 + 0.89*(10.0 - 10.0)  =  10.0
  @Test
  public void testReservoirReplacement()
  {
    ManuallyControllableClock clock = new ManuallyControllableClock();
    MergeableExponentiallyDecayingReservoir res = new MergeableExponentiallyDecayingReservoir(10, 0.015, clock);

    clock.advanceTime(1L * NUM_MSECS_IN_SEC * NUM_NSECS_IN_MSEC);  // initial data show up 1 sec after reservoir created
    res.update(3.0);
    res.update(8.0);
    res.update(9.0);
    res.update(4.0);
    res.update(7.0);
    res.update(5.0);
    res.update(2.0);
    res.update(10.0);
    res.update(6.0);
    res.update(1.0);

    double[] dataValues = res.getUnsortedValues();
    assertEquals("expected non-empty dataValues array", 10, dataValues.length);
    double result = StatUtils.percentile(dataValues, 50.0);
    assertEquals("unexpected 50th percentile", 5.5, result);
    result = StatUtils.percentile(dataValues, 90.0);
    assertEquals("unexpected 90th percentile", 9.9, result);
    result = StatUtils.percentile(dataValues, 95.0);
    assertEquals("unexpected 95th percentile", 10.0, result);
    result = StatUtils.percentile(dataValues, 99.0);
    assertEquals("unexpected 99th percentile", 10.0, result);
    result = StatUtils.max(dataValues);
    assertEquals("unexpected max", 10.0, result);
    result = StatUtils.min(dataValues);
    assertEquals("unexpected min", 1.0, result);

    // Now advance the time and add a couple more values.  We don't control the random-number generation,
    // so we don't know the priorities of either the original 10 data points or the two new ones, but we
    // do expect the new ones to have higher priorities than most or all of the original set, thanks to
    // their "newness" (by half an hour) and the alpha value that exponentially weights data from the most
    // recent 5 minutes.  Since they're bigger/smaller than all the rest of the data values, the new max/min
    // values should reflect them regardless of which older data points they preempted.

    clock.advanceTime(1800L * NUM_MSECS_IN_SEC * NUM_NSECS_IN_MSEC);  // new data show up 30 min after initial set
    res.update(20.0);
    res.update(0.0);

    dataValues = res.getUnsortedValues();
    assertEquals("expected size for dataValues array", 10, dataValues.length);
    result = StatUtils.max(dataValues);
    assertEquals("unexpected max", 20.0, result);
    result = StatUtils.min(dataValues);
    assertEquals("unexpected min", 0.0, result);
  }

  @Test
  public void testReservoirWithIdenticalValues()
  {
    MergeableExponentiallyDecayingReservoir res = new MergeableExponentiallyDecayingReservoir(10, 0.015);

    res.update(7.0);
    res.update(7.0);
    res.update(7.0);
    res.update(7.0);
    res.update(7.0);
    res.update(7.0);
    res.update(7.0);
    res.update(7.0);
    res.update(7.0);
    res.update(7.0);

    double[] dataValues = res.getUnsortedValues();
    assertEquals("expected non-empty dataValues array", 10, dataValues.length);
    double result = StatUtils.percentile(dataValues, 50.0);
    assertEquals("expected 50th percentile to equal (constant) value of data points", 7.0, result);
    result = StatUtils.percentile(dataValues, 90.0);
    assertEquals("expected 90th percentile to equal (constant) value of data points", 7.0, result);
    result = StatUtils.percentile(dataValues, 95.0);
    assertEquals("expected 95th percentile to equal (constant) value of data points", 7.0, result);
    result = StatUtils.percentile(dataValues, 99.0);
    assertEquals("expected 99th percentile to equal (constant) value of data points", 7.0, result);
    result = StatUtils.max(dataValues);
    assertEquals("unexpected max for set of constant data points", 7.0, result);
  }

  @Test
  public void testReservoirWithSingleDatum()
  {
    MergeableExponentiallyDecayingReservoir res = new MergeableExponentiallyDecayingReservoir(10, 0.015);

    res.update(3.0);

    double[] dataValues = res.getUnsortedValues();
    assertEquals("expected non-empty dataValues array", 1, dataValues.length);
    double result = StatUtils.percentile(dataValues, 50.0);
    assertEquals("expected 50th percentile to equal value of single data point", 3.0, result);
    result = StatUtils.percentile(dataValues, 90.0);
    assertEquals("expected 90th percentile to equal value of single data point", 3.0, result);
    result = StatUtils.percentile(dataValues, 95.0);
    assertEquals("expected 95th percentile to equal value of single data point", 3.0, result);
    result = StatUtils.percentile(dataValues, 99.0);
    assertEquals("expected 99th percentile to equal value of single data point", 3.0, result);
    result = StatUtils.max(dataValues);
    assertEquals("expected max to equal value of single data point", 3.0, result);
  }

  public static class ManuallyControllableClock extends Clock
  {
    // 20130106 13:22:22 PST, but could be anything...
    private static long currentTimeNs = 1389043342L * NUM_MSECS_IN_SEC * NUM_NSECS_IN_MSEC;

    @Override
    public long getTick()
    {
      return currentTimeNs;
    }

    @Override
    public long getTime()
    {
      return currentTimeNs / NUM_NSECS_IN_MSEC;
    }

    public void advanceTime(long timeIncrementNs)
    {
      currentTimeNs += timeIncrementNs;
    }
  }

}
