package com.linkedin.databus2.core.monitoring;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.core.monitoring.HistogramBasedIntStats;

public class TestHistogramBaseIntStats {
  @BeforeClass
  public void beforeClass() {
  }

  @AfterClass
  public void afterClass() {
  }

  @Test
  public void testStatsSimple() throws Exception
  {
    HistogramBasedIntStats.StaticConfigBuilder configBuilder = new HistogramBasedIntStats.StaticConfigBuilder();
    configBuilder.setCapacity(20);
    configBuilder.setBucketsNum(11);
    configBuilder.setBucketsMin(0);
    configBuilder.setBucketsMax(11);
    configBuilder.setDropNum(1);

    HistogramBasedIntStats.StaticConfig config = configBuilder.build();

    HistogramBasedIntStats stats = new HistogramBasedIntStats(config);
    HistogramBasedIntStats.Stats s = new HistogramBasedIntStats.Stats();

    //just add some data points less than the capacity and very statistics
    for (int i = 1; i <= 10; ++i)
    {
      stats.add(1);
      Assert.assertEquals(stats.size(), i, "correct size");
    }

    stats.calcStats(s);
    Assert.assertEquals(s.getMax(), 1, "correct max");
    Assert.assertEquals(s.getMin(), 1, "correct min");
    Assert.assertEquals(s.getNum(), 10, "correct num");
    Assert.assertEquals(s.getSum(), 10, "correct sum");
    Assert.assertEquals(s.getMean(), 1.0, "correct mean");
    Assert.assertEquals(s.getMedian(), 1, "correct median");
    Assert.assertEquals(s.getStdDev(), 0.0, "correct stdDev");

    //add more data points so that we push the previous ones out completely
    for (int i = 1; i <= 20; ++i)
    {
      stats.add(5);
      Assert.assertEquals(stats.size(), i <= 10 ? 10 + i : 20, "correct size" );
    }

    stats.calcStats(s);
    Assert.assertEquals(s.getMax(), 5, "correct max");
    Assert.assertEquals(s.getMin(), 5, "correct min");
    Assert.assertEquals(s.getNum(), 20, "correct num");
    Assert.assertEquals(s.getSum(), 100, "correct sum");
    Assert.assertEquals(s.getMean(), 5.0, "correct mean");
    Assert.assertEquals(s.getMedian(), 5, "correct median");
    Assert.assertEquals(s.getStdDev(), 0.0, "correct stdDev");

    //add a few more data points to test a mixture of different values
    for (int i = 1; i <= 8; ++i)
    {
      stats.add(3);
      Assert.assertEquals(stats.size(), 20, "correct size");
    }
    stats.calcStats(s);
    Assert.assertEquals(s.getMax(), 5, "correct max");
    Assert.assertEquals(s.getMin(), 3, "correct min");
    Assert.assertEquals(s.getNum(), 20, "correct num");
    Assert.assertEquals(s.getSum(), 84, "correct sum");
    Assert.assertEquals(s.getMean(), 84.0/20, "correct mean");
    Assert.assertEquals(s.getMedian(), 5, "correct median");
    Assert.assertEquals(s.getPerc75(), 5, "correct 75 percentile");
    Assert.assertEquals(s.getPerc99(), 5, "correct 99 percentile");
    Assert.assertTrue(s.getStdDev() > 0.0, "correct stdDev");

    //add a few more data points to test a mixture of different values
    for (int i = 1; i <= 4; ++i)
    {
      stats.add(3);
      Assert.assertEquals(stats.size(), 20, "correct size");
    }
    stats.calcStats(s);
    Assert.assertEquals(s.getMax(), 5, "correct max");
    Assert.assertEquals(s.getMin(), 3, "correct min");
    Assert.assertEquals(s.getNum(), 20, "correct num");
    Assert.assertEquals(s.getSum(), 76, "correct sum");
    Assert.assertEquals(s.getMean(), 76.0/20, "correct mean");
    Assert.assertEquals(s.getMedian(), 3, "correct median");
    Assert.assertEquals(s.getPerc75(), 5, "correct 75 percentile");
    Assert.assertEquals(s.getPerc95(), 5, "correct 95 percentile");
    Assert.assertTrue(s.getStdDev() > 0.0, "correct stdDev");

    //add a few more data points to test a mixture of different values
    for (int i = 1; i <= 4; ++i)
    {
      stats.add(3);
      Assert.assertEquals(stats.size(), 20, "correct size");
    }
    stats.calcStats(s);
    Assert.assertEquals(s.getMax(), 5, "correct max");
    Assert.assertEquals(s.getMin(), 3, "correct min");
    Assert.assertEquals(s.getNum(), 20, "correct num");
    Assert.assertEquals(s.getSum(), 68, "correct sum");
    Assert.assertEquals(s.getMean(), 68.0/20, "correct mean");
    Assert.assertEquals(s.getMedian(), 3, "correct median");
    Assert.assertEquals(s.getPerc75(), 3, "correct 75 percentile");
    Assert.assertEquals(s.getPerc90(), 5, "correct 90 percentile");
    Assert.assertTrue(s.getStdDev() > 0.0, "correct stdDev");

    //add a few more data points to test a mixture of different values
    for (int i = 1; i <= 4; ++i)
    {
      stats.add(1);
      Assert.assertEquals(stats.size(), 20, "correct size");
    }
    stats.calcStats(s);
    Assert.assertEquals(s.getMax(), 3, "correct max");
    Assert.assertEquals(s.getMin(), 1, "correct min");
    Assert.assertEquals(s.getNum(), 20, "correct num");
    Assert.assertEquals(s.getSum(), 4*1 + 16*3, "correct sum");
    Assert.assertEquals(s.getMean(), s.getSum()/20.0, "correct mean");
    Assert.assertEquals(s.getMedian(), 3, "correct median");
    Assert.assertEquals(s.getPerc75(), 3, "correct 75 percentile");
    Assert.assertEquals(s.getPerc90(), 3, "correct 90 percentile");
    Assert.assertEquals(s.getPerc95(), 3, "correct 95 percentile");
    Assert.assertEquals(s.getPerc99(), 3, "correct 99 percentile");
    Assert.assertTrue(s.getStdDev() > 0.0, "correct stdDev");

  }


  @Test
  public void testBigDrop() throws Exception
  {
    HistogramBasedIntStats.StaticConfigBuilder configBuilder = new HistogramBasedIntStats.StaticConfigBuilder();
    configBuilder.setCapacity(200);
    configBuilder.setBucketsNum(11);
    configBuilder.setBucketsMin(0);
    configBuilder.setBucketsMax(11);
    configBuilder.setDropNum(10);

    HistogramBasedIntStats.StaticConfig config = configBuilder.build();
    HistogramBasedIntStats stats = new HistogramBasedIntStats(config);

    for (int i = 1; i <= 500; ++i)
    {
      stats.add(i);
      int expectedSize = (i <= 200) ? i : 191 + (i - 1) % 10;
      Assert.assertEquals(stats.size(), expectedSize, "correct # of data points for i=" + i);
    }
  }

  @Test
  public void testPercentilesWidth1() throws Exception
  {
    HistogramBasedIntStats.StaticConfigBuilder configBuilder = new HistogramBasedIntStats.StaticConfigBuilder();
    configBuilder.setCapacity(200);
    configBuilder.setBucketsNum(50);
    configBuilder.setBucketsMin(1);
    configBuilder.setBucketsMax(51);
    configBuilder.setDropNum(1);

    HistogramBasedIntStats.StaticConfig config = configBuilder.build();
    HistogramBasedIntStats stats = new HistogramBasedIntStats(config);
    HistogramBasedIntStats.Stats s = new HistogramBasedIntStats.Stats();

    for (int j = 1; j <= 6; ++j)
    {
      for (int i = 1; i <= 50; ++i)
      {
        stats.add(i);
      }
    }

    int[] histo = stats.getHistogram();
    Assert.assertEquals(histo.length, 52, "correct histogram size");
    Assert.assertEquals(histo[0], 0, "correct size for bucket 0");
    Assert.assertEquals(histo[51], 0, "correct size for bucket 51");
    for (int i = 1; i <= 50; ++i) Assert.assertEquals(histo[i], 4, "correct size for bucket " + i);

    int[] bvalues = stats.getBucketValues();
    Assert.assertEquals(bvalues.length, 52, "correct bucket values size");
    Assert.assertEquals(bvalues[0], 0, "correct bucket value 0");
    Assert.assertEquals(bvalues[51], 51, "correct bucket value 51");
    for (int i = 1; i <= 50; ++i) Assert.assertEquals(bvalues[i], i, "correct bucket value " + i);

    stats.calcStats(s);

    Assert.assertEquals(s.getSum(), 5100, "correct sum");
    Assert.assertEquals(s.getNum(), 200, "correct num");
    Assert.assertEquals(s.getMean(), 25.5, "correct mean");
    Assert.assertEquals(s.getMin(), 1, "correct min");
    Assert.assertEquals(s.getMax(), 50, "correct max");
    Assert.assertEquals(s.getMedian(), 25, "correct median");
    Assert.assertEquals(s.getPerc75(), 38, "correct 75th percentile");
    Assert.assertEquals(s.getPerc90(), 45, "correct 90th percentile");
    Assert.assertEquals(s.getPerc95(), 48, "correct 95th percentile");
    Assert.assertEquals(s.getPerc99(), 50, "correct 99th percentile");
  }

  @Test
  public void testPercentilesWidth10() throws Exception
  {
    HistogramBasedIntStats.StaticConfigBuilder configBuilder = new HistogramBasedIntStats.StaticConfigBuilder();
    configBuilder.setCapacity(200);
    configBuilder.setBucketsNum(5);
    configBuilder.setBucketsMin(1);
    configBuilder.setBucketsMax(51);
    configBuilder.setDropNum(1);

    HistogramBasedIntStats.StaticConfig config = configBuilder.build();
    HistogramBasedIntStats stats = new HistogramBasedIntStats(config);
    HistogramBasedIntStats.Stats s = new HistogramBasedIntStats.Stats();

    for (int j = 1; j <= 6; ++j)
    {
      for (int i = 1; i <= 50; ++i)
      {
        stats.add(i);
      }
    }

    int[] histo = stats.getHistogram();
    Assert.assertEquals(histo.length, 7, "correct histogram size");
    Assert.assertEquals(histo[0], 0, "correct size for bucket 0");
    Assert.assertEquals(histo[6], 0, "correct size for bucket 51");
    for (int i = 1; i <= 5; ++i) Assert.assertEquals(histo[i], 40, "correct size for bucket " + i);

    int[] bvalues = stats.getBucketValues();
    Assert.assertEquals(bvalues.length, 7, "correct bucket values size");
    Assert.assertEquals(bvalues[0], -4, "correct bucket value 0");
    Assert.assertEquals(bvalues[6], 56, "correct bucket value 51");
    for (int i = 1; i <= 5; ++i) Assert.assertEquals(bvalues[i], (i - 1) * 10 + 6, "correct bucket value " + i);

    stats.calcStats(s);

    Assert.assertEquals(s.getSum(), 40 * (6 + 16 + 26 + 36 + 46), "correct sum");
    Assert.assertEquals(s.getNum(), 200, "correct num");
    Assert.assertEquals(s.getMean(), 1.0 * s.getSum() / s.getNum(), "correct mean");
    Assert.assertEquals(s.getMin(), 6, "correct min");
    Assert.assertEquals(s.getMax(), 46, "correct max");
    Assert.assertEquals(s.getMedian(), 26, "correct median");
    Assert.assertEquals(s.getPerc75(), 36, "correct 75th percentile");
    Assert.assertEquals(s.getPerc90(), 46, "correct 90th percentile");
    Assert.assertEquals(s.getPerc95(), 46, "correct 95th percentile");
    Assert.assertEquals(s.getPerc99(), 46, "correct 99th percentile");
  }

}
