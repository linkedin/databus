package com.linkedin.databus2.core.monitoring;
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


import java.util.Formatter;

import com.linkedin.databus.core.monitoring.HistogramBasedIntStats;

public class HistogramBaseIntStatsPerf implements Runnable
{
  private static final int DATA_POINTS_NUM = 1400000;
  private static final String CSV_FORMAT = "%d,%d,%d,%10.2f,%10.2f";
  private static final String JSON_FORMAT = "{\"capacity\":%d,\"bucketsNum\":%d,\"dropNum\":%d,\"addTime\":%10.2f,\"calcTime\":%10.2f}";

  private final HistogramBasedIntStats.StaticConfig _config;
  private final HistogramBasedIntStats _stats;
  private long _procTime;
  private long _addTime;
  private long _calcTime;
  private long[][] _dummyData;

  public HistogramBaseIntStatsPerf(HistogramBasedIntStats.StaticConfig config)
  {
    _config = config;
    _stats = new HistogramBasedIntStats(_config);
    _dummyData = new long[1000][1000];
  }

  private void doStuff(int t)
  {
    for (int i = 0; i < 100; ++i) _dummyData[i % 1000][999 - i % 1000] = i * i + t;
  }

  public void run()
  {
    long procStart = System.nanoTime();
    for (int i = 0; i < 300000; ++i) doStuff(-i);
    long procEnd = System.nanoTime();
    _procTime = procEnd - procStart;
    //System.err.println(_procTime / 100.0);

    _dummyData = new long[1000][1000];

    HistogramBasedIntStats.Stats s = new HistogramBasedIntStats.Stats();
    long addStart = System.nanoTime();
    for (int i = 0; i < DATA_POINTS_NUM; ++i)
    {
      doStuff(i);
      _stats.add(i % 1000);
      doStuff(i);
      _stats.add(999 - i % 1000 + 1);
    }
    long addEnd = System.nanoTime();
    _addTime = addEnd - addStart;
    //System.err.println((1.0 * _addTime) / DATA_POINTS_NUM);

    doStuff(-2);

    long calcStart = System.nanoTime();
    _stats.calcStats(s);
    long calcEnd = System.nanoTime();
    _calcTime = calcEnd - calcStart;

    System.out.println(toString());
  }

  public String toCsvString()
  {
    return formatData(CSV_FORMAT);
  }

  public String toJsonString()
  {
    return formatData(JSON_FORMAT);
  }

  private String formatData(String formatStr)
  {
    Formatter fmt = new Formatter();
    fmt.format(formatStr,
               _config.getCapacity(), _config.getBucketsNum(), _config.getDropNum(),
               1.0 * _addTime / (2.0 * DATA_POINTS_NUM) - _procTime / 300000.0 ,
               1.0 * _calcTime);
    fmt.flush();
    return fmt.toString();
  }

  @Override
  public String toString()
  {
    return toJsonString();
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception
  {
    HistogramBasedIntStats.StaticConfigBuilder configBuilder = new HistogramBasedIntStats.StaticConfigBuilder();
    configBuilder.setBucketsMin(1);
    configBuilder.setBucketsMax(101);
    configBuilder.setCapacity(10000);
    configBuilder.setBucketsNum(20);
    configBuilder.setDropNum(1);

    //warm up

    HistogramBasedIntStats.StaticConfig config = configBuilder.build();
    HistogramBaseIntStatsPerf run = new HistogramBaseIntStatsPerf(config);
    run.run();


    for (int capacity = 10000; capacity <= 30000; capacity += 10000)
    {
      for (int bucketsNum = 20; bucketsNum <= 140; bucketsNum += 40)
      {
        for (int dropNum = 1; dropNum <= 16; dropNum += 5)
        {
          configBuilder.setCapacity(capacity);
          configBuilder.setBucketsNum(bucketsNum);
          configBuilder.setDropNum(dropNum);

          config = configBuilder.build();
          run = new HistogramBaseIntStatsPerf(config);
          run.run();
        }
      }

    }
  }

}
