package com.linkedin.databus.core.monitoring;
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





public class SynchornizedHistogramBasedIntStats extends HistogramBasedIntStats
{
  public SynchornizedHistogramBasedIntStats(StaticConfig staticConfig)
  {
    super(staticConfig);
  }

  @Override
  public synchronized void add(int v)
  {
    super.add(v);
  }

  @Override
  public synchronized void calcStats(Stats stats)
  {
    super.calcStats(stats);
  }

  @Override
  public synchronized void clear()
  {
    super.clear();
  }

  @Override
  public synchronized int[] getHistogram()
  {
    return super.getHistogram();
  }

  @Override
  public synchronized int size()
  {
    return super.size();
  }

  @Override
  protected synchronized int[] getData()
  {
    return super.getData();
  }

  @Override
  public synchronized void copyDataTo(HistogramBasedIntStats source)
  {
    super.copyDataTo(source);
  }

  @Override
  protected synchronized void copyDataAndHistogramTo(HistogramBasedIntStats target)
  {
    super.copyDataAndHistogramTo(target);
  }

  @Override
  public synchronized void mergeDataFrom(HistogramBasedIntStats other, int dataPointsNum)
  {
    super.mergeDataFrom(other, dataPointsNum);
  }

}
