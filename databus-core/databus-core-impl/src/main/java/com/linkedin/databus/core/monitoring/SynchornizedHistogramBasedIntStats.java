package com.linkedin.databus.core.monitoring;




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
