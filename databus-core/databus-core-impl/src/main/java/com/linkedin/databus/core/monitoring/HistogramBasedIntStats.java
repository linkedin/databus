package com.linkedin.databus.core.monitoring;

import java.util.Arrays;

import org.apache.log4j.Logger;

import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;

public class HistogramBasedIntStats
{
  public static final String MODULE = HistogramBasedIntStats.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private final int[] _data;
  private final int[] _histogram;
  private final int[] _bucketValues;
  private final StaticConfig _staticConfig;
  private final int _bucketWidth;
  private final int _bucketRealMax;
  private int _dataStart;
  private int _dataSize;

  public static class Stats
  {
    private int _num;
    private int _max;
    private int _min;
    private int _sum;
    private int _median;
    private int _perc75;
    private int _perc90;
    private int _perc95;
    private int _perc99;
    private double _mean;
    private double _stdDev;

    public Stats()
    {
      clear();
    }

    public void clear()
    {
      _max = Integer.MIN_VALUE;
      _min = Integer.MAX_VALUE;
      _mean = Integer.MIN_VALUE;
      _median = Integer.MIN_VALUE;
      _perc75 = Integer.MIN_VALUE;
      _perc90 = Integer.MIN_VALUE;
      _perc95 = Integer.MIN_VALUE;
      _perc99 = Integer.MIN_VALUE;
      _sum = 0;
      _stdDev = 0;
      _num = 0;
    }

    public int getNum()
    {
      return _num;
    }

    public int getMax()
    {
      return _max;
    }

    public int getMin()
    {
      return _min;
    }

    public int getSum()
    {
      return _sum;
    }

    public int getMedian()
    {
      return _median;
    }

    public int getPerc75()
    {
      return _perc75;
    }

    public int getPerc90()
    {
      return _perc90;
    }

    public int getPerc95()
    {
      return _perc95;
    }

    public int getPerc99()
    {
      return _perc99;
    }

    public double getMean()
    {
      return _mean;
    }

    public double getStdDev()
    {
      return _stdDev;
    }


  }

  public HistogramBasedIntStats(StaticConfig staticConfig)
  {
    _staticConfig = staticConfig;
    int bwidth = (staticConfig.getBucketsMax() - staticConfig.getBucketsMin()) / staticConfig.getBucketsNum();
    _bucketWidth = (0 >= bwidth) ? 1 : bwidth;
    _bucketRealMax = staticConfig.getBucketsMin() + staticConfig.getBucketsNum() * _bucketWidth;
    _data = new int[staticConfig.getCapacity()];
    _histogram = new int[staticConfig.getBucketsNum() + 2];
    _bucketValues = new int[staticConfig.getBucketsNum() + 2];
    _dataStart = -1;
    _dataSize = 0;

    //initialize bucket values
    for (int i = 0; i <= _staticConfig.getBucketsNum() + 1; ++i)
    {
      _bucketValues[i] = _staticConfig.getBucketsMin() + _bucketWidth * (i - 1) + _bucketWidth / 2;
    }
  }

  public void add(int v)
  {
    int capacity = _staticConfig.getCapacity();
    if (-1 == _dataStart) _dataStart = 0;
    else if (_dataSize == capacity)
    {
      for (int i = 0; i < _staticConfig.getDropNum(); ++i)
      {
        //drop the oldest data point
        -- _histogram[_data[_dataStart++]];
        _dataStart %= capacity;
      }
      _dataSize -= _staticConfig.getDropNum();
    }

    //update histogram
    int vBucket = getBucket(v);
    ++ _histogram[vBucket];
    _data[(_dataStart + _dataSize++) % capacity] = vBucket;

    //update statistics

  }

  public void clear()
  {
    _dataSize = 0;
  }

  public int size()
  {
    return _dataSize;
  }

  public void calcStats(Stats stats)
  {
    stats.clear();
    stats._num = _dataSize;

    int totalCount = 0;
    long sumOfSq = 0;
    for (int i = 0; i < _staticConfig.getBucketsNum() + 2; ++i)
    {
      int count = _histogram[i];
      totalCount += count;
      int curValue = _bucketValues[i];
      stats._sum += curValue * count;
      sumOfSq += curValue * curValue * count;

      if (count > 0)
      {
        if (curValue < stats._min) stats._min = curValue;
        if (curValue > stats._max) stats._max = curValue;
        if (Integer.MIN_VALUE == stats._median && totalCount >= _dataSize / 2) stats._median = curValue;
        if (Integer.MIN_VALUE == stats._perc75 && totalCount >= _dataSize * 0.75) stats._perc75 = curValue;
        if (Integer.MIN_VALUE == stats._perc90 && totalCount >= _dataSize * 0.90) stats._perc90 = curValue;
        if (Integer.MIN_VALUE == stats._perc95 && totalCount >= _dataSize * 0.95) stats._perc95 = curValue;
        if (Integer.MIN_VALUE == stats._perc99 && totalCount >= _dataSize * 0.99) stats._perc99 = curValue;
      }
    }
    stats._mean = 0 != stats._num ? 1.0 * stats._sum / stats._num : 0.0;
    stats._stdDev = 1 >= _dataSize ? 0.0 : Math.sqrt((sumOfSq - 2.0 * stats._mean * stats._sum +
        _dataSize * stats._mean * stats._mean) / (_dataSize - 1.0));
  }

  public int[] getHistogram()
  {
    return Arrays.copyOf(_histogram, _histogram.length);
  }

  public int[] getBucketValues()
  {
    return Arrays.copyOf(_bucketValues, _bucketValues.length);
  }

  private int getBucket(int v)
  {
    int bmin = _staticConfig.getBucketsMin();
    if (v < bmin) return 0;
    else if (v >= _bucketRealMax) return _staticConfig.getBucketsNum() + 1;
    else return 1 + (v - bmin) / _bucketWidth;
  }

  public void mergeDataFrom(HistogramBasedIntStats source, int dataPointsNum)
  {
    int[] otherData = source.getData();
    int realNum = Math.min(dataPointsNum, otherData.length);
    for (int i = 0; i < realNum; ++i) add(otherData[i]);
  }

  public void copyDataTo(HistogramBasedIntStats source)
  {
    if (_staticConfig.getBucketsMin() != source.getStaticConfig().getBucketsMin() ||
        _staticConfig.getBucketsMax() != source.getStaticConfig().getBucketsMax() ||
        _staticConfig.getBucketsNum() != source.getStaticConfig().getBucketsNum() ||
        _staticConfig.getCapacity() != source.getStaticConfig().getCapacity())
    {
      LOG.error("trying to merge a histogram with different configuration; ignoring");
      return;
    }

    //we do this weird copyFrom/copyTo implementation to make sure that each of the objects
    //can synchronize themselves if necessary
    source.copyDataAndHistogramTo(this);
  }

  public StaticConfig getStaticConfig()
  {
    return _staticConfig;
  }

  protected int[] getData()
  {
    int[] dataCopy = new int[_dataSize];

    return dataCopy;
  }

  protected void copyDataAndHistogramTo(HistogramBasedIntStats target)
  {
    System.arraycopy(_data, 0, target._data, 0, _data.length);
    System.arraycopy(_histogram, 0, target._histogram, 0, _histogram.length);
    target._dataSize = _dataSize;
    target._dataStart = _dataStart;
  }

  public static class StaticConfig
  {
    private final int _capacity;
    private final int _bucketsMin;
    private final int _bucketsMax;
    private final int _bucketsNum;
    private final int _dropNum;

    public StaticConfig(int capacity, int bucketsMin, int bucketsMax, int bucketsNum, int dropNum)
    {
      super();
      _capacity = capacity;
      _bucketsMin = bucketsMin;
      _bucketsMax = bucketsMax;
      _bucketsNum = bucketsNum;
      _dropNum = dropNum;
    }

    public int getCapacity()
    {
      return _capacity;
    }

    public int getBucketsMin()
    {
      return _bucketsMin;
    }

    public int getBucketsMax()
    {
      return _bucketsMax;
    }

    public int getBucketsNum()
    {
      return _bucketsNum;
    }

    public int getDropNum()
    {
      return _dropNum;
    }
  }

  public static class StaticConfigBuilder implements ConfigBuilder<StaticConfig>
  {
    private int _capacity   = 60000;
    private int _bucketsMin = 0;
    private int _bucketsMax = 100;
    private int _bucketsNum = 20;
    private int _dropNum    = -1;

    public StaticConfigBuilder()
    {
    }

    public int getCapacity()
    {
      return _capacity;
    }

    public void setCapacity(int capacity)
    {
      _capacity = capacity;
    }

    public int getBucketsMin()
    {
      return _bucketsMin;
    }

    public void setBucketsMin(int bucketsMin)
    {
      _bucketsMin = bucketsMin;
    }

    public int getBucketsMax()
    {
      return _bucketsMax;
    }

    public void setBucketsMax(int bucketsMax)
    {
      _bucketsMax = bucketsMax;
    }

    public int getBucketsNum()
    {
      return _bucketsNum;
    }

    public void setBucketsNum(int bucketsNum)
    {
      _bucketsNum = bucketsNum;
    }

    public int getDropNum()
    {
      return _dropNum;
    }

    public void setDropNum(int dropNum)
    {
      _dropNum = dropNum;
    }

    @Override
    public StaticConfig build() throws InvalidConfigException
    {
      if (0 >= _dropNum) _dropNum = Math.max((int)(_capacity * 0.01), 1);
      return new StaticConfig(_capacity, _bucketsMin, _bucketsMax, _bucketsNum, _dropNum);
    }

  }

}
