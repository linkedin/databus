package com.linkedin.databus.core;

import com.linkedin.databus.core.DbusEventBuffer.StreamingMode;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus2.core.filter.AllowAllDbusFilter;
import com.linkedin.databus2.core.filter.DbusFilter;

/**
 * class for passing arguments to streamEvents calls
 */
public class StreamEventsArgs
{
  private boolean _streamFromLatestScn;
  private int _batchFetchSize;
  private Encoding _encoding;
  private StreamingMode _sMode;
  private DbusFilter _filter;
  private DbusEventsStatisticsCollector _statsCollector;
  private int _maxClientEventVersion;

  public StreamEventsArgs(int batchSize) {
    _batchFetchSize = batchSize;
    _encoding = Encoding.BINARY;
    _streamFromLatestScn = false;
    _sMode = StreamingMode.CONTINUOUS;
    _filter = new AllowAllDbusFilter();
    _statsCollector = null;
  }
  public boolean isStreamFromLatestScn()
  {
    return _streamFromLatestScn;
  }
  public StreamEventsArgs setStreamFromLatestScn(boolean _streamFromLatestScn)
  {
    this._streamFromLatestScn = _streamFromLatestScn;
    return this;
  }
  public int getMaxClientEventVersion()
  {
    return _maxClientEventVersion;
  }
  public StreamEventsArgs setMaxClientEventVersion(int ver)
  {
    this._maxClientEventVersion = ver;
    return this;
  }
  public int getBatchFetchSize()
  {
    return _batchFetchSize;
  }
  public StreamEventsArgs setBatchFetchSize(int _batchFetchSize)
  {
    this._batchFetchSize = _batchFetchSize;
    return this;
  }
  public Encoding getEncoding()
  {
    return _encoding;
  }
  public StreamEventsArgs setEncoding(Encoding _encoding)
  {
    this._encoding = _encoding;
    return this;
  }
  public StreamingMode getSMode()
  {
    return _sMode;
  }
  public StreamEventsArgs setSMode(StreamingMode _sMode)
  {
    this._sMode = _sMode;
    return this;
  }
  public StreamEventsArgs setFilter(DbusFilter filter)
  {
    this._filter = filter;
    return this;
  }

  public DbusFilter getFilter() {
    return _filter;
  }

  public StreamEventsArgs setStatsCollector(DbusEventsStatisticsCollector statsColl)
  {
    this._statsCollector = statsColl;
    return this;
  }

  public DbusEventsStatisticsCollector getDbusEventsStatisticsCollector() {
    return _statsCollector;
  }


}
