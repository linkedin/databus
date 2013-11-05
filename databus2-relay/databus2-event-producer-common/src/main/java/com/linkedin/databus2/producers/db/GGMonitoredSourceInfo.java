package com.linkedin.databus2.producers.db;

import com.linkedin.databus.monitoring.mbean.EventSourceStatistics;

public class GGMonitoredSourceInfo implements EventSourceStatisticsIface
{
  final private String _sourceName;
  final private short _sourceId;
  final private EventSourceStatistics _statisticsBean;

  public GGMonitoredSourceInfo(short sourceId, String sourceName, EventSourceStatistics statisticsBean) {
    _sourceName = sourceName;
    _sourceId = sourceId;
    _statisticsBean = statisticsBean;
  }

  @Override
  public EventSourceStatistics getStatisticsBean()
  {
    return _statisticsBean;
  }

  @Override
  public String getSourceName() {
    return _sourceName;
  }

  @Override
  public short getSourceId() {
    return _sourceId;
  }
}
