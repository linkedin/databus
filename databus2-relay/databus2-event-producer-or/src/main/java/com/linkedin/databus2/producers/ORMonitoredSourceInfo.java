package com.linkedin.databus2.producers;

import com.linkedin.databus.monitoring.mbean.EventSourceStatistics;
import com.linkedin.databus2.producers.db.EventSourceStatisticsIface;

public class ORMonitoredSourceInfo implements EventSourceStatisticsIface
{
  private final String _sourceName;
  private final short _sourceId;
  private final EventSourceStatistics _statisticsBean;

  public ORMonitoredSourceInfo(short sourceId, String sourceName, EventSourceStatistics statisticsBean) {
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
