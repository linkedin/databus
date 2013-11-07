package com.linkedin.databus2.producers.db;

import com.linkedin.databus.monitoring.mbean.EventSourceStatistics;

public interface EventSourceStatisticsIface
{
  public EventSourceStatistics getStatisticsBean();
  public String getSourceName();
  public short getSourceId();
}
