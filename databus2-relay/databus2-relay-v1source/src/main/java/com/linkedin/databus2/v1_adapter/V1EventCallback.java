package com.linkedin.databus2.v1_adapter;

import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.Event;
import com.linkedin.databus.core.EventCallback;
import com.linkedin.databus.core.EventsSummary;
import com.linkedin.databus.core.Source;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;

public class V1EventCallback implements EventCallback<Object, Object>
{
  private final DbusEventBuffer _eventBuffer;
  private final DbusEventsStatisticsCollector _eventStats;

  public V1EventCallback(DbusEventBuffer eventBuffer, DbusEventsStatisticsCollector eventStats)
  {
    _eventBuffer = eventBuffer;
    _eventStats = eventStats;
  }

  @Override
  public void startEvents(long beginningOfPeriod)
  {
    // Nothing to do
  }

  @Override
  public void startEvents(Source source)
  {
    // Nothing to do
  }

  @Override
  public void onEvent(Event<Object, Object> event)
  {
    // Nothing to do
  }

  @Override
  public void endEvents(Source source)
  {
    // Nothing to do
  }

  @Override
  public void endEvents(EventsSummary summary)
  {
    _eventBuffer.endEvents(summary.getEndOfPeriod(), _eventStats);
  }

}
