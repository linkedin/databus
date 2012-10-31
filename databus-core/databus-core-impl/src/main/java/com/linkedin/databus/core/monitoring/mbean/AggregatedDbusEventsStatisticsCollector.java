package com.linkedin.databus.core.monitoring.mbean;


import com.linkedin.databus.core.monitoring.events.DbusEventsTotalStatsEvent;
import javax.management.MBeanServer;


/**
 * This class exists only so that we can make a different kind of DbusEventsTotalStats
 * (AggregatedDbusEventsTotalStats) that knows how to merge statistics from DbusEventsTotalStats
 * into itself.
 */
public class AggregatedDbusEventsStatisticsCollector extends DbusEventsStatisticsCollector
{
  public AggregatedDbusEventsStatisticsCollector(int relayId,
                                                String name,
                                                boolean enabled,
                                                boolean threadSafe,
                                                MBeanServer mbeanServer)
  {
    super(relayId, name, enabled, threadSafe, mbeanServer);
  }

  @Override
  protected DbusEventsTotalStats makeDbusEventsTotalStats(int ownerId,
                                                          String dimension,
                                                          boolean enabled,
                                                          boolean threadSafe,
                                                          DbusEventsTotalStatsEvent initData)
  {
    return new AggregatedDbusEventsTotalStats(ownerId,
                                              dimension,
                                              enabled,
                                              threadSafe,
                                              initData);
  }
}
