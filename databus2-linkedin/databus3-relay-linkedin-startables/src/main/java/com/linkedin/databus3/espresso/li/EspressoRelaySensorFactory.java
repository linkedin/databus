package com.linkedin.databus3.espresso.li;

import com.linkedin.databus.core.monitoring.StatsCollectorCallback;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsTotalStats;
import com.linkedin.databus.core.monitoring.mbean.StatsCollectors;
import com.linkedin.databus2.core.container.monitoring.ServerContainerSensorFactory;
import com.linkedin.databus2.core.container.monitoring.mbean.ContainerStatsMBean;
import com.linkedin.databus2.core.container.monitoring.mbean.ContainerTrafficTotalStatsMBean;
import com.linkedin.databus2.core.container.monitoring.mbean.DbusHttpTotalStats;
import com.linkedin.databus3.rpl_dbus_manager.RplDbusManager;
import com.linkedin.databus3.rpl_dbus_manager.monitoring.mbean.RplDbusTotalStats;
import com.linkedin.healthcheck.pub.spring.SensorRegistry;
import com.linkedin.databus.internal.monitoring.mbean.DbusGenericSensor;

public class EspressoRelaySensorFactory extends ServerContainerSensorFactory
{
  public EspressoRelaySensorFactory(SensorRegistry sensorRegistry, EspressoRelayStartable relay)
  {
    super(sensorRegistry, "EspressoRelay");

    RplDbusManager.LOG.info("Starting SENSOR factory for: httpTotalStats, in/outBoundEvents, connectionStats, containerStats, rplDbusStatsCollectors");

    DbusHttpTotalStats httpTotalStats = relay.getRelay().getHttpStatisticsCollector().getTotalStats();
    DbusEventsTotalStats inboundEvents = relay.getRelay().getInboundEventStatisticsCollector().getTotalStats();
    DbusEventsTotalStats outboundEvents = relay.getRelay().getOutboundEventStatisticsCollector().getTotalStats();
    ContainerTrafficTotalStatsMBean  connectionStats = relay.getRelay().getContainerStatsCollector().getOutboundTrafficTotalStats();
    ContainerStatsMBean containerStats = relay.getRelay().getContainerStatsCollector().getContainerStats();
    // Add every physical source eventBuffer stats to Autometrics

    //StatsCollectors<DbusEventsStatisticsCollector> inBoundStatsCollectors = relay.getRelay().getInBoundStatsCollectors();
    //StatsCollectors<DbusEventsStatisticsCollector> outBoundStatsCollectors = relay.getRelay().getOutBoundStatsCollectors();
    StatsCollectors<RplDbusTotalStats> rplDbusStatsCollectors = relay.getRelay().getRplDbusStatsCollectors();

    //inBoundStatsCollectors.setStatsCollectorCallback(getSensorCallback());
    //outBoundStatsCollectors.setStatsCollectorCallback(getSensorCallback());

    RplDbusSensorStatsCallback rplDbusStatsCallback = new RplDbusSensorStatsCallback(sensorRegistry);
    rplDbusStatsCollectors.setStatsCollectorCallback(rplDbusStatsCallback);

    /** Create sensors **/
    if (httpTotalStats != null)
    {
      DbusGenericSensor<DbusHttpTotalStats> httpSensor =
          new DbusGenericSensor<DbusHttpTotalStats>("EspressoRelayHttpMetrics",
                                                    httpTotalStats.getDimension(),
                                                    httpTotalStats);
      sensorRegistry.registerSensor("EspressoRelay Sensor Metrics",
                                    httpSensor);
    }
    if (inboundEvents != null && outboundEvents != null)
    {
      DbusGenericSensor<DbusEventsTotalStats> inEventSensor =
          new DbusGenericSensor<DbusEventsTotalStats>("EspressoRelayInboundEventBuffer",
                                                      inboundEvents.getDimension(),
                                                      inboundEvents);
      DbusGenericSensor<DbusEventsTotalStats> outEventSensor =
          new DbusGenericSensor<DbusEventsTotalStats>("EspressoRelayOutboundEventBuffer",
                                                       outboundEvents.getDimension(),
                                                       outboundEvents);
      sensorRegistry.registerSensor("EspressoRelay Sensor Metrics In Events", inEventSensor);
      sensorRegistry.registerSensor("EspressoRelay Sensor Metrics Out Events", outEventSensor);
    }
    if (connectionStats != null)
    {
      DbusGenericSensor<ContainerTrafficTotalStatsMBean> connSensor =
        new DbusGenericSensor<ContainerTrafficTotalStatsMBean>("EspressoRelayOutboundConn",
                                                               "total",
                                                               connectionStats);
      sensorRegistry.registerSensor("EspressoRelay Sensor Metrics Connection",
                                    connSensor);
    }

    if (containerStats != null)
    {
      DbusGenericSensor<ContainerStatsMBean> containerSensor =
        new DbusGenericSensor<ContainerStatsMBean>("EspressoRelayContainerStats",
                                                   "total",
                                                   containerStats);
      sensorRegistry.registerSensor("EspressoRelay Sensor Metrics Container", containerSensor);
    }

  }

  // class for rpldbusstats call back
  static public class RplDbusSensorStatsCallback implements StatsCollectorCallback<RplDbusTotalStats>
  {

    final SensorRegistry _sensorRegistry;

    public RplDbusSensorStatsCallback(SensorRegistry sensorRegistry)
    {
      _sensorRegistry = sensorRegistry;
    }

    @Override
    public synchronized void addedStats(RplDbusTotalStats stats)
    {
      RplDbusManager.LOG.debug("adding RpldbusTotalStats " + stats);
      if (stats != null)
      {
        DbusGenericSensor<RplDbusTotalStats> inEventSensor = new DbusGenericSensor<RplDbusTotalStats>(stats.getSanitizedName(),
            stats.getDimension(),stats);
        _sensorRegistry.registerSensor("Sensor." + stats.getSanitizedName(), inEventSensor);
      }
    }

    @Override
    public void removedStats(RplDbusTotalStats stats)
    {
      //FIXME no way to de-register the sensor?
    }
  }
}
