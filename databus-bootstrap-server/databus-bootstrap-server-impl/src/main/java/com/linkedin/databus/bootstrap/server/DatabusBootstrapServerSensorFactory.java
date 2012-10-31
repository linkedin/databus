package com.linkedin.databus.bootstrap.server;

import com.linkedin.databus.bootstrap.monitoring.server.mbean.DbusBootstrapHttpStatsMBean;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsTotalStats;
import com.linkedin.databus.core.monitoring.mbean.DbusGenericSensor;
import com.linkedin.databus2.core.container.monitoring.ServerContainerSensorFactory;
import com.linkedin.databus2.core.container.monitoring.mbean.ContainerStatsMBean;
import com.linkedin.databus2.core.container.monitoring.mbean.ContainerTrafficTotalStatsMBean;
import com.linkedin.healthcheck.pub.spring.SensorRegistry;

public class DatabusBootstrapServerSensorFactory extends ServerContainerSensorFactory
{
  public DatabusBootstrapServerSensorFactory(SensorRegistry sensorRegistry, BootstrapServerStartable client)
  {
      super(sensorRegistry, "BootstrapServer");

      /** Access stats/MBean objects **/
      DbusBootstrapHttpStatsMBean httpTotalStats = client.getBootstrapHttpServer().getBootstrapStatsCollector().getTotalStats();
      DbusEventsTotalStats outboundEvents = client.getBootstrapHttpServer().getOutboundEventStatisticsCollector().getTotalStats();
      DbusEventsTotalStats inboundEvents = client.getBootstrapHttpServer().getInboundEventStatisticsCollector().getTotalStats();
      ContainerTrafficTotalStatsMBean  connectionInStats = client.getBootstrapHttpServer().getContainerStatsCollector().getInboundTrafficTotalStats();
      ContainerTrafficTotalStatsMBean  connectionOutStats = client.getBootstrapHttpServer().getContainerStatsCollector().getOutboundTrafficTotalStats();
      ContainerStatsMBean containerStats = client.getBootstrapHttpServer().getContainerStatsCollector().getContainerStats();


      /** Create sensors **/
      if (httpTotalStats != null)
      {
        DbusGenericSensor<DbusBootstrapHttpStatsMBean> httpSensor = new DbusGenericSensor<DbusBootstrapHttpStatsMBean>("BootstrapServertHttpMetrics","total"
              ,httpTotalStats);
        sensorRegistry.registerSensor("Bootstrap Server Sensor Metrics",httpSensor);
      }

      if (outboundEvents != null && inboundEvents != null)
      {
        DbusGenericSensor<DbusEventsTotalStats> outEventSensor = new DbusGenericSensor<DbusEventsTotalStats>("BootstrapServerOutboundEventBuffer",
            outboundEvents.getDimension(),outboundEvents);
        DbusGenericSensor<DbusEventsTotalStats> inEventSensor = new DbusGenericSensor<DbusEventsTotalStats>("BootStrapServerInboundEventBuffer",
            inboundEvents.getDimension(),inboundEvents);
        sensorRegistry.registerSensor("Bootstrap Server Sensor Metrics Relay In Events", inEventSensor);
        sensorRegistry.registerSensor("Bootstrap Server Sensor Metrics Relay Out Events", outEventSensor);
      }

      if (connectionInStats != null)
      {
        DbusGenericSensor<ContainerTrafficTotalStatsMBean> connInSensor =
          new DbusGenericSensor<ContainerTrafficTotalStatsMBean>("BootstrapServerInboundConn","total",connectionInStats);
        sensorRegistry.registerSensor("Bootstrap Server Sensor Metrics Connection In", connInSensor);
      }
      if (connectionOutStats != null)
      {
        DbusGenericSensor<ContainerTrafficTotalStatsMBean> connOutSensor =
          new DbusGenericSensor<ContainerTrafficTotalStatsMBean>("BootstrapServerOutboundConn","total",connectionOutStats);
        sensorRegistry.registerSensor("Bootstrap Server Sensor Metrics Connection Out", connOutSensor);
      }

      if (containerStats != null)
      {
        DbusGenericSensor<ContainerStatsMBean> containerSensor =
          new DbusGenericSensor<ContainerStatsMBean>("BootstrapServerContainerStats",
                                                     "total",
                                                     containerStats);
        sensorRegistry.registerSensor("Bootstrap Server Sensor Metrics Container", containerSensor);
      }

  }

}
