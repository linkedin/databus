package com.linkedin.databus.bootstrap.producer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.linkedin.databus.bootstrap.common.BootstrapProducerStatsCollector;
import com.linkedin.databus.bootstrap.monitoring.producer.mbean.DbusBootstrapProducerStatsMBean;
import com.linkedin.databus.client.pub.ServerInfo;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsTotalStats;
import com.linkedin.databus2.core.container.monitoring.ServerContainerSensorFactory;
import com.linkedin.databus2.core.container.monitoring.mbean.ContainerStatsMBean;
import com.linkedin.databus2.core.container.monitoring.mbean.ContainerTrafficTotalStatsMBean;
import com.linkedin.databus2.core.container.monitoring.mbean.DbusHttpTotalStats;
import com.linkedin.healthcheck.pub.spring.SensorRegistry;
import com.linkedin.databus.internal.monitoring.mbean.DbusGenericSensor;

public class DatabusBootstrapProducerSensorFactory extends ServerContainerSensorFactory
{
  public DatabusBootstrapProducerSensorFactory(SensorRegistry sensorRegistry,
                                               BootstrapProducerStartable client)
  {
      super(sensorRegistry, "BootstrapProducer");

      /** Access stats/MBean objects **/
      DbusEventsTotalStats outboundEvents = client.getBootstrapProducer().getOutboundEventStatisticsCollector().getTotalStats();
      DbusEventsTotalStats inboundEvents = client.getBootstrapProducer().getInboundEventStatisticsCollector().getTotalStats();
      List<ServerInfo> relaysInfo = client.getBootstrapProducer().getClientConfigManager().getReadOnlyConfig().getRelays();
      Set<String> sourceNames = new HashSet<String>();

      for( ServerInfo s : relaysInfo)
      {
    	  for (String n : s.getSources())
    	  {
    		  sourceNames.add(n);
    	  }
      }

      BootstrapProducerStatsCollector producerCollector = client.getBootstrapProducer().getProducerStatsCollector();
      BootstrapProducerStatsCollector applierCollector = client.getBootstrapProducer().getApplierStatsCollector();

      DbusBootstrapProducerStatsMBean producerTotalStats = client.getBootstrapProducer().getProducerStatsCollector().getTotalStats();
      DbusBootstrapProducerStatsMBean applierTotalStats = client.getBootstrapProducer().getApplierStatsCollector().getTotalStats();


      ContainerTrafficTotalStatsMBean  connectionInStats = client.getBootstrapProducer().getContainerStatsCollector().getInboundTrafficTotalStats();
      ContainerTrafficTotalStatsMBean  connectionOutStats = client.getBootstrapProducer().getContainerStatsCollector().getOutboundTrafficTotalStats();
      DbusHttpTotalStats httpTotalStats = client.getBootstrapProducer().getHttpStatsCollector().getTotalStats();
      ContainerStatsMBean containerStats = client.getBootstrapProducer().getContainerStatsCollector().getContainerStats();

      if (outboundEvents != null && inboundEvents != null)
      {
        DbusGenericSensor<DbusEventsTotalStats> outEventSensor = new DbusGenericSensor<DbusEventsTotalStats>("BootstrapProducerOutboundEventBuffer",
            outboundEvents.getDimension(),outboundEvents);
        DbusGenericSensor<DbusEventsTotalStats> inEventSensor = new DbusGenericSensor<DbusEventsTotalStats>("BootstrapProducerInboundEventBuffer",
            inboundEvents.getDimension(),inboundEvents);
        sensorRegistry.registerSensor("Bootstrap Producer Sensor Metrics Relay In Events", inEventSensor);
        sensorRegistry.registerSensor("Bootstrap Producer Sensor Metrics Relay Out Events", outEventSensor);
      }

      if ( producerTotalStats != null )
      {
          DbusGenericSensor<DbusBootstrapProducerStatsMBean> prodSensor =
              new DbusGenericSensor<DbusBootstrapProducerStatsMBean>("BootstrapProducerStats","total",producerTotalStats);
          sensorRegistry.registerSensor("Bootstrap Producer Sensor Metrics", prodSensor);

          for (String src :sourceNames)
          {
        	  DbusBootstrapProducerStatsMBean stat = producerCollector.getSourceStats(src);

            if (stat != null)
            {
              DbusGenericSensor<DbusBootstrapProducerStatsMBean> srcProducerStatsSensor =
                new DbusGenericSensor<DbusBootstrapProducerStatsMBean> ("BootstrapProducerStatsPerSource",src,stat);
              sensorRegistry.registerSensor("Bootstrap Producer Stats per src", srcProducerStatsSensor);
            }
          }
      }

      if ( applierTotalStats != null )
      {
          DbusGenericSensor<DbusBootstrapProducerStatsMBean> applierSensor =
              new DbusGenericSensor<DbusBootstrapProducerStatsMBean>("BootstrapApplierStats","total",applierTotalStats);
          sensorRegistry.registerSensor("Bootstrap Applier Sensor Metrics", applierSensor);

          for (String src :sourceNames)
          {
        	  DbusBootstrapProducerStatsMBean stat = applierCollector.getSourceStats(src);

            if (stat != null)
            {
              DbusGenericSensor<DbusBootstrapProducerStatsMBean> srcApplierStatsSensor =
                new DbusGenericSensor<DbusBootstrapProducerStatsMBean> ("BootstrapApplierStatsPerSource",src,stat);
              sensorRegistry.registerSensor("Bootstrap Applier Stats per src", srcApplierStatsSensor);
            }
          }
      }

      if (connectionInStats != null)
      {
        DbusGenericSensor<ContainerTrafficTotalStatsMBean> connInSensor =
          new DbusGenericSensor<ContainerTrafficTotalStatsMBean>("BootstrapProducerInboundConn","total",connectionInStats);
        sensorRegistry.registerSensor("Bootstrap Producer Sensor Metrics Connection In", connInSensor);
      }
      if (connectionOutStats != null)
      {
        DbusGenericSensor<ContainerTrafficTotalStatsMBean> connOutSensor =
          new DbusGenericSensor<ContainerTrafficTotalStatsMBean>("BootstrapProducerOutboundConn","total",connectionOutStats);
        sensorRegistry.registerSensor("Bootstrap Producer Sensor Metrics Connection Out", connOutSensor);
      }
      if (httpTotalStats != null)
      {
        DbusGenericSensor<DbusHttpTotalStats> httpSensor = new DbusGenericSensor<DbusHttpTotalStats>("BootstrapProducerHttpMetrics",httpTotalStats.getDimension()
            ,httpTotalStats);
        sensorRegistry.registerSensor("Bootstrap Producer Sensor HTTP Metrics",httpSensor);
      }

      if (containerStats != null)
      {
        DbusGenericSensor<ContainerStatsMBean> containerSensor =
          new DbusGenericSensor<ContainerStatsMBean>("BootstrapProducerContainerStats",
                                                     "total",
                                                     containerStats);
        sensorRegistry.registerSensor("Bootstrap Producer Sensor Metrics Container", containerSensor);
      }


  }

}
