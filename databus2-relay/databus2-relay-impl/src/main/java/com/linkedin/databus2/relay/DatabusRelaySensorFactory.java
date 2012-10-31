package com.linkedin.databus2.relay;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.monitoring.mbean.StatsCollectors;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsTotalStats;
import com.linkedin.databus.core.monitoring.mbean.DbusGenericSensor;
import com.linkedin.databus.monitoring.mbean.DBStatisticsMBean;
import com.linkedin.databus.monitoring.mbean.EventSourceStatisticsMBean;
import com.linkedin.databus.monitoring.mbean.SourceDBStatisticsMBean;
import com.linkedin.databus2.core.container.monitoring.ServerContainerSensorFactory;
import com.linkedin.databus2.core.container.monitoring.mbean.ContainerStatsMBean;
import com.linkedin.databus2.core.container.monitoring.mbean.ContainerTrafficTotalStatsMBean;
import com.linkedin.databus2.core.container.monitoring.mbean.DbusHttpTotalStats;
import com.linkedin.databus2.core.mbean.DatabusReadOnlyStatusMBean;
import com.linkedin.databus2.producers.EventProducer;
import com.linkedin.databus2.producers.db.MonitoredSourceInfo;
import com.linkedin.databus2.producers.db.OracleEventProducer;
import com.linkedin.databus2.relayli.RelayStartable;
import com.linkedin.healthcheck.pub.spring.SensorRegistry;

public class DatabusRelaySensorFactory extends ServerContainerSensorFactory
{

    public DatabusRelaySensorFactory(SensorRegistry sensorRegistry, RelayStartable relay)
    {
      super(sensorRegistry, "Relay");

        /** Access stats/MBean objects **/
        DbusHttpTotalStats httpTotalStats = relay.getRelay().getHttpStatisticsCollector().getTotalStats();
        ContainerTrafficTotalStatsMBean  connectionStats = relay.getRelay().getContainerStatsCollector().getOutboundTrafficTotalStats();
        ContainerStatsMBean containerStats = relay.getRelay().getContainerStatsCollector().getContainerStats();
        
        StatsCollectors<DbusEventsStatisticsCollector> inBoundStatsCollectors = relay.getRelay().getInBoundStatsCollectors();
        StatsCollectors<DbusEventsStatisticsCollector> outBoundStatsCollectors = relay.getRelay().getOutBoundStatsCollectors();   
        
        inBoundStatsCollectors.setStatsCollectorCallback(getSensorCallback());
        outBoundStatsCollectors.setStatsCollectorCallback(getSensorCallback());
        
        /** Create sensors **/
        if (httpTotalStats != null)
        {
          DbusGenericSensor<DbusHttpTotalStats> httpSensor = new DbusGenericSensor<DbusHttpTotalStats>("RelayHttpMetrics",httpTotalStats.getDimension()
                ,httpTotalStats);
          sensorRegistry.registerSensor("Relay Sensor Metrics",httpSensor);
        }
     
        DatabusRelayMain r =  relay.getRelay();
        DbusEventsTotalStats inboundEvents = r.getInboundEventStatisticsCollector().getTotalStats();
        DbusEventsTotalStats outboundEvents = r.getOutboundEventStatisticsCollector().getTotalStats();
        /* Legacy, non-multi-buffer code */
        if (inboundEvents != null && outboundEvents != null)
        {
          DbusGenericSensor<DbusEventsTotalStats> inEventSensor = new DbusGenericSensor<DbusEventsTotalStats>("RelayInboundEventBuffer",
              inboundEvents.getDimension(),inboundEvents);
          DbusGenericSensor<DbusEventsTotalStats> outEventSensor = new DbusGenericSensor<DbusEventsTotalStats>("RelayOutboundEventBuffer",
              outboundEvents.getDimension(),outboundEvents);
          sensorRegistry.registerSensor("Relay Sensor Metrics In Events", inEventSensor);
          sensorRegistry.registerSensor("Relay Sensor Metrics Out Events", outEventSensor);
        }
        
        registerEventStatsCollectors(inBoundStatsCollectors.getStatsCollectors(),sensorRegistry);
        registerEventStatsCollectors(outBoundStatsCollectors.getStatsCollectors(),sensorRegistry);
        
        if (connectionStats != null)
        {
          DbusGenericSensor<ContainerTrafficTotalStatsMBean> connSensor =
            new DbusGenericSensor<ContainerTrafficTotalStatsMBean>("RelayOutboundConn","total",connectionStats);
          sensorRegistry.registerSensor("Relay Sensor Metrics Connection", connSensor);
        }

        if (containerStats != null)
        {
          DbusGenericSensor<ContainerStatsMBean> containerSensor =
            new DbusGenericSensor<ContainerStatsMBean>("RelayContainerStats",
                                                       "total",
                                                       containerStats);
          sensorRegistry.registerSensor("Relay Sensor Metrics Container", containerSensor);
        }

        /** Per source sensors */
        EventProducer [] prods = relay.getRelay().getProducers();
        MonitoringEventProducer [] monProds = relay.getRelay().getMonitoringProducers();
        int i = 0,j=0;
        for(EventProducer eventProducer : prods) {
          if (eventProducer != null && (eventProducer instanceof OracleEventProducer)) {
            registerOracleProduces(i, sensorRegistry, (OracleEventProducer)eventProducer, monProds[j]);
            j++;
          }
          i++;
        }
    }
    
    private void registerEventStatsCollectors(ArrayList<DbusEventsStatisticsCollector> listStatsColl, SensorRegistry sensorRegistry)
    {
    	for (DbusEventsStatisticsCollector coll: listStatsColl)
    	{
    		 DbusEventsTotalStats stats = coll.getTotalStats();
    		 DbusGenericSensor<DbusEventsTotalStats> sensor = new DbusGenericSensor<DbusEventsTotalStats>(coll.getSanitizedName(),
    	              stats.getDimension(),stats);
    		 sensorRegistry.registerSensor("Sensor." + coll.getSanitizedName(),sensor);
    	}
    }

    private void registerOracleProduces(int idx,
                                  SensorRegistry sensorRegistry, 
                                  OracleEventProducer prod, 
                                  MonitoringEventProducer monProd) 
    {

      List<MonitoredSourceInfo> sources = prod.getSources();
      for (MonitoredSourceInfo src:sources)
      {
        EventSourceStatisticsMBean stats = src.getStatisticsBean();
        DbusGenericSensor<EventSourceStatisticsMBean> sourceSensor =
          new DbusGenericSensor<EventSourceStatisticsMBean> ("RelayEventSource" + idx,src.getSourceName(),stats);
        sensorRegistry.registerSensor("Relay Sensor Src Metrics["+idx+"]", sourceSensor);
      }

      /** Max DB Scn **/

      if (monProd != null)
      {
        DBStatisticsMBean dbStats = monProd.getDBStats();
        DbusGenericSensor<DBStatisticsMBean> dbStatsSensor =
          new DbusGenericSensor<DBStatisticsMBean> ("DBStats" +idx, dbStats.getDBSourceName(), dbStats);
        sensorRegistry.registerSensor("Relay DB Stats["+idx+"]" , dbStatsSensor);
        for (MonitoredSourceInfo src:sources)
        {
          SourceDBStatisticsMBean srcDbStats = dbStats.getPerSourceStatistics(src.getSourceName());
          if (srcDbStats != null)
          {
            DbusGenericSensor<SourceDBStatisticsMBean> srcDbStatsSensor =
              new DbusGenericSensor<SourceDBStatisticsMBean> ("DBStatsPerSource"+idx,
                  src.getSourceName(),srcDbStats);
            sensorRegistry.registerSensor("Relay DB Stats per src["+idx+"]", srcDbStatsSensor);
          }
        }
      }

      DatabusReadOnlyStatusMBean oraProducerStatus = prod.getStatusMBean();
      DbusGenericSensor<DatabusReadOnlyStatusMBean> oraProducerSensor =
        new DbusGenericSensor<DatabusReadOnlyStatusMBean>(
            oraProducerStatus.getComponentName(), "OracleEventProducerStatus"+idx,
            oraProducerStatus);
      sensorRegistry.registerSensor("OracleEventProducer Sensor["+idx+"]", oraProducerSensor);
    }
}
