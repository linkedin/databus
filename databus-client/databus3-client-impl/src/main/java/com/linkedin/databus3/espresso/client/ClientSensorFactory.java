package com.linkedin.databus3.espresso.client;

import com.linkedin.databus.client.DatabusHttpClientImpl;
import com.linkedin.databus.client.pub.mbean.ConsumerCallbackStats;
import com.linkedin.databus.core.monitoring.StatsCollectorCallback;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsTotalStats;
import com.linkedin.databus.core.monitoring.mbean.DbusGenericSensor;
import com.linkedin.databus.core.monitoring.mbean.StatsCollectors;
import com.linkedin.databus2.core.container.monitoring.ServerContainerSensorFactory;
import com.linkedin.databus2.core.container.monitoring.mbean.ContainerStatsMBean;
import com.linkedin.databus2.core.container.monitoring.mbean.ContainerTrafficTotalStatsMBean;
import com.linkedin.databus2.core.container.monitoring.mbean.DbusHttpTotalStats;
import com.linkedin.healthcheck.pub.spring.SensorRegistry;

public class ClientSensorFactory  extends ServerContainerSensorFactory
{

	private final ConsSensorStatsCallback _consumerSensorCallback ;


  public ClientSensorFactory(SensorRegistry sensorRegistry,  DatabusHttpClientImpl client)
  {
    super(sensorRegistry, "EspressoClient");

    _consumerSensorCallback = new ConsSensorStatsCallback(sensorRegistry);

    /** Access stats/MBean objects **/
    DbusHttpTotalStats httpTotalStats = client.getHttpStatsCollector().getTotalStats();
    DbusEventsTotalStats inboundEvents = client.getInboundEventStatisticsCollector().getTotalStats();
    ContainerTrafficTotalStatsMBean  connectionStats = client.getContainerStatsCollector().getInboundTrafficTotalStats();
    ContainerStatsMBean containerStats = client.getContainerStatsCollector().getContainerStats();

    StatsCollectors<DbusEventsStatisticsCollector> inBoundStatsCollectors = client.getInBoundStatsCollectors();


    StatsCollectors<ConsumerCallbackStats> relayConsStats = client.getRelayConsumerStatsCollectors();
    StatsCollectors<ConsumerCallbackStats> bsConsStats = client.getBootstrapConsumerStatsCollectors();
    if (client.getClientStaticConfig().isEnablePerConnectionStats()) 
    {
      relayConsStats.setStatsCollectorCallback(_consumerSensorCallback);
      bsConsStats.setStatsCollectorCallback(_consumerSensorCallback);
      inBoundStatsCollectors.setStatsCollectorCallback(getSensorCallback());
    }

    /** Create sensors **/
    if (httpTotalStats != null)
    {
      DbusGenericSensor<DbusHttpTotalStats> httpSensor = new DbusGenericSensor<DbusHttpTotalStats>("ClientHttpMetrics",httpTotalStats.getDimension()
            ,httpTotalStats);
      sensorRegistry.registerSensor("Client Sensor Metrics",httpSensor);
    }
    if (inboundEvents != null)
    {
      DbusGenericSensor<DbusEventsTotalStats> inEventSensor = new DbusGenericSensor<DbusEventsTotalStats>("ClientInboundEventBuffer",
            inboundEvents.getDimension(),inboundEvents);
      sensorRegistry.registerSensor("Client Sensor Metrics Relay In Events", inEventSensor);
    }
    if (connectionStats != null)
    {
      DbusGenericSensor<ContainerTrafficTotalStatsMBean> connSensor =
        new DbusGenericSensor<ContainerTrafficTotalStatsMBean>("ClientInboundConn","total",connectionStats);
      sensorRegistry.registerSensor("Client Sensor Metrics Connection In", connSensor);
    }

    if (containerStats != null)
    {
      DbusGenericSensor<ContainerStatsMBean> containerSensor =
        new DbusGenericSensor<ContainerStatsMBean>("ClientContainerStats",
                                                   "total",
                                                   containerStats);
      sensorRegistry.registerSensor("Client Sensor Metrics Container", containerSensor);
    }

  }

  /**
	 *
	 * @author snagaraj
	 * Callback for consumer stats sensor registration
	 */

  static public class ConsSensorStatsCallback implements StatsCollectorCallback<ConsumerCallbackStats>
  {

	  final SensorRegistry _sensorRegistry;

	  public  ConsSensorStatsCallback(SensorRegistry sensorRegistry)
	  {
		  _sensorRegistry = sensorRegistry;
	  }

	  @Override
	  public synchronized void addedStats(ConsumerCallbackStats stats)
	  {
		  if (stats != null)
		  {
			  DbusGenericSensor<ConsumerCallbackStats> inEventSensor = new DbusGenericSensor<ConsumerCallbackStats>(stats.getName(),
					  stats.getDimension(),stats);
			  _sensorRegistry.registerSensor("Sensor." + stats.getName(), inEventSensor);
		  }
	  }

	  @Override
	  public synchronized void removedStats(ConsumerCallbackStats stats)
	  {
		  //FIXME no way to de-register the sensor?
	  }

  }




}
