package com.linkedin.databus.client;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.databus.client.pub.mbean.ConsumerCallbackStats;
import com.linkedin.databus.client.pub.mbean.ConsumerCallbackStatsMBean;
import com.linkedin.databus.core.monitoring.StatsCollectorCallback;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.monitoring.mbean.StatsCollectors;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsTotalStats;
import com.linkedin.databus2.core.container.monitoring.mbean.DbusHttpTotalStats;
import com.linkedin.databus2.core.container.monitoring.ServerContainerSensorFactory;
import com.linkedin.databus2.core.container.monitoring.mbean.ContainerStatsMBean;
import com.linkedin.databus2.core.container.monitoring.mbean.ContainerTrafficTotalStatsMBean;
import com.linkedin.healthcheck.pub.spring.SensorRegistry;
import com.linkedin.databus.internal.monitoring.mbean.DbusGenericSensor;

public class DatabusClientSensorFactory extends ServerContainerSensorFactory
{
	public final ConsumerSensorStatsCallback _consumerSensorCallback;
	
	public DatabusClientSensorFactory(SensorRegistry sensorRegistry, DatabusClientRunnable client)
	{
		super(sensorRegistry, "Client");

		_consumerSensorCallback = new ConsumerSensorStatsCallback(sensorRegistry);
		
		/** Access stats/MBean objects **/
		DatabusHttpClientImpl clientImpl = client.getDatabusHttpClientImpl();
		DbusHttpTotalStats httpTotalStats = clientImpl.getHttpStatsCollector().getTotalStats();
		DbusEventsTotalStats inboundEvents = clientImpl.getInboundEventStatisticsCollector().getTotalStats();
		DbusEventsTotalStats outboundEvents = clientImpl.getOutboundEventStatisticsCollector().getTotalStats();
		ContainerTrafficTotalStatsMBean  connectionStats = clientImpl.getContainerStatsCollector().getInboundTrafficTotalStats();
		DbusEventsTotalStats bootStrapInboundEvents = clientImpl.getBootstrapEventsStatsCollector().getTotalStats();
		ContainerStatsMBean containerStats = clientImpl.getContainerStatsCollector().getContainerStats();
	
		StatsCollectors<DbusEventsStatisticsCollector> inBoundStatsCollectors = clientImpl.getInBoundStatsCollectors();
	    StatsCollectors<DbusEventsStatisticsCollector> outBoundStatsCollectors = clientImpl.getOutBoundStatsCollectors();  
	    StatsCollectors<DbusEventsStatisticsCollector> bsInBoundtatsCollectors = clientImpl.getBootstrapEventsStats();  

	    
	    SensorStatsCallback eventSensorCallback = getSensorCallback();
	    inBoundStatsCollectors.setStatsCollectorCallback(eventSensorCallback);
	    outBoundStatsCollectors.setStatsCollectorCallback(eventSensorCallback);
	    bsInBoundtatsCollectors.setStatsCollectorCallback(eventSensorCallback);
	    
		StatsCollectors<ConsumerCallbackStats> relayConsStats = clientImpl.getRelayConsumerStatsCollectors();
		StatsCollectors<ConsumerCallbackStats> bsConsStats = clientImpl.getBootstrapConsumerStatsCollectors();
		relayConsStats.setStatsCollectorCallback(_consumerSensorCallback);
		bsConsStats.setStatsCollectorCallback(_consumerSensorCallback);
		
		/** Create sensors **/
		if (httpTotalStats != null)
		{
			DbusGenericSensor<DbusHttpTotalStats> httpSensor = new DbusGenericSensor<DbusHttpTotalStats>("ClientHttpMetrics",httpTotalStats.getDimension()
					,httpTotalStats);
			sensorRegistry.registerSensor("Client Sensor Metrics",httpSensor);
		}
		
		if (connectionStats != null)
		{
			DbusGenericSensor<ContainerTrafficTotalStatsMBean> connSensor =
					new DbusGenericSensor<ContainerTrafficTotalStatsMBean>("ClientInboundConn","total",connectionStats);
			sensorRegistry.registerSensor("Client Sensor Metrics Connection In", connSensor);
		}
		
		//Overall relay buffer stats, required for backward compatibilty
		if (inboundEvents != null && outboundEvents != null)
		{
			DbusGenericSensor<DbusEventsTotalStats> inEventSensor = new DbusGenericSensor<DbusEventsTotalStats>("ClientInboundEventBuffer",
					inboundEvents.getDimension(),inboundEvents);
			DbusGenericSensor<DbusEventsTotalStats> outEventSensor = new DbusGenericSensor<DbusEventsTotalStats>("ClientOutboundEventBuffer",
					outboundEvents.getDimension(),outboundEvents);
			sensorRegistry.registerSensor("Client Sensor Metrics Relay In Events", inEventSensor);
			sensorRegistry.registerSensor("Client Sensor Metrics Relay Out Events", outEventSensor);
		}
		//Overall buffer stats: required for backward compatibility
		if (bootStrapInboundEvents != null)
		{
			DbusGenericSensor<DbusEventsTotalStats> bootStrapInEventSensor = new DbusGenericSensor<DbusEventsTotalStats>("ClientBootStrapInboundEventBuffer",
					bootStrapInboundEvents.getDimension(),bootStrapInboundEvents);
			sensorRegistry.registerSensor("Client Sensor Metrics Bootstrap In Events", bootStrapInEventSensor);

		}
		//Source specific buffer stats
		registerEventStatsCollectors(inBoundStatsCollectors.getStatsCollectors(),eventSensorCallback);
		registerEventStatsCollectors(outBoundStatsCollectors.getStatsCollectors(),eventSensorCallback);
		registerEventStatsCollectors(clientImpl.getBootstrapEventsStats().getStatsCollectors(),eventSensorCallback);
		
		//Source specific consumer stats registration
		registerConsumerStats(relayConsStats.getStatsCollectors(),_consumerSensorCallback);
		registerConsumerStats(bsConsStats.getStatsCollectors(),_consumerSensorCallback);

		
		if (containerStats != null)
		{
			DbusGenericSensor<ContainerStatsMBean> containerSensor =
					new DbusGenericSensor<ContainerStatsMBean>("ClientContainerStats",
							"total",
							containerStats);
			sensorRegistry.registerSensor("Client Sensor Metrics Container", containerSensor);
		}
		

	}

	private void registerEventStatsCollectors(ArrayList<DbusEventsStatisticsCollector> listStatsColl, SensorStatsCallback sensorCallback)
	{
		for (DbusEventsStatisticsCollector coll: listStatsColl)
		{
			sensorCallback.addedStats(coll);
		}
	}
	
	private void registerConsumerStats(ArrayList<ConsumerCallbackStats> listStats, ConsumerSensorStatsCallback sensorCallback)
	{
		for (ConsumerCallbackStats s: listStats)
		{
			sensorCallback.addedStats(s);
		}
	}
	
	/**
	 * 
	 * @author snagaraj
	 * Callback for consumer stats sensor registration
	 */
	
	static public class ConsumerSensorStatsCallback implements StatsCollectorCallback<ConsumerCallbackStats> 
    {

    	final SensorRegistry _sensorRegistry;
    	
    	public  ConsumerSensorStatsCallback(SensorRegistry sensorRegistry) 
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
