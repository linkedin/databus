package com.linkedin.databus2.core.container.monitoring;

import com.linkedin.databus.core.monitoring.StatsCollectorCallback;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsTotalStats;
import com.linkedin.databus2.core.container.monitoring.mbean.GarbageCollectorSensor;
import com.linkedin.databus2.core.container.monitoring.mbean.MemoryPoolsSensor;
import com.linkedin.healthcheck.pub.spring.SensorRegistry;
import com.linkedin.databus.internal.monitoring.mbean.DbusGenericSensor;

public class ServerContainerSensorFactory
{
	
	final SensorStatsCallback _sensorCallback ;
	
    public ServerContainerSensorFactory(SensorRegistry sensorRegistry, String name)
    {

      GarbageCollectorSensor gcSensor = new GarbageCollectorSensor(name + "databus2.GcSensor", "total");
      sensorRegistry.registerSensor(name + " Sensor Garbage Collection", gcSensor);

      MemoryPoolsSensor memPoolsSensor = new MemoryPoolsSensor(name + "databus2.MemPoolsSensor", "total");
      sensorRegistry.registerSensor(name + " MemPools Sensor", memPoolsSensor);
      
      _sensorCallback = new SensorStatsCallback(sensorRegistry);

    }
    
    public SensorStatsCallback getSensorCallback()
    {
    	return _sensorCallback;
    }
    
    static public class SensorStatsCallback implements StatsCollectorCallback<DbusEventsStatisticsCollector> 
    {

    	final SensorRegistry _sensorRegistry;
    	
    	public SensorStatsCallback(SensorRegistry sensorRegistry) 
    	{
    		_sensorRegistry = sensorRegistry;
		} 
    	
		@Override
		public synchronized void addedStats(DbusEventsStatisticsCollector stats) 
		{
			 DbusEventsTotalStats s = stats.getTotalStats();
			 if (s != null)
			 {
				 DbusGenericSensor<DbusEventsTotalStats> inEventSensor = new DbusGenericSensor<DbusEventsTotalStats>(stats.getSanitizedName(),
						 s.getDimension(),s);
				 _sensorRegistry.registerSensor("Sensor." + stats.getSanitizedName(), inEventSensor);
			 }
		}

		@Override
		public void removedStats(DbusEventsStatisticsCollector stats)
		{
			//FIXME no way to de-register the sensor?
		}
    	
    }
}
