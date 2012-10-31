package com.linkedin.databus.core.util;

import java.util.List;

import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;

public interface DatabusEventProducer {

	public boolean startGeneration(long startScn, int eventsPerSecond,
			long durationInMilliseconds, long numEventToGenerate,
			int percentOfBufferToGenerate, long keyMin, long keyMax,
			List<IdNamePair> sources,
			DbusEventsStatisticsCollector statsCollector);

	public void stopGeneration();

	public boolean checkRunning();

	public void suspendGeneration();

	public void resumeGeneration(long numEventToGenerate,
			int percentOfBufferToGenerate, long keyMin, long keyMax);

}
