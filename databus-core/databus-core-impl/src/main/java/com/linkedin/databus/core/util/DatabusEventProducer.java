package com.linkedin.databus.core.util;
/*
 *
 * Copyright 2013 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/


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
