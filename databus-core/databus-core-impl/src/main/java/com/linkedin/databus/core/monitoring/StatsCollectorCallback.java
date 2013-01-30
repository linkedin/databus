package com.linkedin.databus.core.monitoring;
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



/**
 * Defines callbacks on addition/removal of stats collector objects
 * Used by sensor factories, that enable dynamic registration of stats objects associated with physical sources (databases)
 */
 
public interface StatsCollectorCallback<T> {
	
	/** stats collector object added */
	void addedStats(T stats);

	/** stats collector object removed */
	void removedStats(T stats);
	
	
}
