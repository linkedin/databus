package com.linkedin.databus.client.pub.mbean;
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


public interface ConsumerCallbackStatsMBean {

	/** Metrics for databus2 consumers */

	/**GETTERS*/

	public long getTimestampLastResetMs();

	public long getTimeSinceLastResetMs();

	/** number of data events received by the consumer for processing */
	public long getNumDataEventsReceived();

    /** number of data events consumed/processed successfully by the consumer */
	public long getNumDataEventsProcessed();

	/** number of system events received by consumer */
	public long getNumSysEventsReceived();

	/** the minimum requested window seen by the dispatcher */
	public long getMinSeenWinScn();

	/** the maximum requested window seen by the dispatcher */
	public long getMaxSeenWinScn();

	/** number of erroneous events received */
	public long getNumErrorsReceived();

	/** number of events that weren't processed due to errors */
	public long getNumErrorsProcessed();

	/** number of erroneous end of window events processed */
	public long getNumSysErrorsProcessed();

	/** number of erroneous data events processed */
	public long getNumDataErrorsProcessed();


	/** number of events received by consumer */
	public long getNumEventsReceived();

	/** number of events processed by consumer */
	public long getNumEventsProcessed();

	/** number of end of window events processed */
	public long getNumSysEventsProcessed();

	/** time in ms since creation of the consumer*/
	public long getTimeSinceCreation();

	/** ave time in ms taken by consumer to process an event */
	public double getAveLatencyEventsProcessed();

	/**time in ms taken by consumer to process an event */
	public long getLatencyEventsProcessed();

	/** DEPRECATED : time in ms since local per source stat was merged to global consumer stat */
	public long getTimeSinceLastMergeMs();

	/** time in ms since last event was received */
	public long getTimeSinceLastEventReceived();

	/** time in ms since last event was processed */
	public long getTimeSinceLastEventProcessed();

	/** time diff in ms between now and time at which  data event last received was created */
	public long getTimeDiffLastEventReceived();

	/** time diff in ms between now and time at which  data event last received was created */
	public long getTimeDiffLastEventProcessed();

	/** scn of last data event processed by the consumer */
	public long getScnOfLastEventProcessed();

	/** timestamp of last window seen by the dispatcher */
	public long getMaxSeenWinTimestamp();

	/** MUTATORS*/
	void reset();

}
