package com.linkedin.databus.client.pub;
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


public interface RelayFlushMaxSCNResult 
{
	/**
	 * Summary Result Code
	 */
	public enum SummaryCode
	{		
		MAXSCN_REACHED, /** Flush Succeeded */
		TIMED_OUT,  /** Flush timed out waiting to reach max SCN */
		EMPTY_EXTERNAL_VIEW, /** External view empty either for this partition or for the whole cluster  */
		NO_ONLINE_RELAYS_IN_VIEW, /** No online relays hosting this partition */
		FAIL   /** Flush failed for other reasons */
	}

	/**
	 * 
	 * Return the Requested SCN for flush to achieve and completed
	 */
	public SCN getRequestedMaxSCN();

	/**
	 * Get the current max SCN that the client is seen as part of flush call 
	 * 
	 */
	public SCN getCurrentMaxSCN(); 
	
	/**
	 * Return the summary code for the flush operation
	 */
	public SummaryCode getResultStatus();
	
	/**
	 * Return the result for fetchSCN operation associated with this flush
	 * @return
	 */
	public RelayFindMaxSCNResult getFetchSCNResult();
}
