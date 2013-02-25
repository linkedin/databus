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


/**
 * 
 * Request Container for Flush Request
 */
public class FlushRequest 
{
	public static final long DEFAULT_FLUSH_TIMEOUT_MILLIS = 3000;
	
	private long flushTimeoutMillis; /** Timeout for flush to reach the maxSCN */

	public long getFlushTimeoutMillis() {
		return flushTimeoutMillis;
	}

	public void setFlushTimeoutMillis(long flushTimeoutMillis) {
		this.flushTimeoutMillis = flushTimeoutMillis;
	}

	public FlushRequest(long flushTimeoutMillis) {
		super();
		this.flushTimeoutMillis = flushTimeoutMillis;
	}

	/**
	 * Creates a flush request with default timeout value of 3secs
	 */
	public FlushRequest()
	{
		flushTimeoutMillis = DEFAULT_FLUSH_TIMEOUT_MILLIS;
	}
	
}
