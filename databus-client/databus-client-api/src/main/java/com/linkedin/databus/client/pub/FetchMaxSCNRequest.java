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


import com.linkedin.databus2.core.BackoffTimer;
import com.linkedin.databus2.core.BackoffTimerStaticConfig;

/**
 * 
 * Request Container for fetchMaxSCNRequest
 */
public class FetchMaxSCNRequest 
{
	
	public static final long DEFAULT_RETRY_TIMEOUT_MILLIS = 1000;
	public static final int DEFAULT_NUM_MAXSCN_RETRIES = 3;
	
	private BackoffTimer _errorRetry;

	public FetchMaxSCNRequest(BackoffTimerStaticConfig backoffConfig)
	{
		_errorRetry = new BackoffTimer("FetchMaxSCNRequest", backoffConfig);
	}
		
	public BackoffTimer getErrorRetry() {
		return _errorRetry;
	}

	public void setErrorRetry(BackoffTimer _errorRetry) {
		this._errorRetry = _errorRetry;
	}

	/**
	 * Creates a default fetchMaxSCn request with retry timeout of 1 sec and retries (=3)
	 */
	public FetchMaxSCNRequest()
	{
		this(new BackoffTimerStaticConfig(DEFAULT_RETRY_TIMEOUT_MILLIS, DEFAULT_RETRY_TIMEOUT_MILLIS, 1, 0, DEFAULT_NUM_MAXSCN_RETRIES));
	}
	
	public FetchMaxSCNRequest(long retryTimeoutMillis, int numMaxScnRetries) 
	{
		this(new BackoffTimerStaticConfig(retryTimeoutMillis, retryTimeoutMillis, 1, 0, numMaxScnRetries));
	}	
}
