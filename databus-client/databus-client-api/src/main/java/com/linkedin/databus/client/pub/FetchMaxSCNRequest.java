package com.linkedin.databus.client.pub;

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
