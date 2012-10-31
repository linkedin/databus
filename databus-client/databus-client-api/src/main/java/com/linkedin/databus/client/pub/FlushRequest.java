package com.linkedin.databus.client.pub;

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
