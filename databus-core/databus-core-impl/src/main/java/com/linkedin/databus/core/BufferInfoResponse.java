package com.linkedin.databus.core;

public class BufferInfoResponse 
{
	private long minScn = -1;
	private long maxScn = -1;
	private long timestampLatestEvent = -1;
	private long timestampFirstEvent = -1;
	
	
	public long getMinScn() {
		return minScn;
	}
	public void setMinScn(long minScn) {
		this.minScn = minScn;
	}
	public long getMaxScn() {
		return maxScn;
	}
	public void setMaxScn(long maxScn) {
		this.maxScn = maxScn;
	}
	public long getTimestampLatestEvent() {
		return timestampLatestEvent;
	}
	public void setTimestampLatestEvent(long timestampLatestEvent) {
		this.timestampLatestEvent = timestampLatestEvent;
	}
	public long getTimestampFirstEvent() {
		return timestampFirstEvent;
	}
	public void setTimestampFirstEvent(long timestampFirstEvent) {
		this.timestampFirstEvent = timestampFirstEvent;
	}
}
