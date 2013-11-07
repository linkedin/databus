package com.linkedin.databus.core;
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
