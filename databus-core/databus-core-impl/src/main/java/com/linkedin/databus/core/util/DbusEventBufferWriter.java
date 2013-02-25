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

import java.nio.channels.WritableByteChannel;

import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.Encoding;
import com.linkedin.databus.core.OffsetNotFoundException;
import com.linkedin.databus.core.ScnNotFoundException;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus2.core.filter.AllowAllDbusFilter;

/**
 *
 * @author snagaraj
 * Runnable that reads data from a DbusEventBuffer and writes to a channel if any events are found;
 *
 */

public class DbusEventBufferWriter implements Runnable {


  public DbusEventBufferWriter(DbusEventBuffer buffer, WritableByteChannel channel, int batchsize, DbusEventsStatisticsCollector stats) {
		_channel = channel;
		_buffer = buffer;
		_batchsize = batchsize;
		_stop=false;
		_count=0;
		_expectedEvents = -1;
		_stats = stats;
	}
	@Override
	//run in a thread please;
	public void run() {
	    _stop = false;
	    _expectedEvents = -1;
	    _count = 0;
		try {
			Checkpoint cp = new Checkpoint();
			//is there anything from the checkpoint I can infer that it's end of stream? control message?
			cp.setFlexible();
			AllowAllDbusFilter allowAllDbusFilter = new AllowAllDbusFilter();
			do {
				int streamedEvents=0;
				while ((streamedEvents = _buffer.streamEvents(cp, _batchsize, _channel,Encoding.BINARY, allowAllDbusFilter,_stats)) > 0) {
					_count += streamedEvents;
				}
				//the writer hangs around - cannot count events; cp provides current window and window offset ; and streamedEvents has the count of all events - not just data events
			} while (!_stop &&  !endOfEvents()) ;
		} catch (ScnNotFoundException e) {
			System.err.println("Scn Not Found Exception! ");
		}
		catch (OffsetNotFoundException e) {
			System.err.println("Offset Not Found Exception! ");
		}
		finally {
		  _stop = false;
		}
	}

	public void stop() {
	    _stop = true;
	}

	public long eventsWritten() {
	  return _count;
	}

	public long expectedEvents() {
	  return _expectedEvents;
	}

	public void setExpectedEvents(long e) {
	    _expectedEvents = e;
	}

	private boolean endOfEvents() {
	    return (_expectedEvents > 0) && (eventsWritten() >= expectedEvents());
	}

	private final WritableByteChannel _channel;
	private final DbusEventBuffer _buffer;
	private final int _batchsize;
	private boolean _stop;
	private long _count;
	private long _expectedEvents;
	private final DbusEventsStatisticsCollector _stats;
}
