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


import java.nio.channels.ReadableByteChannel;

import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;

/**
 * allows to append events to the buffer from a stream
 *
 */
public interface DbusEventBufferStreamAppendable
{

  /*
   * Read events from a channel and append them to a buffer.
   * @param readChannel : ByteChannel to read events from
   * @param eventListeners : List of listeners interested in the events in the channel
   * @param statsCollector : Stats Collector for this event
   * @return Number of events appended
   */
  int readEvents(ReadableByteChannel readChannel,
          Iterable<InternalDatabusEventsListener> eventListeners,
          DbusEventsStatisticsCollector statsCollector)
          throws InvalidEventException;
}
