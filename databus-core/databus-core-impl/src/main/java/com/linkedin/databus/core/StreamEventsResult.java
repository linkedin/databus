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

/**
 * This class holds the result of a DbusEventBuffer.streamEvents() call.
 */
public class StreamEventsResult
{
  /**
   * The number of events streamed in this call.
   */
  private int _numEventsStreamed = 0;
  /**
   * If the size of the buffer to stream into was not big enough then this variable
   * has the size of the event that did not fit into the buffer.
   */
  private int _sizeOfPendingEvent = 0;

  public StreamEventsResult()
  {
  }

  public StreamEventsResult(int numEventsStreamed, int sizeOfPendingEvent)
  {
    _numEventsStreamed = numEventsStreamed;
    _sizeOfPendingEvent = sizeOfPendingEvent;
  }
  public int getNumEventsStreamed()
  {
    return _numEventsStreamed;
  }

  public int getSizeOfPendingEvent()
  {
    return _sizeOfPendingEvent;
  }

  public void setSizeOfPendingEvent(int sizeOfPendingEvent)
  {
    _sizeOfPendingEvent = sizeOfPendingEvent;
  }

  public void incNumEventsStreamed(int incr)
  {
    _numEventsStreamed += incr;
  }
}
