package com.linkedin.databus.core.test;
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
import java.util.Iterator;
import java.util.Vector;

import org.apache.log4j.Logger;

import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.InvalidEventException;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.util.EventBufferConsumer;

/**
 * Runnable that reads a channel and expects to receive numEvents into vector
 * of DbusEvents; buffer can be blocking/non-blocking.
 *
 * This class is intended only for testing!
 *
 * @author snagaraj
 */

public class DbusEventBufferReader implements Runnable
{
  public static final Logger LOG = Logger.getLogger(DbusEventBufferReader.class.getName());

  public DbusEventBufferReader(DbusEventBuffer buffer,
                               ReadableByteChannel channel,
                               Vector<EventBufferConsumer> consumers,
                               DbusEventsStatisticsCollector stats)
  {
    _buffer = buffer;
    _channel = channel;
    _stop = false;
    _eventsRead = 0;
    _consumers = consumers;
    _stats = stats;
  }

  @Override
  public void run()
  {
    _stop = false;
    try {
      //assumes that events read can fit into _buffer; in practice, the client requests the capacity available in
      //the buffer from the server (which writes to the writeChannel from its buffer using the streamEvents() call)
      do {
        int numEvents=0;
        while ((numEvents = _buffer.readEvents(_channel,null,_stats)) > 0) {
          LOG.info("run():  readEvents() returned " + numEvents + " events written to client event buffer from channel");
          _eventsRead += numEvents;
        }
      } while (!_stop);
    } catch (InvalidEventException e) {
      for (Iterator<EventBufferConsumer> it = _consumers.iterator(); it.hasNext(); ) {
        EventBufferConsumer cons = it.next();
        cons.onInvalidEvent(eventsRead());
      }
      LOG.error("Invalid event! " + e);
      return;
    }
    finally {
      _stop = false;
    }
  }

  public void stop()
  {
    _stop = true;
  }

  public long eventsRead()
  {
    return _eventsRead;
  }

  private final ReadableByteChannel _channel;
  private final DbusEventBuffer _buffer;
  private boolean _stop;
  private long _eventsRead;
  private final Vector<EventBufferConsumer> _consumers;
  private final DbusEventsStatisticsCollector _stats;
}
