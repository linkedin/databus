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

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Vector;
import java.util.logging.Logger;

import com.linkedin.databus.core.BootstrapCheckpointHandler;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventInternalReadable;
import com.linkedin.databus.core.DbusEventInternalWritable;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.DbusEventV2Factory;
import com.linkedin.databus.core.InvalidEventException;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.test.DbusEventCorrupter.EventCorruptionType;

/**
 *
 * @author snagaraj
 * Helper class to test concurrent appends into EventBuffer
 * Creates random window sizes in the list of events available
 */

public class DbusEventAppender implements Runnable
{
  public String MODULE = DbusEventAppender.class.getName();
  public Logger LOG = Logger.getLogger(MODULE);
  private final boolean _invokeStartOnBuffer;
  private final int _numDataEventsBeforeSkip;
  private final boolean _callInternalListeners;
  private final int _bootstrapCheckpointPerWindow;
  // Adding a dummy handler since no more info is available
  private final BootstrapCheckpointHandler _bstCheckpointHandler =
      new BootstrapCheckpointHandler("source1", "source2", "source3");

  public DbusEventAppender(Vector<DbusEvent> events,
                           DbusEventBuffer buffer,
                           DbusEventsStatisticsCollector stats)
  throws Exception
  {
    this(events, buffer, stats, 1.0, true,-1);
  }

  public DbusEventAppender(Vector<DbusEvent> events,
                           DbusEventBuffer buffer,
                           int bootstrapCheckpointsPerWindow,
                           DbusEventsStatisticsCollector stats)
  throws Exception
  {
    this(events, buffer, stats, 1.0, true, -1,true,bootstrapCheckpointsPerWindow);
  }

  public DbusEventAppender(Vector<DbusEvent> events,
                           DbusEventBuffer buffer,
                           DbusEventsStatisticsCollector stats,
                           boolean invokeStartOnBuffer)
  throws Exception
  {
    this(events, buffer, stats, 1.0, invokeStartOnBuffer,-1);
  }

  public DbusEventAppender(Vector<DbusEvent> events,
                           DbusEventBuffer buffer,
                           DbusEventsStatisticsCollector stats,
                           double fraction)
  throws Exception
  {
    this(events, buffer, stats, fraction, true,-1);
  }


  public DbusEventAppender(Vector<DbusEvent> events,
                           DbusEventBuffer buffer,
                           DbusEventsStatisticsCollector stats,
                           double fraction,
                           boolean invokeStartOnBuffer,
                           int numDataEventsBeforeSkip)
  throws Exception
  {
    this(events, buffer, stats, fraction, invokeStartOnBuffer, numDataEventsBeforeSkip, true,0);
  }

  public DbusEventAppender(Vector<DbusEvent> events,
                           DbusEventBuffer buffer,
                           DbusEventsStatisticsCollector stats,
                           double fraction,
                           boolean invokeStartOnBuffer,
                           int numDataEventsBeforeSkip,
                           boolean callInternalListeners,
                           int bootstrapCheckpointPerWindow)
  throws Exception
  {
    _events = events;
    _buffer = buffer;
    _bufferReflector = new DbusEventBufferReflector(_buffer);
    _count = 0;
    _stats = stats;
    _fraction = fraction;
    _invokeStartOnBuffer = invokeStartOnBuffer;
    _numDataEventsBeforeSkip = numDataEventsBeforeSkip;
    _callInternalListeners = callInternalListeners;
    _bootstrapCheckpointPerWindow = bootstrapCheckpointPerWindow;
  }

  public long eventsEmitted()
  {
    return _count;
  }

  public int addEventToBuffer(DbusEvent ev, int dataEventCount)
  {
    byte[] payload = new byte[((DbusEventInternalReadable)ev).payloadLength()];
    ev.value().get(payload);
    if ((_numDataEventsBeforeSkip < 0) || (dataEventCount < _numDataEventsBeforeSkip))
    {
      _buffer.appendEvent(new DbusEventKey(ev.key()),
                          ev.physicalPartitionId(),
                          ev.logicalPartitionId(),
                          ev.timestampInNanos(),
                          ev.srcId(),
                          ev.schemaId(),
                          payload,
                          false,
                          _stats);
      if (!_bufferReflector.validateBuffer())
      {
        throw new RuntimeException("Buffer validation 3 failed");
      }
      ++dataEventCount;
    }
    return dataEventCount;
  }

  public void addBootstrapCheckpointEventToBuffer(long lastScn, long dataEventCount, int numCheckpoints)
  {
    Checkpoint cp = _bstCheckpointHandler.createInitialBootstrapCheckpoint(null, 0L);
    cp.setBootstrapStartScn(0L);
    cp.setWindowScn(lastScn);
    DbusEventInternalReadable ev = new DbusEventV2Factory().createCheckpointEvent(cp);
    ByteBuffer b = ev.value();
    byte[] bytes = new byte[b.remaining()];
    b.get(bytes);
    for (int i=0;i < numCheckpoints;++i)
    {
      _buffer.appendEvent(new DbusEventKey(ev.key()),
                          ev.physicalPartitionId(),
                          ev.logicalPartitionId(),
                          ev.timestampInNanos(),
                          ev.srcId(),
                          ev.schemaId(),
                          bytes,
                          false,
                          _stats);
    }
  }

  public DbusEventBufferReflector getDbusEventReflector()
  {
    return _bufferReflector;
  }

  @Override
  public void run()
  {
    //append events into buffer serially with varying  window sizes;
    long lastScn = -1;
    _count = 0;
    int dataEventCount = 0;
    int bootstrapCheckpoints=_bootstrapCheckpointPerWindow;
    int maxCount = (int)(_fraction * _events.size());
    for (DbusEvent ev : _events)
    {
      if (dataEventCount >= maxCount)
      {
        break;
      }
      long evScn = ev.sequence();
      if (lastScn != evScn)
      {
        //new window;
        if (lastScn==-1)
        {
           //note: start should provide the first preceding scn;
           // Test DDSDBUS-1109 by skipping the start() call. The scn Index should be set for streamEvents() to work correctly
          if (_invokeStartOnBuffer)
          {
            _buffer.start(evScn-1);
          }
          _buffer.startEvents();
        }
        else
        {
          ++_count;
          if (_callInternalListeners)
          {
            if (0 == bootstrapCheckpoints)
            {
              _buffer.endEvents(lastScn,_stats);
            }
            else
            {
              addBootstrapCheckpointEventToBuffer(lastScn,dataEventCount,1);
            }
            --bootstrapCheckpoints;
          }
          else
          {
            if (0 == bootstrapCheckpoints)
            {
              _buffer.endEvents(true, lastScn, false, false, _stats);
            }
            else
            {
              //simulate bootstrap calls (snapshot)
              addBootstrapCheckpointEventToBuffer(lastScn,dataEventCount,1);
            }
            --bootstrapCheckpoints;
          }
          if (!_bufferReflector.validateBuffer())
          {
            throw new RuntimeException("Buffer validation 1 failed");
          }
          if (bootstrapCheckpoints < 0)
          {
            _buffer.startEvents();
            bootstrapCheckpoints = _bootstrapCheckpointPerWindow;
          }
        }
        if (!_bufferReflector.validateBuffer())
        {
          throw new RuntimeException("Buffer validation 2 failed");
        }
        lastScn = evScn;
      }
      dataEventCount = addEventToBuffer(ev, dataEventCount);
      ++_count;
    }
    if ((lastScn != -1) && (maxCount == _events.size()))
    {
      ++_count;
      _buffer.endEvents(lastScn,_stats);
    }
    _dataEvents = dataEventCount;
  }

  public long numDataEvents()
  {
    return _dataEvents;
  }

  /**
   * Alter event fields by XORing against a known fixed value; each invocation toggles between
   * 'good' state and the 'tarnished' (corrupted) state.
   *
   * @param type : event field that will be tarnished
   * @param positions : list of event positions in *sorted order* that will be tarnished
   * @return number of events tarnished
   */
  public int tarnishEventsInBuffer(int[] positions, EventCorruptionType type)
  throws InvalidEventException
  {
    int tarnishedEvents = 0;
    int count = 0;
    int posIndex = 0;
    boolean onlyDataEvents = (type==EventCorruptionType.PAYLOAD);
    for (Iterator<DbusEventInternalWritable> di = _buffer.iterator();
         (posIndex < positions.length) && di.hasNext(); )
    {
      DbusEventInternalWritable ev = di.next();
      if (!onlyDataEvents || !ev.isControlMessage())
      {
        if (count == positions[posIndex])
        {
          DbusEventCorrupter.toggleEventCorruption(type, ev);
          ++tarnishedEvents;
          ++posIndex;
        }
        ++count;
      }
    }
    return tarnishedEvents;
  }

  private final Vector<DbusEvent> _events;
  private final DbusEventBuffer _buffer;
  private long _count;
  protected DbusEventsStatisticsCollector _stats;
  private final double _fraction;
  private final DbusEventBufferReflector _bufferReflector;
  private long _dataEvents=0;
}
