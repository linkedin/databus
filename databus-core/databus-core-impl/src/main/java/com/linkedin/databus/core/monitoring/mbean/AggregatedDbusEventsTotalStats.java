package com.linkedin.databus.core.monitoring.mbean;
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



import com.linkedin.databus.core.monitoring.events.DbusEventsTotalStatsEvent;
import java.util.concurrent.locks.Lock;
import org.apache.log4j.Logger;


/**
 * A class to keep track of aggregated events across DbusEventsTotalStats across multiple buffers
 * aggregated by source, peer or just buffers in dbusMulti (physicalSource)
 */
public class AggregatedDbusEventsTotalStats extends DbusEventsTotalStats
{
  public static final String MODULE = AggregatedDbusEventsTotalStats.class.getName();
  private final Logger _log;
  public AggregatedDbusEventsTotalStats(int ownerId,
                                        String dimension,
                                        boolean enabled,
                                        boolean threadSafe,
                                        DbusEventsTotalStatsEvent initData)
  {
    super(ownerId, dimension, enabled, threadSafe, initData);
    _log = Logger.getLogger(MODULE + "." + dimension);
  }

  
  
  public AggregatedDbusEventsTotalStats clone(boolean threadSafe)
  {
	  AggregatedDbusEventsTotalStats s = new AggregatedDbusEventsTotalStats(_event.ownerId, _dimension, _enabled.get(), threadSafe, null);
	  //copy this to s
	  cloneData(s._event);
	  return s;
  }
  
  public long getMinTimeSinceLastAccess()
  {
    Lock readLock = acquireReadLock();
    long result = 0;
    try
    {
      result = System.currentTimeMillis() - _event.maxTimestampAccessed;
    }
    finally
    {
      releaseLock(readLock);
    }
    return result;
  }

  @Override
  public long getMaxTimeSinceLastAccess()
  {
    Lock readLock = acquireReadLock();
    long result = 0;
    try
    {
      result = System.currentTimeMillis() - _event.minTimestampAccessed;
    }
    finally
    {
      releaseLock(readLock);
    }
    return result;
  }

  @Override
  public long getMinTimeSinceLastEvent()
  {
    Lock readLock = acquireReadLock();
    long result = 0;
    try
    {
      result = System.currentTimeMillis() - _event.maxTimestampMaxScnEvent;
    }
    finally
    {
      releaseLock(readLock);
    }
    return result;
  }

  @Override
  public long getMaxTimeSinceLastEvent()
  {
    Lock readLock = acquireReadLock();
    long result = 0;
    try
    {
      result = System.currentTimeMillis() - _event.minTimestampMaxScnEvent;
    }
    finally
    {
      releaseLock(readLock);
    }
    return result;
  }

  @Override
  public long getMinTimeSpan()
  {
    Lock readLock = acquireReadLock();
    long result = 0;
    try
    {
      result = _event.minTimeSpan;
    }
    finally
    {
      releaseLock(readLock);
    }
    return result;
  }

  @Override
  public long getMaxTimeSpan()
  {
    Lock readLock = acquireReadLock();
    long result = 0;
    try
    {
      result = _event.maxTimeSpan;
    }
    finally
    {
      releaseLock(readLock);
    }
    return result;
  }

  @Override
  public long getMinTimeLag()
  {
    Lock readLock = acquireReadLock();
    try
    {
      return _event.minTimeLag;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  @Override
  public long getMaxTimeLag()
  {
    Lock readLock = acquireReadLock();
    try
    {
      return _event.maxTimeLag;
    }
    finally
    {
      releaseLock(readLock);
    }
  }

  /* If EspressoRelay, we would like to do the following, but we need to keep backward compatibility */

//  @Override
//  public long getTimestampMinScnEvent()
//  {
//    Lock readLock = acquireReadLock();
//    long result = 0;
//    try
//    {
//      result = _event.timestampMinScnEvent; // Timestamp of oldest event amongst all buffers
//    }
//    finally
//    {
//      releaseLock(readLock);
//    }
//    return result;
//  }
//
//  @Override
//  public long getTimestampMaxScnEvent()
//  {
//    Lock readLock = acquireReadLock();
//    long result = 0;
//    try
//    {
//      result = _event.timestampMaxScnEvent; // Timestamp of newest event amongst all buffers
//    }
//    finally
//    {
//      releaseLock(readLock);
//    }
//    return result;
//  }

  @Override
  protected void doMergeStats(Object eventData)
  {
    if (! (eventData instanceof DbusEventsTotalStatsEvent))
    {
      _log.warn("Attempt to merge unknown event class" + eventData.getClass().getName());
      return;
    }
    DbusEventsTotalStatsEvent e = (DbusEventsTotalStatsEvent)eventData;

    /** Allow use negative relay IDs for aggregation across multiple relays */
    if (_event.ownerId > 0 && e.ownerId != _event.ownerId)
    {
      _log.warn("Attempt to data for a different relay " + e.ownerId);
      return;
    }

    _event.numDataEvents += e.numDataEvents;
    _event.sizeDataEvents += e.sizeDataEvents;
    _event.sizeDataEventsPayload += e.sizeDataEventsPayload;
    _event.numDataEventsFiltered += e.numDataEventsFiltered;
    _event.sizeDataEventsFiltered += e.sizeDataEventsFiltered;
    _event.sizeDataEventsPayloadFiltered += e.sizeDataEventsPayloadFiltered;
    _event.maxSeenWinScn = Math.max(_event.maxSeenWinScn, e.maxSeenWinScn);
    _event.minSeenWinScn = Math.min(_event.minSeenWinScn, e.minSeenWinScn);
    _event.numSysEvents += e.numSysEvents;
    _event.sizeSysEvents += e.sizeSysEvents;
    _event.numErrHeader += e.numErrHeader;
    _event.numErrPayload += e.numErrPayload;
    _event.numInvalidEvents += e.numInvalidEvents;
    _event.timestampAccessed = Math.max(_event.timestampAccessed,e.timestampAccessed);
    _event.timestampCreated = Math.min(_event.timestampCreated,e.timestampCreated);
    _event.sinceWinScn = e.sinceWinScn > 0 ? Math.min(_event.sinceWinScn, e.sinceWinScn) : _event.sinceWinScn;
    _event.minWinScn = e.minWinScn > 0 ? Math.min(_event.minWinScn,e.minWinScn): _event.minWinScn;
    _event.maxWinScn = Math.max(_event.maxWinScn,e.maxWinScn);
    _event.maxFilteredWinScn = Math.max(_event.maxFilteredWinScn, e.maxFilteredWinScn);
    _event.timestampMaxScnEvent = Math.max(_event.timestampMaxScnEvent, e.timestampMaxScnEvent);
    _event.timestampMinScnEvent = e.timestampMinScnEvent > 0 ? Math.min(_event.timestampMinScnEvent, e.timestampMinScnEvent): _event.timestampMinScnEvent;
    _event.numFreeBytes = (_event.numFreeBytes > 0) ?  Math.min(_event.numFreeBytes, e.numFreeBytes) : e.numFreeBytes;
    _event.latencyEvent += e.latencyEvent;
    // numPeers cannot be merged

    long msecs = e.timestampMaxScnEvent - e.timestampMinScnEvent;
    _event.minTimeSpan = Math.min(msecs, _event.minTimeSpan);
    _event.maxTimeSpan = Math.max(msecs, _event.maxTimeSpan);

    _event.minTimestampAccessed = Math.min(e.timestampAccessed, _event.minTimestampAccessed);
    _event.maxTimestampAccessed = Math.max(e.timestampAccessed, _event.maxTimestampAccessed);

    _event.minTimestampMaxScnEvent = Math.min(e.timestampMaxScnEvent, _event.minTimestampMaxScnEvent);
    _event.maxTimestampMaxScnEvent = Math.max(e.timestampMaxScnEvent, _event.maxTimestampMaxScnEvent);

    _event.minTimeLag = Math.min(e.timeLag, _event.minTimeLag);
    _event.maxTimeLag = Math.max(e.timeLag, _event.maxTimeLag);
  }

  /* If EspressoRelay, we would like to do the following, but we need to keep backward compat. */

//  @Override
//  public long getTimeSinceLastAccess()
//  {
//    return getMinTimeSinceLastAccess(); // Most recent access amongst all buffers
//  }
//
//  @Override
//  public long getTimeSpan()
//  {
//    return 0L;
//  }
//
//  @Override
//  public long getTimeSinceLastEvent()
//  {
//    return getMinTimeSinceLastEvent();  // Most recent event written or read from all the buffers.
//  }
//
//  @Override
//  public long getPrevScn()
//  {
//    return 0L;
//  }
//
//  @Override
//  public long getMaxScn()
//  {
//    return 0L;
//  }
//
//  @Override
//  public long getMinScn()
//  {
//    return 0L;
//  }
//
//  @Override
//  public long getMinSeenWinScn()
//  {
//    return 0L;
//  }
//
//  @Override
//  public long getMaxFilteredWinScn()
//  {
//    return 0L;
//  }
//
//  @Override
//  public long getMaxSeenWinScn()
//  {
//    return 0L;
//  }
}
