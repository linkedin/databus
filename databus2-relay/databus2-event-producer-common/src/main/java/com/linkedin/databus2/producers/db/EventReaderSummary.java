package com.linkedin.databus2.producers.db;
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


import java.util.Formatter;

//TODO add javadocs
public class EventReaderSummary
{
  public static final long NO_EVENTS_SCN = -1;
  public static final String EVENT_LOG_FORMAT = "src:%s(%d) #src:%d #evt:%d scn:%d ms:%d sizeInBytes:%d msEvent:%d msTimeElapsed:%d msQueryExec:%d prodRate:%.2f consRate:%.2f";

  private final short _sourceId;
  private final String _sourceName;
  private final long _endOfPeriodSCN;
  private final int _numberOfEvents;
  /** size of serialized payload, does not include meta data inserted by databus */
  private final long _sizeOfSerializedEvents;
  /** overall time taken to query,retrieve and insert data from the db to the event buffer */
  private final long _readMillis;
  /** time taken to enter retrieved data into the buffer, includes serialization and insertion */
  private final long _msEvent;
  /** earliest timestamp of the row seen in  the batch of _numberOfEvents events.  */
  private final long _timeProdStart;
  /** latest timestamp of the row seen in  the batch of _numberOfEvents events.  */
  private final long _timeProdEnd;
  /** time taken by jdbc 'execute query'  */
  private final long _queryExecTime;

  public long getReadMillis()
  {
    return _readMillis;
  }
  public short getSourceId()
  {
    return _sourceId;
  }
  public String getSourceName()
  {
    return _sourceName;
  }
  public long getEndOfPeriodSCN()
  {
    return _endOfPeriodSCN;
  }
  public int getNumberOfEvents()
  {
    return _numberOfEvents;
  }

  public long getSizeOfSerializedEvents()
  {
    return _sizeOfSerializedEvents;
  }

  public long getEventMillis()
  {
	  return _msEvent;
  }

  public long getQueryExecTime()
  {
	  return _queryExecTime;
  }

  public long getTimeProdStart ()
  {
	  return _timeProdStart;
  }

  public long getTimeProdEnd()
  {
	  return _timeProdEnd;
  }
  /** returns the size of time window in ms, of this batch ( _numberOfEvents)  as seen by the database */
  public long getTimeElapsed()
  {
	  return _timeProdEnd-_timeProdStart;
  }

  public EventReaderSummary(short sourceId,
                            String sourceName,
                            long endOfPeriodSCN,
                            int numberOfEvents,
                            long sizeOfSerializedEvents,
                            long readMillis,
                            long msEvent,
                            long timeProdStart,
                            long timeProdEnd,
                            long queryExecTime)
  {
    _sourceId = sourceId;
    _sourceName = sourceName;
    _endOfPeriodSCN = endOfPeriodSCN;
    _numberOfEvents = numberOfEvents;
    _sizeOfSerializedEvents = sizeOfSerializedEvents;
    _readMillis = readMillis;
    _msEvent = msEvent;
    _timeProdStart = timeProdStart;
    _timeProdEnd = timeProdEnd;
    _queryExecTime = queryExecTime;
  }


  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder(256);
    Formatter fmt = new Formatter(sb);
    long timeElapsed = getTimeElapsed();
    double prodRate = timeElapsed == 0 ? 0 : (double) _sizeOfSerializedEvents/ timeElapsed;
    double consRate = _readMillis == 0 ? 0 : (double) _sizeOfSerializedEvents/ _readMillis;
    fmt.format(EVENT_LOG_FORMAT,_sourceName, _sourceId, 1, _numberOfEvents, _endOfPeriodSCN,
               _readMillis,_sizeOfSerializedEvents,_msEvent,timeElapsed,_queryExecTime,prodRate,consRate);
    fmt.flush();

    return fmt.toString();
  }
}
