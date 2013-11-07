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


import java.util.Collections;
import java.util.Formatter;
import java.util.List;

public class ReadEventCycleSummary
{
  private final List<EventReaderSummary> _sourceSummaries;
  private final long _endOfWindowScn;
  private final int _totalEventNum;
  /** overall time taken to query,retrieve and insert data from the db to the event buffer */
  private final long _readMillis;
  /** time taken to enter retrieved data into the buffer, includes serialization and insertion */
  private long _eventMillis;
  private final String _eventSourceName;
  /** total size of serialized payload across sources */
  private long _sizeInBytes;
  /** the size of time window in ms, of this batch ( _numberOfEvents)  as seen by the database ,across sources */
  private long _elapsedTimeMillis;
  /** time taken by jdbc 'execute query'  */
  private long _queryTimeMillis;

  public String getEventSourceName()
  {
    return _eventSourceName;
  }

  public long getReadMillis()
  {
    return _readMillis;
  }

  public int getTotalEventNum()
  {
    return _totalEventNum;
  }

  public long getEventMillis()
  {
	  return _eventMillis;
  }
  public long getElapsedTimeMillis()
  {
	  return _elapsedTimeMillis;
  }

  public long getQueryTimeMillis()
  {
	  return _queryTimeMillis;
  }

  public List<EventReaderSummary> getSourceSummaries()
  {
    return _sourceSummaries;
  }

  public long getEndOfWindowScn()
  {
    return _endOfWindowScn;
  }

  public long getTotalEventSizeInBytes()
  {
	  return _sizeInBytes;
  }

  public ReadEventCycleSummary(String eventSourceName,
                               List<EventReaderSummary> sourceSummaries,
                               long endOfWindowScn,
                               long readMillis)
  {
    _eventSourceName = eventSourceName;
    _sourceSummaries = Collections.unmodifiableList(sourceSummaries);
    _endOfWindowScn = endOfWindowScn;
    _readMillis = readMillis;
    long minTimeProdStart = Long.MAX_VALUE;
    long maxTimeProdEnd = 0;
    long queryTimeMillis = 0;

    int sum = 0; long evSerMillis = 0; long size=0;
    for(EventReaderSummary sourceSummary : _sourceSummaries)
    {
      sum += sourceSummary.getNumberOfEvents();
      evSerMillis += sourceSummary.getEventMillis();
      size += sourceSummary.getSizeOfSerializedEvents();
      maxTimeProdEnd = Math.max(maxTimeProdEnd, sourceSummary.getTimeProdEnd());
      minTimeProdStart = Math.min(minTimeProdStart, sourceSummary.getTimeProdStart());
      queryTimeMillis += sourceSummary.getQueryExecTime();

    }
    _elapsedTimeMillis = maxTimeProdEnd - minTimeProdStart;
    _totalEventNum = sum;
    _eventMillis = evSerMillis;
    _sizeInBytes = size;
    _queryTimeMillis = queryTimeMillis;
  }

  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder(256);
    Formatter fmt = new Formatter(sb);
    double prodRate = _elapsedTimeMillis == 0 ? 0 : (double) _sizeInBytes/_elapsedTimeMillis;
    double consRate = _readMillis == 0 ? 0 : (double) _sizeInBytes/ _readMillis;
    fmt.format(EventReaderSummary.EVENT_LOG_FORMAT, _eventSourceName, 0, _sourceSummaries.size(),
               _totalEventNum, _endOfWindowScn, _readMillis, _sizeInBytes, _eventMillis, _elapsedTimeMillis, _queryTimeMillis,prodRate,consRate);
    fmt.flush();

    return fmt.toString();
  }

}
