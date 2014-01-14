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
import java.nio.charset.Charset;
import java.util.Vector;

import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventFactory;
import com.linkedin.databus.core.DbusEventInfo;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.DbusEventV1Factory;
import com.linkedin.databus.core.DbusOpcode;
import com.linkedin.databus.core.KeyTypeNotImplementedException;
import com.linkedin.databus.core.test.DbusEventFactoryForTesting;
import com.linkedin.databus.core.util.RngUtils;

/**
 * Utility to generate a specified number of DbusEvents.
 *
 * This class is for TESTING ONLY!
 *
 * @author snagaraj
 */
public class DbusEventGenerator
{

  private final long _startScn;
  private final DbusEventFactory _eventFactory;
  private Vector<Short> _srcIdList = null;

  public DbusEventGenerator()
  {
    this(0, null);
  }

  public DbusEventGenerator(long startScn)
  {
    this(startScn, null);
  }

  public DbusEventGenerator(long startScn, Vector<Short> srcIdList)
  {
    _startScn = startScn;
    _srcIdList= srcIdList;
    _eventFactory = new DbusEventV1Factory();
  }

  /**
   * Generate specified number of constant sized events.
   *
   * @param numEvents : Number of events desired
   * @param windowSize : Max window size (transaction size)
   * @param maxEventSize : maximum event size expected
   * @param payloadSize : payload size in bytes
   * @param useLastEventAsScn : if true, use count of last event of window as window scn; else use i/windowSize+1
   * @param eventVector : output container that is populated with the events
   * @return last window number generated
   */
  public long generateEvents(int numEvents,
                             int windowSize,
                             int maxEventSize,
                             int payloadSize,
                             boolean useLastEventAsScn,
                             Vector<DbusEvent> eventVector)
  {
    long lastScn = 0;
    try
    {
      long beginningOfTime = System.currentTimeMillis()/1000;
      beginningOfTime *= 1000;
      short srcId = 1;
      for (int i=0 ; i < numEvents; ++i)
      {
        if (_srcIdList != null && _srcIdList.size() > 0)
        {
          int srcIdIndex =  RngUtils.randomPositiveInt() % _srcIdList.size();
          srcId = _srcIdList.get(srcIdIndex);
        }
        else
        {
          srcId = RngUtils.randomPositiveShort();
        }
        if (srcId == 0) {
          //0 srcId not allowed
          srcId = 1;
        }
        //assumption: serialized event fits in maxEventSize
        ByteBuffer buf = ByteBuffer.allocate(maxEventSize).order(_eventFactory.getByteOrder());
        DbusEventInfo eventInfo = new DbusEventInfo(DbusOpcode.UPSERT,
                                                    0L, // sequence number
                                                    (short) 0, // physical Partition
                                                    RngUtils.randomPositiveShort(),
                                                    (beginningOfTime - ((numEvents - i) * 1000)) * 1000 * 1000,
                                                    //nanoseconds ; first event is numEvents seconds ago
                                                    srcId,
                                                    RngUtils.schemaMd5,
                                                    RngUtils.randomString(payloadSize).getBytes(Charset.defaultCharset()),
                                                    false,
                                                    false /*autoCommit */);
        eventInfo.setEventSerializationVersion(DbusEventFactory.DBUS_EVENT_V1);  // make this explicit
        DbusEventFactory.serializeEvent(new DbusEventKey(RngUtils.randomLong()), buf, eventInfo);
        lastScn = (useLastEventAsScn) ? _startScn + ((i/windowSize) + 1) * (long)windowSize
                                      : _startScn + (i/windowSize) + 1;
        eventVector.add((DbusEvent)
            DbusEventFactoryForTesting.createReadOnlyDbusEventFromBuffer(buf, 0, lastScn, DbusEventFactory.DBUS_EVENT_V1));
      }
    }
    catch (KeyTypeNotImplementedException e)
    {
      // ignore
    }
    return lastScn;
  }

  /**
   * Generate specified number of constant sized events.
   *
   * @param numEvents : Number of events desired
   * @param windowSize : Max window size (transaction size)
   * @param maxEventSize : maximum event size expected
   * @param payloadSize : payload size in bytes
   * @param eventVector : output container that is populated with the events
   * @return last window number generated
   */
  public long generateEvents(int numEvents,
                             int windowSize,
                             int maxEventSize,
                             int payloadSize,
                             Vector<DbusEvent> eventVector)
  {
    return generateEvents(numEvents,windowSize,maxEventSize,payloadSize,false,eventVector);
  }

}
