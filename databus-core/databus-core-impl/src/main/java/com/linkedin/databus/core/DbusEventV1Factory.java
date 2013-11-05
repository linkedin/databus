package com.linkedin.databus.core;
/*
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
 */

import java.io.UnsupportedEncodingException;
import java.lang.StackTraceElement;
import java.lang.Thread;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.log4j.Logger;


public class DbusEventV1Factory extends DbusEventFactory
{
  public static final Logger LOG = Logger.getLogger("DbusEventV1Factory");

  public DbusEventV1Factory()
  {
    this(ByteOrder.BIG_ENDIAN);
  }

  public DbusEventV1Factory(ByteOrder byteOrder)
  {
    super(byteOrder);
    LOG.info(byteOrder);

    // Unit tests frequently use short constructors and thereby end up creating
    // multiple, conflicting event factories.  This helps figure out where they
    // all come from.
    if (LOG.isDebugEnabled())
    {
      StringBuilder sb = new StringBuilder();
      for (StackTraceElement ste : Thread.currentThread().getStackTrace())
      {
        sb.append("\n\t").append(ste);
      }
      LOG.info(sb);
    }
  }

  @Override
  public byte getVersion()
  {
    return DbusEventFactory.DBUS_EVENT_V1;
  }

  @Override
  public DbusEventInternalWritable createWritableDbusEvent()
  {
    return new DbusEventV1();
  }

/* NOT YET USED (but intended for BaseEventIterator and subclasses thereof)
  @Override
  public static DbusEventInternalReadable createReadOnlyDbusEvent()
  {
    return new DbusEventV1();
  }
 */

  @Override
  public int serializeLongKeyEndOfPeriodMarker(ByteBuffer buffer, DbusEventInfo eventInfo)
  {
    if (buffer.order() != getByteOrder())
    {
      throw new DatabusRuntimeException("ByteBuffer byte-order mismatch [DbusEventV1Factory.serializeLongKeyEndOfPeriodMarker()]");
    }
    if (eventInfo.getEventSerializationVersion() != DbusEventFactory.DBUS_EVENT_V1)
    {
      throw new UnsupportedDbusEventVersionRuntimeException(eventInfo.getEventSerializationVersion());
    }
    return DbusEventV1.serializeEndOfPeriodMarker(buffer, eventInfo);
  }

  @Override
  public DbusEventInternalReadable createLongKeyEOPEvent(long seq, short partN)
  {
    ByteBuffer buf;
    buf = DbusEventV1.serializeEndOfPeriodMarker(seq, partN, getByteOrder());
    return super.createReadOnlyDbusEventFromBuffer(buf, 0);
  }

  // This method works (and must be used) for all control events except EOP events. EOP events have
  // variants with/without external-replication-bit set, etc. that needs to be ironed out. (DDSDBUS-2296)
  // Some EOP events also need to have a timestamp specified by the caller. See serializeLongKeyEndOfPeriodMarker()
  @Override
  protected DbusEventInternalReadable createLongKeyControlEvent(long sequence, short srcId, String msg)
  {
    DbusEventInfo eventInfo;
    try
    {
      byte[] payload = msg.getBytes("UTF-8");
      eventInfo = new DbusEventInfo(null,       // opcode
                                    sequence,
                                    (short)-1,  // physical partition
                                    (short) 0,  // logical partition
                                    System.nanoTime(),
                                    srcId,
                                    DbusEventInternalWritable.emptyMd5,
                                    payload,
                                    false,      // enable tracing
                                    true,       // autocommit
                                    DbusEventFactory.DBUS_EVENT_V1,
                                    (short)0,   // payload schema version
                                    null);      // metadata
    }
    catch (UnsupportedEncodingException e)
    {
      throw new DatabusRuntimeException("Non-UTF-8 characters in control event: " + msg, e);
    }

    DbusEventKey key = DbusEventInternalWritable.ZeroLongKeyObject;

    int evtLen;
    try
    {
      evtLen = computeEventLength(key, eventInfo);
      ByteBuffer tmpBuffer = ByteBuffer.allocate(evtLen).order(getByteOrder());
      // TODO Go through the common code in the V2 version (serializeEvent()) for V1 as well (DDSDBUS-2282)
      DbusEventV1.serializeFullEventWithEmptyAttributes(DbusEventInternalWritable.ZeroLongKey,
                                                        tmpBuffer,
                                                        eventInfo);
      return super.createReadOnlyDbusEventFromBuffer(tmpBuffer, 0);
    }
    catch (KeyTypeNotImplementedException e)
    {
      throw new DatabusRuntimeException("Unexpected exception on key " + key, e);
    }
  }
}
