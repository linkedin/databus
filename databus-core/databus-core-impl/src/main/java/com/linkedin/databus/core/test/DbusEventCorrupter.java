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


import java.lang.reflect.Field;
import java.nio.ByteBuffer;

import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventInternalWritable;
import com.linkedin.databus.core.DbusEventSerializable;
import com.linkedin.databus.core.DbusEventV1;
import com.linkedin.databus.core.DbusEventV2;
import com.linkedin.databus.core.InvalidEventException;


/**
 * Methods to corrupt (or uncorrupt) various fields of an event.
 *
 * This class is for testing only!
 *
 * @author snagaraj
 */
public class DbusEventCorrupter
{
  private static final byte CORRUPTION_PATTERN = 0x55; // 01010101

  /**
   * Converts an event (readable or otherwise) to writable for testing.  EEEEEEEVIL!
   *
   * Note that all DbusEvents are fundamentally just little windows into some
   * DbusEventBuffer; the various event classes differ only in the methods they
   * expose.  So pulling the buffer and offset variables out of a readable and
   * using them to construct a writable is effectively the same as typecasting
   * the readable to writable (i.e., "const_cast<DbusEventInternalWritable>()",
   * so to speak).
   *
   * @param event  DbusEvent to be abused
   */
  public static DbusEventInternalWritable makeWritable(DbusEvent event)
  throws InvalidEventException
  {
    byte version = event.getRawBytes().get(0);  // version is always in the first byte  // TODO:  add DbusEvent version() accessor?  seems like good idea...
    ByteBuffer buf;
    int pos;

    try
    {
      Field reflectedBuf      = DbusEventSerializable.class.getDeclaredField("_buf");
      Field reflectedPosition = DbusEventSerializable.class.getDeclaredField("_position");
      reflectedBuf.setAccessible(true);
      reflectedPosition.setAccessible(true);
      buf = (ByteBuffer)reflectedBuf.get(event);
      pos = reflectedPosition.getInt(event);
    }
    catch (Exception e)
    {
      throw new InvalidEventException(e);
    }

    return (version == 0)? new DbusEventV1(buf, pos) : new DbusEventV2(buf, pos);
  }

  /**
   * Toggles corrupted state of event.  Final event state is determined by
   # incoming event and specified subfield of event.
   *
   * @param type   part of event in which to inject corruption
   * @param event  DbusEvent that will be modified
   */
  public static void toggleEventCorruption(EventCorruptionType type, DbusEvent event)
  throws InvalidEventException
  {
    // Regardless of the underlying event type (e.g., readable), we're writing to its
    // internal state, so "convert" it to a writable event.
    DbusEventInternalWritable writableEvent = makeWritable(event);  // bruuuuuuutal...

    switch (type)
    {
      case LENGTH:
        int newSize = writableEvent.size() ^ CORRUPTION_PATTERN;
        writableEvent.setSize(newSize);
        break;
      case HEADERCRC:
        long headerCrc = writableEvent.headerCrc() ^ CORRUPTION_PATTERN;
        writableEvent.setHeaderCrc(headerCrc);
        break;
      case PAYLOAD:
        if (writableEvent.payloadLength() > 0)
        {
          byte[] payload = new byte[writableEvent.payloadLength()];
          writableEvent.value().get(payload);
          payload[0] ^= CORRUPTION_PATTERN;
          writableEvent.setValue(payload);
        }
        break;
      case PAYLOADCRC:
        long payloadCrc = writableEvent.bodyCrc() ^ CORRUPTION_PATTERN;
        writableEvent.setValueCrc(payloadCrc);
        break;
    }
  }

  public enum EventCorruptionType
  {
      LENGTH,
      HEADERCRC,
      PAYLOAD,
      PAYLOADCRC,
      NONE,
  }
}
