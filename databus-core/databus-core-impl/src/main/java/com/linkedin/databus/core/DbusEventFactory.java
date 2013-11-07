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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.log4j.Logger;


public abstract class DbusEventFactory
{
  // We define V1 as 0 because the magic byte in the DbusEvent is treated as version
  // and has to be zero for the first version of DbusEvent.
  public static final byte DBUS_EVENT_V1 = 0;
  public static final byte DBUS_EVENT_V2 = 2;
  public static final byte DBUS_EVENT_CURLY_BRACE = 123;  // JSON-encoded exceptions look like "version 123"
  public static final Logger LOG = Logger.getLogger("DbusEventFactory");

  private final ByteOrder _byteOrder;

  protected DbusEventFactory(ByteOrder byteOrder)
  {
    _byteOrder = byteOrder;
  }

  public abstract byte getVersion();

  public ByteOrder getByteOrder()
  {
    return _byteOrder;
  }

  /**
   * @deprecated DO NOT USE THIS METHOD
   */
  @Deprecated
  public static void setByteOrder(ByteOrder byteOrderForBackwardCompatiblity)
  {
    LOG.warn("This method is deprecated and should not be used. This call has been ignored.");
  }

  /**
   * Creates a writable but empty DbusEvent; for internal use by DbusEventBuffer only.
   * The empty event can be initialized via its reset() method.
   *
   * TODO What is the purpose of this API?  TBD...
   #      [currently used for iterators; expect to switch to createReadOnlyDbusEvent() below in upcoming change]
   *
   * @return DbusEventInternalWritable object
   */
  public abstract DbusEventInternalWritable createWritableDbusEvent();

  /**
   * Serializes an event into the provided ByteBuffer at its current position.
   * Does not change the position of the ByteBuffer after serialization.
   *
   * @param key                  Key of the event to be serialized.
   * @param serializationBuffer  ByteBuffer into which the event is to be serialized.
   * @param dbusEventInfo        Parameters describing the event to be serialized.
   *
   * @return The size of the serialized event.
   * @throws KeyTypeNotImplementedException if the key type specified is not supported by the underlying event format.
   */
  public static int serializeEvent(DbusEventKey key,
                                   ByteBuffer serializationBuffer,
                                   DbusEventInfo dbusEventInfo)
  throws KeyTypeNotImplementedException
  {
    byte version = dbusEventInfo.getEventSerializationVersion();
    if (version == DBUS_EVENT_V1)
    {
      return DbusEventV1.serializeEvent(key, serializationBuffer, dbusEventInfo);
    }
    else if (version == DBUS_EVENT_V2)
    {
      return DbusEventV2.serializeEvent(key, serializationBuffer, dbusEventInfo);
    }
    throw new UnsupportedDbusEventVersionRuntimeException(version);
  }

  /* *
   * Creates a read-only but empty DbusEvent for "public" iterators (i.e., those used
   * outside DbusEventBuffer, including in the bootstrap and the client library).  The
   * empty event can be initialized via its reset() method.
   *
   * @param version Version of the DbusEvent protocol to be created.
   * @return DbusEventInternalWritable object
   */
/* NOT YET USED (but intended for BaseEventIterator and subclasses thereof)
   [when uncommenting this, also re-javadoc-ize above comment]
  public static DbusEventInternalReadable createReadOnlyDbusEvent(byte version);
 */

  /**
   * Creates a writable DbusEvent out of an already initialized (serialized) buffer.
   * (For DbusEventBuffer and testing ONLY!  hmmm...also now used by SendEventsRequest.ExecHandler)
   * (TODO:  enforce this?  refactor?)
   *
   * @param buf        buffer containing the serialized event
   * @param position   byte-offset of the serialized event in the buffer
   * @return  a writable DbusEvent
   */
  public DbusEventInternalWritable createWritableDbusEventFromBuffer(ByteBuffer buf, int position)
  {
    if (buf.order() != getByteOrder())
    {
      throw new DatabusRuntimeException("ByteBuffer byte-order mismatch [DbusEventFactory.createWritableDbusEventFromBuffer(): buf = " + buf.order() + ", this = " + getByteOrder() + "]");
    }
    return DbusEventFactory.createWritableDbusEventFromBufferUnchecked(buf, position);
  }

  /**
   * Creates a writable DbusEvent out of an already initialized (serialized) buffer.
   *
   * @param buf        buffer containing the serialized event
   * @param position   byte-offset of the serialized event in the buffer
   * @return  a writable DbusEvent
   */
  private static DbusEventInternalWritable createWritableDbusEventFromBufferUnchecked(ByteBuffer buf, int position)
  {
    byte version = buf.get(position);
    if (version == DBUS_EVENT_V1)
    {
      return new DbusEventV1(buf, position);
    }
    else if (version == DBUS_EVENT_V2)
    {
      return new DbusEventV2(buf, position);
    }
    else if (version == DBUS_EVENT_CURLY_BRACE)
    {
      // Looks like a JSON-encoding exception (e.g., from the relay if we're the client lib), so try to print it.
      // TODO:  Longer-term, handle exceptions at the client by checking the response header first (DDSDBUS-xxx).
      try
      {
        throw new DatabusRuntimeException("apparent remote exception: " + getStringFromBuffer(buf, position));
      }
      catch (UnsupportedEncodingException ex)
      {
        // can't happen; UTF-8 always supported ... but will fall through anyway
      }
    }
    throw new UnsupportedDbusEventVersionRuntimeException(version);
  }

  /**
   * Quick and dirty method to pick out the ASCII characters from a ByteBuffer and stuff them into a string.
   *
   * @param buf        buffer containing the (presumed) JSON-encoded exception in place of a serialized event
   * @param position   byte-offset of the JSON string in the buffer
   * @return  string containing the printable ASCII characters (up to the first non-ASCII one)
   */
  static String getStringFromBuffer(ByteBuffer buf, int position)  // package-private for unit-testing
  throws UnsupportedEncodingException
  {
    byte[] arr;
    int idxFirstPrintable, idxLastPrintable;

    if (buf.hasArray())
    {
      arr = buf.array();
      idxFirstPrintable = position + buf.arrayOffset();
    }
    else // do a copy, sigh
    {
      ByteBuffer roBuf = buf.asReadOnlyBuffer();  // don't want to screw up original buffer's position with get()
      int length = roBuf.position(position).remaining();
      arr = new byte[length];
      roBuf.get(arr);
      idxFirstPrintable = 0;
    }

    assert arr[idxFirstPrintable] == DBUS_EVENT_CURLY_BRACE;
    for (idxLastPrintable = idxFirstPrintable; idxLastPrintable < arr.length; ++idxLastPrintable)
    {
      byte b = arr[idxLastPrintable];
      // accept only space (32) and above, except DEL (127), plus tab (9), newline (10), and CR (13)
      if (((b < 32) && (b != 9) && (b !=10) && (b != 13)) || (b == 127))
      {
        break;
      }
    }
    // idxLastPrintable points either just past the end of the array or at the first unprintable byte
    int length = Math.min(idxLastPrintable-idxFirstPrintable, 512);
    return new String(arr, idxFirstPrintable, length, "UTF-8");  // ASCII is pure subset of UTF-8
  }

  /**
   * Creates a read-only DbusEvent out of an already initialized (serialized) buffer.
   *
   * @param buf        buffer containing the serialized event
   * @param position   byte-offset of the serialized event in the buffer
   * @return  a read-only DbusEvent
   */
  public DbusEventInternalReadable createReadOnlyDbusEventFromBuffer(ByteBuffer buf, int position)
  {
    return createWritableDbusEventFromBuffer(buf, position);
  }

  /**
   * Creates a read-only DbusEvent out of an already initialized (serialized) buffer.
   * Callers are STRONGLY encouraged to use the non-static version of this wherever
   * possible so we can compare the factory's byte order against the buffer's.  A
   * mismatch would be Very Bad.
   *
   * @param buf        buffer containing the serialized event
   * @param position   byte-offset of the serialized event in the buffer
   * @return  a read-only DbusEvent
   */
  public static DbusEventInternalReadable createReadOnlyDbusEventFromBufferUnchecked(ByteBuffer buf, int position)
  {
    return DbusEventFactory.createWritableDbusEventFromBufferUnchecked(buf, position);
  }

  public static int computeEventLength(DbusEventKey key, DbusEventInfo eventInfo)
  throws KeyTypeNotImplementedException
  {
    if (eventInfo.getEventSerializationVersion() == DBUS_EVENT_V1)
    {
      return DbusEventV1.length(key, eventInfo.getValueLength());
    }
    else if (eventInfo.getEventSerializationVersion() == DBUS_EVENT_V2)
    {
      return DbusEventV2.computeEventLength(key, eventInfo);
    }
    throw new UnsupportedDbusEventVersionRuntimeException(eventInfo.getEventSerializationVersion());
  }

  public abstract int serializeLongKeyEndOfPeriodMarker(ByteBuffer buffer, DbusEventInfo eventInfo);

  /**
   * Creates an EOP event with a given sequence and partition number.
   *
   * @return the event object
   */
  public abstract DbusEventInternalReadable createLongKeyEOPEvent(long seq, short partN);

  // This method works (and must be used) for all control events except EOP events. EOP events have
  // variants with/without external-replication-bit set, etc., that needs to be ironed out. (DDSDBUS-2296)
  // Some EOP events also need to have a timestamp specified by the caller. See serializeLongKeyEndOfPeriodMarker()
  protected abstract DbusEventInternalReadable createLongKeyControlEvent(long sequence, short srcId, String msg);

  /**
   * Create a checkpoint event in the version specified. The event will not have extReplicated bit or
   * any other special attributes set.
   */
  public DbusEventInternalReadable createCheckpointEvent(Checkpoint checkpoint)
  {
    return createLongKeyControlEvent(0L,  // sequence
                                     DbusEventInternalWritable.CHECKPOINT_SRCID,
                                     checkpoint.toString());
  }

  /**
   * Create an error event in the version specified. The event will not have extReplicated bit or
   * any other special attributes set.
   */
  public DbusEventInternalReadable createErrorEvent(DbusErrorEvent dbusErrorEvent)
  {
    return createLongKeyControlEvent(0L,  // sequence
                                     dbusErrorEvent.getErrorId(),
                                     dbusErrorEvent.toString());
  }

  /**
   * Create an SCN regression event in the version specified. The event will not have extReplicated bit or
   * any other special attributes set.
   */
  public DbusEvent createSCNRegressEvent(SCNRegressMessage scnRegressMessage)
  {
    return createLongKeyControlEvent(scnRegressMessage.getCheckpoint().getWindowScn(),
                                     DbusEventInternalWritable.SCN_REGRESS,
                                     SCNRegressMessage.toJSONString(scnRegressMessage));
  }
}
