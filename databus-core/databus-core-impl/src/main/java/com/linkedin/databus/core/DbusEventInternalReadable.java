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

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;



/**
 * Read-only interface for Databus events, used by
 * internal Databus classes.
 **/
public abstract class DbusEventInternalReadable extends DbusEvent
{
  /**
   * @return version of the event. Currently supported versions are 0 and 2.
   * @Note this is the version of the DbusEvent serialization and has nothing to do with
   * Databus 2.0 or Databus 3.0.
   */
  public abstract byte getVersion();
  public abstract int getMagic();

  public abstract boolean isErrorEvent();
  /** Resets this object to point to a ByteBuffer that holds an event */
  public abstract DbusEventInternalReadable reset(ByteBuffer buf, int position);

  public abstract long headerCrc();
  protected abstract EventScanStatus scanEvent(boolean logErrors);

  /**
   * <pre>
   * In DbusEventV1:
   *   For String keys, payloadLength() returns the length of key plus length of payload.
   *   For long keys, payloadlength() returns the length of payload.
   *   payloadLength() is the number of bytes that span the "payload" CRC in DbusEventV1.
   * In DbusEventV2:
   *   payloadLength() and valueLength will return the same, because we do not do any special
   *   CRC computation depending depending on string or long (or other) key types.
   * </pre>
   */
  public abstract int payloadLength();

  public abstract long bodyCrc();
  public abstract long getCalculatedValueCrc();
  public abstract DbusEventInternalWritable createCopy();
  public abstract int writeTo(WritableByteChannel writeChannel, Encoding encoding);

  /**
   * @return the version of the payload schema
   */
  public abstract short schemaVersion();
  public abstract boolean isValid(boolean logErrors);
  protected abstract HeaderScanStatus scanHeader(boolean logErrors);
  protected abstract boolean isPartial();
  public abstract boolean isControlSrcId();
  /** length of the string key */
  public abstract int keyBytesLength();

  public abstract HeaderScanStatus scanHeader();
  public abstract EventScanStatus scanEvent();
  /**
   *
   * @author snagaraj
   *  used to determine status of event when it is read;
   */
  public enum HeaderScanStatus {
    OK,
    ERR,
    PARTIAL,
  }

  /**
   *
   * @author snagaraj
   * used to signal status of event when it is read;
   */
  public enum EventScanStatus {
    OK,
    ERR,
    PARTIAL,
  }

  /**
   *
   * Return DbusEventKey object containing key for the event.
   *
   * @returns key present in the event as DbusEventKey object.
   *          null if event type is not one of (String, Number or Schema)
   */
  public DbusEventKey getDbusEventKey()
  {
    if ( isKeyNumber())
      return new DbusEventKey(key());
    else if ( isKeyString())
      return new DbusEventKey(keyBytes());
    else if ( isKeySchema())
    {
      try
      {
        return new DbusEventKey(getKeyPart());
      } catch (UnsupportedKeyException uke) {
        return null;
      }
    }
    return null;
  }
}
