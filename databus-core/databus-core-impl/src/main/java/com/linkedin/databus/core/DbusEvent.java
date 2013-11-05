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

/**
 * Read-only interface for Databus events.
 **/
public abstract class DbusEvent
{
  public enum SchemaDigestType
  {
    MD5,
    // The algorithm to be used for computing CRC32 is not com.linkedin.databus.core.util.ByteBufferCRC32
    // but standard CRC32 (java.util.zip.CRC32).
    CRC32,
  }

  public final static int MD5_DIGEST_LEN = 16;
  public final static int CRC32_DIGEST_LEN = 4;

  /**
   * @return true if this event is an "externally replicated" event. In case of espresso,this bit is
   * set when the source row is modified by applying a change from another data center
   * (as opposed to modification due to application write/update in the local data center).
   */
  public abstract boolean isExtReplicatedEvent();

  /** Returns true iff the event points to a valid Databus event */
  public abstract boolean isValid();

  //Event type tests

  /** Checks if the event is a control message */
  public abstract boolean isControlMessage();

  /** Checks if the event is a private control message */
  public abstract boolean isPrivateControlMessage();

  /** Checks if the event is a checkpoint message */
  public abstract boolean isCheckpointMessage();

  /** Returns true iff the key of the event is a numeric (long) */
  public abstract boolean isKeyNumber();

  /** Returns true iff the key is a string (byte sequence) */
  public abstract boolean isKeyString();

  /** Returns true if the key is of type schema. */
  public abstract boolean isKeySchema();

  /** Checks if the event is a SCNRegressMessage */
  public abstract boolean isSCNRegressMessage();


  // Attribute tests

  /** Returns true if tracing has been enabled for the event */
  public abstract boolean isTraceEnabled();

  /** Checks if the event denotes the end of an event window */
  public abstract boolean isEndOfPeriodMarker();

  // Event fields

  /** Returns the opcode of the data event; null for non-data events */
  public abstract DbusOpcode getOpcode();

  /** Returns the creation timestamp of the event in nanoseconds from Unix epoch */
  public abstract long timestampInNanos();

  /**
   *  Returns the total size of the event binary serialization including header, metadata and payload.
   */
  public abstract int size();

  /** Returns the sequence number (SCN) of the event */
  public abstract long sequence();

  /** Returns key value for events with numeric keys; undefined for events with string keys. */
  public abstract long key();

  /**
   * @deprecated caller should use getKeyBytes().length.
   *  In V1,  Returns length+4 if sting key, 8 if long key.
   *  V2 will throw an Exception
   */
  @Deprecated
  public abstract int keyLength();

  /** Returns the key value for events with string keys; undefined for events with numeric keys. */
  public abstract byte[] keyBytes();

  /** @deprecated Returns the Databus source id of the event.
   * This method is deprecated in favor of getSourceId()
   */
  @Deprecated
  public abstract short srcId();

  /**
   * As of DbusEvent version 2, sourceId is an int instead of short.
   * SourceIds from DbusEventV1 are guaranteed to be short, but it is strongly suggested that callers
   * change their code to handle an int instead of short.
   *
   * @return source id as an integer.
   */
  public abstract int getSourceId();

  /** @deprecated Returns the physical partition id for the event. Call getPartitionId() instead. */
  @Deprecated
  public abstract short physicalPartitionId();

  /** @deprecated Returns the logical partition id for the event. Call getPartitionId() instead. */
  @Deprecated
  public abstract short logicalPartitionId();

  /**
   * @return The partition id for the event.
   */
  public abstract short getPartitionId();

  /**
   * Returns a byte array with the hash id of the event serialization schema.
   * <p> NOTE: this will most likely lead to a memory allocation. The preferred way to access the
   * schema id is through {@link DbusEvent#schemaId(byte[])}. </p>
   * TODO Deprecate this when getPayloadPart() is fully implemented.
   * */
  public abstract byte[] schemaId();

  /**
   * Stores the hash id of the event serialization schema in an existing byte array.
   * <p>NOTE: The byte array should be at least 16 bytes long. </p>
   * TODO Deprecate this when getPayloadPart() is fully implemented.
   * */
  public abstract void schemaId(byte[] md5);

  /** Returns the length (in bytes) of the data event value (data payload).
   * TODO Deprecate this when getPayloadPart() is fully implemented.
   */
  public abstract int valueLength();

  /**
   * Returns the data payload of the event, i.e., the serialized event minus the header, key,
   * and any metadata.  The returned ByteBuffer is read-only, and its position and limit may
   * be freely modified.
   * <b>NOTE: The data may be subsequently overwritten; if you need it beyond the onDataEvent()
   * call, save your own copy before returning.</b>
   * TODO Deprecate this when getPayloadPart() is fully implemented.
   */
  public abstract ByteBuffer value();

  /**
   * Returns a copy of the serialized event with the (implicitly) specified byte order.
   * TODO Deprecate this when getPayloadPart() is fully implemented.
   */
  public abstract ByteBuffer getRawBytes();

  /**
   * @return The payload part of the DbusEvent that has the payload bytes, and the schema digest and version
   * with which the payload bytes were encoded.
   */
  public abstract DbusEventPart getPayloadPart();

  /**
   *
   * @return The payload-metadata part of the DbusEvent that has the metadata bytes, and the schema digest and
   * version with which the metadata bytes were encoded. Returns null if metadata is not present or
   * dbus event version does not support metadata.
   */
  public abstract DbusEventPart getPayloadMetadataPart();

  // TODO Add a getKeyPart(), but then we have a DbusEventKey class, so we need to figure out how these two play
  // with each other. But then we may not need it until we support schema keys, so we may be ok for now.

  /**
   * Returns the DbusEventPart that corresponds to the key of the event (valid only if the key is of schema type).
   * See isKeySchema();
   * @throws DatabusRuntimeException if this method is called and the key is not of schema type.
   * @return
   */
  public abstract DbusEventPart getKeyPart();
}
