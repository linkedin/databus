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
import java.nio.ByteOrder;

import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;

import com.linkedin.databus.core.util.ByteBufferCRC32;
import com.linkedin.databus.core.util.StringUtils;
import com.linkedin.databus.core.util.TimeUtils;
import com.linkedin.databus.core.util.Utils;

/**
 * This class represents a Databus event stored in a {@link java.nio.ByteBuffer}.
 *
 * <h3>Binary Serialization Format </h3>
 *
 * The table below summarizes the serialization format of Databus events as stored into memory and
 * sent over the wire.
 *
 * <table border="1" >
 * <tr> <th>Field</th> <th> Offset </th> <th>Type</th> <th>Size</th> <th>Description</th> </tr>
 * <tr>
 *    <th colspan="5">Header (61 bytes for events with long keys, 57 bytes + key length)</th>
 * </tr>
 * <tr> <td>Version</td> <td>0</td> <td>byte</td> <td>1</td> <td> A value denoting the
 * version of the message format</td> </tr>
 * <tr> <td>HeaderCrc</td> <td>1</td> <td>int</td> <td>4</td> <td>CRC computed over the header</td> </tr>
 * <tr> <td>Length</td> <td>5</td> <td>int</td> <td>4</td> <td>Total length of the event in bytes
 * (fixed-length header + variable-length payload) </td> </tr>
 * <tr> <td>Attributes</td> <td>9</td> <td>short</td> <td>2</td> <td>Event attributes bitmap (see
 * below)</td> </tr>
 * <tr> <td>Sequence</td> <td>11</td> <td>long</td> <td>8</td> <td>Sequence number for the event
 * window in which this event was generated</td> </tr>
 * <tr> <td>PhysicalPartitionId</td> <td>19</td> <td>short</td> <td>2</td> <td>physical partition-id
 * -> represents a sequence generator</td> </tr>
 * <tr> <td>LogicalPartitionId</td> <td>21</td> <td>short</td> <td>2</td> <td>logical partition-id
 * -> represents a logical partition of the physical stream</td> </tr>
 * <tr> <td>NanoTimestamp</td> <td>23</td> <td>long</td> <td>8</td> <td>Time (in nanoseconds) at
 * which the event was generated</td> </tr>
 * <tr> <td>srcId</td> <td>31</td> <td>short</td> <td>2</td> <td>Databus source id for the event</td> </tr>
 * <tr> <td>SchemaId*</td> <td>33</td> <td>byte[]</td> <td>16</td> <td>hash for the schema used to
 * generate the event</td> </tr>
 * <tr> <td>ValueCrc</td> <td>49</td> <td>int</td> <td>4</td> <td>a CRC computed over the
 * variable-length payload of the event</td> </tr>
 * <tr> <td>Key</td> <td>53</td> <td>long</td> <td>8</td> <td>key value for events with long keys
 * <tr> <td>KeySize</td> <td>53</td> <td>int</td> <td>4</td> <td>key length for byte[]-valued keys </td> </tr>
 * <tr>
 *    <th colspan="5">Variable-size payload</th>
 * </tr>
 * <tr> <td>Key</td> <td>57</td> <td>byte[]</td> <td> 4 or KeySize</td>
 * <td>32 LSB from key value for long-typed keys or key value for byte[] keys</td> </tr>
 * <tr> <td>Value</td> <td>61 or 57 + KeySize</td> <td>byte[]</td> <td>Length - Offset(Value)</td>
 * <td>Serialized event payload</td> </tr>
 * </table>
 * <p>
 * &#42 For REPL_DBUS we are using schemaid field to pass Schema Version. So in this case SchemaId
 * row of the table should be replaced with: <p>
 *  <table border="1">
 * <tr><td>SchemaVersion </td> <td>33</td> <td>short</td> <td>2</td> <td> schema version for the event</td></tr>
 * <tr><td>SchemaId(not valid)</td> <td>35</td> <td>byte[]</td> <td>14</td> <td>hash for the schema used to
 * generate the event</td> </tr>
 * </table>
 *
 * <h3>JSON serialization format </h3>
 *
 * The table below summarizes the JSON serialization format.
 *
 * <table border="1">
 * <tr> <th>Attribute</th> <th>Type</th> <th>Description</th> <th>Optional</th> </tr>
 * <tr> <td>opcode</td> <td>String</td> <td>UPSERT or DELETE</td> <td>Yes</td> </tr>
 * <tr> <td>keyBytes</td> <td>String</td> <td>Base-64 encoding of the key byte sequence for string
 * keys</td> <td rowspan="2">One of the two needs to be present</td> </tr>
 * <tr> <td>key</td> <td>Long</td> <td>key value for numeric keys</td> </tr>
 * <tr> <td>sequence</td> <td>Long</td> <td>event sequence number</td> <td>No</td> </tr>
 * <tr> <td>logicalPartitionId</td> <td>Short</td> <td>logical partition id</td> <td>No</td> </tr>
 * <tr> <td>physicalPartitionId</td> <td>Short</td> <td>physical partition id</td> <td>No</td> </tr>
 * <tr> <td>timestampInNanos</td> <td>Long</td> <td>creation timestamp in nanoseconds since the Unix
 * Epoch</td> <td>No</td> </tr>
 * <tr> <td>srcId</td> <td>Short</td> <td>source id</td> <td>No</td> </tr>
 * <tr> <td>schemaId</td> <td>String</td> <td>Base-64 encoding of the event serialization schema
 * hash id</td> <td>No</td> </tr>
 * <tr> <td>valueEnc</td> <td>String</td> <td>value encoding format: JSON or JSON_PLAIN</td>
 * <td>No</td> </tr>
 * <tr> <td>endOfPeriod</td> <td>Boolean</td> <td>true iff the event denotes end of event window</td>
 * <td>Yes; default is false</td> </tr>
 * <tr> <td>value</td> <td>String</td> <td>Literal value string for JSON_PLAIN encoding or Base-64
 * encoding of the value byte sequence for JSON encoding</td> <td>Yes; default is false</td> </tr>
 * </table>
 *
 * <h3>Event attributes</h3>
 *
 * The table below summarizes the Databus event attribute bits
 *
 * <table border="1">
 * <tr> <th>Attribute</th> <th>Bit N</th> <th>Description</th> </tr>
 * <tr> <td>OpCode0</td> <td>0</td> <td>Bit 0 of event opcode</td> </tr>
 * <tr> <td>OpCode1</td> <td>1</td> <td>bit 1 of event opcode</td> </tr>
 * <tr> <td>Trace</td> <td>2</td> <td>The event is a trace event</td> </tr>
 * <tr> <td>ByteKey</td> <td>3</td> <td>The event has a byte[] key</td> </tr>
 * <tr> <td>EoP</td> <td>4</td> <td>The event is the last event in a event window</td> </tr>
 * <tr> <td>ExtReplEvent</td> <td>8</td> <td>The event was generated through external replication
 *          (e.g. originating in a different data center)</td> </tr>
 * </table>
 *
 * <h3>Event opcodes</h3>
 *
 * Currently, Databus supports two choices of event opcodes
 *
 * <ul>
 * <li>1 - UPSERT</li>
 * <li>2 - DELETE</li>
 * </ul>
 *
 * <h3>Databus source ids</h3>
 *
 * The possible values for Databus source ids are partitioned into several ranges. In general, all
 * positive source ids are used to uniquely represent a Databus source. The source ids are used in
 * Databus data messages. All non-positive values are reserved for Databus system use. These source
 * ids are used in Databus control messages.
 *
 * <ul>
 * <li> [1, {@link java.lang.Short.MAX_VALUE}] - data source ids</li>
 * <li> [{@link java.lang.Short.MIN_VALUE}, 0] - system source ids
 *   <ul>
 *   <li> [{@link com.linkedin.databus.core.DbusEventInternalWritable#PRIVATE_RANGE_MAX_SRCID} + 1, 0] - global system source ids. These control
 *   messages will be transmitted to other Databus components over the network
 *     <ul>
 *     <li>-3 - Checkpoint event</li>
 *     </ul>
 *   </li>
 *   <li> [{@link java.lang.Short.MIN_VALUE}, {@link com.linkedin.databus.core.DbusEventInternalWritable#PRIVATE_RANGE_MAX_SRCID}] - private system
 *   source ids. These messages are used for internal communication inside a Databus component and
 *   are not transmitted to other Databus components.
 *   </ul>
 * </li>
 * </ul>
 *
 * </table>
 */
public class DbusEventV1 extends DbusEventSerializable
implements Cloneable
{
  public static final String MODULE = DbusEventV1.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  /** Serialization Format is :
   * <pre>
   *   Version (1 byte)               // 0 for DbusEventV1 (historical), 2 for DbusEventV2
   *   HeaderCrc (4 bytes)            // CRC to protect the header from being corrupted
   *   Length (4 bytes)
   *   Attributes (2 bytes)           // Key-type, Trace marker, Event-opcode
   *   Sequence (8 bytes)             // Sequence number for the event window in which this event was generated
   *   Physical PartitionId (2 bytes) // Short physical partition-id -> represents a sequence generator
   *   Logical PartitionId (2 bytes)  // Short logical partition-id -> represents a logical partition of the physical stream
   *   NanoTimestamp (8 bytes)        // Time (in nanoseconds) at which the event was generated
   *   SrcId (short)                  // SourceId for the event
   *   SchemaId (16 bytes)            // 16-byte hash (digest) of the schema used to generate the event
   *   ValueCrc (4 bytes)             // CRC to protect the value from being corrupted
   *   Key (8 bytes long key)  or KeySize (4 bytes for byte[] key)
   *   Key Bytes (k bytes for byte[] key)
   *   Value (N bytes)                // Serialized Event
   * </pre>
   */

  private static final int VersionOffset = 0;
  private static final int VersionLength = 1;
  private static final int HeaderCrcOffset = VersionOffset + VersionLength;
  private static final int HeaderCrcLength = 4;
  private static final int HeaderCrcDefault = 0;
  //used by the SendEvents command to determine the size of the incoming event
  private static final int LengthOffset = HeaderCrcOffset + HeaderCrcLength;
  private static final int LengthLength = 4;
  private static final int AttributesOffset = LengthOffset + LengthLength;
  private static final int AttributesLength = 2;
  private static final int SequenceOffset = AttributesOffset + AttributesLength;
  private static final int SequenceLength = 8;
  private static final int PhysicalPartitionIdOffset = SequenceOffset + SequenceLength;
  private static final int PhysicalPartitionIdLength = 2;
  private static final int LogicalPartitionIdOffset = PhysicalPartitionIdOffset + PhysicalPartitionIdLength;
  private static final int LogicalPartitionIdLength = 2;
  private static final int TimestampOffset = LogicalPartitionIdOffset + LogicalPartitionIdLength;
  private static final int TimestampLength = 8;
  private static final int SrcIdOffset = TimestampOffset + TimestampLength;
  private static final int SrcIdLength = 2;
  private static final int SchemaIdOffset = SrcIdOffset + SrcIdLength;
  private static final int SchemaIdLength = 16;
  private static final int ValueCrcOffset = SchemaIdOffset + SchemaIdLength;
  private static final int ValueCrcLength = 4;
  private static final int LongKeyOffset = ValueCrcOffset + ValueCrcLength;
  private static final int LongKeyLength = 8;
  private static final int LongKeyValueOffset = LongKeyOffset + LongKeyLength;
  private static final int StringKeyLengthOffset = ValueCrcOffset + ValueCrcLength;
  private static final int StringKeyLengthLength = 4;
  private static final int StringKeyOffset = StringKeyLengthOffset + StringKeyLengthLength;

  private static int LongKeyHeaderSize = LongKeyValueOffset - LengthOffset;
  private static int StringKeyHeaderSize = StringKeyOffset - LengthOffset;    // everything until the key length field is part of the header, for byte[] keys, we crc the key and value blobs together as part of the value crc

  // Attribute masks follow.  Note that AttributesLength forever limits us to 16 bits for V1 events.
  private static final int UPSERT_MASK               = 0x0001;
  private static final int DELETE_MASK               = 0x0002;
  private static final int TRACE_FLAG_MASK           = 0x0004;
  private static final int KEY_TYPE_MASK             = 0x0008;
  private static final int EXT_REPLICATED_EVENT_MASK = 0x0100; // why is this not 0x0010?  (DbusEvent.pdf concurs, sigh)

  private static final int MinHeaderSize = LongKeyOffset;
  private static final int MaxHeaderSize = Math.max(LongKeyValueOffset, StringKeyOffset);

  private static final byte[] EmptyAttributesBigEndian = new byte[DbusEventV1.AttributesLength];
  private static final byte[] UpsertLongKeyAttributesBigEndian = new byte[DbusEventV1.AttributesLength];
  private static final byte[] UpsertStringKeyAttributesBigEndian = new byte[DbusEventV1.AttributesLength];
  private static final byte[] DeleteLongKeyAttributesBigEndian = new byte[DbusEventV1.AttributesLength];
  private static final byte[] DeleteStringKeyAttributesBigEndian = new byte[DbusEventV1.AttributesLength];

  private static final byte[] EmptyAttributesLittleEndian = new byte[DbusEventV1.AttributesLength];
  private static final byte[] UpsertLongKeyAttributesLittleEndian = new byte[DbusEventV1.AttributesLength];
  private static final byte[] UpsertStringKeyAttributesLittleEndian = new byte[DbusEventV1.AttributesLength];
  private static final byte[] DeleteLongKeyAttributesLittleEndian = new byte[DbusEventV1.AttributesLength];
  private static final byte[] DeleteStringKeyAttributesLittleEndian = new byte[DbusEventV1.AttributesLength];

  static
  {
    // array elements automatically initialized to default value for type (byte:  0)

    setOpcode(UpsertLongKeyAttributesBigEndian, DbusOpcode.UPSERT, ByteOrder.BIG_ENDIAN);
    setOpcode(UpsertStringKeyAttributesBigEndian, DbusOpcode.UPSERT, ByteOrder.BIG_ENDIAN);
    setKeyTypeString(UpsertStringKeyAttributesBigEndian, ByteOrder.BIG_ENDIAN);

    setOpcode(DeleteLongKeyAttributesBigEndian, DbusOpcode.DELETE, ByteOrder.BIG_ENDIAN);
    setOpcode(DeleteStringKeyAttributesBigEndian, DbusOpcode.DELETE, ByteOrder.BIG_ENDIAN);
    setKeyTypeString(DeleteStringKeyAttributesBigEndian, ByteOrder.BIG_ENDIAN);

    setOpcode(UpsertLongKeyAttributesLittleEndian, DbusOpcode.UPSERT, ByteOrder.LITTLE_ENDIAN);
    setOpcode(UpsertStringKeyAttributesLittleEndian, DbusOpcode.UPSERT, ByteOrder.LITTLE_ENDIAN);
    setKeyTypeString(UpsertStringKeyAttributesLittleEndian, ByteOrder.LITTLE_ENDIAN);

    setOpcode(DeleteLongKeyAttributesLittleEndian, DbusOpcode.DELETE, ByteOrder.LITTLE_ENDIAN);
    setOpcode(DeleteStringKeyAttributesLittleEndian, DbusOpcode.DELETE, ByteOrder.LITTLE_ENDIAN);
    setKeyTypeString(DeleteStringKeyAttributesLittleEndian, ByteOrder.LITTLE_ENDIAN);
  }


  // near-empty constructor that doesn't create a useful event
  public DbusEventV1()
  {
    _inited = false;
  }

  public DbusEventV1(ByteBuffer buf, int position)
  {
    resetInternal(buf, position);
  }

  private int serializedKeyLength()
  {
    if (isKeyString())
    {
      return (StringKeyLengthLength + _buf.getInt(_position+StringKeyLengthOffset));
    }
    else
    {
      return LongKeyLength;
    }
  }

  private static int KeyLength(byte[] key) { return (key.length + StringKeyLengthLength); } // length of the string + 4 bytes to write the length
  private static int LongKeyValueOffset() { return (LongKeyOffset + LongKeyLength); }

  private static int StringValueOffset(int keyLength) { return (StringKeyOffset + keyLength); }

  public static int getLengthOffset()
  {
    return LengthOffset;
  }

  public static int getLengthLength()
  {
    return LengthLength;
  }

  /**
   * Serializes an End-Of-Period Marker onto the ByteBuffer passed in.
   * @param serializationBuffer - The ByteBuffer to serialize the event in. The buffer must have enough space to accommodate
   *                              the event. (76 bytes)
   * @param eventInfo - The timestamp to use for the EOP marker
   * @return the number of bytes written
   */
  public static int serializeEndOfPeriodMarker(ByteBuffer serializationBuffer, DbusEventInfo eventInfo)
  {
    byte[] attributes = (serializationBuffer.order() == ByteOrder.BIG_ENDIAN)?
                        EmptyAttributesBigEndian : EmptyAttributesLittleEndian;
    return serializeFullEvent(DbusEventInternalWritable.EOPMarkerKey.getLongKey(), serializationBuffer, eventInfo, attributes);
  }

  /**
   * Creates an EOP event with a given sequence and partition number
   * @return a ByteBuffer containing the event
   */
  public static ByteBuffer serializeEndOfPeriodMarker(long seq, short partN, ByteOrder byteOrder)
  {
    DbusEventInfo eventInfo = new DbusEventInfo(DbusOpcode.UPSERT, seq, partN, partN,
                                                TimeUtils.currentNanoTime(),
                                                DbusEventInternalWritable.EOPMarkerSrcId,
                                                new byte[16],
                                                new byte[0], false, true);
    ByteBuffer buf = ByteBuffer.allocate(DbusEventV1.MaxHeaderSize).order(byteOrder);
    DbusEventV1.serializeEndOfPeriodMarker(buf, eventInfo);
    return buf;
  }

  /**
   * non-threadsafe : serializationBuffer needs to be protected if multiple threads are writing to it concurrently
   */
  public static int serializeEvent(DbusEventKey key,
                                   short pPartitionId,
                                   short lPartitionId,
                                   long timeStampInNanos,
                                   short srcId,
                                   byte[] schemaId,
                                   byte[] value,
                                   boolean enableTracing,
                                   ByteBuffer serializationBuffer)
  throws KeyTypeNotImplementedException
  {
    DbusEventInfo dbusEventInfo = new DbusEventInfo(DbusOpcode.UPSERT,
                                                    0L,
                                                    pPartitionId,
                                                    lPartitionId,
                                                    timeStampInNanos,
                                                    srcId,
                                                    schemaId,
                                                    value,
                                                    enableTracing,
                                                    false /*autocommit*/);
    return serializeEvent(key, serializationBuffer, dbusEventInfo);
  }

  public static int serializeEvent(DbusEventKey key,
                                   ByteBuffer serializationBuffer,
                                   DbusEventInfo dbusEventInfo)
  throws KeyTypeNotImplementedException
  {
    switch (key.getKeyType())
    {
    case LONG:
      return serializeLongKeyEvent(key.getLongKey(), serializationBuffer, dbusEventInfo);
    case STRING:
      return serializeStringKeyEvent(key.getStringKeyInBytes(), serializationBuffer, dbusEventInfo);
    default:
      throw new KeyTypeNotImplementedException();
    }
  }

  private static int serializeLongKeyEvent(long key,
                                           ByteBuffer serializationBuffer,
                                           DbusEventInfo eventInfo)
  {
    byte[] attributes = null;

    // Event without explicit opcode specified should always be considered UPSERT or existing code will break
    if (eventInfo.getOpCode() == DbusOpcode.DELETE) {
      if (serializationBuffer.order() == ByteOrder.BIG_ENDIAN)
        attributes = DeleteLongKeyAttributesBigEndian.clone();
      else
        attributes = DeleteLongKeyAttributesLittleEndian.clone();
    } else {
      if (serializationBuffer.order() == ByteOrder.BIG_ENDIAN)
        attributes = UpsertLongKeyAttributesBigEndian.clone();
      else
        attributes = UpsertLongKeyAttributesLittleEndian.clone();
    }

    if (eventInfo.isEnableTracing())
    {
      setTraceFlag(attributes, serializationBuffer.order());
    }
    if (eventInfo.isReplicated())
      setExtReplicationFlag(attributes, serializationBuffer.order());

    return serializeFullEvent(key,
                              serializationBuffer,
                              eventInfo,
                              attributes);
  }

  /**
   * Exposing this method only until we fix DDSDBUS-2282 is fixed.
   * DO NOT USE THIS METHOD EVEN IN TESTS.
   */
  protected static int serializeFullEventWithEmptyAttributes(long key,
                                                             ByteBuffer buf,
                                                             DbusEventInfo eventInfo)
  {
    return serializeFullEvent(key, buf, eventInfo,
                              (buf.order() == ByteOrder.BIG_ENDIAN)?
                              EmptyAttributesBigEndian : EmptyAttributesLittleEndian);
  }

  private static int serializeFullEvent(long key,
                                        ByteBuffer serializationBuffer,
                                        DbusEventInfo eventInfo,
                                        byte[] attributes)
  {

    ByteBuffer valueBuffer = eventInfo.getValueByteBuffer();
    int payloadLen = (valueBuffer == null) ? eventInfo.getValueLength() : valueBuffer.remaining();

    int startPosition = serializationBuffer.position();
    serializationBuffer.put(DbusEventFactory.DBUS_EVENT_V1)
                       .putInt(HeaderCrcDefault)
                       .putInt(LongKeyValueOffset + payloadLen)
                       .put(attributes)
                       .putLong(eventInfo.getSequenceId())
                       .putShort(eventInfo.getpPartitionId())
                       .putShort(eventInfo.getlPartitionId())
                       .putLong(eventInfo.getTimeStampInNanos())
                       .putShort(eventInfo.getSrcId())
                       .put(eventInfo.getSchemaId(),0,16)
                       .putInt(HeaderCrcDefault)
                       .putLong(key);
    if(valueBuffer != null) {
      // note. put will advance position. In the case of wrapped byte[] it is ok, in the case of
      // ByteBuffer this is actually a read only copy of the buffer passed in.
      serializationBuffer.put(valueBuffer);
    }

    int stopPosition = serializationBuffer.position();

    long valueCrc = ByteBufferCRC32.getChecksum(serializationBuffer,
                                                startPosition+LongKeyValueOffset,
                                                payloadLen);
    Utils.putUnsignedInt(serializationBuffer, startPosition+ValueCrcOffset, valueCrc);

    if (eventInfo.isAutocommit())
    {
      //TODO (DDSDBUS-60): Medium : can avoid new here
      DbusEventV1 e = new DbusEventV1(serializationBuffer, startPosition);
      e.applyCrc();
    }

    serializationBuffer.position(stopPosition);
    return (stopPosition - startPosition);
  }

  private static int serializeStringKeyEvent(byte[] key,
                                             ByteBuffer serializationBuffer,
                                             DbusEventInfo eventInfo)
  {
    ByteBuffer valueBuffer = eventInfo.getValueByteBuffer();
    int payloadLen = (valueBuffer == null) ? eventInfo.getValueLength() : valueBuffer.remaining();

    int startPosition = serializationBuffer.position();
    byte[] attributes = null;

    // Event without explicit opcode specified should always be considered UPSERT or existing code will break
    if (eventInfo.getOpCode() == DbusOpcode.DELETE) {
      if (serializationBuffer.order() == ByteOrder.BIG_ENDIAN)
        attributes = DeleteStringKeyAttributesBigEndian.clone();
      else
        attributes = DeleteStringKeyAttributesLittleEndian.clone();
    } else {
      if (serializationBuffer.order() == ByteOrder.BIG_ENDIAN)
        attributes = UpsertStringKeyAttributesBigEndian.clone();
      else
        attributes = UpsertStringKeyAttributesLittleEndian.clone();
    }

    if (eventInfo.isEnableTracing()) {
      setTraceFlag(attributes, serializationBuffer.order());
    }

    if (eventInfo.isReplicated())
      setExtReplicationFlag(attributes, serializationBuffer.order());

    serializationBuffer.put(DbusEventFactory.DBUS_EVENT_V1)
                       .putInt(HeaderCrcDefault)
                       .putInt(StringValueOffset(key.length) + payloadLen)
                       .put(attributes)
                       .putLong(eventInfo.getSequenceId())
                       .putShort(eventInfo.getpPartitionId())
                       .putShort(eventInfo.getlPartitionId())
                       .putLong(eventInfo.getTimeStampInNanos())
                       .putShort(eventInfo.getSrcId())
                       .put(eventInfo.getSchemaId(),0,16)
                       .putInt(HeaderCrcDefault)
                       .putInt(key.length)
                       .put(key);
    if(valueBuffer != null){
      // note. put will advance position. In the case of wrapped byte[] it is ok, in the case of
      // ByteBuffer this is actually a read only copy of the buffer passed in.
      serializationBuffer.put(valueBuffer);
    }

    int stopPosition = serializationBuffer.position();
    long valueCrc = ByteBufferCRC32.getChecksum(serializationBuffer, startPosition+StringKeyOffset,
                                                key.length+payloadLen);
    Utils.putUnsignedInt(serializationBuffer, startPosition + ValueCrcOffset, valueCrc);
    if (eventInfo.isAutocommit())
    {
      //TODO (DDSDBUS-61): Medium : can avoid new here
      DbusEventV1 e = new DbusEventV1(serializationBuffer, startPosition);
      e.applyCrc();
    }

    serializationBuffer.position(stopPosition);
    return (stopPosition - startPosition);
  }

  private static void setAttributeBit(byte[] attribute, int mask, ByteOrder byteOrder)
  {
    if (mask > 0xffff)  // constrained by AttributesLength, which is exactly 2 for V1 events
    {
      throw new DatabusRuntimeException("attribute mask exceeds max size of attributes");  // or EventCreationException?  InvalidConfigException?
    }
    byte msByte = (byte)((mask & 0xff00) >> 8);
    byte lsByte = (byte)(mask & 0xff);
    if (byteOrder == ByteOrder.BIG_ENDIAN) {
      attribute[0] |= msByte;
      attribute[1] |= lsByte;
    } else {
      attribute[0] |= lsByte;
      attribute[1] |= msByte;
    }
  }

  private boolean isAttributeBitSet(int mask)
  {
    return ((_buf.getShort(_position+AttributesOffset) & mask) == mask);
  }

  private static void setOpcode(byte[] attribute, DbusOpcode opcode, ByteOrder byteOrder)
  {
    switch (opcode)
    {
      case UPSERT:
        setAttributeBit(attribute, UPSERT_MASK, byteOrder);
        break;
      case DELETE:
        setAttributeBit(attribute, DELETE_MASK, byteOrder);
        break;
      default:
        throw new RuntimeException("Unknown DbusOpcode " + opcode.name());
    }
  }

  @Override
  public DbusOpcode getOpcode()
  {
    if (isAttributeBitSet(UPSERT_MASK))
    {
      return DbusOpcode.UPSERT;
    }
    if (isAttributeBitSet(DELETE_MASK))
    {
      return DbusOpcode.DELETE;
    }
    return null;
  }

  private static void setTraceFlag(byte[] attribute, ByteOrder byteOrder)
  {
    setAttributeBit(attribute, TRACE_FLAG_MASK, byteOrder);
  }

  @Override
  public boolean isTraceEnabled()
  {
    return isAttributeBitSet(TRACE_FLAG_MASK);
  }

  private static void setKeyTypeString(byte[] attributes, ByteOrder byteOrder)
  {
    setAttributeBit(attributes, KEY_TYPE_MASK, byteOrder);
  }

  @Override
  public boolean isKeyString()
  {
    return isAttributeBitSet(KEY_TYPE_MASK);
  }

  @Override
  public boolean isKeySchema()
  {
    return false;
  }

  @Override
  public boolean isKeyNumber()
  {
    return !isAttributeBitSet(KEY_TYPE_MASK);
  }

  public static void setExtReplicationFlag(byte[] attribute, ByteOrder byteOrder)
  {
    setAttributeBit(attribute, EXT_REPLICATED_EVENT_MASK, byteOrder);
  }

  @Override
  public boolean isExtReplicatedEvent()
  {
    return isAttributeBitSet(EXT_REPLICATED_EVENT_MASK);
  }


  @Override
  public DbusEventInternalReadable reset(ByteBuffer buf, int position)
  {
    if (buf.get(position) != DbusEventFactory.DBUS_EVENT_V1)
    {
      verifyByteOrderConsistency(buf, "DbusEventV1.reset()");
      return DbusEventV2Factory.createReadOnlyDbusEventFromBufferUnchecked(buf, position);
    }
    resetInternal(buf, position);  // could optionally add "where" arg here, too
    return this;
  }

  public static int length(DbusEventKey key, int valueLen) throws KeyTypeNotImplementedException
  {
    switch (key.getKeyType())
    {
      case LONG:
        return (LongKeyOffset + LongKeyLength + valueLen);
      case STRING:
        return (StringKeyOffset + key.getStringKeyInBytes().length + valueLen);
      default:
        throw new KeyTypeNotImplementedException();
    }
  }

  @Override
  public void applyCrc()  {
    //Re-compute the header crc
    int headerLength = headerLength();
    long headerCrc = ByteBufferCRC32.getChecksum(_buf, _position+LengthOffset, headerLength);
    Utils.putUnsignedInt(_buf, _position + HeaderCrcOffset, headerCrc);
  }

  // Returns the number of bytes over which the HeaderCRC should be computed.
  // For string keys, the Header CRC includes bytes starting with 'Length' field and up to the string-key-size field (inclusive),
  // but not the string key itself. The string key is included in the body CRC.
  // For long keys, the Header CRC includes bytes starting with 'Length' field and up to the long key value (inclusive).
  protected int headerLength()  {
    if (!isKeyString()) {
      return LongKeyHeaderSize;
    }
    else {
      return StringKeyHeaderSize;
    }
  }

  @Override
  // this is different from valueLength because it includes key length for string key
  public int payloadLength() {
    int overhead = LengthOffset;
    return (size() - headerLength() - overhead);
  }

  @Override
  public void setSequence(long sequence) {
    _buf.putLong(_position + SequenceOffset, sequence);
  }

  @Override
  public long sequence()
  {
    return (_buf.getLong(_position + SequenceOffset));
  }

  // If string key, returns the length of the key + 4. If long key, returns 8.
  @Override
  public int keyLength()
  {
    if (isKeyString())
    {
      return (StringKeyLengthLength + _buf.getInt(_position+StringKeyLengthOffset));
    }
    else
    {
      return LongKeyLength;
    }
  }

  @Override
  public int keyBytesLength() {
    assert isKeyString() : "Key type = " + _buf.getShort(_position + AttributesOffset);
    return _buf.getInt(_position+StringKeyLengthOffset);
  }

  // Always returns the size of the payload.
  @Override
  public int valueLength()
  {
    return (size() - (LongKeyOffset + keyLength()));
  }

  @Override
  protected boolean isPartial() {
	  return isPartial(true);
  }

  /**
   * @return PARTIAL if the event appears to be a partial event; ERR if the header is corrupted;
   *         OK if the event header is intact and the event appears to be complete
   */
  @Override
  protected HeaderScanStatus scanHeader(boolean logErrors) {
	  if (getVersion() != DbusEventFactory.DBUS_EVENT_V1) {
	      if (logErrors) {
	        LOG.error("unknown version byte in header: " + getVersion());
	      }
		  return HeaderScanStatus.ERR;
	  }

	  if (isHeaderPartial(logErrors)) {
		  return HeaderScanStatus.PARTIAL;
	  }

	  int size = size();
	  if (size < MinHeaderSize) {
		  //size can never be smaller than min header size
	      if (logErrors) {
	        LOG.error("Event size too small ; size=" + size + " Header size= " + MinHeaderSize);
	      }
		  return HeaderScanStatus.ERR;
	  }
	  int headerLength = headerLength();
	  long calculatedHeaderCrc = ByteBufferCRC32.getChecksum(_buf, _position+LengthOffset, headerLength);
	  if(calculatedHeaderCrc != headerCrc())
	  {
		  if (logErrors)
		  {
		      LOG.error("Header CRC mismatch: ");
			  LOG.error("headerCrc() = "+ headerCrc());
			  LOG.error("calculatedCrc = "+ calculatedHeaderCrc);
		  }
		  return HeaderScanStatus.ERR;
	  }
	  return HeaderScanStatus.OK;
  }


  /**
   * @return one of ERR / OK / PARTIAL
   *
   * TODO:  should this also check _inited?  if _inited == false, presumably we should return ERR
   */
  @Override
  protected EventScanStatus scanEvent(boolean logErrors) {
	  HeaderScanStatus h = scanHeader(logErrors);

	  if (h != HeaderScanStatus.OK)  {
	      if (logErrors){
	        LOG.error("HeaderScan error=" + h);
	      }
		  return (h == HeaderScanStatus.ERR? EventScanStatus.ERR : EventScanStatus.PARTIAL);
	  }

	  //check if event is partial
	  if (isPartial(logErrors)) {
		  return EventScanStatus.PARTIAL;
	  }

	  int payloadLength = payloadLength();
	  long calculatedValueCrc = getCalculatedValueCrc();

	  long bodyCrc = bodyCrc();
	  if (calculatedValueCrc != bodyCrc)
	  {
		  if (logErrors)
		  {
			  LOG.error("_buf.order() = "+ _buf.order() +
			            ", bodyCrc() = "+ bodyCrc +
			            ", crc.getValue() = "+ calculatedValueCrc +
			            ", crc-ed block size = "+ payloadLength +
			            ", event sequence = " + sequence() +
			            ", timestamp = " + timestampInNanos());
		  }
		  return EventScanStatus.ERR;
	  }

	  return EventScanStatus.OK;
  }

  /**
   * @return true if the event appears to be partially read ; does not perform any header checks
   */
  private boolean isPartial (boolean logErrors) {
	  int size = size();
	  if (size > (_buf.limit()-_position)) {
		  if (logErrors)
			  LOG.error("partial event: size() = " + size + " buf_position=" + _position +
			            " limit = " + _buf.limit() +
			            " (_buf.limit()-_position) = "+ (_buf.limit()-_position));
		  return true;
	  }
	  return false;
  }

  private boolean isHeaderPartial(boolean logErrors)
  {
    int limit = _buf.limit();
    int bufHeaderLen = limit - _position; // what we have in buffer

    // make sure we can at least read attributes
    if (bufHeaderLen < (AttributesOffset + AttributesLength))
    {
      return true;
    }

    int headerLen = headerLength() + LengthOffset; // headerLength() ignores first LengthOffset bytes

    // we need to figure out if we got enough to calc the CRC
    // to calc CRC we are using from LengthOffset => KeyOffset
    // headerLen = the required min len of the header
    // bufHeaderLen - what we have in the buffer
    if (bufHeaderLen < headerLen) {
      if (logErrors)
        LOG.error("Partial Header: bufHeaderLen=" + bufHeaderLen + " is less then headerLen=" + headerLen);
      return true;
    }
    return false;
  }

  @Override
  public String toString()
  {
	if ( null == _buf)
	{
		return "_buf=null";
	}

	boolean valid = true;

	try
	{
		valid = isValid(true);
	} catch (Exception ex) {
		LOG.error("DbusEventV1.toString() : Got Exception while trying to validate the event ", ex);
		valid = false;
	}

	if ( !valid )
	{
		StringBuilder sb = new StringBuilder("Position: ");
		sb.append(_position);
		sb.append(", _buf: ");
		sb.append(null != _buf ? _buf.toString(): "null");
		sb.append(", validity: false; hexDump:");
		if (null != _buf && _position >= 0)
		{
		  sb.append(StringUtils.hexdumpByteBufferContents(_buf, _position, 100));
		}

		return sb.toString();
	}

    StringBuilder sb = new StringBuilder(200);
    sb.append("Position=")
    .append(_position)
    .append(";Version=")
    .append(getVersion())
    .append(";isEndOfPeriodMarker=")
    .append(isEndOfPeriodMarker())
    .append(";HeaderCrc=")
    .append(headerCrc())
    .append(";Length=")
    .append(size())
    .append(";Key=");
    if (isKeyString())
    {
      sb.append(new String(keyBytes()));
    }
    else
    {
      sb.append(key());
    }

    sb.append(";Sequence=")
    .append(sequence())
    .append(";LogicalPartitionId=")
    .append(logicalPartitionId())
    .append(";PhysicalPartitionId=")
    .append(physicalPartitionId())
    .append(";Timestamp=")
    .append(timestampInNanos())
    .append(";SrcId=")
    .append(srcId())
    .append(";SchemaId=")
    .append(Hex.encodeHex(schemaId()))
    .append(";ValueCrc=")
    .append(bodyCrc());
    //Do not output value - as it's too long.
    /*
    .append(";Value = ");

    try {
      sb.append(Utils.byteBufferToString(value(), "UTF-8"));
    } catch (UnsupportedEncodingException e) {
      sb.append(value().toString());
    }
    */
    return sb.toString();

  }

  @Override
  public byte getVersion()
  {
    return (_buf.get(_position+ VersionOffset));
  }

  @Override
  public int size()
  {
    //    int size = _buf.getInt(_position+LengthOffset);
    return (_buf.getInt(_position+LengthOffset)); // length bytes
  }

  /**
   * Setter for size
   */
  @Override
  public void setSize(int sz)
  {
    _buf.putInt(_position + LengthOffset, sz);
  }

  @Override
  public long headerCrc()
  {
    return Utils.getUnsignedInt(_buf, _position+HeaderCrcOffset);
  }

  @Override
  public void setHeaderCrc(long crc) {
    Utils.putUnsignedInt(_buf, _position+HeaderCrcOffset, crc);
  }

  @Override
  public long key()
  {
    assert isKeyNumber();
    return _buf.getLong(_position+LongKeyOffset);
  }

  @Override
  public byte[] keyBytes()
  {
    assert isKeyString();
    int keyLength = _buf.getInt(_position + LongKeyOffset);
    byte[] dst = new byte[keyLength];
    for (int i=0; i< keyLength; ++i)
    {
      dst[i] = _buf.get(_position+LongKeyOffset+4+i);
    }
    return dst;
  }

  @Override
  public short physicalPartitionId()
  {
    return _buf.getShort(_position + PhysicalPartitionIdOffset);
  }

  @Override
  public short logicalPartitionId()
  {
    return _buf.getShort(_position + LogicalPartitionIdOffset);
  }

  @Override
  public short getPartitionId()
  {
    return _buf.getShort(_position + PhysicalPartitionIdOffset);
  }

  @Override
  public long timestampInNanos()
  {
    return _buf.getLong(_position+TimestampOffset);
  }

  // two temp method to pass schema version from RPL_DBUS
  public void setSchemaVersion(short schemaVersion)
  {
    _buf.putShort(_position+SchemaIdOffset, schemaVersion);
  }

  @Override
  public short schemaVersion()
  {
    return _buf.getShort(_position+SchemaIdOffset);
  }

  @Override
  public void setSrcId(int srcId)
  {
    if (srcId > Short.MAX_VALUE)
    {
      throw new DatabusRuntimeException("Unsupported source Id in DbusEvent V1:" + srcId);
    }
    _buf.putShort(_position+SrcIdOffset, (short)srcId);
  }

  @Override
  public short srcId()
  {
    return _buf.getShort(_position+SrcIdOffset);
  }

  @Override
  public int getSourceId()
  {
    return srcId();
  }

  /** put a byte[] schemaId into the buffer .
   * Make sure CRC is recomputed after that */

  @Override
  public void setSchemaId(byte[] schemaId) {
    for(int i=0; i<16; i++) {
      _buf.put(_position+SchemaIdOffset+i, schemaId[i]);
    }
  }

  @Override
  public void recomputeCrcsAfterEspressoRewrite()
  {
    applyCrc();
  }

  @Override
  public byte[] schemaId()
  {
    byte[] md5 = new byte[16];
    for (int i = 0; i < 16; i++)
    {
      md5[i] = _buf.get(_position+SchemaIdOffset+i);
    }
    return md5;
  }

  @Override
  public void schemaId(byte[] md5)
  {
    for (int i = 0; i < 16; i++)
    {
      md5[i] = _buf.get(_position+SchemaIdOffset+i);
    }

  }

  @Override
  public long getCalculatedValueCrc() {
    long calcValueCrc = ByteBufferCRC32.getChecksum(_buf, isKeyNumber()?_position+LongKeyValueOffset:_position+StringKeyOffset, payloadLength());
    return calcValueCrc;
  }

  @Override
  public long bodyCrc() {
    return Utils.getUnsignedInt(_buf, _position+ValueCrcOffset);
  }

  @Override
  public void setValueCrc(long crc) {
	  Utils.putUnsignedInt(_buf, _position+ValueCrcOffset, crc);
  }

  @Override
  public ByteBuffer value()
  {
    ByteBuffer value = _buf.asReadOnlyBuffer().order(_buf.order());
    value.position(_position+LongKeyOffset + keyLength());
    value = value.slice().order(_buf.order());
    int valueSize = valueLength();
    value.limit(valueSize);
    value.rewind();
    return value;
  }

  @Override
  public void setValue(byte[] bytes) {
    int offset = _position+LongKeyOffset+serializedKeyLength();
    for (int i=0;i< bytes.length;++i) {
      _buf.put(offset+i,bytes[i]);
    }
  }

  @Override
  public DbusEventPart getPayloadPart()
  {
    return null;
  }

  @Override
  public DbusEventPart getPayloadMetadataPart()
  {
    return null;
  }

  @Override
  public DbusEventPart getKeyPart()
  {
    throw new DatabusRuntimeException("V1 event does not support schema keys");
  }

  @Override
  public DbusEventInternalWritable createCopy()
  {
    ByteBuffer cloned = ByteBuffer.allocate(size()).order(_buf.order());
    cloned.put(getRawBytes());
    DbusEventV1 c = new DbusEventV1(cloned, 0);
    return c;
  }

  @Override
  public boolean equals(Object obj)
  {
    if (obj == null)
    {
      return false;
    }

    if (obj instanceof DbusEventV1)
    {
      DbusEventV1 objEvent = (DbusEventV1) obj;
      return (headerCrc() == objEvent.headerCrc() &&
              bodyCrc() == objEvent.bodyCrc());
    }
    else
    {
      return false;
    }
  }

  @Override
  public int hashCode()
  {
    return ((int)headerCrc() ^ (int)bodyCrc());
  }

  @Override
  public DbusEvent clone(DbusEvent e)
  {
    DbusEventV1 reuse = (DbusEventV1)e;
    if (null == reuse)
    {
      reuse = new DbusEventV1(_buf, _position);
    }
    else
    {
      // TODO:  option to convert (via reset()) instead of throw exception?
      if (!(e instanceof DbusEventV1))
      {
        throw new UnsupportedClassVersionError("Unsupported class:" + e.getClass().getSimpleName());
      }
      reuse.resetInternal(_buf, _position);
    }

    return reuse;
  }

  @Override
  public int getMagic()
  {
    return 0;
  }

}
