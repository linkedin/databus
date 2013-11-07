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
import com.linkedin.databus.core.util.Utils;


public class DbusEventV2 extends DbusEventSerializable
{
  /** Serialization Format is :
   * <pre>
   *   HEADER FIXED PART (43 bytes):
   *     Version (1 byte)         // 0 for DbusEventV1 (historical), 2 for DbusEventV2
   *     Magic (4 bytes)          // Must be 0xCAFEDEED for version 2
   *     Header Length (4 bytes)  // Length of fixed and variable parts of the header
   *     HeaderCrc (4 bytes)      // CRC of the header portion (fixed + variable), from offset 17 through end of header
   *     BodyCRC (4 bytes)        // CRC of the rest of the event
   *     Total Length (4 bytes)   // Total length, including header and body
   *     Attributes (2 bytes)     // (MSB)12-bit flags, 2 bits opcode, 2-bits key-type (LSB)
   *     NanoTimestamp (8 bytes)  // Time (in nanoseconds) at which the event was generated
   *     SourceId (4 bytes)       // SourceId for the event
   *     PartitionId (2 bytes)    // Partition ID for the event
   *     Sequence (8 bytes)       // Sequence number for the event window in which this event was generated
   *
   *   HEADER VARIABLE PART:
   *     if key type is long
   *       Long Key (8 bytes)
   *     else if key type is string
   *       String key length (4 bytes)
   *       String key (as per length)
   *     else // key type must be schema
   *       DbusEventPart
   *
   *   METADATA PART:
   *     DbusEventPart
   *
   *   PAYLOAD PART:
   *     DbusEventPart
   * </pre>
   *
   * A DbusEventPart is serialized as:
   * <pre>
   *   Length (4 bytes)
   *   Schema Attributes (2 bytes having the schema version and schema digest type)
   *   Schema Digest (4 or 16 bytes depending on schema digest type)
   *   Bytes (as per length)
   * </pre>
   * TODO Point to a wiki page
   */
  public static final String MODULE = DbusEventV2.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private static final int VersionOffset = 0;
  private static final int MagicOffset = 1;
  private static final int HeaderLenOffset = 5;
  private static final int HeaderCrcOffset = 9;
  private static final int BodyCrcOffset = 13;
  private static final int TotalLenOffset = 17;
  private static final int AttributesOffset = 21;
  private static final int TimestampOffset = 23;
  private static final int SourceIdOffset = 31;
  private static final int PartitionIdOffset = 35;
  private static final int SequenceOffset = 37;
  private static final int FixedHeaderLen = 45;
  private static final int StringKeyLengthOffset = 45;  // If the key is of type string, the length of the string is here
  private static final int StringKeyLengthSize = 4; // size of storage to store the length of the key
  private static final int LongKeyOffset = 45;
  private static final int LongKeyLength = 8;     // sizedof(long)
  private static final int SchemaKeyOffset = 45;      // If the key is of type schema, it starts at this offset.

  // Bits in the Attributes (short) field
  // |   12 bits flags       | 2 bits key type | 2 bits op code |
  // MSB                                                       LSB
  private static final short KEY_TYPE_MASK = 0x000C;  // Shifted left by 2 bits
  private static final short KEY_TYPE_SHIFT = 2;
  private static final short LONG_KEY_TYPE = 0x01;
  private static final short STRING_KEY_TYPE = 0x02;
  private static final short SCHEMA_KEY_TYPE = 0x03;
  private static final short OPCODE_MASK = 0x0003;
  private static final int DELETE_OP_CODE = 2;
  private static final int UPSERT_OP_CODE = 1;
  private static final int CONTROL_EVENT_OP_CODE = 0;
  // Shifted positions for the attribute bits
  private static final short FLAG_IS_REPLICATED = 0x10;
  private static final short FLAG_TRACE_ON = 0x20;
  private static final short FLAG_HAS_PAYLOAD_METADATA_PART = 0x40;
  private static final short FLAG_HAS_PAYLOAD_PART = 0x80;

  private static final int MAGIC = 0xCAFEDEED;


  // TODO Find a way to not duplicate variables and methods.


  // near-empty constructor that doesn't create a useful event
  public DbusEventV2()
  {
    _inited = false;
  }

  public DbusEventV2(ByteBuffer buf, int position)
  {
    resetInternal(buf, position);
  }

  @Override
  public void setSequence(long sequence)
  {
    _buf.putLong(_position + SequenceOffset, sequence);
  }

  @Override
  public void applyCrc()
  {
    long headerCrc = ByteBufferCRC32.getChecksum(_buf, _position + BodyCrcOffset, numBytesForHeaderCrc());
    Utils.putUnsignedInt(_buf, _position + HeaderCrcOffset, headerCrc);
  }

  @Override
  public void setSize(int sz)
  {
    _buf.putInt(_position + TotalLenOffset, sz);
  }

  @Override
  public void setHeaderCrc(long crc)
  {
    Utils.putUnsignedInt(_buf, _position + HeaderCrcOffset, crc);
  }

  @Override
  public void setValue(byte[] bytes)
  {
    // TODO Remove this after implementing DbusEventPart method to set value?
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setValueCrc(long crc)
  {
    // For V2 events, this translates to setting the CRC for the non-header portion.
    Utils.putUnsignedInt(_buf, _position + BodyCrcOffset, crc);
  }

  @Override
  public DbusEvent clone(DbusEvent e)
  {
    DbusEventV2 reuse = (DbusEventV2)e;
    if (null == reuse)
    {
      reuse = new DbusEventV2(_buf, _position);
    }
    else
    {
      if (!(e instanceof DbusEventV2))
      {
        throw new UnsupportedClassVersionError("Unsupported class:" + e.getClass().getSimpleName());
      }
      reuse.resetInternal(_buf, _position);
    }

    return reuse;
  }

  @Override
  public void setSrcId(int srcId)
  {
    _buf.putInt(_position + SourceIdOffset, srcId);
  }

  /**
   * Replace the schema ID for the payload. The caller knows what they are doing, and
   * will call methods to re-compute CRCs.
   * @param payloadSchemaId the new schema digest
   */
  @Override
  public void setSchemaId(byte[] payloadSchemaId)
  {
    int payloadPartPosition = payloadPartPosition();
    if (payloadPartPosition == -1)
    {
      throw new DatabusRuntimeException("No payload in which to set schema ID");
    }
    DbusEventPart.replaceSchemaDigest(_buf, payloadPartPosition, payloadSchemaId);
  }

  @Override
  public void recomputeCrcsAfterEspressoRewrite()
  {
    long valueCrc = getCalculatedValueCrc();
    setValueCrc(valueCrc);
    applyCrc();
    // Recompute the Body as well as header CRC since both have changed
  }

  @Override
  public DbusEventInternalReadable reset(ByteBuffer buf, int position)
  {
    if (buf.get(position) != DbusEventFactory.DBUS_EVENT_V2)
    {
      verifyByteOrderConsistency(buf, "DbusEventV2.reset()");
      return DbusEventV1Factory.createReadOnlyDbusEventFromBufferUnchecked(buf, position);
    }
    resetInternal(buf, position);
    return this;
  }

  @Override
  public long headerCrc()
  {
    return Utils.getUnsignedInt(_buf, _position+HeaderCrcOffset);
  }

  // DbusEventV1 returns PARTIAL if the header or event is partial, but it logs an error.
  // We return PARTIAL but do not log an error.
  // TODO:  should this also check _inited?  if _inited == false, presumably we should return ERR
  @Override
  public EventScanStatus scanEvent(boolean logErrors)
  {
    HeaderScanStatus h = scanHeader(logErrors);
    if (h != HeaderScanStatus.OK)
    {
      // scanHeader should have logged errors.
      return (h == HeaderScanStatus.ERR? EventScanStatus.ERR : EventScanStatus.PARTIAL);
    }

    int bytesInBuffer = _buf.limit() - _position;
    int eventLengthFromHeader = _buf.getInt(_position + TotalLenOffset);

    if (bytesInBuffer < eventLengthFromHeader)
    {
      return EventScanStatus.PARTIAL;
    }
    // We know that we have exactly the number of bytes we expect to have.
    long calculatedBodyCrc = getCalculatedValueCrc();
    long bodyCrc = bodyCrc();
    if (calculatedBodyCrc != bodyCrc)
    {
      if (logErrors)
      {
        LOG.error("bodyCrcInEvent = "+ bodyCrc);
        LOG.error(",calculatedBodyCrc = "+ calculatedBodyCrc);
        LOG.error(",crc-ed block size = "+ valueLength());
        LOG.error(",event sequence = " + sequence());
        LOG.error(",timestamp = " + timestampInNanos());
      }
      return EventScanStatus.ERR;
    }

    return EventScanStatus.OK;
  }

  @Override
  public int payloadLength()
  {
    return valueLength();
  }

  @Override
  public long bodyCrc()
  {
    return Utils.getUnsignedInt(_buf, _position + BodyCrcOffset);
  }

  private int bodyLength()
  {
    return _buf.getInt(_position+TotalLenOffset) - headerLength();
  }

  @Override
  public long getCalculatedValueCrc()
  {
    return ByteBufferCRC32.getChecksum(_buf, _position + headerLength(), bodyLength());
  }

  @Override
  public DbusEventInternalWritable createCopy()
  {
    throw new UnsupportedOperationException("Not imeplemented");
  }

  // We really mean the payload schema version here.
  // TODO We should optimize by creating the DbusEventPart objects during scan event.
  @Override
  public short schemaVersion()
  {
    DbusEventPart payloadPart = getPayloadPart();
    if (payloadPart == null)
    {
      return 0;
    }
    return payloadPart.getSchemaVersion();
  }

  private int headerLength()
  {
    return _buf.getInt(_position + HeaderLenOffset);
  }

  @Override
  protected HeaderScanStatus scanHeader(boolean logErrors)
  {
    if (getVersion() != DbusEventFactory.DBUS_EVENT_V2)
    {
      if (logErrors)
      {
        LOG.error("Incorrect version byte in header:" + getVersion());
      }
    }

    int bytesInBuffer = _buf.limit() - _position;
    if (bytesInBuffer < HeaderCrcOffset)
    {
      // We can't even get to the header length
      return HeaderScanStatus.PARTIAL;
    }
    if (headerLength() < FixedHeaderLen)
    {
      if (logErrors)
      {
        LOG.error("Header length too small:" + headerLength());
      }
    }
    if (bytesInBuffer < headerLength())
    {
      return HeaderScanStatus.PARTIAL;
    }
    // We have the complete header. Verify the CRC and return the status.
    long calculatedHeaderCrc = getCalculatedHeaderCrc();
    if (calculatedHeaderCrc != headerCrc())
    {
      if (logErrors)
      {
        LOG.error("Header CRC mismatch: Calculated:" + calculatedHeaderCrc + ",found:" + headerCrc());
      }
      return HeaderScanStatus.ERR;
    }

    return HeaderScanStatus.OK;
  }

  @Override
  protected boolean isPartial()
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public byte getVersion()
  {
    return (_buf.get(_position + VersionOffset));
  }

  @Override
  public int getMagic()
  {
    return (_buf.getInt(_position + MagicOffset));
  }

  @Override
  public boolean isExtReplicatedEvent()
  {
    return isAttributeSet(FLAG_IS_REPLICATED);
  }

  private short getKeyTypeAttribute()
  {
    return _buf.getShort(_position + AttributesOffset);
  }

  @Override
  public boolean isKeyNumber()
  {
    return (getKeyType(getKeyTypeAttribute()) == DbusEventKey.KeyType.LONG);
  }

  @Override
  public boolean isKeyString()
  {
    return (getKeyType(getKeyTypeAttribute()) == DbusEventKey.KeyType.STRING);
  }

  @Override
  public boolean isKeySchema()
  {
    return (getKeyType(getKeyTypeAttribute()) == DbusEventKey.KeyType.SCHEMA);
  }

  @Override
  public boolean isTraceEnabled()
  {
    return isAttributeSet(FLAG_TRACE_ON);
  }

  private boolean hasPayloadPart()
  {
    return isAttributeSet(FLAG_HAS_PAYLOAD_PART);
  }

  private boolean hasMetadata()
  {
    return isAttributeSet(FLAG_HAS_PAYLOAD_METADATA_PART);
  }

  @Override
  public DbusOpcode getOpcode()
  {
    return getOpCode(_buf.getShort(_position + AttributesOffset));
  }

  @Override
  public long timestampInNanos()
  {
    return _buf.getLong(_position + TimestampOffset);
  }

  @Override
  public int size()
  {
    return _buf.getInt(_position + TotalLenOffset);
  }

  @Override
  public long sequence()
  {
    return _buf.getLong(_position + SequenceOffset);
  }

  @Override
  public long key()
  {
    assert isKeyNumber();
    return _buf.getLong(_position + LongKeyOffset);
  }

  /**
   * key length includes 4 bytes of the key length size
   * @see com.linkedin.databus.core.DbusEvent#keyLength()
   */
  @Override
  public int keyLength()
  {
    throw new UnsupportedOperationException("DbusEvent.keyLength() is deprecated");
  }

  /**
   * @see com.linkedin.databus.core.DbusEventInternalReadable#keyBytesLength()
   */
  @Override
  public int keyBytesLength()
  {
    assert isKeyString() : "Key type not string = " + getKeyType(getKeyTypeAttribute());
    return _buf.getInt(_position + StringKeyLengthOffset);
  }

  @Override
  public byte[] keyBytes()
  {
    assert isKeyString() : "Key type = " + getKeyType(getKeyTypeAttribute());
    int strKeyLen = keyBytesLength();
    byte dst[] = new byte[strKeyLen];
    int offsetToCopyFrom = StringKeyLengthOffset + StringKeyLengthSize;
    // TODO Use slice() and then bulk-get.
    for (int i = 0; i < strKeyLen; i++)  //not using 'bulk' get, because it would adjust _position
    {
      dst[i] = _buf.get(_position + offsetToCopyFrom + i);
    }
    return dst;
  }

  @Override
  public short srcId()
  {
    int srcId = getSourceId();
    assert srcId <= Short.MAX_VALUE : "Source ID = " + srcId + " not handled by caller";
    return (short)srcId;
  }

  @Override
  public int getSourceId()
  {
    return _buf.getInt(_position + SourceIdOffset);
  }

  @Override
  public short physicalPartitionId()
  {
    return (_buf.getShort(_position + PartitionIdOffset));
  }

  @Override
  public short logicalPartitionId()
  {
    return (_buf.getShort(_position + PartitionIdOffset));
  }

  @Override
  public short getPartitionId()
  {
    return physicalPartitionId();
  }

  @Override
  public byte[] schemaId()
  {
    DbusEventPart payloadPart = getPayloadPart();
    if (payloadPart == null)
    {
      return null;
    }
    return payloadPart.getSchemaDigest(); // clones the byte array already
  }

  @Override
  public void schemaId(byte[] md5)
  {
    DbusEventPart payloadPart = getPayloadPart();
    if (payloadPart == null)
    {
      return;
    }
    // TODO assert that the digest is MD5 type? Optimize to copy once?
    byte[] srcBytes =  payloadPart.getSchemaDigest();
    for (int i = 0; i < 16; i++)
    {
      md5[i] = srcBytes[i];
    }
  }

  // Return the position within the buffer where the payload part starts.
  private int payloadPartPosition()
  {
    if (!hasPayloadPart())
    {
      return -1;
    }
    // We know that payload has MD5 hash as signature.
    int payloadPartPosition = _position + headerLength();
    if (hasMetadata())
    {
      // Skip metadata to get to the payload
      payloadPartPosition += DbusEventPart.partLength(_buf, payloadPartPosition);
    }
    return payloadPartPosition;
  }

  @Override
  public int valueLength()
  {
    // TODO Maybe use DbusEventPart?
    if (!hasPayloadPart())
    {
      return 0;
    }
    return _buf.getInt(payloadPartPosition());
  }

  @Override
  public ByteBuffer value()
  {
    DbusEventPart payloadPart = getPayloadPart();
    if (payloadPart == null)
    {
      return null;
    }
    // this will return a new ByteBuffer object pointing to the
    // original data, with position pointing to beginning of the
    // data and limit set to the end.
    return payloadPart.getData();
  }

  @Override
  public DbusEventPart getPayloadPart()
  {
    int payloadPartPosition = payloadPartPosition();
    if (payloadPartPosition == -1)
    {
      return null;
    }
    return getDbusEventPart(payloadPartPosition);
  }

  @Override
  public DbusEventPart getPayloadMetadataPart()
  {
    // TODO Cache the parts as we de-serialize them. Perhaps best done in scanEvent().
    if (!hasMetadata())
    {
      return null;
    }
    return getDbusEventPart(_position + headerLength());
  }

  @Override
  public DbusEventPart getKeyPart()
  {
    assert isKeySchema() : "Key type = " + getKeyType(getKeyTypeAttribute());
    return getDbusEventPart(_position + SchemaKeyOffset);
  }

  private DbusEventPart getDbusEventPart(int dbusEventPartStartPos)
  {
    ByteBuffer dbusEventPartBB;
    dbusEventPartBB = _buf.asReadOnlyBuffer().order(_buf.order());
    dbusEventPartBB.position(dbusEventPartStartPos);
    DbusEventPart dbusEventPart = DbusEventPart.decode(dbusEventPartBB);
    return dbusEventPart;
  }

  private DbusEventKey.KeyType getKeyType(short attributes)
  {
    switch((attributes & KEY_TYPE_MASK) >> KEY_TYPE_SHIFT)
    {
      case LONG_KEY_TYPE:
        return DbusEventKey.KeyType.LONG;
      case STRING_KEY_TYPE:
        return DbusEventKey.KeyType.STRING;
      case SCHEMA_KEY_TYPE:
        return DbusEventKey.KeyType.SCHEMA;
    }
    throw new DatabusRuntimeException("Unexpected attribute value:" + attributes);
  }

  private DbusOpcode getOpCode(short attributes)
  {
    switch(attributes & OPCODE_MASK)
    {
      case UPSERT_OP_CODE:
        return DbusOpcode.UPSERT;
      case DELETE_OP_CODE:
        return DbusOpcode.DELETE;
      case CONTROL_EVENT_OP_CODE:
        return null;
    }
    throw new DatabusRuntimeException("Unexpected op code " + (attributes & OPCODE_MASK));
  }

  private int numBytesForHeaderCrc()
  {
    // We run the header CRC from the BodyCrcOffset up until the end of the header.
    return (_buf.getInt(_position + HeaderLenOffset) - BodyCrcOffset);
  }

  private boolean isAttributeSet(short attribute)
  {
    return ((_buf.getShort(_position + AttributesOffset) & attribute) == attribute);
  }

  private long getCalculatedHeaderCrc()
  {
    return ByteBufferCRC32.getChecksum(_buf, _position + BodyCrcOffset, numBytesForHeaderCrc());
  }

  private static short setOpCode(DbusOpcode opCode, short attributes, int srcId)
  {
    // DbusEventInfo does not support an opcode reserved for control events.
    if (DbusEventUtils.isControlSrcId(srcId))
    {
      // We know that CONTROL_EVENT_OP_CODE is 0, and findbugs does not like us ORing a 0,
      // so we return here.
//      attributes |= CONTROL_EVENT_OP_CODE;
      return attributes;
    }
    if (opCode == null)
    {
      // Keeping compatiblity with V1. See DDSDBUS-2282.
      opCode = DbusOpcode.UPSERT;
    }
    switch (opCode)
    {
      case UPSERT:
        attributes |= UPSERT_OP_CODE;
        break;
      case DELETE:
        attributes |= DELETE_OP_CODE;
        break;
      default:
        throw new UnsupportedOperationException("Unimplemented opCode:" + opCode);
    }
    return attributes;
  }

  private static short setKeyType(DbusEventKey key, short attributes)
  {
    switch (key.getKeyType())
    {
      case STRING:
        attributes |= (STRING_KEY_TYPE << KEY_TYPE_SHIFT);
        break;
      case LONG:
        attributes |= (LONG_KEY_TYPE << KEY_TYPE_SHIFT);
        break;
      case SCHEMA:
        attributes |= (SCHEMA_KEY_TYPE << KEY_TYPE_SHIFT);
        break;
      default:
        throw new UnsupportedOperationException("Unimplemented key type:" + key.getKeyType());
    }
    return attributes;
  }

  // Since this is in test code path only, we keep it sub-optimal by copying
  // string key multiple times. Better way is to get a reference to the key
  // bytes (sorry, findbugs) or move the encoding logic into DbusEventKey to
  // avoid a short-term key copy plus memory allocation.
  public static void setKey(ByteBuffer buf, DbusEventKey key)
  {
    switch (key.getKeyType())
    {
      case STRING:
        byte[] keyBytes = key.getStringKeyInBytes();
        buf.putInt(keyBytes.length).put(keyBytes);
        break;
      case LONG:
        buf.putLong(key.getLongKey());
        break;
      case SCHEMA:
        key.getSchemaKey().encode(buf);
        break;
      default:
        throw new UnsupportedOperationException("Unimplemented key type:" + key.getKeyType());
    }
  }

  // Having a value length of 0 is a valid case when a row is deleted, or has a schema that produces a 0 length payload.
  // Also, we use DbusEvent internally to create events to carry Checkpoint, DbusErrorEvent or SCNRegressMessage.
  // These events have a payload, but no schema version. The schemaId is ignored in these events, but the payload
  // part must be present.
  private static boolean shouldEncodePayloadPart(DbusEventInfo eventInfo)
  {
    if (eventInfo.getPayloadSchemaVersion() > 0 || eventInfo.getValueLength() > 0)
    {
      return true;
    }
    return false;
  }

  public static int serializeEvent(DbusEventKey key,
                                   ByteBuffer buf,
                                   DbusEventInfo dbusEventInfo)
  {
    // Serialize a DbusEventV2 that has exact same contents as a DbusEventV1.
    final int start = buf.position();
    buf.put(DbusEventFactory.DBUS_EVENT_V2);
    buf.putInt(MAGIC);
    buf.putInt(0);        // Header len placeholder
    buf.putInt(0);        // Header crc placeholder
    buf.putInt(0);       // Body CRC placeholder
    buf.putInt(0);      // total length placeholder

    short attributes = 0;
    attributes = setOpCode(dbusEventInfo.getOpCode(), attributes, dbusEventInfo.getSrcId());
    attributes = setKeyType(key, attributes);
    if (dbusEventInfo.isEnableTracing())
    {
      attributes |= FLAG_TRACE_ON;
    }

    if (dbusEventInfo.isReplicated())
    {
      attributes |= FLAG_IS_REPLICATED;
    }

    DbusEventPart metadata = dbusEventInfo.getMetadata();
    if (shouldEncodePayloadPart(dbusEventInfo))
    {
      attributes |= FLAG_HAS_PAYLOAD_PART;
    }
    if (metadata != null)
    {
      attributes |= FLAG_HAS_PAYLOAD_METADATA_PART;
    }
    buf.putShort(attributes);
    buf.putLong(dbusEventInfo.getTimeStampInNanos());
    buf.putInt(dbusEventInfo.getSrcId());
    buf.putShort(dbusEventInfo.getpPartitionId());
    buf.putLong(dbusEventInfo.getSequenceId());

    // Fixed part of header is done. Now for the variable header part
    setKey(buf, key);
    final int hdrEndPos = buf.position();

    if (metadata != null)
    {
      metadata.encode(buf);
    }

    if ((attributes & FLAG_HAS_PAYLOAD_PART) != 0)
    {
      ByteBuffer bb = dbusEventInfo.getValueByteBuffer();
      if (bb == null)
      {
        // Special case to encode when there is no data.
        bb = ByteBuffer.allocate(1).order(buf.order());
        bb.limit(0);
      }
      DbusEventPart valuePart = new DbusEventPart(SchemaDigestType.MD5,
                                                  dbusEventInfo.getSchemaId(),
                                                  dbusEventInfo.getPayloadSchemaVersion(),
                                                  bb);
      valuePart.encode(buf);
    }
    final int end = buf.position();
    buf.putInt(start+HeaderLenOffset, hdrEndPos-start);
    buf.putInt(start+TotalLenOffset, end-start);

    long bodyCrc = ByteBufferCRC32.getChecksum(buf,
                                               hdrEndPos,
                                               end-hdrEndPos);
    Utils.putUnsignedInt(buf, start+BodyCrcOffset, bodyCrc);
    // Header CRC
    if (dbusEventInfo.isAutocommit())
    {
      // Do the body CRC first, since that is included in the header CRC
      long hdrCrc = ByteBufferCRC32.getChecksum(buf,
                                                start+BodyCrcOffset,
                                                hdrEndPos-start-BodyCrcOffset);
      Utils.putUnsignedInt(buf, start+HeaderCrcOffset, hdrCrc);
    }
    return buf.position() - start;
  }

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
    throw new UnsupportedOperationException("Unimplemented as yet");
  }

  /**
   * Compute how long an event would be, if it were to be serialized.
   */
  public static int computeEventLength(DbusEventKey key, DbusEventInfo eventInfo)
  throws KeyTypeNotImplementedException
  {
    int evtLen = LongKeyOffset;
    switch (key.getKeyType())
    {
      case LONG:
        evtLen += LongKeyLength;
        break;
      case STRING:
        evtLen += key.getStringKeyInBytes().length + StringKeyLengthSize;
        break;
      case SCHEMA:
        evtLen += key.getSchemaKey().computePartLength();
        break;
      default:
        String msg = "Unknown key type:" + key.getKeyType();
        LOG.error(msg);
        throw new KeyTypeNotImplementedException(msg);
    }

    DbusEventPart metadata = eventInfo.getMetadata();
    if (metadata != null)
    {
      evtLen += metadata.computePartLength();
    }

    // In case of Checksum event, the payload is non-null, but there is no schema.
    // We need to encode a payload part if either the payload has some data, or
    // if schema version is non-zero.
    // All payload schema digests are MD5 until we choose to support CRC digest in
    // payload.
    if (shouldEncodePayloadPart(eventInfo))
    {
      evtLen += DbusEventPart.computePartLength(SchemaDigestType.MD5,
                                                eventInfo.getValueLength());
    }

    return evtLen;
  }

  public DbusEventInternalWritable convertToV1() throws KeyTypeNotImplementedException
  {
    DbusEventKey key;
    DbusEventFactory eventV1Factory =  new DbusEventV1Factory();

    // to create new event we need to get data from ByteBuffer of the current event
    ByteBuffer curValue = value();

    // create the key
    if(isKeyNumber()) {
      key = new DbusEventKey(key());
    } else if(isKeyString()) {
      key = new DbusEventKey(keyBytes());
    } else {
      String msg = "Conversion not supported for this key type:" + getKeyType(getKeyTypeAttribute());
      LOG.error(msg);
      throw new KeyTypeNotImplementedException(msg);
    }

    // validate schmeaId - for v1 it should be array of bytes with 0s
    byte [] schemaId = schemaId();
    if(schemaId == null)
    {
      schemaId = DbusEventInternalWritable.emptyMd5;
    }

    boolean autocommit = true; // will generate CRC for the event - should always be true
    DbusEventInfo dbusEventInfo = new DbusEventInfo(getOpcode(), sequence(), getPartitionId(), getPartitionId(),
                                                    timestampInNanos(), (short)getSourceId(), schemaId,
                                                    null, isTraceEnabled(), autocommit);
    if(curValue != null)
    {
      dbusEventInfo.setValueByteBuffer(curValue); // to make it more efficient we should copy directly from the buffer
    }

    // allocate the buffer
    int newEventSize = eventV1Factory.computeEventLength(key, dbusEventInfo);
    ByteBuffer serializationBuffer = ByteBuffer.allocate(newEventSize);
    serializationBuffer.order(_buf.order());
    int size = DbusEventV1.serializeEvent(key, serializationBuffer, dbusEventInfo);

    if(size != newEventSize)
      throw new DatabusRuntimeException("event size doesn't match after conversion from V2 to V1");
    serializationBuffer.limit(size); // set the limit to the end of the event
    // construct the event from the buffer at the position
    return new DbusEventV1(serializationBuffer, 0);
  }

  public static ByteBuffer serializeEndOfPeriodMarker(long seq, short partition, ByteOrder byteOrder)
  {
    DbusEventKey key = DbusEventInternalWritable.EOPMarkerKey;
    DbusEventInfo eventInfo = new DbusEventInfo(null,
                                                seq,
                                                partition,
                                                partition,
                                                System.nanoTime(),
                                                EOPMarkerSrcId,
                                                DbusEventInternalWritable.emptyMd5,
                                                new byte[0],
                                                false, //enable tracing
                                                true, // autocommit
                                                DbusEventFactory.DBUS_EVENT_V2,
                                                (short)0,   // payload schema version
                                                null        // Metadata
                                                );

    try
    {
      int evtLen = computeEventLength(key, eventInfo);
      ByteBuffer buf = ByteBuffer.allocate(evtLen).order(byteOrder);
      serializeEvent(key, buf, eventInfo);
      return buf;
    }
    catch (KeyTypeNotImplementedException e)
    {
      throw new DatabusRuntimeException("Unexpected exception on key :" + key, e);
    }
  }

  @Override
  public String toString()
  {
    if (_buf == null)
    {
      return "buf=null";
    }

    boolean valid = true;

    try
    {
      valid = isValid(true);
    } catch (Exception ex) {
      LOG.error("DbusEventV2.toString() : Got Exception while trying to validate the event ", ex);
      valid = false;
    }

    if (!valid )
    {
      StringBuilder sb = new StringBuilder("Position: ");
      sb.append(_position);
      sb.append(", buf: ");
      sb.append(_buf != null ? _buf.toString(): "null");
      sb.append(", validity: false; hexDump:");
      if (_buf != null && _position >= 0)
      {
        sb.append(StringUtils.hexdumpByteBufferContents(_buf, _position, 100));
      }

      return sb.toString();
    }

    StringBuilder sb = new StringBuilder(200);
    sb.append("Version=")
        .append(getVersion())
        .append(";Position=")
        .append(_position)
        .append(";isEndOfPeriodMarker=")
        .append(isEndOfPeriodMarker())
        .append(";isExtReplicated=")
        .append(isExtReplicatedEvent())
        .append(";HeaderCrc=")
        .append("0x")
        .append(Integer.toHexString((int)headerCrc()))
        .append(";Length=")
        .append(size())
        .append(";KeyType=")
        .append(getKeyType(getKeyTypeAttribute()))
        .append(";Key=");
    if (isKeyString())
    {
      sb.append("0x")
          .append(Hex.encodeHexString(keyBytes()));
    }
    else if (isKeyNumber())
    {
      sb.append(key());
    }
    else if (isKeySchema())
    {
      sb.append(getKeyPart().toString());
    }

    sb.append(";Sequence=")
        .append(sequence())
        .append(";PartitionId=")
        .append(getPartitionId())
        .append(";Timestamp=")
        .append(timestampInNanos())
        .append(";SrcId=")
        .append(srcId())
        .append(";SchemaId=")
        .append(schemaId() == null ? "null" : "0x" + Hex.encodeHexString(schemaId()))
        .append(";ValueCrc=")
        .append("0x")
        .append(Integer.toHexString((int)bodyCrc()))
        .append(";HasMetadata=")
        .append(hasMetadata())
        .append(";hasPayloadPart=")
        .append(hasPayloadPart())
        .append(";PayloadLen=")
        .append(payloadLength());

    if (hasMetadata())
    {
      sb.append(";Metadata={")
          .append(getPayloadMetadataPart().toString())
          .append("}");
    }

    // Do not print data here, it could be big.
    return sb.toString();
  }
}
