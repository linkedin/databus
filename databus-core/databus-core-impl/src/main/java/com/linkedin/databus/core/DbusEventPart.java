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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.commons.codec.binary.Hex;
import org.codehaus.jackson.JsonGenerator;
import com.linkedin.databus.core.DbusEvent.SchemaDigestType;
import com.linkedin.databus.core.util.Base64;
import com.linkedin.databus.core.util.Utils;


/**
 * This class holds fields from a portion of the DbusEvent.
 * DbusEventV2 carries key, metadata, and payload components, each having
 * the following attributes:
 * <pre>
 * - Schema digest type (MD5 or CRC-32)
 * - Schema digest
 * - Schema version
 * - Data blob (meaning the actual key, metadata, or payload)
 * </pre>
 * See wiki: Databus+Event+enhancement+%28Multi-Colo%29 for event layout.
 */
public class DbusEventPart
{
  private static final short SCHEMA_DIGEST_TYPE_MD5 = 0;
  private static final short SCHEMA_DIGEST_TYPE_CRC32 = 1;
  private static final short VERSION_SHIFT = 2;
  private static final short DIGEST_MASK = 0x3;
  private static final int AttributesOffset = 4;
  private static final int AttributesLen = 2;
  private static final int MAX_DATA_BYTES_PRINTED = 64;

  private final SchemaDigestType _schemaDigestType;
  private final byte[] _schemaDigest;
  private final short _schemaVersion;
  private ByteBuffer _data = null;

  public DbusEventPart(SchemaDigestType schemaDigestType, byte[] schemaDigest, short schemaVersion, ByteBuffer data)
  {
    _schemaDigestType = schemaDigestType;
    _schemaDigest = schemaDigest.clone();
    _schemaVersion = schemaVersion;
    _data = data;
    switch(_schemaDigestType)
    {
      case MD5:
        if (_schemaDigest.length != DbusEvent.MD5_DIGEST_LEN)
        {
          throw new DatabusRuntimeException("Invalid MD5 schema digest length:" + _schemaDigest.length);
        }
        break;
      case CRC32:
        if (_schemaDigest.length != DbusEvent.CRC32_DIGEST_LEN)
        {
          throw new DatabusRuntimeException("Invalid CRC-32 schema digest length:" + _schemaDigest.length);
        }
        break;
      default:
        throw new UnsupportedOperationException("Unsupported schema digest type:" + _schemaDigestType);
    }
  }

  public SchemaDigestType getSchemaDigestType()
  {
    return _schemaDigestType;
  }

  public byte[] getSchemaDigest()
  {
    return _schemaDigest.clone();
  }

  public short getSchemaVersion()
  {
    return _schemaVersion;
  }

  /**
   * Returns the actual data blob of the event-part.  The returned ByteBuffer is
   * read-only, and its position and limit may be freely modified.
   * <b>NOTE: The data may be subsequently overwritten; if you need it beyond the
   * onDataEvent() call, save your own copy before returning.</b>
   */
  public ByteBuffer getData()
  {
    return _data.asReadOnlyBuffer().slice().order(_data.order());
  }

  /** Returns number of bytes remaining. */ // TODO:  make this public?
  private int getDataLength()
  {
    return _data.limit() - _data.position();  // why not just _data.remaining() ?  ("elements" here are always bytes)
  }

  public void encode(ByteBuffer buf)
  {
    int curPos = _data.position();
    buf.putInt(getDataLength());
    short attributes = 0;
    switch (_schemaDigestType)
    {
      case MD5:
        attributes = SCHEMA_DIGEST_TYPE_MD5;
        break;
      case CRC32:
        attributes = SCHEMA_DIGEST_TYPE_CRC32;
        break;
      default:
        throw new UnsupportedOperationException("Unsupported schema digest type:" + _schemaDigestType);
    }
    attributes |= (_schemaVersion << VERSION_SHIFT);
    buf.putShort(attributes);
    buf.put(_schemaDigest);
    buf.put(_data);
    _data.position(curPos);
  }

  private static SchemaDigestType digestType(short attributes)
  {
    short digestType = (short)(attributes & DIGEST_MASK);
    switch (digestType)
    {
      case SCHEMA_DIGEST_TYPE_CRC32:
        return SchemaDigestType.CRC32;
      case SCHEMA_DIGEST_TYPE_MD5:
        return SchemaDigestType.MD5;
      default:
        throw new UnsupportedOperationException("Digest type " + digestType + " not supported");
    }
  }

  private static int digestLen(SchemaDigestType digestType)
  {
    switch (digestType)
    {
      case MD5:
        return DbusEvent.MD5_DIGEST_LEN;
      case CRC32:
        return DbusEvent.CRC32_DIGEST_LEN;
      default:
        throw new UnsupportedOperationException("Digest type " + digestType + " not supported");
    }
  }

  /**
   * Decodes a bytebuffer and returns DbusEventPart. Preserves the ByteBuffer position.
   * @param buf
   * @return
   */
  public static DbusEventPart decode(ByteBuffer buf)
  {
    int pos = buf.position();
    int dataLen = buf.getInt(pos);
    if (dataLen < 0)
    {
      throw new UnsupportedOperationException("Data length " + dataLen + " not supported");
    }
    short attributes = buf.getShort(pos+AttributesOffset);
    short schemaVersion  = (short)(attributes >> VERSION_SHIFT);
    SchemaDigestType schemaDigestType = digestType(attributes);
    int digestLen = digestLen(schemaDigestType);
    byte[] digest = new byte[digestLen];
    for (int i = 0; i < digestLen; i++)
    {
      digest[i] = buf.get(pos+AttributesOffset+AttributesLen+i);
    }

    // NOTE - this will create a new ByteBuffer object pointing to the
    // same memory. So the position of this new BB is beginning of the data
    // and limit is set to the end of the data.
    ByteBuffer dataBuf = buf.asReadOnlyBuffer();
    dataBuf.position(pos + AttributesOffset + AttributesLen + digestLen);
    dataBuf.limit(dataBuf.position() + dataLen);

    return new DbusEventPart(schemaDigestType, digest, schemaVersion, dataBuf);
  }

  public static int computePartLength(SchemaDigestType digestType, int dataLen)
  {
    return AttributesOffset+AttributesLen + dataLen + digestLen(digestType);
  }

  /**
   * Replace the schema-digest in a serialized DbusEventPart.
   *
   * @param buf The buffer that contains the serialized DbusEventPart.
   * @param position the position in the buffer where the DbusEventPart starts.
   * @param schemaDigest The digest value to substitute. The value must match in length to the existing value.
   */
  public static void replaceSchemaDigest(ByteBuffer buf, int position, byte[] schemaDigest)
  {
    DbusEvent.SchemaDigestType digestType = digestType(buf.getShort(position+AttributesOffset));
    int digestLen = digestLen(digestType);
    if (schemaDigest.length != digestLen)
    {
      throw new RuntimeException("Expecting length " + digestLen + " for type " + digestType + ", found " + schemaDigest.length);
    }
    for (int i = 0; i < digestLen; i++)
    {
      buf.put(position+AttributesOffset+AttributesLen+i, schemaDigest[i]);
    }
  }

  public void printString (String prefix, JsonGenerator g, Encoding encoding ) throws IOException
  {
    g.writeNumberField(prefix + "Length", getDataLength());
    g.writeNumberField(prefix + "SchemaVersion", getSchemaVersion());
    //This is really the payload schema digest, but for legacy reasons we have to call it schemaid
    g.writeStringField(prefix + "SchemaId",  Base64.encodeBytes(getSchemaDigest()));
    //This is really the payload but for historical reasons we have to call it value

    if (encoding.equals(Encoding.JSON))
    {
      g.writeStringField(prefix + "Value", Base64.encodeBytes(Utils.byteBufferToBytes(getData())));
    }
    else
    {
      g.writeStringField(prefix + "Value", Utils.byteBufferToString(getData()));
    }
  }

  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder(64);
    sb.append("Length=")
        .append(getDataLength())
        .append(";SchemaVersion=")
        .append(getSchemaVersion())
        .append(";SchemaId=")
        .append("0x")
        .append(Hex.encodeHexString(getSchemaDigest()));

    ByteBuffer dataBB = getData();
    if (dataBB.remaining() > MAX_DATA_BYTES_PRINTED)
    {
      dataBB.limit(MAX_DATA_BYTES_PRINTED);
    }
    byte[] data = new byte[dataBB.remaining()];
    dataBB.get(data);
    sb.append(";Value=")
        .append("0x")
        .append(Hex.encodeHexString(data));
    return sb.toString();
  }

  /**
   * @return The number of bytes that this DbusEventPart would take up, if it were to be serialized.
   */
  public int computePartLength()
  {
    return AttributesOffset+AttributesLen + _data.remaining() + _schemaDigest.length;
  }

  /**
   * @return the length of the DbusEventPart that is encoded in 'buf' at position 'position'.
   * Callers can use this method to advance across the DbusEventPart in a serialized V2 event.
   */
  public static int partLength(ByteBuffer buf, int position)
  {
    DbusEvent.SchemaDigestType digestType = digestType(buf.getShort(position+AttributesOffset));
    int digestLen = digestLen(digestType);
    return AttributesOffset + AttributesLen + digestLen + buf.getInt(position);
  }
  @Override
  public boolean equals(Object obj)
  {
    if (this == obj)
    {
      return true;
    }
    if (obj == null)
    {
      return false;
    }
    if (getClass() != obj.getClass())
    {
      return false;
    }
    DbusEventPart other = (DbusEventPart)obj;
    if (_schemaVersion != other._schemaVersion)
    {
      return false;
    }
    if (_schemaDigestType != other._schemaDigestType)
    {
      return false;
    }
    if (_schemaDigest == null && other._schemaDigest != null ||
        _schemaDigest != null && other._schemaDigest == null)
    {
      return false;
    }
    if (!Arrays.equals(_schemaDigest, other._schemaDigest))
    {
      return false;
    }

    if (_data == null && other._data != null ||
        _data != null && other._data == null)
    {
      return false;
    }
    if (!_data.equals(other._data))
    {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    final int prime = 31;
    int result = 1;
    result = prime * result + (_schemaDigest == null ? 0 : Arrays.hashCode(_schemaDigest));
    result = prime * result + (_data == null ? 0 : _data.hashCode());
    return result;
  }
}
