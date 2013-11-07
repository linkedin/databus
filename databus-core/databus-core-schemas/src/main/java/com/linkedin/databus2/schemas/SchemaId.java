package com.linkedin.databus2.schemas;
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


import java.util.Arrays;

import org.apache.avro.Schema;
import org.apache.commons.codec.binary.Hex;

import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus2.schemas.utils.Utils;

/**
 * This class contains a byte-array that is used as a unique identifier for an Avro schema.
 * The byte array is of size 16 (if we use MD5 hash) or 4 (if we use CRC32).
 *
 * "Borrowed"  largely from com.linkedin.avro.SchemaId
 */
public class SchemaId
{
  private final byte[] _idBytes;

  public SchemaId(byte[] newIdBytes)
  {
    super();
    Utils.notNull(newIdBytes);
    // As of now, we support either MD5 or CRC32.
    //restrict byte sequences to either handle 128 bits or 32 bits
    if((newIdBytes.length != DbusEvent.MD5_DIGEST_LEN)  && (newIdBytes.length != DbusEvent.CRC32_DIGEST_LEN))
    {
      throw new IllegalArgumentException("schema id is of length is " + newIdBytes.length +  " Expected length: "
                                         + DbusEvent.CRC32_DIGEST_LEN + " or " + DbusEvent.MD5_DIGEST_LEN);
    }
    _idBytes = newIdBytes.clone();
  }

  /**
   * Create a schema ID with an MD5  for a given Avro schema.
   */
  public static SchemaId createWithMd5(Schema schema)
  {
    return new SchemaId(Utils.md5(Utils.utf8(schema.toString(false))));
  }

  public static SchemaId createWithMd5(String schema)
  {
    return createWithMd5(Schema.parse(schema));
  }

  @Override
  public boolean equals(Object obj)
  {
    if(obj == null || !(obj instanceof SchemaId)) return false;
    if (this == obj) return true; //shortcut
    SchemaId id = (SchemaId) obj;
    return Arrays.equals(_idBytes, id._idBytes);
  }

  @Override
  public int hashCode()
  {
    return Arrays.hashCode(_idBytes);
  }

  @Override
  public String toString()
  {
    return Hex.encodeHexString(_idBytes);
  }

  public byte[] getByteArray() {
    return _idBytes;
  }
}
