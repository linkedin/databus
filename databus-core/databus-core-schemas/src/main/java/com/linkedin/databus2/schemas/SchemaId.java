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

import com.linkedin.databus2.schemas.utils.Utils;

/**
 * "Borrowed"  largely from com.linkedin.avro.SchemaId
 */
public class SchemaId
{
  private final byte[] md5 = new byte[16];

  public SchemaId(byte[] new_md5)
  {
    super();
    Utils.notNull(new_md5);
    if(md5.length != new_md5.length)
      throw new IllegalArgumentException("schema id is of the wrong length (should be 16).");
    for(int i=0; i<new_md5.length; i++)
    	md5[i] = new_md5[i];
  }

  public static SchemaId forSchema(Schema schema)
  {
    return new SchemaId(Utils.md5(Utils.utf8(schema.toString(false))));
  }

  public static SchemaId forSchema(String schema)
  {
    return forSchema(Schema.parse(schema));
  }

  @Override
  public boolean equals(Object obj)
  {
    if(obj == null || !(obj instanceof SchemaId)) return false;
    SchemaId id = (SchemaId) obj;
    return Arrays.equals(md5, id.md5);
  }

  @Override
  public int hashCode()
  {
    return Arrays.hashCode(md5);
  }

  @Override
  public String toString()
  {
    return Utils.hex(md5);
  }

  public byte[] getByteArray() {
    return md5;
  }
}
