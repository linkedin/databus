package com.linkedin.databus2.producers.ds;

import org.apache.avro.Schema;

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
public class KeyPair
{
    Object key;
    Schema.Type keyType;

    public Schema.Type getKeyType()
    {
      return keyType;
    }

    public Object getKey()
    {
      return key;
    }

    public KeyPair(Object key, Schema.Type keyType)
    {
      this.key = key;
      this.keyType = keyType;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      KeyPair keyPair = (KeyPair) o;

      if (!key.equals(keyPair.key)) return false;
      if (keyType != keyPair.keyType) return false;

      return true;
    }

    @Override
    public int hashCode()
    {
      int result = key.hashCode();
      result = 31 * result + keyType.hashCode();
      return result;
    }
}
