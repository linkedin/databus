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

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Arrays;

import org.apache.avro.util.Utf8;

public class DbusEventKey
{
  // TODO Decide if we want to expose this object or move this enum to DbusEvent
  public enum KeyType
  {
    LONG,
    STRING,
    SCHEMA,
  }

  private final KeyType _keyType;
  private final Long _longKey;
  // TODO Remove this in favor of _strinKeyInBytes
  private final String _stringKey;
  private final byte[] _stringKeyInBytes;
  private final DbusEventPart _schemaKey;

  public DbusEventKey(long key)
  {
    _longKey = key;
    _stringKey = null;
    _keyType = KeyType.LONG;
    _stringKeyInBytes = null;
    _schemaKey = null;
  }

  /**
   * @deprecated Use the constructor with byte[] instead.
   * @param key
   */
  @Deprecated
  public DbusEventKey(String key)
  {
    _stringKey = key;
    _longKey = null;
    _keyType = KeyType.STRING;
    _stringKeyInBytes = null;
    _schemaKey = null;
  }

  public DbusEventKey(byte[] key)
  {
    _keyType = KeyType.STRING;
    _longKey = null;
    _stringKey = null;
    _stringKeyInBytes = Arrays.copyOf(key, key.length);
    _schemaKey = null;
  }

  public DbusEventKey(Object key)
      throws UnsupportedKeyException
  {
    if(key == null)
    {
      throw new IllegalArgumentException("Key cannot be null.");
    }

    if ((key instanceof Long) || (key instanceof Integer))
    {
      _longKey = ((Number)key).longValue();
      _stringKey = null;
      _keyType = KeyType.LONG;
      _stringKeyInBytes = null;
      _schemaKey = null;
    }
    else if ((key instanceof String))
    {
      _longKey = null;
      _stringKey = (String) key;
      _keyType = KeyType.STRING;
      _stringKeyInBytes = null;
      _schemaKey = null;
    }
    else if ((key instanceof Utf8))
    {
      _longKey = null;
      _stringKey = ((Utf8)key).toString();
      _keyType = KeyType.STRING;
      _stringKeyInBytes = null;
      _schemaKey = null;
    }
    else if ((key instanceof byte[]))
    {
      _longKey = null;
      _stringKey = null;
      _keyType = KeyType.STRING;
      _stringKeyInBytes = Arrays.copyOf((byte[])key, ((byte[]) key).length);
      _schemaKey = null;
    }
    else if ((key instanceof  DbusEventPart))
    {
      _longKey = null;
      _stringKeyInBytes = null;
      _stringKey = null;
      _schemaKey = (DbusEventPart)key;
      _keyType = KeyType.SCHEMA;
    }
    else
    {
      throw new UnsupportedKeyException("Bad key type: " + key.getClass().getName());
    }
  }

  public KeyType getKeyType()
  {
    return _keyType;
  }
  public Long getLongKey()
  {
    return _longKey;
  }

  /**
   * @deprecated Use getStringKeyInBytes() instead.
   * For now, use this API only if DbusEventKey is constructed with a string.
   * @return
   */
  public String getStringKey()
  {
    if (_stringKey == null)
    {
      throw new RuntimeException("Invalid method invocation on key type " + _keyType);
    }
    return _stringKey;
  }

  /**
   * Returns the key in a byte array.
   * If the (deprecated) String-based constructor was used to construct the object
   * then the UTF-8 representation of the string is returned.
   */
  public byte[] getStringKeyInBytes()
  {
    if (_stringKeyInBytes != null)
      return Arrays.copyOf(_stringKeyInBytes, _stringKeyInBytes.length);
    if (_stringKey != null)
      return _stringKey.getBytes(Charset.forName("UTF-8"));
    throw new RuntimeException("Invalid method invocation on key type " + _keyType);
  }

  public DbusEventPart getSchemaKey()
  {
    if (_schemaKey == null)
    {
      throw new RuntimeException("Invalid method invocation on key type " + _keyType);
    }
    return _schemaKey;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder(  "DbusEventKey [_keyType=");
    builder.append(_keyType);
    builder.append(", _longKey=");
    builder.append(_longKey);
    builder.append(", _stringKey=");
    String stringKey;
    try {
      stringKey = _stringKey != null ? _stringKey : (_stringKeyInBytes == null ? "NULL" : new String(_stringKeyInBytes, "UTF-8"));
    } catch (UnsupportedEncodingException e) {
      stringKey = e.getLocalizedMessage();
    } 
    builder.append(stringKey);
    builder.append(", _schemaKey=" + _schemaKey);
    builder.append("]");
    return builder.toString();
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((_keyType == null) ? 0 : _keyType.hashCode());
    result = prime * result + ((_longKey == null) ? 0 : _longKey.hashCode());
    result = prime * result
        + ((_stringKey != null) ? _stringKey.hashCode() : (_stringKeyInBytes == null) ? 0 :
            Arrays.hashCode(_stringKeyInBytes));
    result = prime * result + ((_schemaKey == null) ? 0 : _schemaKey.hashCode());
    return result;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    DbusEventKey other = (DbusEventKey) obj;
    if (_keyType != other._keyType)
      return false;
    if (_longKey == null) {
      if (other._longKey != null)
        return false;
    } else if (!_longKey.equals(other._longKey))
      return false;
    if (_stringKey == null) {
      if (other._stringKey != null)
        return false;
    } else if (!_stringKey.equals(other._stringKey))
      return false;
    if (_stringKeyInBytes == null) {
      if (other._stringKeyInBytes != null)
        return false;
    } else if (!Arrays.equals(_stringKeyInBytes, other._stringKeyInBytes))
      return false;
    if (_schemaKey == null) {
      if (other._schemaKey != null) {
        return false;
      }
    } else {
      if (other._schemaKey == null) {
        return false;
      }
      if (!_schemaKey.equals(other._schemaKey)) {
        return false;
      }
    }
    return true;
  }
}
