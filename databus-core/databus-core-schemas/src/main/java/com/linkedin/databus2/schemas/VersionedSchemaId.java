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



public class VersionedSchemaId
{
  private final String _baseSchemaName;
  private final short _version;

  public VersionedSchemaId(String baseSchemaName, short version)
  {
    super();
    _baseSchemaName = baseSchemaName;
    _version = version;
  }

  public String getBaseSchemaName()
  {
    return _baseSchemaName;
  }

  public short getVersion()
  {
    return _version;
  }

  @Override
  public boolean equals(Object o)
  {
    if (null == o || !(o instanceof VersionedSchemaId)) return false;
    VersionedSchemaId other = (VersionedSchemaId)o;
    return _version == other._version && _baseSchemaName.equals(other._baseSchemaName);
  }

  @Override
  public int hashCode()
  {
    return _baseSchemaName.hashCode() ^ _version;
  }

  @Override
  public String toString()
  {
    StringBuilder res = new StringBuilder();
    res.append(_baseSchemaName);
    res.append(':');
    res.append(_version);

    return res.toString();
  }

}
