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


import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;

/**
 * "Borrowed"  largely from com.linkedin.avro.SchemaId
 */
public class VersionedSchema
{
  private final Schema _schema;
  private final VersionedSchemaId _id;

  private final String _origSchemaStr;
  private final List<Schema.Field> _pkFieldList;

  public VersionedSchema(VersionedSchemaId id, Schema s, String origSchemaStr)
  {
    _schema = s;
    _id = id;
    _origSchemaStr = origSchemaStr;
    _pkFieldList = new ArrayList<Schema.Field>();
  }

  public VersionedSchema(String baseName, short id, Schema s, String origSchemaStr)
  {
    this(new VersionedSchemaId(baseName, id), s, origSchemaStr);
  }

  public int getVersion()
  {
    return this._id.getVersion();
  }

  public Schema getSchema()
  {
    return _schema;
  }

  @Override
  public String toString()
  {
    return "(" + getSchemaBaseName() + ","  + getVersion() + "," + _schema + ")";
  }

  /**
   * @return The source name (table name)
   */
  public String getSchemaBaseName()
  {
    return _id.getBaseSchemaName();
  }

  @Override
  public boolean equals(Object o)
  {
    if (null == o || ! (o instanceof VersionedSchema)) return false;
    VersionedSchema other = (VersionedSchema)o;
    return _id.equals(other._id);
  }

  @Override
  public int hashCode()
  {
    return _id.hashCode();
  }

  public VersionedSchemaId getId()
  {
    return _id;
  }
  
  public List<Schema.Field> getPkFieldList()
  {
	return _pkFieldList;
  }

  /**
   * @return The original schema string as registered. Returns null if the original schema string is not available.
   */
  public String getOrigSchemaStr()
  {
    return _origSchemaStr;
  }
}
