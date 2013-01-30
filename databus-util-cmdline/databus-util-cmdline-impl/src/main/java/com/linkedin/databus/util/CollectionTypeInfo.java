package com.linkedin.databus.util;
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



/**
 * TypeInfo implementation for a database collection type. In Oracle, an array column is implemented as a VARRAY
 * collection of a built in type (VARCHAR2, NUMBER, etc.) or user type (like DBUS_EDU_PROF_T). These show up in tables
 * like profile, where every row has a collection of education history records and employment history records.
 */
public class CollectionTypeInfo
  implements TypeInfo
{
  private final String _ownerName;
  private final String _name;
  private final TypeInfo _elementTypeInfo;

  CollectionTypeInfo(String ownerName, String name, TypeInfo elementTypeInfo)
  {
    _ownerName = ownerName;
    _name = name;
    _elementTypeInfo = elementTypeInfo;
  }

  /**
   * @return owner of this collection, typically the schema in which the collection exists
   */
  public String getOwnerName()
  {
    return _ownerName;
  }

  /**
   * @return name of the collection
   */
  public String getName()
  {
    return _name;
  }

  /**
   * @return name of the data type stored in this collection, such as 'VARCHAR2', 'NUMBER', 'DBUS_EDU_PROF_T', etc.
   */
  public TypeInfo getElementTypeInfo()
  {
    return _elementTypeInfo;
  }

  @Override
  public String toString()
  {
    return "CollectionType: " + _name + "; ElementTypeInfo: " + _elementTypeInfo;
  }
}
