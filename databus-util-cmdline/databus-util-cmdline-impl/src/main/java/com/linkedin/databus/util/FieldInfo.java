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
 * Information about a single field (column) in a table, user type, etc.
 */
public class FieldInfo
{
  private final String _fieldName;
  private final TypeInfo _fieldTypeInfo;
  private final int _fieldPosition;
  private final String _avroFieldName;

  public FieldInfo(String fieldName, String avroFieldName, TypeInfo fieldTypeInfo, int fieldPosition)
  {
    _fieldName = fieldName;
    _avroFieldName = avroFieldName;
    _fieldTypeInfo = fieldTypeInfo;
    _fieldPosition = fieldPosition;
  }

  public FieldInfo(String fieldName, TypeInfo fieldTypeInfo, int fieldPosition)
  {
    this(fieldName, fieldName, fieldTypeInfo, fieldPosition);
  }

  /**
   * @return name of the field
   */
  public String getFieldName()
  {
    return _fieldName;
  }

  public String getAvroFieldName()
  {
    return _avroFieldName;
  }

  /**
   * @return the TypeInfo object for this field
   */
  public TypeInfo getFieldTypeInfo()
  {
    return _fieldTypeInfo;
  }

  /**
   * @return the position (index) of this field; note that indexes are always zero based even though rs.getObject(...) is 1 based!
   */
  public int getFieldPosition()
  {
    return _fieldPosition;
  }

  @Override
  public String toString()
  {
    return "Field: " + _fieldName + "; " + _fieldTypeInfo;
  }
}
