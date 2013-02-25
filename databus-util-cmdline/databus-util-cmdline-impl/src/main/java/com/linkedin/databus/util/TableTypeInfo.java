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



import java.util.Collections;
import java.util.List;


/**
 * TypeInfo implementation for a database table.
 */
public class TableTypeInfo
  implements TypeInfo
{
  private final String _ownerName;
  private final String _name;

  private final List<FieldInfo> _fields;
  private final String _pk;

  public TableTypeInfo(String ownerName, String name, List<FieldInfo> fields,String pk)
  {
    _ownerName = ownerName;
    _name = name;
    _pk=pk;
    _fields = Collections.unmodifiableList(fields);
  }

  /**
   * @return owner of the table, typically the schema name in which the table exists
   */
  public String getOwnerName()
  {
    return _ownerName;
  }

  /**
   * @return name of the table
   */
  public String getName()
  {
    return _name;
  }
  
  /**
   * @retrun name of primary key field
   */
  public String getPrimaryKey()
  {
	  return _pk;
  }

  /**
   * @return info for all fields (columns) in the table
   */
  public List<FieldInfo> getFields()
  {
    return _fields;
  }

  public String toString()
  {
    return "Table: " + _name + "; Fields: " + _fields + " Pk:" + _pk;
  }
}
