package com.linkedin.databus.util;


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
