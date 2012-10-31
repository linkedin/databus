package com.linkedin.databus.util;


import java.util.Collections;
import java.util.List;


/**
 * TypeInfo implementation for user defined column types. A user type is typically comprised of a number of columns
 * of built in types, basically like a table within a table. We see examples of this in member2, which has an education
 * history type with the name of the institution (VARCHAR2), dates attended (DATE), etc.
 */
public class UserTypeInfo
  implements TypeInfo
{
  private final String _ownerName;
  private final String _name;
  private final List<FieldInfo> _fields;

  public UserTypeInfo(String ownerName, String name, List<FieldInfo> fields)
  {
    _ownerName = ownerName;
    _name = name;
    _fields = Collections.unmodifiableList(fields);
  }

  /**
   * @return owner of the database type; typically the schema in which it was created
   */
  public String getOwnerName()
  {
    return _ownerName;
  }

  /**
   * @return name of this user type (like DATABUS_PROF_EDU_T)
   */
  public String getName()
  {
    return _name;
  }

  /**
   * @return info for all fields (columns) in this user defined type
   */
  public List<FieldInfo> getFields()
  {
    return _fields;
  }

  public String toString()
  {
    return "UserType: " + _name + "; Fields: " + _fields;
  }
}
