package com.linkedin.databus.util;


/**
 * Type info for a field with a simple (typically built in) data type such as VARCHAR2, NUMBER, etc.
 */
public class SimpleTypeInfo
  implements TypeInfo
{
  private final AvroPrimitiveTypes _type;

  public SimpleTypeInfo(String name, int precision, int scale)
  {
    if(name.equals("NUMBER"))
    {
      if(scale > 0)
      {
        _type = AvroPrimitiveTypes.FLOAT;
      }
      else if((precision > 9) || (precision==0))
      {
        _type = AvroPrimitiveTypes.LONG;
      }
      else
      {
        _type = AvroPrimitiveTypes.INTEGER;
      }
    }
    else
    {
      _type = AvroPrimitiveTypes.valueOf(name);
    }
  }

  public SimpleTypeInfo(AvroPrimitiveTypes type)
  {
    _type = type;
  }

  /**
   * @return who owns the type, typically ignored since this is usually a built in type
   */
  public String getOwnerName()
  {
    return null;
  }

  /**
   * @return name of the type, like 'NUMBER', 'VARCHAR2', etc.
   */
  public String getName()
  {
    return _type.toString();
  }

  @Override
  public String toString()
  {
    return "SimpleType: " + _type;
  }

  public AvroPrimitiveTypes getPrimitiveType()
  {
    return _type;
  }
}
