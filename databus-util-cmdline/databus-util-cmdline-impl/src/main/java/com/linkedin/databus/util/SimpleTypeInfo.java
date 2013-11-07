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
        if (scale <= 6)
        {
          _type = AvroPrimitiveTypes.FLOAT;
        }
        else if (scale <= 17)
        {
          _type = AvroPrimitiveTypes.DOUBLE;
        }
        else
        {
          throw new RuntimeException("Cannot handle scale of greater than 17");
        }
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
      // to remove the SYS. prefix for XMLTYPE;
      if (name.startsWith("SYS.")) 
      {
         name = name.substring(4);
      }   
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
