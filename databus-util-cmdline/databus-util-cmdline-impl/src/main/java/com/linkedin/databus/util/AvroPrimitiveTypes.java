/*
 * $Id: AvroPrimitiveTypes.java 151262 2010-11-17 23:00:29Z jwesterm $
 */
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
 * @author Jemiah Westerman<jwesterman@linkedin.com>
 * @version $Revision: 151262 $
 */
public enum AvroPrimitiveTypes
{
  INTEGER("int"),
  LONG("long"),
  RAW("bytes"),
  FLOAT("float"),
  DOUBLE("float"),
  CLOB("string"),
  VARCHAR("string"),
  VARCHAR2("string"),
  NVARCHAR("string"),
  NVARCHAR2("string"),
  TIMESTAMP("long"),
  CHAR("string"),
  DATE("long"),
  BLOB("bytes"),
  ARRAY("array"), 
  TABLE("record"),
  XMLTYPE("string");

  private final String _avroType;
  private AvroPrimitiveTypes(String avroType)
  {
    _avroType = avroType;
  }
  public String getAvroType()
  {
    return _avroType;
  }
}
