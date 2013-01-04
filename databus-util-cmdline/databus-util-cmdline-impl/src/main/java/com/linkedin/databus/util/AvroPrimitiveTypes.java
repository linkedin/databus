/*
 * $Id: AvroPrimitiveTypes.java 151262 2010-11-17 23:00:29Z jwesterm $
 */
package com.linkedin.databus.util;

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
