/*
 * $Id: SchemaHelper.java 269330 2011-05-11 23:34:36Z cbotev $
 */
package com.linkedin.databus2.schemas.utils;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;


/**
 * Static helper methods for common Avro schema related operations.
 *
 * @author Jemiah Westerman<jwesterman@linkedin.com>
 * @version $Revision: 269330 $
 */
public class SchemaHelper
{
  /**
   * @return true if the given field is an Avro array type
   */
  public static boolean isArray(Field field)
  {
    return containsType(field, Type.ARRAY);
  }

  /**
   * @return true if the given field is nullable
   */
  public static boolean isNullable(Field field)
  {
    return containsType(field, Type.NULL);
  }

  /**
   * @return true if the given field schema is nullable
   */
  public static boolean isNullable(Schema fieldSchema)
  {
    return containsType(fieldSchema, Type.NULL);
  }

  /**
   * @return the fields type; if the field is a union, the first non-null type from the union is returned.
   */
  public static Type getAnyType(Field field)
  {
    Schema schema = field.schema();
    Type fieldType = schema.getType();

    // If this is a union, then check the child types and see if the type exists here
    if(fieldType == Type.UNION)
    {
      List<Schema> unionTypes = schema.getTypes();
      for(Schema unionSubSchema : unionTypes)
      {
        if(unionSubSchema.getType() != Type.NULL)
        {
          return unionSubSchema.getType();
        }
      }
    }
    else
    {
      return fieldType;
    }

    // Not a match, return false.
    return fieldType;
  }

  public static Schema unwindUnionSchema(Field field)
  {
    Schema schema = field.schema();
    Type fieldType = schema.getType();

    // If this is a union, then check the child types and see if the type exists here
    if(fieldType == Type.UNION)
    {
      List<Schema> unionTypes = schema.getTypes();
      for(Schema unionSubSchema : unionTypes)
      {
        if(unionSubSchema.getType() != Type.NULL)
        {
          return unionSubSchema;
        }
      }
    }
    else
    {
      return schema;
    }

    // Not a match, return false.
    return schema;
  }

  /**
   * @return true if the field is of type {@code type} or is a union containing {@code type}
   */
  public static boolean containsType(Field field, Type type)
  {
    return containsType(field.schema(), type);
  }

  /**
   * @return true if the schema is of type {@code type} or is a union containing {@code type}
   */
  public static boolean containsType(Schema schema, Type type)
  {
    Type fieldType = schema.getType();

    // If the types are equal, then we're done
    if(fieldType == type)
    {
      return true;
    }

    // If this is a union, then check the child types and see if the type exists here
    if(fieldType == Type.UNION)
    {
      List<Schema> unionTypes = schema.getTypes();
      for(Schema unionSubSchema : unionTypes)
      {
        if(unionSubSchema.getType() == type)
        {
          return true;
        }
      }
    }

    // Not a match, return false.
    return false;
  }

  /**
   * @return the value of the meta field name contained in this Schema's "meta" attribute
   */
  public static final String getMetaField(Schema schema, String metaFieldName)
  {
    if(schema == null) return null;

    String metaValue = schema.getProp(metaFieldName);
    if (null != metaValue) return metaValue;

    String meta = schema.getProp("meta");
    if(meta == null) return null;

    String[] metaSplit = meta.split(";");
    for(String s : metaSplit)
    {
      int eqIdx = s.indexOf('=');
      if (eqIdx > 0)
      {
        String itemKey = s.substring(0, eqIdx).trim();
        String itemValue = s.substring(eqIdx + 1).trim();
        schema.addProp(itemKey, itemValue); //cache the value
        if(null == metaValue && metaFieldName.equals(itemKey))
        {
          metaValue = itemValue;
        }
      }
    }
    return metaValue;
  }

  /**
   * @return the value of the meta field name contained in this Field's "meta" attribute
   */
  public static final String getMetaField(Field field, String metaFieldName)
  {
    if(field == null) return null;

    String metaValue = field.getProp(metaFieldName);
    if (null != metaValue) return metaValue;

    String meta = field.getProp("meta");
    if(meta == null) return null;

    String[] metaSplit = meta.split(";");
    for(String s : metaSplit)
    {
      int eqIdx = s.indexOf('=');
      if (eqIdx > 0)
      {
        String itemKey = s.substring(0, eqIdx).trim();
        String itemValue = s.substring(eqIdx + 1).trim();
        field.addProp(itemKey, itemValue); //cache the value
        if(null == metaValue && metaFieldName.equals(itemKey))
        {
          metaValue = itemValue;
        }
      }
    }
    return metaValue;
  }

  /**
   * @return the schemaId for this schema
   */
  public static final byte[] getSchemaId(String schema)
  {
    return Utils.md5(schema.toString().getBytes());
  }
}
