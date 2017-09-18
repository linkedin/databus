package com.linkedin.databus2.schemas.utils;
/*
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
 */


import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import com.linkedin.databus2.schemas.VersionedSchema;


/**
 * Static helper methods for common Avro schema-related operations.
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
   * @return the field's type; if the field is a union, the first non-null type from the union is returned.
   */
  public static Type getAnyType(Field field)
  {
    return unwindUnionSchema(field).getType();
  }

  /**
   * @return the field's schema; if the field is a union, the schema for the first non-null type from the
   *         union is returned.
   */
  public static Schema unwindUnionSchema(Field field)
  {
    Schema schema = field.schema();
    Type fieldType = schema.getType();

    // If this is a union, check the child types and return the first non-null schema
    if (fieldType == Type.UNION)
    {
      List<Schema> unionTypes = schema.getTypes();
      for (Schema unionSubSchema : unionTypes)
      {
        if (unionSubSchema.getType() != Type.NULL)
        {
          return unionSubSchema;
        }
      }
    }

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
    if (fieldType == type)
    {
      return true;
    }

    // If this is a union, then check the child types and see if the type exists here
    if (fieldType == Type.UNION)
    {
      List<Schema> unionTypes = schema.getTypes();
      for (Schema unionSubSchema : unionTypes)
      {
        if (unionSubSchema.getType() == type)
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
  // TODO:  merge this and subsequent method using generics for first arg
  public static final String getMetaField(Schema schema, String metaFieldName)
  {
    if (schema == null)
    {
      return null;
    }

    String metaValue = schema.getProp(metaFieldName);
    if (null != metaValue)
    {
      return metaValue;
    }

    String meta = schema.getProp("meta");
    if (meta == null)
    {
      return null;
    }

    String[] metaSplit = meta.split(";");
    for (String s : metaSplit)
    {
      int eqIdx = s.indexOf('=');
      if (eqIdx > 0)
      {
        String itemKey = s.substring(0, eqIdx).trim();
        String itemValue = s.substring(eqIdx + 1).trim();
        if (null == metaValue && metaFieldName.equals(itemKey))
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
    if (field == null)
    {
      return null;
    }

    String metaValue = field.getProp(metaFieldName);
    if (null != metaValue)
    {
      return metaValue;
    }

    String meta = field.getProp("meta");
    if (meta == null)
    {
      return null;
    }

    String[] metaSplit = meta.split(";");
    for (String s : metaSplit)
    {
      int eqIdx = s.indexOf('=');
      if (eqIdx > 0)
      {
        String itemKey = s.substring(0, eqIdx).trim();
        String itemValue = s.substring(eqIdx + 1).trim();
        if (null == metaValue && metaFieldName.equals(itemKey))
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
    return Utils.md5(schema.getBytes(Charset.defaultCharset()));
  }
  
  private static Map<String,List<Field>> orderedMap = new ConcurrentHashMap<String, List<Field>>();
  /**
   * Order the fields  present in the schema based on the metaFieldName and comparator being passed.
   *
   * @param schema  Schema containing the fields to be ordered
   * @param metaFieldName Meta Field for ordering
   * @param comparator comparator for sorting
   * @return ordered Field list or null if schema is null
   */
  public static List<Field> getOrderedFieldsByDBFieldPosition(final VersionedSchema vs)
  {
    if ( null == vs || null == vs.getSchema())
      return null;
    List<Field> fieldList = orderedMap.get(vs.getId().toString());
    if(fieldList != null)
    {
    	return fieldList;
    }
    
    Schema schema = vs.getSchema();
    fieldList = getOrderedFieldsByMetaField(schema, "dbFieldPosition", new Comparator<String>() {
		@Override
		public int compare(String o1, String o2) {
			// TODO Auto-generated method stub
			int m1 = Integer.parseInt(o1);
			int m2 = Integer.parseInt(o2);
			return m1-m2;
		}
	});
    orderedMap.put(vs.getId().toString(), fieldList);
    return fieldList;
  }

  /**
   * Order the fields  present in the schema based on the metaFieldName and comparator being passed.
   *
   * @param schema  Schema containing the fields to be ordered
   * @param metaFieldName Meta Field for ordering
   * @param comparator comparator for sorting
   * @return ordered Field list or null if schema is null
   */
  public static List<Field> getOrderedFieldsByMetaField(final Schema schema,
                                                        final String metaFieldName,
                                                        final Comparator<String> comparator )
  {
    if ( null == schema)
      return null;

    // It is safer to create a copy of fields list, as we want to change the order only on the copy
    // so as to not confuse AVRO in case it returns the internal fields collection
    List<Field> fields = new ArrayList<Field>(schema.getFields());

    Collections.sort(fields, new Comparator<Field>() {

      @Override
      public int compare(Field o1, Field o2)
      {
        String m1 = getMetaField(o1, metaFieldName);
        String m2 = getMetaField(o2, metaFieldName);
        return comparator.compare(m1, m2);
      }

    });
    return fields;

  }
}
