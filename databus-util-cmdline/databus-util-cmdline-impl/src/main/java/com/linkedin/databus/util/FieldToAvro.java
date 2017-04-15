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


import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Generate an Avro schema to describe the fields of a database table.
 */
public class FieldToAvro
{

  public String buildAvroSchema(String namespace,
                                String topRecordAvroName,
                                String topRecordDatabaseName,
                                String[][] headers,
                                TableTypeInfo topRecordTypeInfo)
  {
    if (namespace == null)
      throw new IllegalArgumentException("namespace should not be null.");
    if (topRecordAvroName == null)
      throw new IllegalArgumentException("topRecordAvroName should not be null.");
    if (topRecordDatabaseName == null)
      throw new IllegalArgumentException("topRecordDatabaseName should not be null.");
    if (topRecordTypeInfo == null)
      throw new IllegalArgumentException("topRecordTypeInfo should not be null.");

    FieldInfo fieldInfo = new FieldInfo(topRecordDatabaseName, topRecordTypeInfo, -1);
    Map<String, Object> field = fieldToAvro(fieldInfo, true);

    // Overwrite the name with the nice Java record name
    field.put("name", topRecordAvroName);

    // Add namespace
    field.put("namespace", namespace);

    // Add doc and serialize to JSON
    try
    {
      SimpleDateFormat df = new SimpleDateFormat("MMM dd, yyyy hh:mm:ss a zzz");
      field.put("doc", "Auto-generated Avro schema for " + topRecordDatabaseName +
                       ". Generated at " + df.format(new Date(System.currentTimeMillis())));

      ObjectMapper mapper = new ObjectMapper();
      JsonFactory factory = new JsonFactory();
      StringWriter writer = new StringWriter();
      JsonGenerator jgen = factory.createJsonGenerator(writer);
      jgen.useDefaultPrettyPrinter();
      mapper.writeValue(jgen, field);
      return writer.getBuffer().toString();
    }
    catch(Exception ex)
    {
      throw new RuntimeException(ex);
    }
  }

  private Map<String,Object> fieldToAvro(FieldInfo fieldInfo, boolean asSchema)
  {
    TypeInfo typeInfo = fieldInfo.getFieldTypeInfo();
    //System.out.println(fieldInfo.getFieldName() + ":" + typeInfo.getClass().getSimpleName() + " --> " + asSchema);

    if (typeInfo instanceof SimpleTypeInfo)
    {
      return simpleTypeToAvro(fieldInfo, (SimpleTypeInfo) typeInfo);
    }
    else if (typeInfo instanceof UserTypeInfo)  // TableTypeInfo is now a subclass of this
    {
      return tableOrUserTypeToAvro(fieldInfo, (UserTypeInfo) typeInfo, asSchema);
    }
    else if (typeInfo instanceof CollectionTypeInfo)
    {
      return collectionTypeToAvro(fieldInfo, (CollectionTypeInfo)typeInfo);
    }
    return null;
  }

  private Map<String,Object> collectionTypeToAvro(FieldInfo fieldInfo, CollectionTypeInfo typeInfo)
  {
    Map<String,Object> field = new HashMap<String,Object>();

    // Field name
    String name = SchemaUtils.toCamelCase(fieldInfo.getFieldName());
    field.put("name", name);

    // Field type
    FieldInfo elementFieldInfo = new FieldInfo(typeInfo.getElementTypeInfo().getName(), typeInfo.getElementTypeInfo(), 0);
    Map<String, Object> itemsRecordType = fieldToAvro(elementFieldInfo, true);

    Map<String, Object> arrayType = new HashMap<String, Object>();
    arrayType.put("name", name + "Array");
    arrayType.put("type", "array");
    arrayType.put("items", itemsRecordType);

    List<Object> nullableType = new ArrayList<Object>();  // ["null", { .. arrayType .. }]
    nullableType.add("null");
    nullableType.add(arrayType);

    field.put("type", nullableType);
    field.put("default", null);

    // Field metadata
    String dbFieldName = fieldInfo.getFieldName();
    int dbFieldPosition = fieldInfo.getFieldPosition();
    String dbFieldType  = fieldInfo.getFieldTypeInfo().getName();
    String meta = buildMetaString(dbFieldName, dbFieldPosition, dbFieldType, null);
    itemsRecordType.put("meta", meta);

    return field;
  }

  private Map<String,Object> tableOrUserTypeToAvro(FieldInfo fieldInfo,
                                                   UserTypeInfo typeInfo,
                                                   boolean asSchema)
  {
    Map<String,Object> field = new HashMap<String,Object>();

    // Field name
    String name = SchemaUtils.toCamelCase(fieldInfo.getFieldName());
    field.put("name", name);

    // Field type
    Map<String,Object> realType = new HashMap<String, Object>();
    // check if we are a "top-level" record or not
    Map<String,Object> fieldsDest = asSchema ? field : realType;
    if (asSchema)
    {
      // asSchema is true only for the very topmost level of the schema (type = record; should never be null)
      // and for the "items" descriptor in collectionTypeToAvro() (aggregate descriptor of sub-fields; latter
      // may be null individually, but descriptor presumably never can be).  Ergo, "default":null makes sense
      // only in the other half of this conditional.
      field.put("type", "record");
    }
    else
    {
      realType.put("type", "record");                       // inner, curly-brace level ("real" structure)
      realType.put("name", typeInfo.getName());
      List<Object> nullableType = new ArrayList<Object>();  // outer, square-brackets level (solely for nullability)
      nullableType.add("null");
      nullableType.add(realType);
      field.put("type", nullableType);
      field.put("default", null); // field default value:  only for this level?
    }

    // Child fields
    List<Map<String, Object>> fields = new ArrayList<Map<String, Object>>();
    for (FieldInfo childField : typeInfo.getFields())
    {
      Map<String, Object> childFieldMap = fieldToAvro(childField, false);
      fields.add(childFieldMap);
    }
    fieldsDest.put("fields", fields);

    // Field metadata
    String dbFieldName = fieldInfo.getFieldName();
    int dbFieldPosition = fieldInfo.getFieldPosition();
    String dbFieldType = fieldInfo.getFieldTypeInfo().getName();
    String pk = typeInfo.getPrimaryKey();  // null unless TableTypeInfo (== top-level table)
    String meta = buildMetaString(dbFieldName, dbFieldPosition, dbFieldType, pk);
    field.put("meta", meta);

    // Return the Map for this field
    return field;
  }

  private Map<String, Object> simpleTypeToAvro(FieldInfo fieldInfo, SimpleTypeInfo typeInfo)
  {
    Map<String,Object> field = new HashMap<String,Object>();

    // Field name
    String name = SchemaUtils.toCamelCase(fieldInfo.getFieldName());
    field.put("name", name);

    // Field default value (for Avro unions, corresponds to _first_ field type in list)
    field.put("default", null);

    // Field type
    String[] type = new String[] {"null", typeInfo.getPrimitiveType().getAvroType()};
    field.put("type", type);

    // Field metadata
    String dbFieldName = fieldInfo.getFieldName();
    int dbFieldPosition = fieldInfo.getFieldPosition();
    String dbFieldType = fieldInfo.getFieldTypeInfo().getName();

    String meta = buildMetaString(dbFieldName, dbFieldPosition, dbFieldType, null);
    field.put("meta", meta);

    // Return the Map for this field
    return field;
  }

  private String buildMetaString(String dbFieldName, int dbFieldPosition, String dbFieldType, String pk)
  {
    // Metadata for database field name and position.
    // Have to store this as a serialized String, since Avro's "getProp()" method will not return
    // a complex object. We still write it in JSON, but it will be escaped and put in a String that
    // we have to deserialize later.
    StringBuilder meta = new StringBuilder();
    if(dbFieldName != null)
    {
      meta.append("dbFieldName=" + dbFieldName + ";");
    }

    if(dbFieldPosition != -1)
    {
      meta.append("dbFieldPosition=" + dbFieldPosition + ";");
    }

    if (dbFieldType != null)
    {
      meta.append("dbFieldType=" + dbFieldType + ";");
    }

    if ((null != pk) && (!pk.isEmpty()))
    {
      meta.append("pk=" + pk + ";");
    }

    return meta.toString();
  }

}
