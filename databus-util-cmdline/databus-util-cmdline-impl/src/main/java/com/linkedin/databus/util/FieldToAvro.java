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
 * User: jwesterm Date: Oct 15, 2010 Time: 3:15:29 PM
 */
public class FieldToAvro
{

  public String buildAvroSchema(String namespace,
                                String topRecordAvroName,
                                String topRecordDatabaseName,
                                String[][] headers,
                                TableTypeInfo topRecordTypeInfo)
  {
    if(namespace == null)
      throw new IllegalArgumentException("namespace should not be null.");
    if(topRecordAvroName == null)
      throw new IllegalArgumentException("topRecordAvroName should not be null.");
    if(topRecordDatabaseName == null)
      throw new IllegalArgumentException("topRecordDatabaseName should not be null.");
    if(topRecordTypeInfo == null)
      throw new IllegalArgumentException("topRecordTypeInfo should not be null.");

    FieldInfo fieldInfo = new FieldInfo(topRecordDatabaseName, topRecordTypeInfo, -1);
    Map<String, Object> field = fieldToAvro(fieldInfo, true);

    // Overwrite the name with the nice Java record name
    field.put("name", topRecordAvroName);

    // Add namespace
    field.put("namespace", namespace);

    // Serialize to JSON
    try
    {
      SimpleDateFormat df = new SimpleDateFormat("MMM dd, yyyy hh:mm:ss a zzz");
      field.put("doc", "Auto-generated Avro schema for " + topRecordDatabaseName + ". Generated at " + df.format(new Date(System.currentTimeMillis())));

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

    if(typeInfo instanceof SimpleTypeInfo)
    {
      return simpleTypeToAvro(fieldInfo, (SimpleTypeInfo) typeInfo);
    }
    else if(typeInfo instanceof TableTypeInfo)
    {
      return tableTypeToAvro(fieldInfo, (TableTypeInfo) typeInfo, asSchema);
    }
    else if(typeInfo instanceof UserTypeInfo)
    {
      return userTypeToAvro(fieldInfo, (UserTypeInfo) typeInfo, asSchema);
    }
    else if(typeInfo instanceof CollectionTypeInfo)
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

    field.put("type", arrayType);

    // Field metadata
    String dbFieldName = fieldInfo.getFieldName();
    int dbFieldPosition = fieldInfo.getFieldPosition();
    String meta = buildMetaString(dbFieldName, dbFieldPosition,"");
    itemsRecordType.put("meta", meta);

    return field;
  }

  private Map<String,Object> userTypeToAvro(FieldInfo fieldInfo, UserTypeInfo typeInfo, boolean asSchema)
  {
    Map<String,Object> field = new HashMap<String,Object>();

    // Field name
    String name = SchemaUtils.toCamelCase(fieldInfo.getFieldName());
    field.put("name", name);

    // Field type
    Map<String,Object> type = new HashMap<String, Object>();
    //check if we are a top-level record or not
    Map<String,Object> fieldsDest = asSchema ? field : type;
    if (asSchema)
    {
      field.put("type", "record");
    }
    else
    {
       type.put("type", "record");
       type.put("name", typeInfo.getName());
       field.put("type", type);
    }

    // Child fields
    List<Map<String, Object>> fields = new ArrayList<Map<String, Object>>();
    for(FieldInfo childField : typeInfo.getFields())
    {
      Map<String, Object> childFieldMap = fieldToAvro(childField, false);
      fields.add(childFieldMap);
    }
    fieldsDest.put("fields", fields);

    // Field metadata
    String dbFieldName = fieldInfo.getFieldName();
    int dbFieldPosition = fieldInfo.getFieldPosition();
    String meta = buildMetaString(dbFieldName, dbFieldPosition,"");
    field.put("meta", meta);

    // Return the Map for this field
    return field;
  }

  private Map<String,Object> tableTypeToAvro(FieldInfo fieldInfo, TableTypeInfo typeInfo, boolean asSchema)
  {
    Map<String,Object> field = new HashMap<String,Object>();

    // Field name
    String name = SchemaUtils.toCamelCase(fieldInfo.getFieldName());
    field.put("name", name);

    // Field type
    Map<String,Object> type = new HashMap<String, Object>();
    //check if we are a top-level record or not
    Map<String,Object> fieldsDest = asSchema ? field : type;
    if (asSchema)
    {
      field.put("type", "record");
    }
    else
    {
       type.put("type", "record");
       type.put("name", typeInfo.getName());
       field.put("type", type);
    }

    // Child fields
    List<Map<String, Object>> fields = new ArrayList<Map<String, Object>>();
    for(FieldInfo childField : typeInfo.getFields())
    {
      Map<String, Object> childFieldMap = fieldToAvro(childField, false);
      fields.add(childFieldMap);
    }
    fieldsDest.put("fields", fields);

    // Field metadata
    String dbFieldName = fieldInfo.getFieldName();
    int dbFieldPosition = fieldInfo.getFieldPosition();
    String pk = typeInfo.getPrimaryKey();
    String meta = buildMetaString(dbFieldName, dbFieldPosition,pk);
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

    // Field type
    String[] type = new String[] { typeInfo.getPrimitiveType().getAvroType(), "null" };
    field.put("type", type);

    // Field metadata
    String dbFieldName = fieldInfo.getFieldName();
    int dbFieldPosition = fieldInfo.getFieldPosition();
    
    String meta = buildMetaString(dbFieldName, dbFieldPosition,"");
    field.put("meta", meta);
    
    // Return the Map for this field
    return field;
  }

  private String buildMetaString(String dbFieldName, int dbFieldPosition,String pk)
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
    if (!pk.isEmpty())
    {
    	meta.append("pk=" + pk + ";");
    }
    
    return meta.toString();
  }

}
