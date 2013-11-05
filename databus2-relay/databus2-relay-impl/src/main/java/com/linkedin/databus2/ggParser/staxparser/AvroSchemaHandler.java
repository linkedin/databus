package com.linkedin.databus2.ggParser.staxparser;
import java.util.HashMap;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;

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


public class AvroSchemaHandler
{

  HashMap<String,String> dbusToDBFieldNameMap;
  HashMap<String,String> dbusToDBFieldTypeMap;

  public  AvroSchemaHandler(Schema schema) throws Exception
  {
    dbusToDBFieldNameMap = new HashMap<String, String>();
    dbusToDBFieldTypeMap = new HashMap<String, String>();
    for(Field field : schema.getFields())
    {
        String dbusFieldName = field.name();
        dbusToDBFieldNameMap.put(getMetaField(field, "dbFieldName"), dbusFieldName);
        dbusToDBFieldTypeMap.put(getMetaField(field, "dbFieldType"), dbusFieldName);
    }
  }

  public static  String getMetaField(Field field, String metaFieldName)
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


  /*
   * gets the databus field name from the db meta information
   */
  public String getDbusFieldName(String dbFieldName)
  {
    return dbusToDBFieldNameMap.get(dbFieldName);
  }

}

