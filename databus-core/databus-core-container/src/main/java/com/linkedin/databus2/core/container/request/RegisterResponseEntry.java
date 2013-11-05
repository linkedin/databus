package com.linkedin.databus2.core.container.request;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;


// WARNING:
// Databus clients using clientRelayProtocol versions 3 or older do not handle
// any extra fields in this object.
// Clients using protocol versions 4 or above will ignore new fields added.
public class RegisterResponseEntry
{
  public final static String SOURCE_SCHEMAS_KEY = "sourceSchemas";
  public final static String KEY_SCHEMAS_KEY = "keySchemas";
  public static final String MODULE = RegisterResponseEntry.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private long _id;
  private String _schema;
  private short _version;

  public RegisterResponseEntry(long id, short version, String schema)
  {
    super();
    _id = id;
    _version = version;
    _schema = schema;
  }

  public RegisterResponseEntry()
  {
    this(0, (short)0,"N/A");
  }

  public long getId()
  {
    return _id;
  }

  public void setId(long id)
  {
    _id = id;
  }

  public short getVersion()
  {
    return _version;
  }

  public void setVersion(short version)
  {
    _version = version;
  }

  public String getSchema()
  {
    return _schema;
  }

  public void setSchema(String schema)
  {
    _schema = schema;
  }

  public static Map<Long, List<RegisterResponseEntry>> createFromResponse(Map<String, List<Object>> responseMap,
                                                                          String hashKey,
                                                                          boolean optional)
  {
    List<Object> schemasList = responseMap.get(hashKey);
    if (optional)
    {
      if (schemasList == null)
      {
        LOG.info("/register response v4 has no '" + hashKey + "' entry");
      }
      return null;
    }
    else
    {
      if (schemasList == null || schemasList.size() <= 0)
      {
        throw new RuntimeException("/register response v4 has no '" + hashKey + "' entry (or entry is empty)");
      }
    }
    List<RegisterResponseEntry> sourcesSchemasList = reconstructSchemasList(schemasList);
    Map<Long, List<RegisterResponseEntry>> schemasMap = convertSchemaListToMap(sourcesSchemasList);
    return schemasMap;
  }

  // WARNING
  // Databus clients using clientRelayProtocol versions 3 or older handle the response to 'register' command by
  // directly serializing the response byte stream to a list of RegisterResponseEntry objects.
  //   List<RegisterResponseEntry> schemas =
  //           mapper.readValue(bodyStream,
  //           new TypeReference<List<RegisterResponseEntry>>(){});
  // The code below (versions 4+) looks only for fields known to the implementation.
  private static List<RegisterResponseEntry> reconstructSchemasList(List<Object> objectsList)
  {
    List<RegisterResponseEntry> schemasList = new ArrayList<RegisterResponseEntry>();
    for (Object obj : objectsList)
    {
      // ObjectMapper encodes plain Object as LinkedHashMap (currently); must construct RRE manually
      boolean ok = false;
      if (obj instanceof Map)
      {
        @SuppressWarnings("unchecked") // just map
        Map<String, Object> map = (Map<String, Object>)obj;
        if (map.containsKey("id") && map.containsKey("version") && map.containsKey("schema"))
        {
          schemasList.add(new RegisterResponseEntry((Integer)map.get("id"),
                                                    ((Integer)map.get("version")).shortValue(),
                                                    (String)map.get("schema")));
          ok = true;
        }
      }
      if (!ok)
      {
        throw new RuntimeException("/register response v4 schemas deserialization error: object type = " +
                                       obj.getClass().getName());
      }
    }
    return schemasList;
  }

  /**
   * @return map from schema ID to a list of its schemas (typically different versions)
   */
  public static Map<Long, List<RegisterResponseEntry>> convertSchemaListToMap(List<RegisterResponseEntry> schemasList)
  {
    HashMap<Long, List<RegisterResponseEntry>> schemasMap =
        new HashMap<Long, List<RegisterResponseEntry>>(schemasList.size() * 2);
    for (RegisterResponseEntry entry : schemasList)
    {
      List<RegisterResponseEntry> val = schemasMap.get(entry.getId());
      if (null == val)
      {
        val = new ArrayList<RegisterResponseEntry>();
        val.add(entry);
        schemasMap.put(entry.getId(), val);
      }
      else
      {
        val.add(entry);
      }
    }
    return schemasMap;
  }
}
