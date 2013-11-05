package com.linkedin.databus2.core.container.request;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.databus.core.util.Base64;


/**
 * Store a single version of Espresso's replication metadata schema for use in
 * version 2 of the relay-client "/register" protocol.
 *
 * See https://iwww.corp.linkedin.com/wiki/cf/display/ENGS/Espresso+Metadata+Schema
 * for specification.
 *
 * This class currently is a near-clone of RegisterResponseEntry, which is used
 * for database-source shemas in the /register call, and it could act as the base
 * class for RegisterResponseEntry (or derive from a common base class).  But
 * conceptually it's a separate thing that could someday have its own, non-source-
 * related attributes or other behaviors, so keep it independent for now.
 *
 * TODO:  rename RegisterResponseEntry to RegisterResponseSourceEntry?
 */
public class RegisterResponseMetadataEntry
{
  public final static String METADATA_SCHEMAS_KEY = "metadataSchemas";
  public static final String MODULE = RegisterResponseMetadataEntry.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private String _schema;
  private short _version;
  private byte[] _crc32;

  public RegisterResponseMetadataEntry(short version, String schema, byte[] crc32)
  {
    super();
    _version = version;
    _schema = schema;
    _crc32 = crc32;
  }

  public RegisterResponseMetadataEntry()
  {
    this((short)0, "n/a", new byte[4]);
  }

  public short getVersion()
  {
    return _version;
  }

  public byte[] getCrc32()
  {
    return _crc32.clone();
  }

  public void setVersion(short version)
  {
    _version = version;
  }

  public String getSchema()
  {
    return _schema;
  }

  public static List<RegisterResponseMetadataEntry> createFromResponse(Map<String, List<Object>> responseMap,
                                                                                  String hashKey,
                                                                                  boolean optional)
  {
    // We initially expect only a single metadataSchema (for replication), but it's
    // versioned, so a List is the minimal requirement.  If other kinds of metadata
    // schemas are ever added (e.g., with ID values to differentiate them), we'll
    // have to change this block to look more like the source- and key-schemas ones
    // above (i.e., build a hashmap of ID-to-list entries).
    List<Object> metadataObjectsList = responseMap.get(RegisterResponseMetadataEntry.METADATA_SCHEMAS_KEY);
    List<RegisterResponseMetadataEntry> metadataSchemasList = null;
    if (metadataObjectsList == null)
    {
      LOG.info("/register response v4 has no '" + RegisterResponseMetadataEntry.METADATA_SCHEMAS_KEY + "' entry");
    }
    else
    {
      metadataSchemasList = reconstructMetadataSchemasList(metadataObjectsList);
    }

    return metadataSchemasList;
  }

  private static List<RegisterResponseMetadataEntry> reconstructMetadataSchemasList(List<Object> objectsList)
  {
    List<RegisterResponseMetadataEntry> schemasList = new ArrayList<RegisterResponseMetadataEntry>();
    for (Object obj : objectsList)
    {
      boolean ok = false;
      if (obj instanceof Map)
      {
        @SuppressWarnings("unchecked") // just map
            Map<String, Object> map = (Map<String, Object>)obj;
        if (map.containsKey("version") && map.containsKey("schema") && map.containsKey("crc32"))
        {
          byte[] crc32Digest = Base64.decode((String) (map.get("crc32")));
          schemasList.add(new RegisterResponseMetadataEntry(((Integer)map.get("version")).shortValue(),
                                                            (String)map.get("schema"),
                                                            crc32Digest));
          ok = true;
        }
      }
      if (!ok)
      {
        throw new RuntimeException("/register response v4 metadata schemas deserialization error: object type = " +
                                       obj.getClass().getName());
      }
    }
    return schemasList;
  }

  public void setSchema(String schema)
  {
    _schema = schema;
  }
}
