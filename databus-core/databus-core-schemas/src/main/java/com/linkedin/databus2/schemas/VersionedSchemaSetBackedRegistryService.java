package com.linkedin.databus2.schemas;
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


import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;

import org.apache.log4j.Logger;

import com.linkedin.databus2.core.DatabusException;

/**
 * Implements a simple {@link SchemaRegistryService} where schemas are stored in an in-memory
 * {@link VersionedSchemaSet}.
 */
public class VersionedSchemaSetBackedRegistryService implements SchemaRegistryService
{
  public static final String MODULE = VersionedSchemaSetBackedRegistryService.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  protected volatile VersionedSchemaSet _curSchemaSet;

  public VersionedSchemaSetBackedRegistryService()
  {
    _curSchemaSet = new VersionedSchemaSet();
  }

  @Override
  public Map<Short, String> fetchAllSchemaVersionsByType(String type)
         throws NoSuchSchemaException, DatabusException
  {
	  boolean isDebugEnabled = LOG.isDebugEnabled();

	  Map<Short, String> resultMap = new HashMap<Short, String>();
	  SortedMap<VersionedSchemaId, VersionedSchema>  schemaMap = _curSchemaSet.getAllVersionsByName(type);

	  if (null != schemaMap)
	  {
    	  for (Entry<VersionedSchemaId, VersionedSchema> e : schemaMap.entrySet())
    	  {
    		  resultMap.put(e.getKey().getVersion(), e.getValue().getSchema().toString());

    		  if ( isDebugEnabled)
    		  {
    			  LOG.debug("fetchAllSchemaVersionsByType: Source (" + type +
    			            "). For Version (" +  e.getKey().getVersion() + ") adding schema ("
    			            + e.getValue().getSchema().toString() + ") to result set.");
    		  }
    	  }
	  }
	  else
	  {
	    LOG.warn("unkown source: " + type);
	  }

	  return resultMap;
  }


  @Override
  public String fetchLatestSchemaByType(String type)
         throws NoSuchSchemaException, DatabusException
  {
    VersionedSchema vSchema = _curSchemaSet.getLatestVersionByName(type);
    String result = null != vSchema ? vSchema.getSchema().toString() : null;
    if (LOG.isDebugEnabled())
    {
      if (null == result) LOG.debug("No schema found for source " + type);
      else LOG.debug("Schema for source " + type + ": " + result);

      //LOG.debug("Schema set: " + curSchemaSet.toString());
    }
    return result;
  }

  @Override
  public VersionedSchema fetchLatestVersionedSchemaByType(String type) throws NoSuchSchemaException, DatabusException
  {
	  VersionedSchema vSchema = _curSchemaSet.getLatestVersionByName(type);
	  return vSchema;
  }
  
  @Override
  public String fetchSchema(String schemaId) throws NoSuchSchemaException, DatabusException
  {
    byte[] idBytes = getBytesFromHexSchemaId(schemaId);
    VersionedSchema vSchema = _curSchemaSet.getById(new SchemaId(idBytes));
    return vSchema.getSchema().toString();
  }

  /**
   * Registers a schema represented by its JSON schema definition. Note that the schema will not be
   * persisted on disk. If there is a schema registered with the same name, it's version will be
   * increased by one.
   */
  @Override
  public void registerSchema(VersionedSchema schema) throws DatabusException
  {
    if (LOG.isDebugEnabled())
    {
      LOG.debug("Registering schema for source " + schema.getSchemaBaseName() + " v." +
                schema.getVersion() + ": " + schema.getSchema().toString());
    }

    _curSchemaSet.add(schema);
  }

  @Override
  public void dropDatabase(String dbName) throws DatabusException 
  {
	  throw new DatabusException("Unsupported method dropDatabase");
  }
  /**
   * Inverse of {@link com.linkedin.avro.utils.Utils#hex(byte[])
   * @param  hexSchemaId     the string hex representation of the schema id
   * @return the byte[] representation of the schema id
   */
  private static byte[] getBytesFromHexSchemaId(String hexSchemaId)
  {
    int bytesNum = hexSchemaId.length() / 2;
    byte[] result = new byte[bytesNum];
    for (int i = 0; i < bytesNum; ++i)
    {
      char c1 = hexSchemaId.charAt(2 * i);
      char c2 = hexSchemaId.charAt(2 * i + 1);
      int byteValue = Character.digit(c1, 16) << 4 + Character.digit(c2, 16);
      result[i] = (byte)byteValue;
    }

    return result;
  }

  public VersionedSchemaSet getCurSchemaSet()
  {
    return _curSchemaSet;
  }

  public void setCurSchemaSet(VersionedSchemaSet curSchemaSet)
  {
    _curSchemaSet = curSchemaSet;
  }

  @Override
  public SchemaId fetchSchemaIdForSourceNameAndVersion(String lSourceName, int version) throws DatabusException
  {
    throw new DatabusException("method should never be called for this type");
  }

}
