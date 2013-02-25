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


import java.util.Map;

import com.linkedin.databus2.core.DatabusException;


public interface SchemaRegistryService
{
	  /**
	   * Register event schema.
	   * Schema can be extracted from event class.
	   * @param schema the versioned event schema
	   */
	  public void registerSchema(VersionedSchema schema) throws DatabusException;

	  /**
	   * Fetch event schema for the given schemaId.
	   * @param schemaId Hex encoded value of MD5 of event schema
	   * @return Event schema
	   */
	  public String fetchSchema(String schemaId) throws NoSuchSchemaException, DatabusException;

	  /**
	   * Fetch latest event schema for a given event type.
	   * @param eventType Type of the event
	   * @return Latest event schema
	   */
	  public String fetchLatestSchemaByType(String eventType) throws NoSuchSchemaException, DatabusException;

	  /**
	   * Fetch latest event schema object for a given event type
	   * @param eventType Type of the event
	   * @return Latest event schema object
	   */
	  public VersionedSchema fetchLatestVersionedSchemaByType(String eventType) throws NoSuchSchemaException, DatabusException;
	  
	  /**
	   * Fetch all schemas for a given event type.
	   * @param eventType Type of the event
	   * @return All Schema keyed by their versions
	   */
	  public Map<Short, String> fetchAllSchemaVersionsByType(String eventType)
	         throws NoSuchSchemaException, DatabusException;

	  /**
	   * fetch schema id given LogicalSource Id
	   * @param logical Source Name
	   * @return schema Id( md5(avro schema)
	   */
	  public SchemaId fetchSchemaIdForSourceNameAndVersion(String lSourceName, int version)
	  throws DatabusException;

	  /**
	   * Drop schemas for a database
	   */
	  public void dropDatabase(String dbName)
	  throws DatabusException;
}
