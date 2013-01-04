package com.linkedin.databus2.schemas;

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
