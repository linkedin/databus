package com.linkedin.databus2.schemas;

/**
 * "Borrowed"  largely from com.linkedin.avro.SchemaSetProvider
 */
public interface VersionedSchemaSetProvider
{
	public VersionedSchemaSet loadSchemas();
}
