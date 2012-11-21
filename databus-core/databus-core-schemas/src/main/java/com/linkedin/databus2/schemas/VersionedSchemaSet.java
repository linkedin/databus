package com.linkedin.databus2.schemas;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.avro.Schema;

/**
 * "Borrowed"  largely from com.linkedin.avro.SchemaSet
 */
public class VersionedSchemaSet
{
  private final ReadWriteLock _lock;
  private final Map<SchemaId, VersionedSchema> _idToSchema;
  private final Map<String, SortedMap<VersionedSchemaId, VersionedSchema>> _nameToSchemas;

  public VersionedSchemaSet()
  {
    this._lock = new ReentrantReadWriteLock(true);
    this._idToSchema = new HashMap<SchemaId, VersionedSchema>();
    this._nameToSchemas = new HashMap<String, SortedMap<VersionedSchemaId, VersionedSchema>>();
  }

  public int size()
  {
    return _idToSchema.size();
  }

  public boolean has(SchemaId id)
  {
    Lock readLock = _lock.readLock();
    readLock.lock();
    try
    {
      return _idToSchema.containsKey(id);
    }
    finally
    {
      readLock.unlock();
    }
  }

  public VersionedSchema getById(SchemaId id)
  {
    Lock readLock = _lock.readLock();
    readLock.lock();
    try
    {
      return _idToSchema.get(id);
    }
    finally
    {
      readLock.unlock();
    }
  }

  public VersionedSchema getLatestVersionByName(String schemaBaseName)
  {
	Lock readLock = _lock.readLock();
    readLock.lock();
	try
	{
	    SortedMap<VersionedSchemaId, VersionedSchema> versions = _nameToSchemas.get(schemaBaseName);
	    if(versions == null || versions.size() == 0) return null;
	    else return versions.get(versions.lastKey());
	}
	finally
	{
	  readLock.unlock();
	}
  }

  public SortedMap<VersionedSchemaId, VersionedSchema> getAllVersionsByName(String schemaBaseName)
  {
    Lock readLock = _lock.readLock();
    readLock.lock();
    try
    {
      return _nameToSchemas.get(schemaBaseName);
    }
    finally
    {
      readLock.unlock();
    }
  }

  public VersionedSchema getSchemaByNameVersion(String baseName, short version)
  {
    VersionedSchemaId lookupKey = new VersionedSchemaId(baseName, version);
    return getSchema(lookupKey);
  }

  public VersionedSchema getSchema(VersionedSchemaId versionedSchemaId)
  {
    Lock readLock = _lock.readLock();
    readLock.lock();
    try
    {
      SortedMap<VersionedSchemaId, VersionedSchema> versions =
          getAllVersionsByName(versionedSchemaId.getBaseSchemaName());
      VersionedSchema vs = null != versions ? versions.get(versionedSchemaId) : null;
      return vs;
    }
    finally
    {
      readLock.unlock();
    }
  }

  /**
   * Adds a schema with a given name and version if it does not already exist
   * @return true if the schema was added, false if a schema already exists
   */
  public boolean add(String name, short version, String schemaStr)
  {
    Lock writeLock = _lock.writeLock();
    writeLock.lock();
    try
    {
      //first check if the schema is already there
      if (null != getSchemaByNameVersion(name, version))
        return false; //schema is already there
      Schema avroSchema = Schema.parse(schemaStr);
      VersionedSchema schema = new VersionedSchema(name, version, avroSchema);
      addSchemaInternal(schema);
      return true;
    }
    finally
    {
      writeLock.unlock();
    }
  }

  /**
   * Adds a versioned schema if it does not already exist
   * @return true if the schema was added, false if a schema already exists
   */
  public boolean add(VersionedSchema schema)
  {
    Lock writeLock = _lock.writeLock();
    writeLock.lock();
    try
    {
      //first check if the schema is already there
      if (null != getSchema(schema.getId()))
        return false; //schema is already there
      addSchemaInternal(schema);
      return true;
    }
    finally
    {
      writeLock.unlock();
    }
  }

  /** Assumes there is already a write lock*/
  private void addSchemaInternal(VersionedSchema schema)
  {
    SchemaId id = SchemaId.forSchema(schema.getSchema());
    _idToSchema.put(id, schema);
    SortedMap<VersionedSchemaId, VersionedSchema> versions = _nameToSchemas.get(schema.getSchemaBaseName());
    if(versions == null)
    {
      versions = new TreeMap<VersionedSchemaId, VersionedSchema>(new Comparator<VersionedSchemaId>()
          {
            @Override
            public int compare(VersionedSchemaId s1, VersionedSchemaId s2)
            {
              return s1.getVersion() - s2.getVersion();
            }
          });
      _nameToSchemas.put(schema.getSchemaBaseName(), versions);
    }
    versions.put(schema.getId(), schema);
  }

  @Override
  public String toString()
  {
    Lock readLock = _lock.readLock();
    readLock.lock();
    try
    {
      StringBuilder builder = new StringBuilder("SchemaSet(");
      for(Map.Entry<String, SortedMap<VersionedSchemaId, VersionedSchema>> entry: _nameToSchemas.entrySet())
      {
        builder.append(entry.getKey());
        builder.append(" -> ");
        builder.append(entry.getValue());
        builder.append(", ");
      }
      builder.append(")");
      return builder.toString();
    }
    finally
    {
      readLock.unlock();
    }
  }

  public Set<String> getSchemaBaseNames()
  {
    return _nameToSchemas.keySet();
  }

}
