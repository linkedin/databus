package com.linkedin.databus2.schemas;

import org.apache.avro.Schema;

/**
 * "Borrowed"  largely from com.linkedin.avro.SchemaId
 */
public class VersionedSchema
{
  private final Schema _schema;
  private final VersionedSchemaId _id;

  public VersionedSchema(VersionedSchemaId id, Schema s)
  {
    _schema = s;
    _id = id;
  }

  public VersionedSchema(String baseName, short id, Schema s)
  {
    this(new VersionedSchemaId(baseName, id), s);
  }

  public int getVersion()
  {
    return this._id.getVersion();
  }

  public Schema getSchema()
  {
    return _schema;
  }

  @Override
  public String toString()
  {
    return "(" + getSchemaBaseName() + ","  + getVersion() + "," + _schema + ")";
  }

  public String getSchemaBaseName()
  {
    return _id.getBaseSchemaName();
  }

  @Override
  public boolean equals(Object o)
  {
    if (null == o || ! (o instanceof VersionedSchema)) return false;
    VersionedSchema other = (VersionedSchema)o;
    return _id.equals(other._id);
  }

  @Override
  public int hashCode()
  {
    return _id.hashCode();
  }

  public VersionedSchemaId getId()
  {
    return _id;
  }
}
