package com.linkedin.databus2.schemas;


public class VersionedSchemaId
{
  private final String _baseSchemaName;
  private final short _version;

  public VersionedSchemaId(String baseSchemaName, short version)
  {
    super();
    _baseSchemaName = baseSchemaName;
    _version = version;
  }

  public String getBaseSchemaName()
  {
    return _baseSchemaName;
  }

  public short getVersion()
  {
    return _version;
  }

  @Override
  public boolean equals(Object o)
  {
    if (null == o || !(o instanceof VersionedSchemaId)) return false;
    VersionedSchemaId other = (VersionedSchemaId)o;
    return _version == other._version && _baseSchemaName.equals(other._baseSchemaName);
  }

  @Override
  public int hashCode()
  {
    return _baseSchemaName.hashCode() ^ _version;
  }

  @Override
  public String toString()
  {
    StringBuilder res = new StringBuilder();
    res.append(_baseSchemaName);
    res.append(':');
    res.append(_version);

    return res.toString();
  }

}
