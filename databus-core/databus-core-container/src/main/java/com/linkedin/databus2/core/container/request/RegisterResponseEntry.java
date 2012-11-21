package com.linkedin.databus2.core.container.request;

public class RegisterResponseEntry 
{
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

}
