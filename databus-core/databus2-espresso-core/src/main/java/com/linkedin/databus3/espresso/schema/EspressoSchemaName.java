package com.linkedin.databus3.espresso.schema;

import com.linkedin.databus2.core.DatabusException;

public class EspressoSchemaName
{
  public static final char DB_DOC_SEPARATOR = '.';

  private final String _dbName;
  private final String _tableName;
  private final String _dbusSourceName;

  private EspressoSchemaName(String dbName, String tableName, String dbusSourceName)
  {
    super();
    _dbName = dbName;
    _tableName = tableName;
    _dbusSourceName = dbusSourceName;
  }

  public static EspressoSchemaName create(String dbName, String tableName)
  {
    return new EspressoSchemaName(dbName, tableName, dbName + DB_DOC_SEPARATOR + tableName);
  }

  public static EspressoSchemaName create(String dbusSourceName) throws DatabusException
  {
    int separatorIdx = dbusSourceName.indexOf(DB_DOC_SEPARATOR);
    if (0 >= separatorIdx || separatorIdx == dbusSourceName.length() - 1)
    {
      throw new DatabusException("invalid source name: " + dbusSourceName);
    }
    return new EspressoSchemaName(dbusSourceName.substring(0, separatorIdx),
                                  dbusSourceName.substring(separatorIdx + 1),
                                  dbusSourceName);
  }

  public String getDatabusSourceName()
  {
    return _dbusSourceName;
  }

  public String getDbName()
  {
    return _dbName;
  }

  public String getTableName()
  {
    return _tableName;
  }

}