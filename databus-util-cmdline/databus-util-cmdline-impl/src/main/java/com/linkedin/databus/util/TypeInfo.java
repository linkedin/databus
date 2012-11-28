package com.linkedin.databus.util;


/**
 * Type information for an object in the database. The object may be a column with a simple built in type like
 * VARCHAR2 or NUMBER, or it may be a database TABLE, VIEW, COLLECTION, or a user defined type which in turn is made
 * up of a number of fields each with their own TypeInfo.
 */
public interface TypeInfo
{
  /**
   * @return owner of the type; typically the schema where the type was created
   */
  public String getOwnerName();

  /**
   * @return name of the type; may be the name of a column, table, user defined type, etc.
   */
  public String getName();
}
