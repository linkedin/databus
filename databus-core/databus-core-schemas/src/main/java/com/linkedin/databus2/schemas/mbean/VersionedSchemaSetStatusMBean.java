package com.linkedin.databus2.schemas.mbean;

import com.linkedin.databus2.schemas.VersionedSchemaSet;

/**
 * An MBean to probe the status of a {@link VersionedSchemaSet}.
 * @author cbotev
 */
public interface VersionedSchemaSetStatusMBean
{

  /**
   * Returns a list of the names of loaded schemas and their versions.
   *
   * Each schema is a separate element in the array. The format is <b>Schema base name: ver1, ver2, ...
   * </b>.
   * */
  String[] getLoadedSchemas();

  /**
   * Get the string representation of a schema
   */
  String getSchema(String baseName, short version);


}
