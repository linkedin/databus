package com.linkedin.databus2.schemas.mbean;
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
