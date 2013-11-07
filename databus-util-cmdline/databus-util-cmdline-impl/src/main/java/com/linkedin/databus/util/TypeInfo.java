package com.linkedin.databus.util;
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
