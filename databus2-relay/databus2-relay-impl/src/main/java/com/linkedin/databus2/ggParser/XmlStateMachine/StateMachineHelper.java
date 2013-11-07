package com.linkedin.databus2.ggParser.XmlStateMachine;


import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.relay.config.ReplicationBitSetterStaticConfig;
import com.linkedin.databus2.relay.config.ReplicationBitSetterStaticConfig.MissingValueBehavior;
import com.linkedin.databus2.schemas.SchemaRegistryService;
import com.linkedin.databus2.schemas.VersionedSchema;
import java.util.HashMap;
import java.util.regex.Pattern;
import org.apache.avro.Schema;
import org.apache.log4j.Logger;


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
 public class StateMachineHelper
{

  public static final String MODULE = StateMachineHelper.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);
  /**
   * Given a pattern and the database field value, the method returns true if the value matches, false otherwise
   * @param pattern (e.g. O)
   * @param databaseFieldValue
   * @return true if the pattern matches, false otherwise.
   */
  public static boolean verifyReplicationStatus(Pattern pattern, String databaseFieldValue, MissingValueBehavior missingValueBehavior)
  {
    if(databaseFieldValue == null)
    {
      switch(missingValueBehavior)
      {
        case TREAT_EVENT_REPLICATED :
                      return true;
        case STOP_WITH_ERROR :
                      // For behavior type: STOP_ON_ERROR also, we are returning false as the Exception-throwing 
                      // happens after all the tokens are processed.
        case TREAT_EVENT_LOCAL :
                      return false;
      }
    }

    return pattern.matcher(databaseFieldValue).matches();
  }

  /**
   * Given a table name, extracts the avro schema corresponding to the table. If the table is not found (i.e., the relay is not configured
   * to host this table), returns null.
   * @param currentTable The table for which you want the avro schema (e.g. PERSON.ADDRESSBOOKTABLE)
   * @param tableToSourceName The map which has the table to the namespace of the schema (e.g. PERSON.ADDRESSBOOKTABLE => com.linkedin.com.person.addressBook)
   * @param schemaRegistryService The schema registry which holds all the schemas from where the schemas are looked up based on namespace
   * @return
   * @throws DatabusException
   */
  public static Schema tableToSchema(String currentTable, HashMap<String, String> tableToSourceName, SchemaRegistryService schemaRegistryService)
  {
    String sourceName = tableToSourceName.get(currentTable);
    Schema schema = null;
    try
    {
      VersionedSchema vSchema = schemaRegistryService.fetchLatestVersionedSchemaBySourceName(sourceName);
      if(vSchema != null)
        schema = vSchema.getSchema();
    }
    catch (RuntimeException re)
    {
      throw re;
    }
    catch (Exception e)
    {
      LOG.error("Unable to fetch the schema for the table: " + currentTable + " with source name: " + sourceName + " because of the following error : " + e, e);
      return null;
    }

    if(schema == null)
        return null;
    return schema;
  }
}
