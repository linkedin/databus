package com.linkedin.databus2.relay.config;


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

public interface DatabusRelaySources
{

  /**
   * add relay configuration to the source collection;
   * @param sourceName : name of physical DB
   * @param config : corresponding relay configuration of specified physical DB
   * @return true if configuration was added successfully, false if ignored
   */
  public boolean add(String sourceName, PhysicalSourceConfig config);


  /**
   * retrieve relay configuration of specified DB; usually after a successful load()
   * @param sourceName : name of physical DB
   * @return corresponding configuration of relay or null if none exists;
   */
  public PhysicalSourceConfig get(String sourceName);

  /**
   *
   * @return all relay configurations or null if none exists
   */
  public PhysicalSourceConfig[] getAll();


  /**
   * Remove entry of source from the collection of configurations
   * @param sourceName : name of physical DB
   * @return true if sourceName existed and was successfully removed
   */
  public boolean remove(String sourceName);


  /**
   * Remove all configurations ; subsequent save should wipe out persisted copy if one exists
   * @return true if remove was successful
   */
  public boolean removeAll();

  /**
   * save existing mappings
   * @return true if persistence succeeded
   */
  public boolean save();

  /**
   * load existing mappings from persistence layer
   * @return true if load succeeded
   */
  public boolean load();

  /**
   *
   * @return number of current mappings (source to configurations)
   */
  int size();

}
