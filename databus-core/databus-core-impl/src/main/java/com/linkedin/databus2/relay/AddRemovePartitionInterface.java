package com.linkedin.databus2.relay;
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


import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;

/**
 * supports adding/removing partitions dynamically.
 * One needs to provide configuration for these partitions.
 */
public interface AddRemovePartitionInterface
{
  public void addPartition(PhysicalSourceStaticConfig pConfig) throws DatabusException;
  public void removePartition(PhysicalSourceStaticConfig pConfig) throws DatabusException;
  public void dropDatabase(String dbName) throws DatabusException;
}
