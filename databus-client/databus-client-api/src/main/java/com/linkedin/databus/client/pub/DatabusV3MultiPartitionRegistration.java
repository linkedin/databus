package com.linkedin.databus.client.pub;
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


import java.util.Map;

import com.linkedin.databus.core.data_model.PhysicalPartition;

/** A consumer registration for subscriptions across multiple physical partitions */
public interface DatabusV3MultiPartitionRegistration extends DatabusV3Registration
{
  /**
   * Children registrations per partition
   * @return a read-only copy of the {@link PhysicalPartition} to {@link DatabusV3Registration} mapping */
  public Map<PhysicalPartition, DatabusV3Registration> getPartionRegs();
}
