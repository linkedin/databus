package com.linkedin.databus.client.pub;

import com.linkedin.databus.core.data_model.PhysicalPartition;

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
 * 
 * Databus Partition Info 
 *
 */
public interface DbusPartitionInfo 
{
	/**
	 * 
	 * @return numeric id of this partition
	 */
	public long getPartitionId();

	/**
	 *
	 * Checks if other partition is equal to this instance
	 * @param other
	 * @return true if equal otherwise false
	 */
	public boolean equalsPartition(DbusPartitionInfo other);

}
