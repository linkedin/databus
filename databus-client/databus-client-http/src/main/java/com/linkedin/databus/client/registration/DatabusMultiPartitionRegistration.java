package com.linkedin.databus.client.registration;
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

import com.linkedin.databus.client.pub.DatabusRegistration;
import com.linkedin.databus.client.pub.DbusPartitionInfo;


/**
 * 
 * Registration which allows registering consumer(s) to subscribe to sources across source partitions (physical partitions).
 * This is not external-facing interface and client-application is not expected to code against it. 
 */
public interface DatabusMultiPartitionRegistration 
    extends DatabusRegistration 
{	
	/**
	 * Children registrations per partition
	 * @return a read-only copy of the {@link DbusPartitionInfo} to {@link DatabusSinglePartitionRegistration} mapping 
	 **/
	public Map<DbusPartitionInfo, DatabusRegistration> getPartitionRegs();	
}
