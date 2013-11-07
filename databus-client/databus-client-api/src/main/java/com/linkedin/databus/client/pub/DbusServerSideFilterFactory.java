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

import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.filter.DbusKeyCompositeFilterConfig;


/**
 * Factory interface for generating server-side filter corresponding to the partition 
 * that is getting added.
 * 
 * @author bvaradar
 *
 */
public interface DbusServerSideFilterFactory 
{
	public DbusKeyCompositeFilterConfig createServerSideFilter(DbusClusterInfo cluster, 
			                                                   DbusPartitionInfo partition) throws InvalidConfigException;
}
