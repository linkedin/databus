package com.linkedin.databus.bootstrap.utils;
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

import org.apache.log4j.Logger;

import com.linkedin.databus.core.UnsupportedKeyException;
import com.linkedin.databus2.producers.EventCreationException;
import com.linkedin.databus2.producers.PartitionFunction;
import com.linkedin.databus2.producers.db.OracleAvroGenericEventFactory;
import com.linkedin.databus2.relay.OracleEventProducerFactory;
import com.linkedin.databus2.relay.config.LogicalSourceStaticConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;

public class BootstrapSeederOracleEventProducerFactory extends
		OracleEventProducerFactory 
{
	private final Logger _log = Logger.getLogger(getClass());
	  
	// Key used by Chunk Query for seeding need not always be same as the Primary Key of the Source
	// In this case, we want to use seederCHunkKey to track progress (in bootstrap_seeder_state table) 
	// but still want the original primary key to be stored in "srckey" column of tab tabke.
	private Map<String,String> _seederChunkKeyColumnNamesMap = null;

	public BootstrapSeederOracleEventProducerFactory(Map<String,String> seederChunkKeyColumnNamesMap)
	{
		super();
		_seederChunkKeyColumnNamesMap = seederChunkKeyColumnNamesMap;
	}

	@Override
	protected OracleAvroGenericEventFactory createEventFactory(String eventViewSchema, String eventView,
			LogicalSourceStaticConfig sourceConfig, PhysicalSourceStaticConfig pConfig,
			String eventSchema, PartitionFunction partitionFunction)
					throws EventCreationException, UnsupportedKeyException
	{
		_log.info("Creating OracleAvroGenericEventFactory with seeder Chunk Key :" + _seederChunkKeyColumnNamesMap.get(eventView));
		return new BootstrapSeederOracleAvroGenericEventFactory(sourceConfig.getId(), (short)pConfig.getId(),
				eventSchema, partitionFunction, _seederChunkKeyColumnNamesMap.get(eventView));
	}
}
