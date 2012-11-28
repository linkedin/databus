package com.linkedin.databus.bootstrap.utils;

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
