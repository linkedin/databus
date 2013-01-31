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


import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;

import com.linkedin.databus.core.DbusEventBufferAppendable;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.UnsupportedKeyException;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus2.producers.EventCreationException;
import com.linkedin.databus2.producers.PartitionFunction;
import com.linkedin.databus2.producers.db.OracleAvroGenericEventFactory;
import com.linkedin.databus2.schemas.utils.SchemaHelper;

public class BootstrapSeederOracleAvroGenericEventFactory extends
		OracleAvroGenericEventFactory 
{
	private final Logger _log = Logger.getLogger(getClass());

	
	// Key used by Chunk Query for seeding need not always be same as the Primary Key of the Source
	// In this case, we want to use seederCHunkKey to track progress (in bootstrap_seeder_state table) 
	// but still want the original primary key to be stored in "srckey" column of tab table.
	private String _seederChunkKeyName = null;

	public BootstrapSeederOracleAvroGenericEventFactory(short sourceId,
			short pSourceId, String eventSchema,
			PartitionFunction partitionFunction,
			String seederChunkKeyColumnName) throws EventCreationException,
			UnsupportedKeyException {
		super(sourceId, pSourceId, eventSchema, partitionFunction);
	    List<Field> fields = _eventSchema.getFields();
	    String avroFieldName = null;
	    for (Field f : fields)
	    {
	        String databaseFieldName = SchemaHelper.getMetaField(f, "dbFieldName");
	        if ( ( null != databaseFieldName) && (databaseFieldName.equalsIgnoreCase(seederChunkKeyColumnName)))
	        {
	        	_seederChunkKeyName = f.name();
	        	break;
	        }	    
	    }
	    _log.info("SeederChunkKey Field is :" + _seederChunkKeyName);
	}

	/*
	 * @see com.linkedin.databus2.monitors.db.EventFactory#createEvent(long, long, java.sql.ResultSet)
	 */
	@Override
	public long createAndAppendEvent(long scn,
			long timestamp,
	        GenericRecord record,
			ResultSet row,
			DbusEventBufferAppendable eventBuffer,
			boolean enableTracing,
			DbusEventsStatisticsCollector dbusEventsStatisticsCollector)
					throws EventCreationException, UnsupportedKeyException
	{
		if (! (eventBuffer instanceof BootstrapEventBuffer))
			throw new RuntimeException("Expected BootstrapEventBuffer instance to be passed but received:" + eventBuffer);

		BootstrapEventBuffer bEvb = (BootstrapEventBuffer)eventBuffer;

		byte[] serializedValue = serializeEvent(record, scn, timestamp, row, eventBuffer, enableTracing, dbusEventsStatisticsCollector);

		// Append the event to the databus event buffer
		//DbusEventKey eventKey = new DbusEventKey(record.get("key"));
		DbusEventKey eventKey = new DbusEventKey(record.get(keyColumnName));
		
		DbusEventKey seederChunkKey = new DbusEventKey(record.get(_seederChunkKeyName));

		short lPartitionId = _partitionFunction.getPartition(eventKey);
		//short pPartitionId = PhysicalSourceConfig.DEFAULT_PHYSICAL_PARTITION.shortValue();
		bEvb.appendEvent(eventKey, seederChunkKey,_pSourceId, lPartitionId, timestamp * 1000000, _sourceId,
				_schemaId, serializedValue, enableTracing, dbusEventsStatisticsCollector);
		return serializedValue.length;
	}
}
