/*
 * $Id: RelayFactory.java 272015 2011-05-21 03:03:57Z cbotev $
 */
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


import java.lang.management.ManagementFactory;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.management.MBeanServer;
import javax.sql.DataSource;

import org.apache.log4j.Logger;

import com.linkedin.databus.core.DbusEventBufferAppendable;
import com.linkedin.databus.core.UnsupportedKeyException;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus.monitoring.mbean.EventSourceStatistics;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.seq.MaxSCNReaderWriter;
import com.linkedin.databus2.producers.ConstantPartitionFunction;
import com.linkedin.databus2.producers.EventCreationException;
import com.linkedin.databus2.producers.EventProducer;
import com.linkedin.databus2.producers.PartitionFunction;
import com.linkedin.databus2.producers.db.EventFactory;
import com.linkedin.databus2.producers.db.OracleTriggerMonitoredSourceInfo;
import com.linkedin.databus2.producers.db.OracleAvroGenericEventFactory;
import com.linkedin.databus2.producers.db.OracleEventProducer;
import com.linkedin.databus2.relay.config.LogicalSourceStaticConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import com.linkedin.databus2.schemas.NoSuchSchemaException;
import com.linkedin.databus2.schemas.SchemaRegistryService;

/**
 * @author Jemiah Westerman<jwesterman@linkedin.com>
 * @version $Revision: 272015 $
 */
public class OracleEventProducerFactory
{
  private final Logger _log = Logger.getLogger(getClass());

  public EventProducer buildEventProducer(PhysicalSourceStaticConfig physicalSourceConfig,
                                 SchemaRegistryService schemaRegistryService,
                                 DbusEventBufferAppendable dbusEventBuffer,
                                 MBeanServer mbeanServer,
                                 DbusEventsStatisticsCollector dbusEventsStatisticsCollector,
                                 MaxSCNReaderWriter _maxScnReaderWriter
                                 )
  throws DatabusException, EventCreationException, UnsupportedKeyException, SQLException, InvalidConfigException
  {
    // Make sure the URI from the configuration file identifies an Oracle JDBC source.
    String uri = physicalSourceConfig.getUri();
    if(!uri.startsWith("jdbc:oracle"))
    {
      throw new InvalidConfigException("Invalid source URI (" + physicalSourceConfig.getUri() + "). Only jdbc:oracle: URIs are supported.");
    }

    // Parse each one of the logical sources
    List<OracleTriggerMonitoredSourceInfo> sources = new ArrayList<OracleTriggerMonitoredSourceInfo>();
    for(LogicalSourceStaticConfig sourceConfig : physicalSourceConfig.getSources())
    {
      OracleTriggerMonitoredSourceInfo source = buildOracleMonitoredSourceInfo(sourceConfig, physicalSourceConfig, schemaRegistryService);
      sources.add(source);
    }

    DataSource ds = null;
    try
    {
        ds = OracleJarUtils.createOracleDataSource(uri);
    } catch (Exception e)
    {
    	String errMsg = "Oracle URI likely not supported. Trouble creating OracleDataSource";
    	_log.error(errMsg);
    	throw new InvalidConfigException(errMsg + e.getMessage());
    }

    // Create the event producer
    EventProducer eventProducer = new OracleEventProducer(sources,
                                                          ds,
                                                          dbusEventBuffer,
                                                          true,
                                                          dbusEventsStatisticsCollector,
                                                          _maxScnReaderWriter,
                                                          physicalSourceConfig,
                                                          ManagementFactory.getPlatformMBeanServer());

    _log.info("Created OracleEventProducer for config:  " + physicalSourceConfig +
              " with slowSourceQueryThreshold = " + physicalSourceConfig.getSlowSourceQueryThreshold());
    return eventProducer;
  }

  protected OracleAvroGenericEventFactory createEventFactory( String eventViewSchema, String eventView,
		                                                      LogicalSourceStaticConfig sourceConfig, PhysicalSourceStaticConfig pConfig,
          													  String eventSchema, PartitionFunction partitionFunction)
      throws EventCreationException, UnsupportedKeyException
  {
	  return new OracleAvroGenericEventFactory(sourceConfig.getId(), (short)pConfig.getId(),
              eventSchema, partitionFunction,pConfig.getReplBitSetter());
  }

  public OracleTriggerMonitoredSourceInfo buildOracleMonitoredSourceInfo(
      LogicalSourceStaticConfig sourceConfig, PhysicalSourceStaticConfig pConfig, SchemaRegistryService schemaRegistryService)
      throws DatabusException, EventCreationException, UnsupportedKeyException,
             InvalidConfigException
  {
    String schema = null;
	try {
		schema = schemaRegistryService.fetchLatestSchemaBySourceName(sourceConfig.getName());
	} catch (NoSuchSchemaException e) {
	      throw new InvalidConfigException("Unable to load the schema for source (" + sourceConfig.getName() + ").");
	}

    if(schema == null)
    {
      throw new InvalidConfigException("Unable to load the schema for source (" + sourceConfig.getName() + ").");
    }

    _log.info("Loading schema for source id " + sourceConfig.getId() + ": " + schema);


    String eventViewSchema;
    String eventView;

    if(sourceConfig.getUri().indexOf('.') != -1)
    {
      String[] parts = sourceConfig.getUri().split("\\.");
      eventViewSchema = parts[0];
      eventView = parts[1];
    }
    else
    {
      eventViewSchema = null;
      eventView = sourceConfig.getUri();
    }
    if(eventView.toLowerCase().startsWith("sy$"))
    {
      eventView = eventView.substring(3);
    }

    PartitionFunction partitionFunction = buildPartitionFunction(sourceConfig);
    EventFactory factory = createEventFactory(eventViewSchema, eventView, sourceConfig, pConfig,
                                              schema, partitionFunction);

    EventSourceStatistics statisticsBean = new EventSourceStatistics(sourceConfig.getName());


    OracleTriggerMonitoredSourceInfo sourceInfo = new OracleTriggerMonitoredSourceInfo(sourceConfig.getId(),
                                                             sourceConfig.getName(),
                                                             eventViewSchema,
                                                             eventView, factory,
                                                             statisticsBean,
                                                             sourceConfig.getRegularQueryHints(),
                                                             sourceConfig.getChunkedTxnQueryHints(),
                                                             sourceConfig.getChunkedScnQueryHints(),
                                                             sourceConfig.isSkipInfinityScn());
    return sourceInfo;
  }

  public PartitionFunction buildPartitionFunction(LogicalSourceStaticConfig sourceConfig)
  throws InvalidConfigException
  {
    String partitionFunction = sourceConfig.getPartitionFunction();
    if(partitionFunction.startsWith("constant:"))
    {
      try
      {
        String numberPart = partitionFunction.substring("constant:".length()).trim();
        short constantPartitionNumber = Short.valueOf(numberPart);
        return new ConstantPartitionFunction(constantPartitionNumber);
      }
      catch(Exception ex)
      {
        // Could be a NumberFormatException, IndexOutOfBoundsException or other exception when trying to parse the partition number.
        throw new InvalidConfigException("Invalid partition configuration (" + partitionFunction + "). " +
        		"Could not parse the constant partition number.");
      }
    }
    else
    {
      throw new InvalidConfigException("Invalid partition configuration (" + partitionFunction + ").");
    }
  }


}
