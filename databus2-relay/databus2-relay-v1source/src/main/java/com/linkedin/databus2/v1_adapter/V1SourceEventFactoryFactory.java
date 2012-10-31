package com.linkedin.databus2.v1_adapter;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import com.linkedin.databus.core.DatabusException;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.Source;
import com.linkedin.databus.core.UnsupportedKeyException;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.monitors.db.EventFactory;
import com.linkedin.databus.monitors.db.EventFactoryFactory;
import com.linkedin.databus2.producers.EventCreationException;
import com.linkedin.databus2.producers.PartitionFunction;
import com.linkedin.databus2.producers.db.OracleAvroGenericEventFactory;
import com.linkedin.databus2.relay.OracleEventProducerFactory;
import com.linkedin.databus2.relay.config.LogicalSourceStaticConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import com.linkedin.databus2.schemas.NoSuchSchemaException;
import com.linkedin.databus2.schemas.SchemaRegistryService;

public class V1SourceEventFactoryFactory implements EventFactoryFactory<Object, Object>
{
  private HashMap<Source, V1SourceEventFactory> _eventFactories;

  public V1SourceEventFactoryFactory(PhysicalSourceStaticConfig physConf,
                                     SchemaRegistryService schemaRegistryService,
                                     DbusEventBuffer eventBuffer,
                                     DbusEventsStatisticsCollector eventStats)
         throws NoSuchSchemaException, com.linkedin.databus2.core.DatabusException,
                EventCreationException, UnsupportedKeyException
  {
    OracleEventProducerFactory helperFactory = new OracleEventProducerFactory();
    _eventFactories = new HashMap<Source, V1SourceEventFactory>(physConf.getSources().length);

    for (LogicalSourceStaticConfig logicalConf: physConf.getSources())
    {
      String schema = schemaRegistryService.fetchLatestSchemaByType(logicalConf.getName());
      PartitionFunction ppartFunc = helperFactory.buildPartitionFunction(logicalConf);
      OracleAvroGenericEventFactory avroFactory =
          new OracleAvroGenericEventFactory(logicalConf.getId(), (short)physConf.getId(),
                                            schema, ppartFunc);
      V1SourceEventFactory v1eventFactory = new V1SourceEventFactory(avroFactory, eventBuffer,
                                                                     eventStats);
      String sourceUri = logicalConf.getUri();
      int nameStart = sourceUri.indexOf('.');
      nameStart++;
      Source v1Source = Source.create(sourceUri.substring(nameStart));

      _eventFactories.put(v1Source, v1eventFactory);
    }
  }

  @Override
  public EventFactory<Object, Object> createEventFactory(Source source, ResultSet allRows)
         throws SQLException, DatabusException
  {
    return _eventFactories.get(source);
  }

}
