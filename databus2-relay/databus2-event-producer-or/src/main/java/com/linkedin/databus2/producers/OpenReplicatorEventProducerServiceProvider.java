package com.linkedin.databus2.producers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.linkedin.databus2.producers.PartitionFunction;
import com.linkedin.databus.core.DbusEventBufferAppendable;
import com.linkedin.databus.core.UnsupportedKeyException;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.seq.MaxSCNReaderWriter;
import com.linkedin.databus2.relay.config.LogicalSourceStaticConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import com.linkedin.databus2.schemas.NoSuchSchemaException;
import com.linkedin.databus2.schemas.SchemaRegistryService;

/** Manages {@link OpenReplicatorEventProducer} instances. */
public class OpenReplicatorEventProducerServiceProvider implements EventProducerServiceProvider
{
  public static final String SCHEME = "or";
  public static final String MODULE = OpenReplicatorEventProducerServiceProvider.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  // This is copied from ServerContainer to avoid the dependency
  public static final String JMX_DOMAIN = "com.linkedin.databus2";

  @Override
  public String getUriScheme()
  {
    return SCHEME;
  }

  @Override
  public EventProducer createProducer(PhysicalSourceStaticConfig physicalSourceConfig,
                                      SchemaRegistryService schemaRegistryService,
                                      DbusEventBufferAppendable eventBuffer,
                                      DbusEventsStatisticsCollector statsCollector,
                                      MaxSCNReaderWriter checkpointWriter) throws InvalidConfigException
  {
    // Parse each one of the logical sources
    List<OpenReplicatorAvroEventFactory> eventFactories = new ArrayList<OpenReplicatorAvroEventFactory>();
    for(LogicalSourceStaticConfig sourceConfig : physicalSourceConfig.getSources())
    {
      OpenReplicatorAvroEventFactory factory = null;
      try
      {
        factory = buildEventFactory(sourceConfig, physicalSourceConfig, schemaRegistryService);
      } catch (Exception ex) {
        LOG.error("Got exception while building monitored sources for config :" + sourceConfig, ex);
        throw new InvalidConfigException(ex);
      }

      eventFactories.add(factory);
    }

    EventProducer producer = null;
    try
    {
      producer =  new OpenReplicatorEventProducer(eventFactories,
                                           eventBuffer,
                                           checkpointWriter,
                                           physicalSourceConfig,
                                           statsCollector,
                                           null,
                                           null,
                                           schemaRegistryService,
                                           JMX_DOMAIN);
    } catch (DatabusException e) {
      LOG.error("Got databus exception when instantiating Open Replicator event producer for source : " + physicalSourceConfig.getName(), e);
      throw new InvalidConfigException(e);
    }
    return producer;
  }

  @Override
  public ConfigBuilder<? extends PhysicalSourceStaticConfig> createConfigBuilder(String propPrefix)
  {
    return new PhysicalSourceConfig();
  }

  protected OpenReplicatorAvroEventFactory createEventFactory( String eventViewSchema, String eventView,
                                                              LogicalSourceStaticConfig sourceConfig, PhysicalSourceStaticConfig pConfig,
                                                              String eventSchema, PartitionFunction partitionFunction)
      throws DatabusException
  {
      return new OpenReplicatorAvroEventFactory(sourceConfig.getId(),
                                                (short)pConfig.getId(),
                                                eventSchema,
                                                partitionFunction,
                                                pConfig.getReplBitSetter());
  }

  public OpenReplicatorAvroEventFactory buildEventFactory(
                                        LogicalSourceStaticConfig sourceConfig,
                                        PhysicalSourceStaticConfig pConfig,
                                        SchemaRegistryService schemaRegistryService)
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

    LOG.info("Loading schema for source id " + sourceConfig.getId() + ": " + schema);


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

    PartitionFunction partitionFunction = buildPartitionFunction(sourceConfig);
    OpenReplicatorAvroEventFactory factory = createEventFactory(eventViewSchema, eventView, sourceConfig, pConfig,
                                              schema, partitionFunction);
    return factory;
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
