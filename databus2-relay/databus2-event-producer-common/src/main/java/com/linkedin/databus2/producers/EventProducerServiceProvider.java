package com.linkedin.databus2.producers;
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

import com.linkedin.databus.core.DbusEventBufferAppendable;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.seq.MaxSCNReaderWriter;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import com.linkedin.databus2.schemas.SchemaRegistryService;

/**
 * A factory for {@link EventProducer} instances for a  given URI scheme. This class is used by
 * {@link RelayEventProducersRegistry} for EventProducer service loading ]
 * {@link java.util.ServiceLoader}). */
public interface EventProducerServiceProvider
{
  /** The URI scheme for {@link EventProducer}s supported by this provider. */
  String getUriScheme();

  /**
   * Creates a new relay event producer instance for a given data source.
   * @param config              the configuration for the event producer
   * @param eventBuffer         where the events are to be produced
   * @param statsCollector      a collector for statistics for the event producer
   * @param checkpointWriter    the writer to be used for saving the state of the producer
   */
  //TODO the statsCollector should be created and owned by the producer and injected in
  EventProducer createProducer(PhysicalSourceStaticConfig config,
                               SchemaRegistryService schemaRegistryService,
                               DbusEventBufferAppendable eventBuffer,
                               DbusEventsStatisticsCollector statsCollector,
                               MaxSCNReaderWriter checkpointWriter) throws InvalidConfigException;

  /**
   * Creates an instance of a {@link ConfigBuilder} for the producers supported by this service provider.
   * @param propPrefix
   * @return
   */
  ConfigBuilder<? extends PhysicalSourceStaticConfig> createConfigBuilder(String propPrefix);
}
