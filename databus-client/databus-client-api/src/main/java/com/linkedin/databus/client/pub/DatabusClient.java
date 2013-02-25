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


import java.util.Collection;

import com.linkedin.databus2.core.filter.DbusKeyCompositeFilterConfig;

public interface DatabusClient
{

  /** 
   * Subscribes a single listener to the on-line update stream for the specified sources. 
   * @Deprecated. Replaced by {@link #register(DatabusCombinedConsumer, String...)} 
   */
  @Deprecated
  public void registerDatabusStreamListener(DatabusStreamConsumer listener,
                                            DbusKeyCompositeFilterConfig filterConfig,
                                            String ... sources)
         throws DatabusClientException;

  /**
   * Subscribes a group listener to the on-line update stream for the specified sources. The onEvent
   * callbacks will be spread and run in parallel across all listeners. 
   * @Deprecated. Replaced by {@link #register(Collection, String...)}  
   */
  @Deprecated
  public void registerDatabusStreamListener(DatabusStreamConsumer[] listeners,
                                            DbusKeyCompositeFilterConfig filterConfig,
                                            String ... sources)
         throws DatabusClientException;

  /** 
   * Subscribes a single listener to the bootstrap stream for the specified sources 
   *  Deprecated. Replaced by {@link #register(Collection, String...)}  
   */
  @Deprecated
  public void registerDatabusBootstrapListener(DatabusBootstrapConsumer[] listeners,
                                               DbusKeyCompositeFilterConfig filterConfig,
                                               String ... sources)
         throws DatabusClientException;

  /**
   * Subscribes a group listener to the bootstrap stream for the specified sources. The onEvent
   * callbacks will be spread and run in parallel across all listeners. 
   * 
   * @Deprecated. Replaced by {@link #register(DatabusCombinedConsumer, String...)} 
   */
  @Deprecated
  public void registerDatabusBootstrapListener(DatabusBootstrapConsumer listener,
                                               DbusKeyCompositeFilterConfig filterConfig,
                                               String ... sources)
         throws DatabusClientException;
  
  
   
  /**
   * Register for a list of sourceURIs for one consumer.
   * 
   * The consumption will not be started at the end of this call. Clients have to
   * either call start() on the DatabusRegistration object or on this client instance.
   * 
   * @param consumer Consumer Callback instance to consume events
   * @param sources Source URIs to subscribe
   * 
   * @return DatabusRegistration handle to operate the consumption
   */
  public DatabusRegistration register(DatabusCombinedConsumer consumer, String ... sourceUri)         
                   throws DatabusClientException;
    
  /**
   * Register for a list of sources for a collection of consumers
   * The consumption will not be started at the end of this call. Clients have to
   * either call start() on the DatabusRegistration object or on this client instance.
   * 
   * @param consumer Collection of Consumer Callback instances to consume events
   * @param sources Source URIs to subscribe
   * 
   * @return DatabusRegistration handle to operate the consumption
   */
  public DatabusRegistration register(Collection<DatabusCombinedConsumer> consumers, String ... sourceUri)
                    throws DatabusClientException;
  
   
  /**
   * Creates cluster-aware registration. This API can be used to implement a
   * (a) Load Balanced Clients 
   *        where many client instances registered to a single client cluster will form a group and load partitioned (through Server-Side Filters)
   *        is distributed among them. Whenever a new client instance comes up or an group member instance shuts-down, the partitions are re-balanced 
   *        across the alive instances. 
   * 
   * There can be only one registration for a client cluster in a single client instance.  
   * @param cluster Client Cluster name to subscribe
   * @param consumerFactory Consumer Factory for instantiating consumer callbacks when new partitions gets assigned
   * @param filterFactory ServerSide Filter Factory for instantiating Server-Side filter. If no server-side factory, the consumer is pass null
   * @param partitionListenr PartitionListener for getting notified about partition migrations. If not needed, pass null
   * @param sources Source URIs to subscribe
   * 
   * @return DatabusRegistration handle to operate the client cluster.
   * 
   * @throws DatabusClientException if 
   *   (a) duplicate registration for the same cluster 
   *   (b) sourceUris is empty or null
   *   (c) ConsumerFactory is null
   *   (d) Cluster Configuration not provided for the client cluster
   */
  public DatabusRegistration registerCluster(String cluster,
                                             DbusClusterConsumerFactory consumerFactory,
                                             DbusServerSideFilterFactory filterFactory,
                                             DbusPartitionListener partitionListener,
                                             String ... sourceUris)
           throws DatabusClientException;
  
}
