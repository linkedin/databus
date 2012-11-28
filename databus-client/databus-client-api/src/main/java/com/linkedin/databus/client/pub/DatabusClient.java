package com.linkedin.databus.client.pub;

import com.linkedin.databus2.core.filter.DbusKeyCompositeFilterConfig;

public interface DatabusClient
{

  /** Subscribes a single listener to the on-line update stream for the specified sources */
  public void registerDatabusStreamListener(DatabusStreamConsumer listener,
                                            DbusKeyCompositeFilterConfig filterConfig,
                                            String ... sources)
         throws DatabusClientException;

  /**
   * Subscribes a group listener to the on-line update stream for the specified sources. The onEvent
   * callbacks will be spread and run in parallel across all listeners. */
  public void registerDatabusStreamListener(DatabusStreamConsumer[] listeners,
                                            DbusKeyCompositeFilterConfig filterConfig,
                                            String ... sources)
         throws DatabusClientException;

  /** Subscribes a single listener to the bootstrap stream for the specified sources */
  public void registerDatabusBootstrapListener(DatabusBootstrapConsumer[] listeners,
                                               DbusKeyCompositeFilterConfig filterConfig,
                                               String ... sources)
         throws DatabusClientException;

  /**
   * Subscribes a group listener to the bootstrap stream for the specified sources. The onEvent
   * callbacks will be spread and run in parallel across all listeners. */
  public void registerDatabusBootstrapListener(DatabusBootstrapConsumer listener,
                                               DbusKeyCompositeFilterConfig filterConfig,
                                               String ... sources)
         throws DatabusClientException;
}
