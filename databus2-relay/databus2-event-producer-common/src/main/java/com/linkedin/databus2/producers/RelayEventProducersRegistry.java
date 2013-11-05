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

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

import org.apache.log4j.Logger;

public class RelayEventProducersRegistry
{
  private static final RelayEventProducersRegistry THE_INSTANCE = new RelayEventProducersRegistry();

  private final Map<String, EventProducerServiceProvider> _providers =
      new HashMap<String, EventProducerServiceProvider>();
  private final ServiceLoader<EventProducerServiceProvider> _serviceLoader;
  private final Logger _log = Logger.getLogger(RelayEventProducersRegistry.class);

  private RelayEventProducersRegistry()
  {
    _serviceLoader = ServiceLoader.load(EventProducerServiceProvider.class);
    loadEventProducerServices();
  }

  private void loadEventProducerServices()
  {
    for (EventProducerServiceProvider provider: _serviceLoader)
    {
      register(provider.getUriScheme(), provider);
    }
  }

  /**
   * Registers an event producer service provider for a given URI scheme
   * @param uriScheme   the scheme of the URIs handled by the producers supported by the provider
   * @param provider    the provider itself
   */
  public synchronized void register(String uriScheme, EventProducerServiceProvider provider)
  {
    if (null == provider)
    {
      throw new NullPointerException("null event producer");
    }
    uriScheme = normalizeUriScheme(uriScheme);
    _log.info("registering relay event producer for scheme " + uriScheme + ": " + provider);
    EventProducerServiceProvider oldProducer = _providers.put(uriScheme, provider);
    if (null != oldProducer)
    {
      throw new RuntimeException("overriding old relay event producer for scheme " + uriScheme +": "
                                 + oldProducer);
    }
  }

  /**
   * Obtains the relay event producer for a given URI scheme (say, "jdbc:..." or "mock:...")
   * @param     uriScheme   the scheme for URIs of the event producer
   * @return    the relay event producer
   */
  public synchronized EventProducerServiceProvider getEventProducerServiceProvider(String uriScheme)
  {
    return _providers.get(normalizeUriScheme(uriScheme));
  }

  private static String normalizeUriScheme(String uriScheme)
  {
    if (null == uriScheme)
    {
      throw new NullPointerException("null URI scheme");
    }
    uriScheme = uriScheme.trim();
    if (uriScheme.endsWith(":"))
    {
      uriScheme = uriScheme.substring(0, uriScheme.length() - 1);
    }

    return uriScheme;
  }

  /** The registry singleton */
  public static RelayEventProducersRegistry getInstance()
  {
    return THE_INSTANCE;
  }

}
