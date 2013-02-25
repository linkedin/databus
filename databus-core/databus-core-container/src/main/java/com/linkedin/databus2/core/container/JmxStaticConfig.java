package com.linkedin.databus2.core.container;
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


/**
 * Static configuration for JMX
 * @author cbotev
 *
 */
public class JmxStaticConfig
{
  private final int _jmxServicePort;
  private final String _jmxServiceHost;
  private final int _rmiRegistryPort;
  private final String _rmiRegistryHost;
  private final boolean _rmiEnabled;

  public JmxStaticConfig(int jmxServicePort,
                         String jmxServiceHost,
                         int rmiRegistryPort,
                         String rmiRegistryHost,
                         boolean rmiEnabled)
  {
    super();
    _jmxServicePort = jmxServicePort;
    _jmxServiceHost = jmxServiceHost;
    _rmiRegistryPort = rmiRegistryPort;
    _rmiRegistryHost = rmiRegistryHost;
    _rmiEnabled = rmiEnabled;
  }

  /** The port for the JMX service */
  public int getJmxServicePort()
  {
    return _jmxServicePort;
  }

  /** The hostname for the JMX service*/
  public String getJmxServiceHost()
  {
    return _jmxServiceHost;
  }

  /** The port of the RMI registry where the JMX server will be registered */
  public int getRmiRegistryPort()
  {
    return _rmiRegistryPort;
  }

  /** The hostname of the RMI registry where the JMX server will be registered */
  public String getRmiRegistryHost()
  {
    return _rmiRegistryHost;
  }

  /** A flag if the RMI connector is to be enabled */
  public boolean isRmiEnabled()
  {
    return _rmiEnabled;
  }
}
