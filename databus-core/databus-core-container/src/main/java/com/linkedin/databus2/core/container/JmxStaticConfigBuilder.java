package com.linkedin.databus2.core.container;

import org.apache.log4j.Logger;

import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;

public class JmxStaticConfigBuilder implements ConfigBuilder<JmxStaticConfig>
{
  public static final String MODULE = JmxStaticConfigBuilder.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  public static final int DEFAULT_JMX_SERVICE_PORT = 9999;
  public static final String DEFAULT_JMX_SERVICE_HOST = "localhost";
  public static final int DEFAULT_RMI_REGISTRY_PORT = 1099;
  public static final String DEFAULT_RMI_REGISTRY_HOST = "localhost";

  private int _jmxServicePort;
  private String _jmxServiceHost;
  private int _rmiRegistryPort;
  private String _rmiRegistryHost;
  private boolean _rmiEnabled;

  public JmxStaticConfigBuilder()
  {
    _jmxServicePort = DEFAULT_JMX_SERVICE_PORT;
    _jmxServiceHost = DEFAULT_JMX_SERVICE_HOST;
    _rmiRegistryPort = DEFAULT_RMI_REGISTRY_PORT;
    _rmiRegistryHost = DEFAULT_RMI_REGISTRY_HOST;
    _rmiEnabled = true;
  }

  public int getJmxServicePort()
  {
    return _jmxServicePort;
  }
  public void setJmxServicePort(int jmxServicePort)
  {
    _jmxServicePort = jmxServicePort;
  }
  public String getJmxServiceHost()
  {
    return _jmxServiceHost;
  }
  public void setJmxServiceHost(String jmxServiceHost)
  {
    _jmxServiceHost = jmxServiceHost;
  }
  public int getRmiRegistryPort()
  {
    return _rmiRegistryPort;
  }
  public void setRmiRegistryPort(int rmiRegistryPort)
  {
    _rmiRegistryPort = rmiRegistryPort;
  }
  public String getRmiRegistryHost()
  {
    return _rmiRegistryHost;
  }
  public void setRmiRegistryHost(String rmiRegistryHost)
  {
    _rmiRegistryHost = rmiRegistryHost;
  }

  @Override
  public JmxStaticConfig build() throws InvalidConfigException
  {
    // TODO config validation (DDSDBUS-102)
    LOG.info("Using JMX service: " + _jmxServiceHost + ":" + _jmxServicePort);
    LOG.info("Using RMI registry: rmi://" + _rmiRegistryHost + ":" + _rmiRegistryPort);
    return new JmxStaticConfig(_jmxServicePort, _jmxServiceHost, _rmiRegistryPort, _rmiRegistryHost,
                               _rmiEnabled);
  }

  public boolean isRmiEnabled()
  {
    return _rmiEnabled;
  }

  public void setRmiEnabled(boolean rmiEnabled)
  {
    _rmiEnabled = rmiEnabled;
  }

}
