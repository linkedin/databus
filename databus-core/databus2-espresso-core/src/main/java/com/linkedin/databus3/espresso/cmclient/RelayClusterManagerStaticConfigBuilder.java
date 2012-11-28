package com.linkedin.databus3.espresso.cmclient;

import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus3.cm_utils.ClusterManagerStaticConfigBaseBuilder;

/**
 * Configuration Builder for setup information regarding Relay Cluster Manager
 * 
 * @author Phanindra Ganti<pganti@linkedin.com>
 */
public class RelayClusterManagerStaticConfigBuilder extends ClusterManagerStaticConfigBaseBuilder
			 implements ConfigBuilder<RelayClusterManagerStaticConfig>
{

  @Override
  public RelayClusterManagerStaticConfig build() throws InvalidConfigException
  {
	  return new RelayClusterManagerStaticConfig(_enabled, _enableDynamic, _version, _relayZkConnectString, _relayClusterName, 
			  									 _instanceName, _fileName);
  }
  
  public RelayClusterManagerStaticConfigBuilder()
  {
	  super();
  }
  
}
