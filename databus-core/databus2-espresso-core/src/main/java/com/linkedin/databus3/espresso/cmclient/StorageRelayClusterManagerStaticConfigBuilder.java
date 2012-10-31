package com.linkedin.databus3.espresso.cmclient;

import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus3.cm_utils.ClusterManagerStaticConfigBaseBuilder;

/**
 * Configuration Builder for setup information regarding Helix
 * 
 * @author Phanindra Ganti<pganti@linkedin.com>
 */

public class StorageRelayClusterManagerStaticConfigBuilder extends ClusterManagerStaticConfigBaseBuilder
			 implements ConfigBuilder<StorageRelayClusterManagerStaticConfig>
{
  public static final int DEFAULT_RELAY_REPLICATION_FACTOR = 1;
  
  private String _storageZkConnectString;
  private String _storageClusterName;
  private int relayReplicationFactor = DEFAULT_RELAY_REPLICATION_FACTOR;
  
  @Override
  public StorageRelayClusterManagerStaticConfig build() throws InvalidConfigException
  {
	  return new StorageRelayClusterManagerStaticConfig(getEnabled(), getEnableDynamic(), getRelayReplicationFactor(),
			  											getVersion(), getRelayZkConnectString(), getRelayClusterName(), 
			  											getStorageZkConnectString(), getStorageClusterName(), 
			  											getInstanceName(), getFileName());
  }
  
  public StorageRelayClusterManagerStaticConfigBuilder()
  {
	  super();
  }
  
  public String getStorageZkConnectString() {
	  return _storageZkConnectString;
  }

  public void setStorageZkConnectString(String storageZkConnectString) {
	  _storageZkConnectString = storageZkConnectString;
  }

  public String getStorageClusterName() {
	  return _storageClusterName;
  }

  public void setStorageClusterName(String storageClusterName) {
	  _storageClusterName = storageClusterName;
  }

  public int getRelayReplicationFactor() {
	return relayReplicationFactor;
  }

  public void setRelayReplicationFactor(int relayReplicationFactor) {
	this.relayReplicationFactor = relayReplicationFactor;
  }
}
