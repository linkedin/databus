package com.linkedin.databus3.espresso.cmclient;

import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;

/**
 * Configuration Builder for setup information regarding Helix
 * 
 * @author Phanindra Ganti<pganti@linkedin.com>
 */
public class ClusterManagerStaticConfigBuilder
       implements ConfigBuilder<ClusterManagerStaticConfig>
{

  private boolean _enabled = false;	
  private String _relayZkConnectString = null;
  private String _relayClusterName = null;
  private String _storageZkConnectString = null;
  private String _storageClusterName = null;
  private String _instanceName = null;
  private String _fileName = null;
  
  @Override
  public ClusterManagerStaticConfig build() throws InvalidConfigException
  {
	  return new ClusterManagerStaticConfig(_enabled, _relayZkConnectString, _relayClusterName, 
			  								_storageZkConnectString, _storageClusterName, 
			  								_instanceName, _fileName);
  }
  
  public boolean getEnabled() {
	return _enabled;
  }

  public void setEnabled(boolean enabled) {
	_enabled = enabled;
  }

  public String getRelayZkConnectString() {
	  return _relayZkConnectString;
  }

  public void setRelayZkConnectString(String relayZkConnectString) {
	  _relayZkConnectString = relayZkConnectString;
  }

  public String getRelayClusterName() {
	  return _relayClusterName;
  }

  public void setRelayClusterName(String relayClusterName) {
	  _relayClusterName = relayClusterName;
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

  public String getInstanceName() {
	  return _instanceName;
  }

  public void setInstanceName(String instanceName) {
	  _instanceName = instanceName;
  }  
  public String getFileName() {
	  return _fileName;
  }

  public void setFileName(String fileName) {
	  _fileName = fileName;
  }  

}
