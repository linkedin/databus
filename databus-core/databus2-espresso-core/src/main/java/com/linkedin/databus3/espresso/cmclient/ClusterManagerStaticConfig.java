package com.linkedin.databus3.espresso.cmclient;


/**
 * Static configuration for setup information regarding Helix
 * These parameters are read in as java properties
 * 
 * @author Phanindra Ganti<pganti@linkedin.com>
 */
public class ClusterManagerStaticConfig
{

  private boolean _enabled;	
  private String _relayZkConnectString;
  private String _relayClusterName;
  private String _storageZkConnectString;
  private String _storageClusterName;
  private String _instanceName;
  private String _fileName;

  /**
   * 
   * @param enabled
   * @param relayZkConnectString
   * @param relayClusterName
   * @param storageZkConnectString
   * @param storageClusterName
   * @param instanceName
   * @param fileName
   */
  public ClusterManagerStaticConfig(boolean enabled, String relayZkConnectString, String relayClusterName,
		  							String storageZkConnectString, String storageClusterName,
		  							String instanceName, String fileName)
  {
    super();
    _enabled = enabled;
    _relayZkConnectString = relayZkConnectString;
    _relayClusterName = relayClusterName;
    _storageZkConnectString = storageZkConnectString;
    _storageClusterName = storageClusterName;
    _instanceName = instanceName;
    _fileName = fileName;
  }

  public boolean getEnabled() {
	  return _enabled;
  }
  
  public String getRelayZkConnectString() {
	  return _relayZkConnectString;
  }

  public String getRelayClusterName() {
	  return _relayClusterName;
  }

  public String getStorageZkConnectString() {
	  return _storageZkConnectString;
  }

  public String getStorageClusterName() {
	  return _storageClusterName;
  }

  public String getInstanceName() {
	  return _instanceName;
  }
  
  public String getFileName() {
	  return _fileName;
  }

}
