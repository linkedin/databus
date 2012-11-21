package com.linkedin.databus3.espresso.cmclient;

import java.net.UnknownHostException;

import org.apache.log4j.Logger;


/**
 * Static configuration for setup information regarding relay and storage clusters
 * These parameters are read in as java properties
 * 
 * This is typically required for v3 ( espresso like ) relay that requires access to 
 * both storage and relay cluster co-ordinates
 * 
 * @author Phanindra Ganti<pganti@linkedin.com>
 */
public class ClusterManagerStaticConfigBase
{
	protected boolean _enableDynamic = false;
	protected boolean _enabled = false;
	protected String _version = null;
	protected String _relayZkConnectString;
	protected String _relayClusterName;
	protected String _instanceName;
	protected String _fileName;

  
  /**
   * 
   * @param enabled Enable/Disable Cluster Manager Connection on startup
   * @param enableDynamic Enable/Disable Cluster Manager dynamic externalView notification (can be enabled only if enabled = true)
   * @param relayZkConnectString
   * @param relayClusterName
   * @param storageZkConnectString
   * @param storageClusterName
   * @param instanceName
   * @param fileName
   */
  public ClusterManagerStaticConfigBase(boolean enabled, boolean enableDynamic, 
		                                String version, String relayZkConnectString,
		                                String relayClusterName, String instanceName, String fileName)
  {
    super();
    _enabled = enabled;
    _enableDynamic = enableDynamic;
    _version = version;
    _relayZkConnectString = relayZkConnectString;
    _relayClusterName = relayClusterName;
    _instanceName = instanceName;
    _fileName = fileName;
  }
  
  public boolean getEnabled()
  {
	  return _enabled;
  }
  
  public String getVersion()
  {
	  return _version;
  }
  
  public String getRelayZkConnectString()
  {
	  return _relayZkConnectString;
  }

  public String getRelayClusterName()
  {
	  return _relayClusterName;
  }
  
  public String getInstanceName()
  {
	  return _instanceName;
  }
  
  public String getFileName()
  {
	  return _fileName;
  }

  public boolean getEnableDynamic() {
	return _enableDynamic;
  }

  public void setEnableDynamic(boolean _enableDynamic) {
	this._enableDynamic = _enableDynamic;
  }
}
