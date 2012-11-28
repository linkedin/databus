package com.linkedin.databus3.espresso.cmclient;

/**
 * Static configuration for setup information regarding relay cluster
 * These parameters are read in as java properties
 * 
 * This is used by databus client which requires access to the relay cluster's external view only
 * 
 * @author Phanindra Ganti<pganti@linkedin.com>
 */
public class RelayClusterManagerStaticConfig extends ClusterManagerStaticConfigBase
{

	/**
	 * 
	 * @param enabled
	 * @param enableDynamic
	 * @param relayZkConnectString
	 * @param relayClusterName
	 * @param storageZkConnectString
	 * @param storageClusterName
	 * @param instanceName
	 * @param fileName
	 */
	public RelayClusterManagerStaticConfig(boolean enabled, boolean enableDynamic, String version, String relayZkConnectString, String relayClusterName,
										   String instanceName, String fileName)
	{
		super(enabled, enableDynamic, version, relayZkConnectString, relayClusterName, instanceName, fileName);
	}
}
