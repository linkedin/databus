package com.linkedin.databus3.espresso.client.cmclient;

import java.util.List;
import java.util.Map;

import com.linkedin.databus.client.pub.DatabusServerCoordinates;
import com.linkedin.databus.core.cmclient.ResourceKey;

/*
 * Callback Interface for ExternalView changes of relays and other components managed by ClusterManager 
 */
public interface DatabusExternalViewChangeObserver 
{
	/**
	 * Callback for ExternalViewChange
	 * 
	 * @param dbName : DBName corresponding to the external view
	 * @param oldResourceToServerCoordinatesMap : Previous state of resourceToServerCoordinatesMap
	 * @param oldServerCoordinatesToResourceMap : Previous state of serverToResourceMap
	 * @param newResourceToServerCoordinatesMap : Current state of resourceToServerCoordinatesMap
	 * @param newServerCoordinatesToResourceMap : Current state of serverToResourceMap
	 */
	public void onExternalViewChange(String dbName, 
									 Map<ResourceKey, List<DatabusServerCoordinates>> oldResourceToServerCoordinatesMap, 
									 Map<DatabusServerCoordinates, List<ResourceKey>> oldServerCoordinatesToResourceMap,
									 Map<ResourceKey, List<DatabusServerCoordinates>> newResourceToServerCoordinatesMap,
									 Map<DatabusServerCoordinates, List<ResourceKey>> newServerCoordinatesToResourceMap);
}
