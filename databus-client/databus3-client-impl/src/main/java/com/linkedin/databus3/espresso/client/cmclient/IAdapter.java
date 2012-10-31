package com.linkedin.databus3.espresso.client.cmclient;

import java.util.List;
import java.util.Map;

import com.linkedin.databus.client.pub.DatabusServerCoordinates;
import com.linkedin.databus.core.cmclient.ResourceKey;

public interface IAdapter {
	
	public Map<ResourceKey, List<DatabusServerCoordinates>> getExternalView(boolean cached, String dbName);

	/**
	 * Returns a map between a relay and the subscriptions it hosts ( described by ResourceKey ).
	 * 
	 * @param : cached , use cached znRecord if it already exists
	 * @return
	 */
	public Map<DatabusServerCoordinates, List<ResourceKey>> getInverseExternalView(boolean cached, String dbName);
	
	/**
	 * Add external view change observer 
	 * 
	 * @param DatabusExternalViewChangeObserver Callback instance to be added to observers list
	 */
	public void addExternalViewChangeObservers(DatabusExternalViewChangeObserver observer);
	
	/**
	 * Remove external view change observer 
	 * 
	 * @param DatabusExternalViewChangeObserver Callback instance to be removed from observers list
	 */
	public void removeExternalViewChangeObservers(DatabusExternalViewChangeObserver observer);
	
	/**
	 * Returns number of partitions in the cluster
	 * 
	 * @param dbName 
	 */
	public int getNumPartitions(String dbName);

}
