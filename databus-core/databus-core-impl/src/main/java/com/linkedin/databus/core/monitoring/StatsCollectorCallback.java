package com.linkedin.databus.core.monitoring;


/**
 * Defines callbacks on addition/removal of stats collector objects
 * Used by sensor factories, that enable dynamic registration of stats objects associated with physical sources (databases)
 */
 
public interface StatsCollectorCallback<T> {
	
	/** stats collector object added */
	void addedStats(T stats);

	/** stats collector object removed */
	void removedStats(T stats);
	
	
}
