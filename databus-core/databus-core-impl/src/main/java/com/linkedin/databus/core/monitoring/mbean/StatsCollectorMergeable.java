package com.linkedin.databus.core.monitoring.mbean;

/**
 * 
 * @author snagaraj
 *  Stats collector objects that can be merged and reset 
 *  The purpose of this interface is to define methods for stats objects at a physical source (database) level in relay,bootstrap and client 
 *  components of databus2
 */

public interface StatsCollectorMergeable<T> 
{
	/** Initialize; reset internal state */
	public void reset();
	
	/** combine internal state with external object of same type **/
	public void merge(T obj);
	
}
