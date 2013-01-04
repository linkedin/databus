package com.linkedin.databus.client.pub;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.filter.DbusKeyCompositeFilterConfig;


/**
 * Factory interface for generating server-side filter corresponding to the partition 
 * that is getting added.
 * 
 * @author bvaradar
 *
 */
public interface DbusServerSideFilterFactory 
{
	public DbusKeyCompositeFilterConfig createServerSideFilter(DbusClusterInfo cluster, 
			                                                   DbusPartitionInfo partition) throws InvalidConfigException;
}
