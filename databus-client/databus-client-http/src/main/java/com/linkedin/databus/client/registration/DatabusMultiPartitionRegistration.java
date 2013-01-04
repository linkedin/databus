package com.linkedin.databus.client.registration;

import java.util.Map;

import com.linkedin.databus.client.pub.DatabusRegistration;
import com.linkedin.databus.client.pub.DbusPartitionInfo;


/**
 * 
 * Registration which allows registering consumer(s) to subscribe to sources across source partitions (physical partitions).
 * This is not external-facing interface and client-application is not expected to code against it. 
 */
public interface DatabusMultiPartitionRegistration 
    extends DatabusRegistration 
{	
	/**
	 * Children registrations per partition
	 * @return a read-only copy of the {@link DbusPartitionInfo} to {@link DatabusSinglePartitionRegistration} mapping 
	 **/
	public Map<DbusPartitionInfo, DatabusRegistration> getPartitionRegs();	
}
