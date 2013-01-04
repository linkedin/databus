package com.linkedin.databus.client.pub;

import java.util.Arrays;
import java.util.Collection;

public class DefaultDbusClusterConsumerFactory<T extends DatabusCombinedConsumer> implements DbusClusterConsumerFactory 
{
	  private final Class<T> _class;
	
	  public DefaultDbusClusterConsumerFactory(Class<T> c)
	  {
	    _class = c;
	  }

	@Override
	public Collection<DatabusCombinedConsumer> createPartitionedConsumers(
			DbusClusterInfo clusterInfo, DbusPartitionInfo partitionInfo) 
	{
		DatabusCombinedConsumer c;
		try {
			c = _class.newInstance();
		} catch (InstantiationException e) {
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
		
		return Arrays.asList(c);
	}

}
