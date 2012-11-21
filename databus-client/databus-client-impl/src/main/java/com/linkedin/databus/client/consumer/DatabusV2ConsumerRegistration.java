package com.linkedin.databus.client.consumer;

import java.util.List;

import com.linkedin.databus.client.pub.DatabusCombinedConsumer;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus2.core.filter.DbusKeyCompositeFilterConfig;

/**
 *
 * @author pganti
 *
 * A class to make it convenient to use templated ConsumerRegistration
 */
public class DatabusV2ConsumerRegistration extends ConsumerRegistration<DatabusCombinedConsumer>{

  protected DatabusV2ConsumerRegistration(DatabusCombinedConsumer consumer,
                                       List<DatabusCombinedConsumer> consumers,
                                       List<DatabusSubscription> sources,
                                       DbusKeyCompositeFilterConfig filterConfig)
  {
    super(consumer, consumers, sources, filterConfig);
  }

  public DatabusV2ConsumerRegistration(
			List<DatabusCombinedConsumer> consumers,
			List<String> sources, DbusKeyCompositeFilterConfig filterConfig) {
		super(consumers, sources, filterConfig);
	}

	public DatabusV2ConsumerRegistration(DatabusCombinedConsumer consumer,
										 List<String> sources,
										 DbusKeyCompositeFilterConfig filterConfig)
	{
		super(consumer, sources, filterConfig);
	}

	public DatabusV2ConsumerRegistration(DatabusCombinedConsumer[] consumers,
										 List<String> sources,
										 DbusKeyCompositeFilterConfig filterConfig)
	{
		super(consumers, sources, filterConfig);
	}

}
