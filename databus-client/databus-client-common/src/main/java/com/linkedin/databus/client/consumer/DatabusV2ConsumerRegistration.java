package com.linkedin.databus.client.consumer;
/*
 *
 * Copyright 2013 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/


import java.util.List;

import com.linkedin.databus.client.pub.DatabusCombinedConsumer;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus2.core.filter.DbusKeyCompositeFilterConfig;

/**
 * A class to make it convenient to use templated ConsumerRegistration
 */
public class DatabusV2ConsumerRegistration extends ConsumerRegistration
{

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

}
