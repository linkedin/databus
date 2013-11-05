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


import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.linkedin.databus.client.pub.DatabusCombinedConsumer;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus2.core.filter.DbusKeyCompositeFilterConfig;

/**
 * A class that describes the registration of a consumer or a group of consumers to a list of sources
 */
public class ConsumerRegistration
{
  protected final List<DatabusCombinedConsumer> _consumers;
  protected final List<DatabusSubscription> _subs;
  protected final Random _rng;
  protected  final DbusKeyCompositeFilterConfig _filterConfig;

  /**
   * Keep the ConsumerRegistration with List<String> sources interface around for backward
   * compatibility
   */
  public ConsumerRegistration(DatabusCombinedConsumer consumer, List<String> sources, DbusKeyCompositeFilterConfig filterConfig)
  {
    this(consumer, null, DatabusSubscription.createSubscriptionList(sources), filterConfig);
  }

  /**
   * Registration for a group of consumers to a list of sources
   */
  public ConsumerRegistration(List<DatabusCombinedConsumer> consumers, List<String> sources, DbusKeyCompositeFilterConfig filterConfig)
  {
    this(null, consumers, DatabusSubscription.createSubscriptionList(sources), filterConfig);
  }

  /**
   * For single-consumer registrations, returns the consumer. For multi-consumer registration,
   * returns a random consumer from the group (for load balancing purposes).
   */
  public DatabusCombinedConsumer getConsumer()
  {
	  /**
	   * A single consumer is a trivial case of a multi-consumer with num consumers = 1
	   */
      int index = _rng.nextInt(_consumers.size());
      return _consumers.get(index);
  }

  /**
   * Returns the group of consumers for multi-consumer registrations or a singleton list for
   * single-consumer registrations.
   */
  public List<DatabusCombinedConsumer> getConsumers()
  {
    return _consumers;
  }

  /**
   * Returns the sources for this registration
   */
  public List<String> getSources()
  {
    return DatabusSubscription.getStrList(_subs);
  }

  /**
   * Returns the subscriptions for this registration
   */
  public List<DatabusSubscription> getSubscriptions()
  {
    return _subs;
  }

  /**
   * Checks if the registration covers a given source
   */
  public boolean checkSourceSubscription(DatabusSubscription sub)
  {
    for(DatabusSubscription dSub : _subs) {
      if(dSub.equals(sub))
        return true;
    }
    return false;
  }

  public DbusKeyCompositeFilterConfig getFilterConfig() {
	return _filterConfig;
  }

  protected ConsumerRegistration(DatabusCombinedConsumer consumer, List<DatabusCombinedConsumer> consumers,
		  List<DatabusSubscription> subs,
		  DbusKeyCompositeFilterConfig filterConfig)
  {
	  super();

	  /**
	   * Only one of consumer, consumers can be valid in a given call
	   */
	  boolean bothConsumerParamsNull = (consumer == null && ( (consumers == null) || (consumers.size() == 0)));
	  boolean neitherConsumerParamsNull = (consumer != null && (consumers !=  null));
	  if (bothConsumerParamsNull || neitherConsumerParamsNull)
	  {
		  throw new InvalidParameterException("Only one of consumer / consumers should be null");
	  }

	  if (null == consumers)
	  {
		  _consumers = new ArrayList<DatabusCombinedConsumer>();
		  _consumers.add(consumer);
	  }
	  else
	  {
		  _consumers = consumers;
	  }

	  _subs = new ArrayList<DatabusSubscription>(subs);
	  _filterConfig = filterConfig;
	  _rng = new Random();
  }


  public static List<String> createStringFromAllSubscriptionFromRegistration(ConsumerRegistration reg)
  {
	  List<DatabusSubscription> subscriptions = reg.getSubscriptions();

	  return DatabusSubscription.getStrList(subscriptions);
  }

}
