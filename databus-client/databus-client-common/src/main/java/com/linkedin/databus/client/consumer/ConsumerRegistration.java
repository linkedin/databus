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
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus2.core.filter.DbusKeyCompositeFilterConfig;

/**
 *
 * @author pganti
 *
 * A class that describes the registration of a consumer or a group of consumers
 * to a list of sources
 */
public class ConsumerRegistration<C>
{
  protected final List<C> _consumers;
  protected final List<DatabusSubscription> _sources;
  protected final Random _rng;
  protected  final DbusKeyCompositeFilterConfig _filterConfig;

  /**
   * Keep the ConsumerRegistration with List<String> sources interface around for backward
   * compatibility
   */
  public ConsumerRegistration(C consumer, List<String> sources, DbusKeyCompositeFilterConfig filterConfig)
  {
    this(consumer, null, DatabusSubscription.createSubscriptionList(sources), filterConfig);
  }

  /**
   *
   * Registration for a group of consumers to a list of sources
   */
  public ConsumerRegistration(C[] consumers, List<String> sources, DbusKeyCompositeFilterConfig filterConfig)
  {
    this(null, Arrays.asList(consumers), DatabusSubscription.createSubscriptionList(sources), filterConfig );
  }

  /**
   * Registration for a group of consumers to a list of sources
   */
  public ConsumerRegistration(List<C> consumers, List<String> sources, DbusKeyCompositeFilterConfig filterConfig)
  {
    this(null, consumers, DatabusSubscription.createSubscriptionList(sources), filterConfig);
  }

  /**
   * For single-consumer registrations, returns the consumer. For multi-consumer registration,
   * returns a random consumer from the group (for load balancing purposes).
   */
  public C getConsumer()
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
  public List<C> getConsumers()
  {
    return _consumers;
  }

  /**
   * Returns the sources for this registration
   */
  public List<String> getSources()
  {
    return DatabusSubscription.getStrList(_sources);
  }

  /**
   * Returns the subscriptions for this registration
   */
  public List<DatabusSubscription> getSubscriptions()
  {
    return _sources;
  }

  /**
   * Checks if the registration covers a given source
   */
  public boolean checkSourceSubscription(DatabusSubscription sourceName)
  {
    for(DatabusSubscription dSub : _sources) {
      if(dSub.equals(sourceName))
        return true;
    }
    return false;
  }

  public DbusKeyCompositeFilterConfig getFilterConfig() {
	return _filterConfig;
  }

  protected ConsumerRegistration(C consumer, List<C> consumers,
		  List<DatabusSubscription> sources,
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
		  _consumers = new ArrayList<C>();
		  _consumers.add(consumer);
	  }
	  else
	  {
		  _consumers = consumers;
	  }

	  _sources = sources;
	  _filterConfig = filterConfig;
	  _rng = new Random();
  }


  public static List<String> createStringFromAllSubscriptionFromRegistration(ConsumerRegistration<?> reg)
  {
	  List<DatabusSubscription> subscriptions = reg.getSubscriptions();

	  return DatabusSubscription.getStrList(subscriptions);
  }

}
