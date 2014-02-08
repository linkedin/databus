package com.linkedin.databus.core.data_model;
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


import java.net.URI;
import java.net.URISyntaxException;

import com.linkedin.databus2.core.DatabusException;

/**
 * A codec for the legacy string representation of subscriptions
 * The format is either:
 * <ul>
 *      <li>LogicalSourceName for Databus V2 sources, or
 *      <li>EspressoDBName.TableName:PartitionNum for Espresso V3 sources
 * </ul>
 */
public class LegacySubscriptionUriCodec implements SubscriptionUriCodec
{
  private static final LegacySubscriptionUriCodec THE_INSTANCE = new LegacySubscriptionUriCodec();

  public static LegacySubscriptionUriCodec getInstance()
  {
    return THE_INSTANCE;
  }

  @Override
  public String getScheme()
  {
    return "legacy";
  }

  @Override
  public DatabusSubscription decode(URI uri) throws DatabusException
  {
    String scheme = uri.getScheme();
    String s = getScheme().equals(scheme) ? uri.getSchemeSpecificPart() : uri.toString();

    return DatabusSubscription.createSimpleSourceSubscription(new LogicalSource(s));
  }

  @Override
  public URI encode(DatabusSubscription sub)
  {
    try
    {
      String uriSsc = sub.generateSubscriptionString();;
      URI result = uriSsc.indexOf(':') >=0 ? new URI(getScheme(), uriSsc, null) : new URI(uriSsc);
      return result;
    }
    catch (URISyntaxException e)
    {
      throw new RuntimeException("unable to generate legacy subscription URI: " + e.getMessage(), e);
    }
  }

}
