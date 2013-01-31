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

import com.linkedin.databus2.core.DatabusException;

/**
 * An interface for {@link DatabusSubscription} URI codec.
 */
public interface SubscriptionUriCodec
{
  /** The unique prefix (e.g. oracle or espresso) for this codec.
   * Note: it must not include a colon.  */
  String getScheme();

  /**
   * Attempts to parse the specified URI into a {@link DatabusSubscription} object
   * @return the resulting subscription object
   */
  DatabusSubscription decode(URI uri) throws DatabusException;

  /** Creates a URI from a subscription */
  URI encode(DatabusSubscription sub);
}
