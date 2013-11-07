package com.linkedin.databus.client.pub;
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


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.data_model.DatabusSubscription;

public abstract class CheckpointPersistenceProviderAbstract implements CheckpointPersistenceProvider
{
  /**
   * The version of the relay-client protocol we're using.  Reference wiki:  Databus+v2.0++Protocol
   *
   * <ul>
   *   <li> 2 - client specifies sources rather than subscriptions and uses single-source checkpoints;
   *            /register returns list of source schemas only </li>
   *   <li> 3 - client specifies subscriptions and uses multi-source checkpoints;
   *            /register returns list of source schemas only </li>
   *   <li> 4 - as with v3, but /register returns map containing schemas for keys and metadata in
   *            addition to sources </li>
   * </ul>
   */
  protected final int _protocolVersion;

  // TODO:  We want this or DatabusHttpClientImpl's copy to be sole copy (not counting
  //        DatabusHttpV3ClientImpl's copy), but they live in different packages, so
  //        package-private won't work; and a public static variable (or getter) is
  //        undesirable, too (and perhaps unworkable in some places?).  DDSDBUS-2107 item 14
  private static final int DEFAULT_PROTOCOL_VERSION = 2;  // default version

  public CheckpointPersistenceProviderAbstract()
  {
    this(DEFAULT_PROTOCOL_VERSION);
  }

  public CheckpointPersistenceProviderAbstract(int protocolVersion)
  {
    _protocolVersion = protocolVersion;
  }

  protected List<String> convertSubsToListOfStrings(List<DatabusSubscription> subs)
  {
    List<String> subsSrcStringList = new ArrayList<String> (subs.size());

    for (DatabusSubscription sub : subs)
    {
      if (_protocolVersion >= 3) // for Espresso and other v3+ relays, use all available subs information
      {
        subsSrcStringList.add(sub.uniqString());
      }
      else
      {
        subsSrcStringList.add(sub.getLogicalSource().getName()); // for v2 relays, just use source names
      }
    }
    return subsSrcStringList;
  }

  @Override
  public void storeCheckpoint(List<String> sourceNames, Checkpoint checkpoint) throws IOException
  {
  }

  @Override
  public Checkpoint loadCheckpoint(List<String> sourceNames)
  {
    return null;
  }

  @Override
  public void removeCheckpoint(List<String> sourceNames)
  {
  }

  @Override
  public void storeCheckpointV3(List<DatabusSubscription> subs,
                                Checkpoint checkpoint,
                                RegistrationId registrationId) throws IOException
  {
    storeCheckpoint(convertSubsToListOfStrings(subs), checkpoint);
  }

  @Override
  public Checkpoint loadCheckpointV3(List<DatabusSubscription> subs, RegistrationId registrationId)
  {
    return loadCheckpoint(convertSubsToListOfStrings(subs));
  }

  @Override
  public void removeCheckpointV3(List<DatabusSubscription> subs, RegistrationId registrationId)
  {
   removeCheckpoint(convertSubsToListOfStrings(subs));
  }
}
