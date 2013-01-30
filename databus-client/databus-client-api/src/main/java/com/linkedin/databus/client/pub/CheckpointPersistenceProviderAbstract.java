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

public abstract class CheckpointPersistenceProviderAbstract implements
    CheckpointPersistenceProvider
{
  protected final int _version; // 2 for all v2 clients, 3 - for espresso
  
  public CheckpointPersistenceProviderAbstract() {
    this(2);
  }
  public CheckpointPersistenceProviderAbstract(int version) {
    _version = version;
  }
  
  /** returns the version the provider was setup with */
  public int getVersion() {
    return _version;
  }

  protected List<String> convertSubsToListOfStrings(List<DatabusSubscription> subs) {
    List<String> subsSrcStringList = new ArrayList<String> (subs.size());
    
    for(DatabusSubscription sub : subs) {
      if(getVersion() >= 3) { // for espresso we use all available subs information
        subsSrcStringList.add(sub.uniqString());
      } else {
        subsSrcStringList.add(sub.getLogicalSource().getName()); // for v2 we use source name
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
  public Checkpoint loadCheckpointV3(List<DatabusSubscription> subs,
		  							 RegistrationId registrationId)
  {
    return loadCheckpoint(convertSubsToListOfStrings(subs));
  }

  @Override
  public void removeCheckpointV3(List<DatabusSubscription> subs,
		  						 RegistrationId registrationId)
  {
   removeCheckpoint(convertSubsToListOfStrings(subs)); 
  }
}
