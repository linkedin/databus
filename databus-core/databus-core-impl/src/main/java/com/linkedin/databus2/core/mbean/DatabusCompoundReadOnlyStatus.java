package com.linkedin.databus2.core.mbean;
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

import com.linkedin.databus.core.CompoundDatabusComponentStatus;

public class DatabusCompoundReadOnlyStatus extends DatabusReadOnlyStatus
                                           implements DatabusCompoundReadOnlyStatusMBean
{
  protected CompoundDatabusComponentStatus _compoundStatus;

  public DatabusCompoundReadOnlyStatus(String name,
                                       CompoundDatabusComponentStatus status,
                                       long ownerId)
  {
    super(name, status, ownerId);
    _compoundStatus = status;
  }

  @Override
  public List<String> getInitializing()
  {
    return _compoundStatus.getInitializing();
  }

  @Override
  public int getNumInitializing()
  {
    return _compoundStatus.getInitializing().size();
  }

  @Override
  public List<String> getSuspended()
  {
    return _compoundStatus.getSuspended();
  }

  @Override
  public int getNumSuspended()
  {
    return _compoundStatus.getSuspended().size();
  }

  @Override
  public List<String> getPaused()
  {
    return _compoundStatus.getPaused();
  }

  @Override
  public int getNumPaused()
  {
    return _compoundStatus.getPaused().size();
  }

  @Override
  public List<String> getShutdown()
  {
    return _compoundStatus.getShutdown();
  }

  @Override
  public int getNumShutdown()
  {
    return _compoundStatus.getShutdown().size();
  }

  @Override
  public List<String> getRetryingOnError()
  {
    return _compoundStatus.getRetryingOnError();
  }

  @Override
  public int getNumRetryingOnError()
  {
    return _compoundStatus.getRetryingOnError().size();
  }

}
