package com.linkedin.databus.client;
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

import com.linkedin.databus.client.pub.LastWriteTimeTracker;

public class LastWriteTimeTrackerImpl implements LastWriteTimeTracker
{

  private long _lastWriteTimeMillis = 0;
  private long _overallLastWriteTimeMillis = 0;

  public LastWriteTimeTrackerImpl()
  {

  }

  public synchronized void setLastWriteTimeMillis(long lastWriteTimeMillis)
  {
    _lastWriteTimeMillis = lastWriteTimeMillis;
    _overallLastWriteTimeMillis = _lastWriteTimeMillis;

  }

  public synchronized void setOverallLastWriteTimeMillis(long overallLastWriteMillis)
  {
    _overallLastWriteTimeMillis = overallLastWriteMillis;
  }

  @Override
  public synchronized  long getLastWriteTimeMillis()
  {
    return _lastWriteTimeMillis;
  }

  @Override
  public synchronized long getOverallLastWriteTimeMillis()
  {
    return _overallLastWriteTimeMillis;
  }

}
