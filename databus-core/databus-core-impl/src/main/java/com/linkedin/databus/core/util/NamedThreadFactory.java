package com.linkedin.databus.core.util;
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


import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedThreadFactory implements ThreadFactory
{
  private final AtomicInteger _tCnt = new AtomicInteger(0);
  private final String _namePrefix;
  private final boolean _spawnDaemons;

  public NamedThreadFactory(String namePrefix, boolean spawnDaemons)
  {
    _namePrefix = namePrefix + "-";
    _spawnDaemons = spawnDaemons;
  }

  public NamedThreadFactory(String namePrefix)
  {
    this(namePrefix, false);
  }

  @Override
  public Thread newThread(Runnable r)
  {
    Thread result = new Thread(r, _namePrefix + _tCnt.incrementAndGet());
    result.setDaemon(_spawnDaemons);
    return result;
  }
}
