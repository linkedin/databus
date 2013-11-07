package com.linkedin.databus.bootstrap.common;

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

import com.linkedin.databus.core.DbusEvent;

public class BootstrapDBRow
{
  private final long id;
  private final String key;
  private final long scn;
  private final DbusEvent event;

  public long getId()
  {
    return id;
  }

  public String getKey()
  {
    return key;
  }

  public long getScn()
  {
    return scn;
  }

  public DbusEvent getEvent()
  {
    return event;
  }

  public BootstrapDBRow(long id, String key, long scn, DbusEvent event)
  {
    super();
    this.id = id;
    this.key = key;
    this.scn = scn;
    this.event = event;
  }

  @Override
  public String toString()
  {
    return "Row [id=" + id + ", key=" + key + ", scn=" + scn + ", event="
        + event + "]";
  }
}
