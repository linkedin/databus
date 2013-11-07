package com.linkedin.databus2.core.container.request;
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

import com.linkedin.databus2.core.DatabusException;

/**
 * An exception denoting that a producer is trying to push events and the relay is unable to verify
 * that there is no gap in the events.
 * */
public class SourcesTooOldException extends DatabusException
{
  private static final long serialVersionUID = 1L;
  public static final String MESSAGE_PREFIX = "sources to old: ";

  private final List<Short> _srcIds;

  public SourcesTooOldException(List<Short> srcIds)
  {
    super(generateMessage(srcIds));
    _srcIds = srcIds;
  }

  public SourcesTooOldException(int serverId)
  {
    super(MESSAGE_PREFIX + "server " + serverId);
    _srcIds = null;
  }

  public List<Short> getSrcIds()
  {
    return _srcIds;
  }

  public static String generateMessage(List<Short> srcIds)
  {
    StringBuilder sb = new StringBuilder(1000);
    sb.append(MESSAGE_PREFIX);
    boolean first = true;
    for (Short srcId: srcIds)
    {
      if (!first) sb.append(',');
      first = false;
      sb.append(srcId);
    }

    return sb.toString();
  }

}
