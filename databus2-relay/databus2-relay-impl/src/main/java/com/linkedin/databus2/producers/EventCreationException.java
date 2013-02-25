package com.linkedin.databus2.producers;
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


/**
 * Thrown when event creation failed for a databus source.
 *
 * @author Jemiah Westerman<jwesterman@linkedin.com>
 * @version $Revision: 153294 $
 */
public class EventCreationException
extends Exception
{
  private static final long serialVersionUID = 1L;

  private final String _sourceName;

  public EventCreationException(String sourceName)
  {
    super("Event creation failed. (source: " + sourceName + ")");
    _sourceName = sourceName;
  }

  public EventCreationException(String message, String sourceName)
  {
    super(message + " (source: " + sourceName + ")");
    _sourceName = sourceName;
  }

  public EventCreationException(String sourceName, Throwable cause)
  {
    super("Event creation failed. (source: " + sourceName + ")", cause);
    _sourceName = sourceName;
  }

  public EventCreationException(String message, String sourceName, Throwable cause)
  {
    super(message + " (source: " + sourceName + ")", cause);
    _sourceName = sourceName;
  }

  public String getSourceName()
  {
    return _sourceName;
  }
}
