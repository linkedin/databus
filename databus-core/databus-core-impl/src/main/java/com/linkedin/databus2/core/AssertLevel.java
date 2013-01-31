package com.linkedin.databus2.core;
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


/** Describes what level of asserts should be evaluated */
public enum AssertLevel
{
  NONE (0),
  QUICK (100),
  MEDIUM (200),
  ALL (300);

  private final int _intValue;
  private AssertLevel(int intValue)
  {
    _intValue = intValue;
  }

  public int getIntValue()
  {
    return _intValue;
  }

  public boolean isSameOrStricter(AssertLevel other)
  {
    return _intValue >= other._intValue;
  }

  public boolean enabled()
  {
    return _intValue > NONE._intValue;
  }

  public boolean quickEnabled()
  {
    return isSameOrStricter(QUICK);
  }

  public boolean mediumEnabled()
  {
    return isSameOrStricter(MEDIUM);
  }

  public boolean allEnabled()
  {
    return isSameOrStricter(ALL);
  }
}
