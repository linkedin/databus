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


import org.testng.Assert;
import org.testng.annotations.Test;


public class TestRngUtils
{
  @Test
  public void testRandomShort()
  {
    for (int j=0; j < Integer.MAX_VALUE/100; ++j)
    {
      short s = RngUtils.randomPositiveShort();
      if (s < 0)
      {
        Assert.fail(" short < 0: " + s);
      }
    }
  }

  @Test
  public void testRandomInt()
  {

    for (int j=0; j < Integer.MAX_VALUE/100; ++j)
    {
      int i = RngUtils.randomPositiveInt();
      if (i < 0)
      {
        Assert.fail("int < 0: " + i);
      }
    }
  }

  @Test
  public void testRandomLong()
  {

    for (int j=0; j < Integer.MAX_VALUE/100; ++j)
    {
      long l = RngUtils.randomPositiveLong();
      if (l < 0)
      {
        Assert.fail("long < 0: " + l);
      }
    }
  }
}
