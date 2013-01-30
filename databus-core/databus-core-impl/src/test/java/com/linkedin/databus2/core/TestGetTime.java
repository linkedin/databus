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


import org.testng.annotations.Test;

/**
 * Tests performance of java calls to get time.
 * @author cbotev
 */
public class TestGetTime {

  private static final int ITER_NUM = 10000000;

  @Test(groups = {"medium", "perf"})
  public void testGetTime()
  {

    long startNanos, endNanos;

    startNanos = System.nanoTime();
    for (int i = 0; i < ITER_NUM; ++i)
    {
      System.nanoTime();
    }
    endNanos = System.nanoTime();

    long nanoTimeNanos = endNanos - startNanos;

    startNanos = System.nanoTime();
    for (int i = 0; i < ITER_NUM; ++i)
    {
      System.currentTimeMillis();
    }
    endNanos = System.nanoTime();

    long currentTimeMillisNanos = endNanos - startNanos;

    System.out.println("         nanoTime(), ns: " + 1.0 * nanoTimeNanos / ITER_NUM);
    System.out.println("currentTimeMillis(), ns: " + 1.0 * currentTimeMillisNanos / ITER_NUM);

  }


}
