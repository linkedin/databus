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


import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.databus.core.util.InvalidConfigException;

public class TestErrorRetriesCounter
{

  @Test
  public void testNoRetries()
  {
    BackoffTimer counter  = new BackoffTimer("test", BackoffTimerStaticConfig.NO_RETRIES);
    Assert.assertTrue(counter.backoff() < 0, "no retries");
  }

  @Test
  public void testUnlimitedRetriesKindOf()
  {
    BackoffTimer counter = new BackoffTimer("test", BackoffTimerStaticConfig.UNLIMITED_RETRIES);
    for (int i  = 0; i < 100000000; ++i) Assert.assertTrue(counter.backoff() >= 0, "keep on retrying");
  }

  @Test
  public void testHappyPath() throws Exception
  {
    BackoffTimerStaticConfigBuilder builder = new BackoffTimerStaticConfigBuilder();
    builder.setMaxRetryNum(1000);
    builder.setMaxSleep(100L);
    builder.setSleepIncFactor(2.0);
    builder.setSleepIncDelta(0);
    builder.setInitSleep(10);

    BackoffTimer counter = new BackoffTimer("test", builder.build());

    Assert.assertEquals(counter.getCurrentSleepMs(), 10L, "correct initial sleep");
    long s = 10L;
    for (int i = 1; i <= 1000; ++i)
    {
      long c = counter.backoff();
      Assert.assertTrue(c <= 100, "less than max sleep");
      s = Math.min((long)(2.0 * s), 100L);
      Assert.assertTrue(s <= c, "more than 2^n; iteration: " + i);
    }

    Assert.assertTrue(counter.backoff() < 0, "no more retries");
  }

  public void testBuilderNegativeMaxSleep() throws Exception
  {
    try
    {
      BackoffTimerStaticConfigBuilder builder = new BackoffTimerStaticConfigBuilder();
      builder.setMaxRetryNum(1000);
      builder.setMaxSleep(-1);
      builder.setSleepIncFactor(2.0);
      builder.setSleepIncDelta(0);
      builder.setInitSleep(10);

      builder.build();
      Assert.fail("expected InvalidConfigException");
    }
    catch (InvalidConfigException ice)
    {
      //we are good
    }
  }

  public void testBuilderNegativeInitSleep() throws Exception
  {
    try
    {
      BackoffTimerStaticConfigBuilder builder = new BackoffTimerStaticConfigBuilder();
      builder.setMaxRetryNum(1000);
      builder.setMaxSleep(1000);
      builder.setSleepIncFactor(2.0);
      builder.setSleepIncDelta(0);
      builder.setInitSleep(-1);

      builder.build();
      Assert.fail("expected InvalidConfigException");
    }
    catch (InvalidConfigException ice)
    {
      //we are good
    }
  }

  @Test
  public void testDecreasingSleep() throws Exception
  {
    BackoffTimerStaticConfigBuilder builder = new BackoffTimerStaticConfigBuilder();
    builder.setMaxRetryNum(1000);
    builder.setMaxSleep(100);
    builder.setSleepIncFactor(-2.0);
    builder.setSleepIncDelta(0);
    builder.setInitSleep(30L);

    BackoffTimer counter = new BackoffTimer("test", builder.build());
    Assert.assertEquals(counter.getCurrentSleepMs(), 30L, "correct initial sleep");
    Assert.assertTrue(counter.backoff() > 30L, "correct second sleep");
  }
}
