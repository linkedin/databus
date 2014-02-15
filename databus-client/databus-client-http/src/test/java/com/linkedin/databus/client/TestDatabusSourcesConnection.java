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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventBuffer.AllocationPolicy;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus2.test.TestUtil;

public class TestDatabusSourcesConnection
{

  @BeforeClass
  public void classSetup()
  {
    TestUtil.setupLoggingWithTimestampedFile(true, "/tmp/TestDatabusSourcesConnection_", ".log", Level.WARN);
  }

  @Test
  public void testConfig() throws Exception
  {
    DatabusSourcesConnection.Config config;
    DatabusSourcesConnection.StaticConfig sConfig;
    {
      // Make sure that ConsumerTimeBudget defaults to the relay's settings as long as
      // the bootstrap's budget is not set.
      config= new DatabusSourcesConnection.Config();
      sConfig = config.build();
      Assert.assertEquals(sConfig.getConsumerTimeBudgetMs(), sConfig.getBstConsumerTimeBudgetMs());
      long timeBudget = sConfig.getConsumerTimeBudgetMs();
      config.setConsumerTimeBudgetMs(timeBudget+1);
      sConfig = config.build();
      Assert.assertEquals(timeBudget+1, sConfig.getConsumerTimeBudgetMs());
      Assert.assertEquals(timeBudget+1, sConfig.getBstConsumerTimeBudgetMs());
      config.setBstConsumerTimeBudgetMs(timeBudget + 2);
      sConfig = config.build();
      Assert.assertEquals(timeBudget+2, sConfig.getBstConsumerTimeBudgetMs());
      Assert.assertEquals(timeBudget+1, sConfig.getConsumerTimeBudgetMs());
    }

    {
      // Make sure that EventBuffer and BstEventBuffer are distinct only after some parameter in BstEventBuffer is set.
      config = new DatabusSourcesConnection.Config();
      Assert.assertFalse(config.hasBstEventBuffer());
      sConfig = config.build();
      Assert.assertEquals(sConfig.getEventBuffer().getBufferRemoveWaitPeriod(), sConfig.getBstEventBuffer().getBufferRemoveWaitPeriod());
      long waitPeriod = sConfig.getEventBuffer().getBufferRemoveWaitPeriod();
      config.getEventBuffer().setBufferRemoveWaitPeriodSec(waitPeriod + 1);
      sConfig = config.build();
      Assert.assertEquals(waitPeriod+1, sConfig.getEventBuffer().getBufferRemoveWaitPeriod());
      Assert.assertEquals(waitPeriod+1, sConfig.getBstEventBuffer().getBufferRemoveWaitPeriod());
      Assert.assertFalse(config.hasBstEventBuffer());
      config.getBstEventBuffer().setBufferRemoveWaitPeriodSec(waitPeriod+2);
      Assert.assertTrue(config.hasBstEventBuffer());
      sConfig = config.build();
      Assert.assertEquals(waitPeriod+1, sConfig.getEventBuffer().getBufferRemoveWaitPeriod());
      Assert.assertEquals(waitPeriod+2, sConfig.getBstEventBuffer().getBufferRemoveWaitPeriod());
      // We should return the same object no matter how many times get is called.
      Assert.assertEquals(System.identityHashCode(config.getBstEventBuffer()),
                          System.identityHashCode(config.getBstEventBuffer()));
    }

    {
      // Make sure that DispatcherRetries and BstDispatcherRetries are distinct only after some parameter in BstDispatcherRetries is set.
      config = new DatabusSourcesConnection.Config();
      Assert.assertFalse(config.hasBstDispatcherRetries());
      sConfig = config.build();
      Assert.assertEquals(sConfig.getDispatcherRetries().getMaxRetryNum(), sConfig.getBstDispatcherRetries().getMaxRetryNum());
      int retryNum = sConfig.getDispatcherRetries().getMaxRetryNum();
      config.getDispatcherRetries().setMaxRetryNum(retryNum+1);
      sConfig = config.build();
      Assert.assertEquals(retryNum+1, sConfig.getDispatcherRetries().getMaxRetryNum());
      Assert.assertEquals(retryNum+1, sConfig.getBstDispatcherRetries().getMaxRetryNum());
      Assert.assertFalse(config.hasBstDispatcherRetries());
      config.getBstDispatcherRetries().setMaxRetryNum(retryNum+2);
      Assert.assertTrue(config.hasBstDispatcherRetries());
      sConfig = config.build();
      Assert.assertEquals(retryNum+1, sConfig.getDispatcherRetries().getMaxRetryNum());
      Assert.assertEquals(retryNum+2, sConfig.getBstDispatcherRetries().getMaxRetryNum());
      // We should return the same object no matter how many times get is called.
      Assert.assertEquals(System.identityHashCode(config.getBstDispatcherRetries()),
                          System.identityHashCode(config.getBstDispatcherRetries()));
    }

    {
      // Make sure that PullerRetries and BstPullerRetries are distinct only after some parameter in BstPullerRetries is set.
      config = new DatabusSourcesConnection.Config();
      Assert.assertFalse(config.hasBstPullerRetries());
      sConfig = config.build();
      Assert.assertEquals(sConfig.getPullerRetries().getMaxRetryNum(), sConfig.getBstPullerRetries().getMaxRetryNum());
      int retryNum = sConfig.getPullerRetries().getMaxRetryNum();
      config.getPullerRetries().setMaxRetryNum(retryNum+1);
      sConfig = config.build();
      Assert.assertEquals(retryNum+1, sConfig.getPullerRetries().getMaxRetryNum());
      Assert.assertEquals(retryNum+1, sConfig.getBstPullerRetries().getMaxRetryNum());
      Assert.assertFalse(config.hasBstPullerRetries());
      config.getBstPullerRetries().setMaxRetryNum(retryNum+2);
      Assert.assertTrue(config.hasBstPullerRetries());
      sConfig = config.build();
      Assert.assertEquals(retryNum+1, sConfig.getPullerRetries().getMaxRetryNum());
      Assert.assertEquals(retryNum+2, sConfig.getBstPullerRetries().getMaxRetryNum());
      // We should return the same object no matter how many times get is called.
      Assert.assertEquals(System.identityHashCode(config.getBstPullerRetries()),
                          System.identityHashCode(config.getBstPullerRetries()));
    }

    {
      config = new DatabusSourcesConnection.Config();
      sConfig = config.build();
      Assert.assertEquals(sConfig.getNoEventsConnectionResetTimeSec(), 15*60, "default setting for NoEventsConnectionResetTime");
      config.setNoEventsConnectionResetTimeSec(3600);
      sConfig = config.build();
      Assert.assertEquals(sConfig.getNoEventsConnectionResetTimeSec(), 3600, "applied setting for NoEventsConnectionResetTime");
    }

    //validate the adjustment of maxEventSize based on checkpointThresholdPct
    config = new DatabusSourcesConnection.Config();
    config.setCheckpointThresholdPct(30.0);
    config.setFreeBufferThreshold(1);

    DbusEventBuffer.Config bufCfg = config.getEventBuffer();
    bufCfg.setAllocationPolicy(AllocationPolicy.HEAP_MEMORY.toString());
    bufCfg.setEnableScnIndex(false);
    bufCfg.setMaxSize(1024);

    //default maxEventSize
    sConfig = config.build();
    Assert.assertEquals(sConfig.getEventBuffer().getMaxEventSize(), (int)(bufCfg.maxMaxEventSize() * 0.7));
    Assert.assertEquals(sConfig.getBstEventBuffer().getMaxEventSize(), (int)(bufCfg.maxMaxEventSize() * 0.7));

    //default maxEventSize with bootstrap override
    DbusEventBuffer.Config bstBufCfg = config.getBstEventBuffer();
    bstBufCfg.setAllocationPolicy(AllocationPolicy.HEAP_MEMORY.toString());
    bstBufCfg.setEnableScnIndex(false);
    bstBufCfg.setMaxSize(2048);

    sConfig = config.build();
    Assert.assertEquals(sConfig.getEventBuffer().getMaxEventSize(), (int)(bufCfg.maxMaxEventSize() * 0.7));
    Assert.assertEquals(sConfig.getBstEventBuffer().getMaxEventSize(), (int)(bstBufCfg.maxMaxEventSize() * 0.7));

    //too big max event size
    bufCfg.setMaxEventSize(10240);
    sConfig = config.build();
    Assert.assertEquals(sConfig.getEventBuffer().getMaxEventSize(), (int)(bufCfg.maxMaxEventSize() * 0.7));
    Assert.assertEquals(sConfig.getBstEventBuffer().getMaxEventSize(), (int)(bstBufCfg.maxMaxEventSize() * 0.7));
  }

  @Test
  public void testLoggerNameV2_1()
  throws InvocationTargetException,NoSuchMethodException,IllegalAccessException
  {
  	String source = "com.linkedin.events.dbName.tableName";
  	List<DatabusSubscription> ds = DatabusSubscription.createSubscriptionList(Arrays.asList(source));
  	DatabusSourcesConnection dsc = DatabusSourcesConnection.createDatabusSourcesConnectionForTesting();
    String partName = DatabusSubscription.getPrettyNameForListOfSubscriptions(ds);
    Assert.assertEquals(partName, "dbName_tableName");

    Class<?>[] arg1 = new Class<?>[] {List.class, String.class};
    Method constructPrettyNameForLogging = DatabusSourcesConnection.class.getDeclaredMethod("constructPrettyNameForLogging", arg1);
    constructPrettyNameForLogging.setAccessible(true);
    Object prettyName = constructPrettyNameForLogging.invoke(dsc, ds, "test_1234");
    Assert.assertEquals(prettyName, "dbName_tableName_test_1234");
  }

  @Test
  public void testLoggerNameV2_2()
  throws InvocationTargetException,NoSuchMethodException,IllegalAccessException
  {
    // They are of same db, but may have different
    String source1 = "com.linkedin.events.db.dbPrefix1.tableName1";
    String source2 = "com.linkedin.events.db.dbPrefix2.tableName2";
    List<String> ls = new ArrayList<String>();
    ls.add(source1);
    ls.add(source2);
    List<DatabusSubscription> ds = DatabusSubscription.createSubscriptionList(ls);
    DatabusSourcesConnection dsc = DatabusSourcesConnection.createDatabusSourcesConnectionForTesting();

    String partName = DatabusSubscription.getPrettyNameForListOfSubscriptions(ds);
    Assert.assertEquals(partName, "dbPrefix1_tableName1_dbPrefix2_tableName2");

    Class<?>[] arg1 = new Class<?>[] {List.class, String.class};
    Method constructPrettyNameForLogging = DatabusSourcesConnection.class.getDeclaredMethod("constructPrettyNameForLogging", arg1);
    constructPrettyNameForLogging.setAccessible(true);
    Object prettyName = constructPrettyNameForLogging.invoke(dsc, ds, "test_1234");
    Assert.assertEquals(prettyName, "dbPrefix1_tableName1_dbPrefix2_tableName2_test_1234");
  }
}
