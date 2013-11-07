package com.linkedin.databus.core.util;

import java.lang.reflect.Field;

import junit.framework.Assert;

import org.testng.annotations.Test;

import com.linkedin.databus.core.DbusConstants;
import com.linkedin.databus.core.util.RateMonitor.MockRateMonitor;

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

public class TestRateControl
{
  @Test
  public void testInvalidRate() throws Exception
  {
    RateControl rc = new RateControl(Long.MIN_VALUE, 2L);
    Assert.assertEquals(rc.isEnabled(), false);

    RateControl rc1 = new RateControl(0, 2L);
    Assert.assertEquals(rc1.isEnabled(), false);

    RateControl rc2 = new RateControl(1, 2L);
    Assert.assertEquals(rc2.isEnabled(), true);

    RateControl rc3 = new RateControl(Long.MAX_VALUE, 2L);
    Assert.assertEquals(rc3.isEnabled(), true);

    RateControl rc4 = new RateControl(2L, Long.MIN_VALUE);
    Assert.assertEquals(rc4.isEnabled(), false);

    RateControl rc5 = new RateControl(2L, 0);
    Assert.assertEquals(rc5.isEnabled(), false);

    RateControl rc6 = new RateControl(2L, 1L);
    Assert.assertEquals(rc6.isEnabled(), true);

    RateControl rc7 = new RateControl(2L, Long.MAX_VALUE);
    Assert.assertEquals(rc7.isEnabled(), true);

  }

  @Test
  public void testInvalidRateMethod() throws Exception
  {
    RateControl rc = new RateControl(Long.MIN_VALUE, Long.MIN_VALUE);
    Assert.assertEquals(Long.MIN_VALUE, rc.incrementEventCount());
  }

  // Test invariant that after incrementEventCount, checkrateExceeded always returns false
  // i.e., event is accepted, and if required throttle is initiated. Upon exiting the method, 
  // we are ready to accept another event
  @Test
  public void testCheckIfRateExceeded() throws Exception
  {
    // 2 events/sec, with duration for 1 sec
    RateControl rc = new RateControl(4,5);
    Assert.assertEquals(true, rc.isEnabled());

    long curTime = 0L;
    MockRateMonitor mrm = new MockRateMonitor("mock");
    mrm.setNanoTime(curTime);
    mrm.start();

    Field field = rc.getClass().getDeclaredField("_ra");
    field.setAccessible(true);
    field.set(rc, mrm);

    // Current time = 10ns. No event is in so far.
    curTime = 10L;
    mrm.setNanoTime(curTime);
    Assert.assertEquals(false, rc.checkExpired());
    Assert.assertEquals(false, rc.checkRateExceeded());

    // Event #1. Accept the event, but sleep until 0.25secs
    long tc = rc.incrementEventCount();
    Assert.assertEquals(tc, 1);
    Assert.assertEquals(false, rc.checkExpired());
    Assert.assertEquals(false, rc.checkRateExceeded());

    // Set Current time as ( 0.5s - 10ns ). Rate has been met
    curTime = (DbusConstants.NUM_NSECS_IN_SEC/2) - 10L;
    mrm.setNanoTime(curTime);
    Assert.assertEquals(false, rc.checkExpired());
    Assert.assertEquals(false, rc.checkRateExceeded());
    tc = rc.incrementEventCount();
    Assert.assertEquals(tc, 2);
    
    // Set Current time as ( 0.5s ). Rate has been met
    curTime = (DbusConstants.NUM_NSECS_IN_SEC/2);
    mrm.setNanoTime(curTime);
    Assert.assertEquals(false, rc.checkExpired());
    Assert.assertEquals(false, rc.checkRateExceeded());

    // Rate has fallen below accepted threshold
    curTime = (DbusConstants.NUM_NSECS_IN_SEC/2) + 10L;
    mrm.setNanoTime(curTime);
    Assert.assertEquals(false, rc.checkExpired());
    Assert.assertEquals(false, rc.checkRateExceeded());

    // Event #3
    tc = rc.incrementEventCount();
    Assert.assertEquals(tc, 3);
    Assert.assertEquals(false, rc.checkExpired());
    Assert.assertEquals(false, rc.checkRateExceeded());

  }

  @Test
  public void testIncrementEventCount() throws Exception
  {
    RateControl rc = new RateControl(2,1);
    MockRateMonitor mrm = new MockRateMonitor("mock");
    mrm.setNanoTime(0L);
    mrm.start();

    Field field = rc.getClass().getDeclaredField("_ra");
    field.setAccessible(true);
    field.set(rc, mrm);

    // #events=1, #time=2ns. Event accepted, rate exceeded
    long expWakeupTime = DbusConstants.NUM_NSECS_IN_SEC /2;
    long tc = rc.incrementEventCount();
    Assert.assertEquals(tc, 1L);
    Assert.assertEquals(rc.getNumSleeps(),1);
    Assert.assertEquals(mrm.getNanoTime(),expWakeupTime);

    // #events=1, #time=0.5s + 1. Event accepted, rate not exceeded
    long curTime = expWakeupTime + 1;
    mrm.setNanoTime(curTime);
    tc = rc.incrementEventCount();
    Assert.assertEquals(tc, 2L);
    Assert.assertEquals(rc.getNumSleeps(),2);
    
    // it is DbusConstants.NUM_NSECS_IN_SEC+1, instead of DbusConstants.NUM_NSECS_IN_SEC because of approximating sleeps to a ms
    Assert.assertEquals(DbusConstants.NUM_NSECS_IN_SEC+1, mrm.getNanoTime());
  }

  @Test
  public void testSleepToMaintainRate() throws Exception
  {
    RateControl rc = new RateControl(2,1);

    long duration = 10000;
    long msecToSleep = rc.sleepToMaintainRate(duration);
    Assert.assertEquals(msecToSleep, 1L);

    duration = 100000;
    msecToSleep = rc.sleepToMaintainRate(duration);
    Assert.assertEquals(msecToSleep, 1L);

    duration = 1000000;
    msecToSleep = rc.sleepToMaintainRate(duration);
    Assert.assertEquals(msecToSleep, 1L);

    duration = 100000000;
    msecToSleep = rc.sleepToMaintainRate(duration);
    Assert.assertEquals(msecToSleep, 100L);

    duration = 1000000000;
    msecToSleep = rc.sleepToMaintainRate(duration);
    Assert.assertEquals(msecToSleep, 1000L);

  }

  @Test
  public void testCheckExpired() throws Exception
  {
    RateControl rc1 = new RateControl(10,-1);
    Assert.assertEquals(false, rc1.checkExpired());
    Assert.assertEquals(false, rc1.isEnabled());

    RateControl rc2 = new RateControl(10,1);
    Assert.assertEquals(true, rc2.isEnabled());
    boolean expired = true;
    Field field = rc2.getClass().getDeclaredField("_expired");
    field.setAccessible(true);
    field.set(rc2, expired);
    Assert.assertEquals(expired, rc2.checkExpired());

    RateControl rc = new RateControl(10,1);
    Assert.assertEquals(true, rc.isEnabled());
    long curTime = 0L;
    MockRateMonitor mrm = new MockRateMonitor("mock");
    mrm.setNanoTime(curTime);
    mrm.start();
    Field field2 = rc.getClass().getDeclaredField("_ra");
    field2.setAccessible(true);
    field2.set(rc, mrm);
      
    curTime = (1L * DbusConstants.NUM_NSECS_IN_SEC) - 10;
    mrm.setNanoTime(curTime);
    Assert.assertEquals(rc.checkExpired(), false);

    curTime = (1L * DbusConstants.NUM_NSECS_IN_SEC);
    mrm.setNanoTime(curTime);
    Assert.assertEquals(rc.checkExpired(), true);

    curTime = (1L * DbusConstants.NUM_NSECS_IN_SEC) + 10;
    mrm.setNanoTime(curTime);
    Assert.assertEquals(rc.checkExpired(), true);
  }

  @Test
  public void testRateExceeded() throws Exception
  {
    RateControl rc = new RateControl(10,1);
    Assert.assertEquals(true, rc.isEnabled());
    long curTime = 0L;
    MockRateMonitor mrm = new MockRateMonitor("mock");
    mrm.setNanoTime(curTime);
    mrm.start();

    Field field = rc.getClass().getDeclaredField("_ra");
    field.setAccessible(true);
    field.set(rc, mrm);
      
    curTime = (1L * DbusConstants.NUM_NSECS_IN_SEC) - 10 ;
    mrm.ticks(10L);
    mrm.setNanoTime(curTime);
    Assert.assertEquals(true, rc.checkRateExceeded());

    curTime = (1L * DbusConstants.NUM_NSECS_IN_SEC);
    mrm.setNanoTime(curTime);
    Assert.assertEquals(false, rc.checkRateExceeded());

    curTime = (1L * DbusConstants.NUM_NSECS_IN_SEC);
    mrm.setNanoTime(curTime);
    Assert.assertEquals(false, rc.checkRateExceeded());

    return;
  }

  @Test
  public void testComputeSleepDuration() throws Exception
  {
    RateControl rc = new RateControl(10,1);
    Assert.assertEquals(true, rc.isEnabled());
    long curTime = 0L;
    MockRateMonitor mrm = new MockRateMonitor("mock");
    mrm.setNanoTime(curTime);
    mrm.start();

    Field field = rc.getClass().getDeclaredField("_ra");
    field.setAccessible(true);
    field.set(rc, mrm);
    
    curTime = (1L * DbusConstants.NUM_NSECS_IN_SEC) - 10 ;
    mrm.ticks(10L);
    mrm.setNanoTime(curTime);
    Assert.assertEquals(10, rc.computeSleepDuration());

    curTime = (1L * DbusConstants.NUM_NSECS_IN_SEC) ;
    mrm.setNanoTime(curTime);
    Assert.assertEquals(0, rc.computeSleepDuration());

    curTime = (1L * DbusConstants.NUM_NSECS_IN_SEC) + 10 ;
    mrm.setNanoTime(curTime);
    Assert.assertEquals(0, rc.computeSleepDuration());

    return;
  }

}
