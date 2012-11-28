package com.linkedin.databus.core.util;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import org.testng.annotations.Test;


public class TestRateMonitor
{

  @Test
  public void testGetRate() 
  {
    
    /*
     * Use Case:
     * RateMonitor.start();
     * ....ticks goes here
     * RateMonitor.getRate();
     */
    RateMonitor monitor = new RateMonitor("test");
    long startTime = System.nanoTime();
    monitor.start();
    for (int i = 0; i < 1000; ++i)
    {
      monitor.tick();
      trySleep(1);
    }
    
    long endTime = System.nanoTime();
    double monitorRate = monitor.getRate();
    double calculatedRate = 1000 * 1000000000.0 / (endTime - startTime);
    String rateStr = monitor.toString();
    // Difference should not be more than 5 qps & State = started
    assertTrue("CalculatedRate = " + calculatedRate + " : MonitorRate = " + monitorRate,  Math.abs(calculatedRate - monitorRate) < 5);
    assertTrue("Monitor = " + rateStr,rateStr.contains("state = STARTED"));
    
    /*
     * Use Case 
     * RateMonitor.start();
     * ... ticks goes here
     * RateMonitor.suspend();
     * .. other stmts
     * RateMonitor.getRate();
     * RateMonitor.resume();
     * ... ticks goes here
     * RateMonitor.getRate();
     * ... ticks goes here
     * RateMonitor.stop();
     * RateMonitor.getRate();
     */
    monitor = new RateMonitor("test");
    startTime = System.nanoTime();
    monitor.start();
   
    for (int i = 0; i < 1000; ++i)
    {
      monitor.tick();
      trySleep(1);
    }
    
    endTime = System.nanoTime();
    monitor.suspend();
    
    trySleep(100); //Sleep vor 100 milliSec before getting Rate
    
    monitorRate = monitor.getRate();
    calculatedRate = 1000 * 1000000000.0 / (endTime - startTime);
    rateStr = monitor.toString();
    // Difference should not be more than 5 qps & State = started
    assertTrue("CalculatedRate = " + calculatedRate + " : MonitorRate = " + monitorRate,  Math.abs(calculatedRate - monitorRate) < 5);
    assertTrue("Monitor = " + rateStr,rateStr.contains("state = SUSPENDED"));
    
    long startTime2 = System.nanoTime();
    monitor.resume();
    for (int i = 0; i < 200; ++i)
    {
      monitor.tick();
      trySleep(1);
    }
    long endTime2 = System.nanoTime(); 
    monitorRate = monitor.getRate();
    calculatedRate = 1200 * 1000000000.0 / ((endTime - startTime) + (endTime2 - startTime2));
    rateStr = monitor.toString();

    // Difference should not be more than 5 qps & State = started
    assertTrue("CalculatedRate = " + calculatedRate + " : MonitorRate = " + monitorRate,  Math.abs(calculatedRate - monitorRate) < 5);
    assertTrue("Monitor = " + rateStr,rateStr.contains("state = RESUMED"));
    
    //Test For Stop State
    endTime2 = System.nanoTime(); 
    monitor.stop();
    calculatedRate = 1200 * 1000000000.0 / ((endTime - startTime) + (endTime2 - startTime2));
    rateStr = monitor.toString();
    
    // Difference should not be more than 5 qps & State = started
    assertTrue("CalculatedRate = " + calculatedRate + " : MonitorRate = " + monitorRate,  Math.abs(calculatedRate - monitorRate) < 5);
    assertTrue("Monitor = " + rateStr,rateStr.contains("state = STOPPED"));
    
    //Sleep for 500 msec and getRate. Should be same as before
    trySleep(500);
    String rateStr2 = monitor.toString(); //toString() calls getRate() internally
    assertEquals("STOP State :" + rateStr2, rateStr, rateStr2);
    System.out.println("Done !!");
  }
  
  public void trySleep(long millis)
  {
    try
    {
      Thread.sleep(millis);
    }
    catch (InterruptedException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
