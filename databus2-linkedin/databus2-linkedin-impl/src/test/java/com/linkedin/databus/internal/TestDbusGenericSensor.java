package com.linkedin.databus.internal;

import static org.testng.AssertJUnit.assertTrue;

import org.testng.annotations.Test;

import com.linkedin.databus.internal.monitoring.mbean.DbusGenericSensor;
import com.linkedin.healthcheck.pub.AttributeDataType;
import com.linkedin.healthcheck.pub.AttributeMetricType;
import com.linkedin.healthcheck.pub.SensorAttribute;


public class TestDbusGenericSensor
{
  public static class TestMetric {

    /**
     * @param numMetric
     * @param numMetric2
     * @param count1
     * @param count2
     * @param timeMetric1
     */
    public TestMetric(int numMetric,
                      long numMetric2,
                      int count1,
                      long count2,
                      long timeMetric1)
    {
      _numMetric = numMetric;
      _numMetric2 = numMetric2;
      _count1 = count1;
      _count2 = count2;
      _timeMetric1 = timeMetric1;
    }

    private int _numMetric;
    private long _numMetric2;
    private int _count1;
    private long _count2;

    private long _timeMetric1;

    public int getNumMetric()
    {
      return _numMetric;
    }

    public long getNumMetric2()
    {
      return _numMetric2;
    }

    public int getCount1()
    {
      return _count1;
    }

    public long getCount2()
    {
      return _count2;
    }

    public long getTimeMetric1()
    {
      return _timeMetric1;
    }

    public void setNumMetric(int numMetric)
    {
      _numMetric = numMetric;
    }

    public void setNumMetric2(long numMetric2)
    {
      _numMetric2 = numMetric2;
    }

    public void setCount1(int count1)
    {
      _count1 = count1;
    }

    public void setCount2(long count2)
    {
      _count2 = count2;
    }

    public void setTimeMetric1(long timeMetric1)
    {
      _timeMetric1 = timeMetric1;
    }

  }

  @Test
  public void testGenericSensor()
  {
    int numMetric = 250;
    int count1 = 5;
    long numMetric2 = 90230;
    long count2 = 1920109L;
    long timeMetric1 = 1920392094L;

    TestMetric tm = new TestMetric(numMetric,numMetric2,count1,count2,timeMetric1);
    DbusGenericSensor<TestMetric> sensor = new DbusGenericSensor<TestMetric>("testSensor", "testType", tm);

    SensorAttribute sens = sensor.getAttribute("NumMetric1");
    assertTrue(sens==null);

    SensorAttribute[] sensorList = sensor.getAttributes();
    assertTrue(sensorList.length==5);

    SensorAttribute sensorNumMetric = sensor.getAttribute("NumMetric");
    assertTrue(sensorNumMetric != null);
    assertTrue(sensorNumMetric.getMetricType()==AttributeMetricType.COUNTER);
    assertTrue(sensorNumMetric.getType()==AttributeDataType.INTEGER);
    assertTrue((Integer) sensorNumMetric.getValue()==numMetric);


    SensorAttribute sensorNumMetric2 = sensor.getAttribute("NumMetric2");
    assertTrue(sensorNumMetric2 != null);
    assertTrue(sensorNumMetric2.getMetricType()==AttributeMetricType.COUNTER);
    assertTrue(sensorNumMetric2.getType()==AttributeDataType.LONG);
    assertTrue((Long) sensorNumMetric2.getValue()==numMetric2);


    SensorAttribute sensorNumMetric3 = sensor.getAttribute("TimeMetric1");
    assertTrue(sensorNumMetric3 != null);
    assertTrue(sensorNumMetric3.getMetricType()==AttributeMetricType.GAUGE);
    assertTrue(sensorNumMetric3.getType()==AttributeDataType.LONG);
    assertTrue((Long) sensorNumMetric3.getValue()==timeMetric1);

    SensorAttribute sensorNumMetric4 = sensor.getAttribute("Count1");
    assertTrue(sensorNumMetric4 != null);
    assertTrue(sensorNumMetric4.getMetricType()==AttributeMetricType.COUNTER);
    assertTrue(sensorNumMetric4.getType()==AttributeDataType.INTEGER);
    assertTrue((Integer) sensorNumMetric4.getValue()==count1);

    SensorAttribute sensorNumMetric5 = sensor.getAttribute("Count2");
    assertTrue(sensorNumMetric5 != null);
    assertTrue(sensorNumMetric5.getMetricType()==AttributeMetricType.COUNTER);
    assertTrue(sensorNumMetric5.getType()==AttributeDataType.LONG);
    assertTrue((Long) sensorNumMetric5.getValue()==count2);


  }



}
