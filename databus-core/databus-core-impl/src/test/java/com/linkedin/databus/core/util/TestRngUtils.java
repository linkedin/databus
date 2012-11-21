package com.linkedin.databus.core.util;

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
