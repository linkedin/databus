package com.linkedin.databus2.core;

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
