package com.linkedin.databus.core;

import static org.testng.AssertJUnit.*;

import java.util.HashSet;

import org.apache.log4j.Logger;

import org.testng.annotations.Test;

import com.linkedin.databus.core.util.RngUtils;
import com.linkedin.databus2.core.filter.SourceDbusFilter;

public class TestSourceDbusFilter
{
  public static final String MODULE = TestSourceDbusFilter.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  @Test
  public void testAllow()
  {

    HashSet<Integer> sources = new HashSet<Integer>();
    sources.add(23);
    sources.add(45);
    DbusEvent e = RngUtils.randomEvent((short) 23);
    LOG.debug(e);
    SourceDbusFilter sdFilter = new SourceDbusFilter(sources);
    assertTrue(sdFilter.allow(e));
  }

}
