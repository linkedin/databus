package com.linkedin.databus2.core.filter;

import java.util.Set;

import com.linkedin.databus.core.DbusEvent;

public class SourceDbusFilter implements DbusFilter
{

  private final Set<Integer> sources;

  public SourceDbusFilter(Set<Integer> sources)
  {
    this.sources = sources;
  }

  @Override
  public boolean allow(DbusEvent e)
  {
    if (sources.contains((int)e.srcId())) {
      return true;
    }
    return false;
  }

}
