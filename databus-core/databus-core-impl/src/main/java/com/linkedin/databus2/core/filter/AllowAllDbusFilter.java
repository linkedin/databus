package com.linkedin.databus2.core.filter;

import com.linkedin.databus.core.DbusEvent;

public class AllowAllDbusFilter implements DbusFilter
{
  public static final AllowAllDbusFilter THE_INSTANCE = new AllowAllDbusFilter();

  @Override
  public boolean allow(DbusEvent e)
  {
    return true;
  }

}
