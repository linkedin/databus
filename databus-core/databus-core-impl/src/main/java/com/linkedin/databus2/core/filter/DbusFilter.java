package com.linkedin.databus2.core.filter;

import com.linkedin.databus.core.DbusEvent;

public interface DbusFilter
{
  public boolean allow(DbusEvent e);
  
}
