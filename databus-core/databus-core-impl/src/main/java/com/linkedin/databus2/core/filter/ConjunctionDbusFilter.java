package com.linkedin.databus2.core.filter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import com.linkedin.databus.core.DbusEvent;

public class ConjunctionDbusFilter implements DbusFilter
{

  private ArrayList<DbusFilter> _filterList = new ArrayList<DbusFilter>();

  public boolean addFilter(DbusFilter filter)
  {
    return _filterList.add(filter);
  }

  @Override
  public boolean allow(DbusEvent e)
  {
    for (DbusFilter f: _filterList)
    {
      if (!f.allow(e))
      {
        return false;
      }
    }
    return true;
  }

  public Collection<DbusFilter> getFilterList()
  {
    return Collections.unmodifiableList(_filterList);
  }

}
