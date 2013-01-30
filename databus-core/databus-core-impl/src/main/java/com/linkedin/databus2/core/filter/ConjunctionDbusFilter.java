package com.linkedin.databus2.core.filter;
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
