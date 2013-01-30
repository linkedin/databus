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


import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.data_model.PhysicalPartition;

/**
 * A filter that applies a nested filter to a given physical partition. If the nested filter is null,
 * the effect is to filter out events that are not in the said partition.
 */
public class PhysicalPartitionDbusFilter implements DbusFilter
{
  private final DbusFilter _nestedFilter;
  private final PhysicalPartition _ppart;

  /**
   * Constructor
   * @param  ppart         the physical partition to filter
   * @param  nestedFilter  the additional filter to apply to events from the above partition (can
   *                       be null)
   */
  public PhysicalPartitionDbusFilter(PhysicalPartition ppart, DbusFilter nestedFilter)
  {
    _ppart = ppart;
    _nestedFilter = nestedFilter;
  }

  @Override
  public boolean allow(DbusEvent e)
  {
    boolean success = _ppart.isAnyPartitionWildcard() ||
                      (e.physicalPartitionId() == _ppart.getId().intValue());
    success = success && ((null == _nestedFilter) || _nestedFilter.allow(e));
    return success;
  }

  public DbusFilter getNestedFilter()
  {
    return _nestedFilter;
  }

  public PhysicalPartition getPhysicalPartition()
  {
    return _ppart;
  }
}
