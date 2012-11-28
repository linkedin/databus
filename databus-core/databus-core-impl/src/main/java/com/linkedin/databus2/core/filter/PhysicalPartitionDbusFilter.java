package com.linkedin.databus2.core.filter;

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
