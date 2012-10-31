package com.linkedin.databus2.core.filter;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.data_model.LogicalSourceId;

/**
 * Implements a disjunction junction of logical source partition filters. Filters are grouped by
 * the logical source (id). All logical partitions for that source are represented as a set. The
 * implementation can handle partition wildcards but not source wildcards.
 * */
public class LogicalSourceAndPartitionDbusFilter implements DbusFilter
{
  /** A map from source id to a filter for the partitions of this source */
  private final HashMap<Integer, LogicalPartitionDbusFilter> _sources;

  public LogicalSourceAndPartitionDbusFilter()
  {
    _sources = new HashMap<Integer, LogicalPartitionDbusFilter>();
  }

  public LogicalSourceAndPartitionDbusFilter(LogicalSourceId sourceCond)
  {
    this();
    addSourceCondition(sourceCond);
  }

  public LogicalSourceAndPartitionDbusFilter(Set<LogicalSourceId> sourceConds)
  {
    this();
    for (LogicalSourceId sourceCond: sourceConds) addSourceCondition(sourceCond);
  }

  public void addSourceCondition(LogicalSourceId sourceCond)
  {
    //Logical source wildcards are not supported
    if (sourceCond.getSource().isWildcard()) return;
    Integer srcId = sourceCond.getSource().getId();
    LogicalPartitionDbusFilter sourceFilter = _sources.get(srcId);
    if (null == sourceFilter)
    {
      sourceFilter = new LogicalPartitionDbusFilter(sourceCond);
      _sources.put(srcId, sourceFilter);
    }
    else
    {
      sourceFilter.addPartitionCondition(sourceCond);
    }
  }

  @Override
  public boolean allow(DbusEvent e)
  {
    if (e.isControlMessage()) return true;
    Integer srcId = (int)e.srcId();
    LogicalPartitionDbusFilter srcFilter = _sources.get(srcId);
    if (null == srcFilter) return false;

    return srcFilter.allow(e);
  }

  public LogicalPartitionDbusFilter getSourceFilter(Integer sourceId)
  {
    return _sources.get(sourceId);
  }

  /**
   * Matches events which belong to one or more partitions for a given source. The filter
   * only checks the partitions and assumes the source id filtering has already been done. */
  public static class  LogicalPartitionDbusFilter implements DbusFilter
  {
    private boolean _isAllPartitionsWildcard = false;
    private HashSet<Integer> _ids = new HashSet<Integer>(8);

    public LogicalPartitionDbusFilter(LogicalSourceId initCond)
    {
      addPartitionCondition(initCond);
    }


    public void addPartitionCondition(LogicalSourceId partitionCond)
    {
      if (partitionCond.isAllPartitionsWildcard())
      {
        _isAllPartitionsWildcard = true;
      }
      else if (! _isAllPartitionsWildcard)
      {
        _ids.add(partitionCond.getId().intValue());
      }
    }

    @Override
    public boolean allow(DbusEvent e)
    {
      return _isAllPartitionsWildcard || _ids.contains(Integer.valueOf(e.logicalPartitionId()));
    }

    public boolean isAllPartitionsWildcard()
    {
      return _isAllPartitionsWildcard;
    }

    public Set<Integer> getPartitionsMask()
    {
      return Collections.unmodifiableSet(_ids);
    }

  }

}
