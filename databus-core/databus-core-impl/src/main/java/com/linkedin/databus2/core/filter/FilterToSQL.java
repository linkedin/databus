package com.linkedin.databus2.core.filter;

import org.apache.log4j.Logger;

import com.linkedin.databus.core.util.Range;


public class FilterToSQL
{
  public static final String MODULE = FilterToSQL.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);
  public static final String KEYCOLUMN = "srckey";
  private static final String EMPTY_STRING = "";
  
  public static String convertToSQL(DbusFilter filter)
  {
    if(filter instanceof KeyModFilter)
    {
      return convertKeyModFilter((KeyModFilter) filter);
    }
    else if(filter instanceof KeyRangeFilter)
    {
       return convertKeyRangeFilter((KeyRangeFilter) filter);
    }
    else
    {
      LOG.error("Unimplemented FilterToSQL for this filter, cannot convert to SQL!");
      return EMPTY_STRING;
    }
  }

  private static String convertKeyRangeFilter(KeyRangeFilter filter)
  {
    Range range = filter.getKeyRange();
    StringBuffer sql = new StringBuffer();
    // MySQL SIGNED INT is always 64 bits (Confirmed with antony), this should support java long keys.
    sql.append(KEYCOLUMN+ " >= ");  
    sql.append(range.getStart());
    sql.append(" AND ");
    sql.append(KEYCOLUMN+ " < ");
    sql.append(range.getEnd());
    return sql.toString();
  }

  private static String convertKeyModFilter(KeyModFilter filter)
  {
    long buckets = filter.getNumBuckets();
    Range range = filter.getBktRange();
    StringBuffer sql = new StringBuffer();
    // MySQL SIGNED INT is always 64 bits (Confirmed with antony), this should support java long keys.
    sql.append(KEYCOLUMN + "%" +buckets+" >= ");  
    sql.append(range.getStart());
    sql.append(" AND ");
    sql.append(KEYCOLUMN + "%" +buckets+ " < ");
    sql.append(range.getEnd());
    return sql.toString();
  } 
}
