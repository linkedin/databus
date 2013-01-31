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
