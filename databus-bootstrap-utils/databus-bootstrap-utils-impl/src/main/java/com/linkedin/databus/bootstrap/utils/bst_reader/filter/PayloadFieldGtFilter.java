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
package com.linkedin.databus.bootstrap.utils.bst_reader.filter;

import org.apache.avro.generic.GenericRecord;

import com.linkedin.databus.core.DbusEvent;

/**
 * Implements a ">" constant filter for a payload field
 */
public class PayloadFieldGtFilter implements BootstrapReaderFilter
{
  final String _fieldName;
  final Object _constValue;

  public PayloadFieldGtFilter(String fieldName, Object constValue)
  {
    _fieldName = fieldName;
    _constValue = constValue;
  }

  /**
   * @see com.linkedin.databus.bootstrap.utils.bst_reader.filter.BootstrapReaderFilter#matches(com.linkedin.databus.core.DbusEvent, org.apache.avro.generic.GenericRecord)
   */
  @Override
  public boolean matches(DbusEvent event, GenericRecord payload)
  {
    Object val = payload.get(_fieldName);
    if (null == val) return false;
    if (val instanceof Float)
    {
      return ((Float)val).floatValue() > ((Number)_constValue).floatValue();
    }
    else if (val instanceof Double)
    {
      return ((Double)val).doubleValue() > ((Number)_constValue).doubleValue();
    }
    else if (val instanceof Number)
    {
      return ((Number)val).longValue() > ((Number)_constValue).longValue();
    }
    else
    {
      return val.toString().compareTo(_constValue.toString()) > 0;
    }
  }

}
