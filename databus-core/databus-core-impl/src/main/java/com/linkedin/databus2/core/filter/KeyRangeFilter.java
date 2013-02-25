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
import com.linkedin.databus.core.util.Range;

public class KeyRangeFilter implements DbusFilter
{
  // Comparison on non-numbers is not meaningful
  private Range keyRange;
  
  public KeyRangeFilter(long minKey, long maxKey)
  {
    keyRange = new Range(minKey, maxKey);
  }
  
  public KeyRangeFilter()
  {
  }
  
  @Override
  public boolean allow(DbusEvent e)
  {
    if (e.isKeyNumber())
    {
      long eventKey = e.key();
      return keyRange.contains(eventKey);
    }
    else
    {
      // key is string, so we need to convert it to a number
      try {
        long eventKey = Long.parseLong(new String(e.keyBytes()));
        return keyRange.contains(eventKey);
      } 
      catch (NumberFormatException nfe) 
      {
        throw new RuntimeException(nfe);
      }
    }
  }

  public Range getKeyRange() {
	return keyRange;
  }

  public void setKeyRange(Range keyRange) {
	this.keyRange = keyRange;
  }

  @Override
  public String toString() 
  {
	return "KeyRangeFilter [keyRange=" + keyRange + "]";
  }  
  
	@Override
	public boolean equals(Object obj)
	{
		if ( ! ( obj instanceof KeyRangeFilter))
		{
			return false;
		}
		
		KeyRangeFilter r = (KeyRangeFilter)obj;
		
		
		if ((keyRange != null ) && (keyRange.equals(r.getKeyRange()))
				|| ( keyRange == r.getKeyRange()))
		{
			return true;
		}
		
		return false;
	}
	
	@Override
	public int hashCode()
	{
		return (null == keyRange ? 0 : keyRange.hashCode());
	}
  
}
