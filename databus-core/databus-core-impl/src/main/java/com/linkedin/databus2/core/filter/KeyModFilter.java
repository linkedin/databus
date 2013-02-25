/**
 * 
 */
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

/**
 * @author bvaradar
 *
 * Implements Mod based partitioning on DbusKey. 
 * This implementation allows clients to specify ranges of buckets that they are interested in.
 */
public class KeyModFilter implements DbusFilter 
{

	private long  numBuckets;
	private Range bktRange;
	
	public KeyModFilter(long minBucket, long maxBucket, long numBuckets)
	{
		this.numBuckets = numBuckets;
		this.bktRange = new Range(minBucket,maxBucket);
		
	}
	
	public KeyModFilter()
	{
	}
	
	/* (non-Javadoc)
	 * @see com.linkedin.databus.core.DbusFilter#allow(com.linkedin.databus.core.DbusEvent)
	 */
	@Override
	public boolean allow(DbusEvent e) 
	{
		long key = -1;
	    if (e.isKeyNumber())
	    {
	    	key = e.key();
	    } else {
	      // key is string, so we need to convert it to a number
	      String str = new String(e.keyBytes());
	      try 
	      {
	    	  key = Long.parseLong(str);
	      } 
	      catch (NumberFormatException nfe) 
	      {
	    	 /**
	    	  * Needed for load balancing client. 
	    	  * For Mod partitioning, we use hash-code of string type keys containing non-numeric values to determine bktId.
	    	  */
	    	 key = str.hashCode();
	      }
	    }
	    
	    long bktId = Math.abs(key)%numBuckets;
	    return bktRange.contains(bktId);	    
	}

	public long getNumBuckets() {
		return numBuckets;
	}

	public void setNumBuckets(long numBuckets) {
		this.numBuckets = numBuckets;
	}

	public Range getBktRange() {
		return bktRange;
	}

	public void setBktRange(Range bktRange) {
		this.bktRange = bktRange;
	}
	
	@Override
	public boolean equals(Object obj)
	{
		if ( ! ( obj instanceof KeyModFilter))
		{
			return false;
		}
		
		KeyModFilter modF = (KeyModFilter)obj;
		
		
		if ( ((bktRange != null ) && (bktRange.equals(modF.getBktRange()))
				|| ( bktRange == modF.getBktRange())
				&& numBuckets == modF.getNumBuckets()))
		{
			return true;
		}
		
		return false;
	}
	
	@Override
	public int hashCode()
	{
		return (null == bktRange ? 0 : bktRange.hashCode());
	}
}
