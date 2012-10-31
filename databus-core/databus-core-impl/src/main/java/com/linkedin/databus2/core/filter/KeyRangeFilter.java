package com.linkedin.databus2.core.filter;

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
