package com.linkedin.databus.client.pub;

/**
 ** Similar to com.linkedin.dal.pub.LastWriteTimeTracker
 **/
public interface LastWriteTimeTracker
{
  public static final long LAST_WRITE_TIME_UNKNOWN = -1L;
  
  /**
   ** Return the last write time in milliseconds since 1/1/1970 UTC. I.e., the
   ** same type of value as returned by {@link System#currentTimeMillis()}.
   ** <p>
   ** If the last write time is unknown, returns {@link #LAST_WRITE_TIME_UNKNOWN}.
   ** 
   ** @return the last write time in milliseconds,
   ** or {@code LAST_WRITE_TIME_UNKNOWN} if unknown
   **/
  long getLastWriteTimeMillis();
  
/**
 ** Returns the same value as {@link #getLastWriteTimeMillis()}, except that in some
 ** implementations, {@link #getLastWriteTimeMillis()} is specific to the caller / session /
 ** context, whereas this method always returns the overall last write time regardless
 ** of context. 
 ** <p>
 ** If the overall last write time is unknown, returns {@link #LAST_WRITE_TIME_UNKNOWN}.
 ** 
 ** @return the last write time in milliseconds,
 ** or {@code LAST_WRITE_TIME_UNKNOWN} if unknown
 **/
  long getOverallLastWriteTimeMillis();
  
}

