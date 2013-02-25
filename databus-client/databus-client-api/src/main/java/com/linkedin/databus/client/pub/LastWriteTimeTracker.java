package com.linkedin.databus.client.pub;
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

