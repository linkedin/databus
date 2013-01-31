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


import java.io.Serializable;

/**
 * An SCN represents a logical clock for one or more databus sources.
 * @author cbotev
 */
public interface SCN extends Serializable, Comparable<SCN>
{ 
  
  /**
   * Checks if this SCN is comparable with another SCN.
   * @param  otherSCN           the other SCN
   * @return true iff they are comparable
   */
  boolean isComparable(SCN otherSCN);
  
  /** 
   * Compares this SCN to another SCN. The two SCNs should be comparable (see 
   * {@link #isComparable(SCN)}).
   * @return < 0 if this is smaller; == 0 if they are equal; >0 if this is larger; result is
   *         undefined if the SCNs are not comparable .  
   */
  int compareTo(SCN otherSCN);
}
