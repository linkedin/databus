package com.linkedin.databus.client.pub;

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
