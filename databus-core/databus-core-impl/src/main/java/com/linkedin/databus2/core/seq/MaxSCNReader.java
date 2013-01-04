/**
 * $Id: MaxSCNReader.java 33147 2007-11-18 22:29:12Z dmccutch $ */
package com.linkedin.databus2.core.seq;

import com.linkedin.databus2.core.DatabusException;

/**
 * Interface to read the maxSCN
 *
 * @author ypujante
 */
public interface MaxSCNReader
{
  /**
   * @return the max scn
   * @throws DatabusException if an error occurs
   */
  public long getMaxScn() throws DatabusException;
}
