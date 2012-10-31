/**
 * $Id: MaxSCNWriter.java 33147 2007-11-18 22:29:12Z dmccutch $ */
package com.linkedin.databus2.core.seq;

import com.linkedin.databus2.core.DatabusException;

/**
 * Interface to write the maxSCN
 *
 * @author James Richards
 * @version $Revision: 33147 $
 */
public interface MaxSCNWriter
{
  /**
   * Saves max scn
   *
   * @param endOfPeriod the scn
   * @throws DatabusException if an error occurs
   */
  void saveMaxScn(long endOfPeriod) throws DatabusException;
}