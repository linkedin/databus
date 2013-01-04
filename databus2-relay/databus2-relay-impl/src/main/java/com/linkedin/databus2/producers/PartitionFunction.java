/*
 * $Id: PartitionFunction.java 153291 2010-12-02 20:40:47Z jwesterm $
 */
package com.linkedin.databus2.producers;

import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.UnsupportedKeyException;

/**
 * Interface to return the partition for a given key.
 *
 * @author Jemiah Westerman<jwesterman@linkedin.com>
 * @version $Revision: 153291 $
 */
public interface PartitionFunction
{
  /**
   * Return the partition for the given key.
   * @param key
   * @return the partition for the given key
   * @throws UnsupportedKeyException if key is an unsupported key type
   */
  public short getPartition(DbusEventKey key)
  throws UnsupportedKeyException;
}
