/*
 * $Id: ConstantPartitionFunction.java 153291 2010-12-02 20:40:47Z jwesterm $
 */
package com.linkedin.databus2.producers;

import com.linkedin.databus.core.DbusEventKey;

/**
 * PartitionFunction implementation that returns a constant partition regardless of key.
 *
 * @author Jemiah Westerman<jwesterman@linkedin.com>
 * @version $Revision: 153291 $
 */
public class ConstantPartitionFunction
    implements PartitionFunction
{
  private final short _partition;
  public static final short DEFAULT_PARTITION = 1;

  public ConstantPartitionFunction()
  {
    this(DEFAULT_PARTITION);
  }

  public ConstantPartitionFunction(short partition)
  {
    _partition = partition;
  }

  /*
   * @see com.linkedin.databus2.monitors.PartitionFunction#getPartition(java.lang.Object)
   */
  @Override
  public short getPartition(DbusEventKey key)
  {
    return _partition;
  }

}
