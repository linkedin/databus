package com.linkedin.databus3.espresso.client.consumer;

import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus2.core.DatabusException;

/** Denotes an exception while processing events from a given physical partition */
public class PhysicalPartitionConsumptionException extends DatabusException
{
  private static final long serialVersionUID = 1L;
  final PhysicalPartition _partition;

  public PhysicalPartitionConsumptionException(PhysicalPartition pp, Throwable cause)
  {
    super("exception processing partition " + pp, cause);
    _partition = pp;
  }

  public PhysicalPartition getPartition()
  {
    return _partition;
  }

}
