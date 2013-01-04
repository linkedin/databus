package com.linkedin.databus2.relay;

import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;

/**
 * supports adding/removing partitions dynamically.
 * One needs to provide configuration for these partitions.
 */
public interface AddRemovePartitionInterface
{
  public void addPartition(PhysicalSourceStaticConfig pConfig) throws DatabusException;
  public void removePartition(PhysicalSourceStaticConfig pConfig) throws DatabusException;
  public void dropDatabase(String dbName) throws DatabusException;
}
