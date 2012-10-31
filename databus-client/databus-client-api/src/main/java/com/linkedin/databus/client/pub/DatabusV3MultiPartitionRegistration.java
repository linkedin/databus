package com.linkedin.databus.client.pub;

import java.util.Map;

import com.linkedin.databus.core.data_model.PhysicalPartition;

/** A consumer registration for subscriptions across multiple physical partitions */
public interface DatabusV3MultiPartitionRegistration extends DatabusV3Registration
{
  /**
   * Children registrations per partition
   * @return a read-only copy of the {@link PhysicalPartition} to {@link DatabusV3Registration} mapping */
  public Map<PhysicalPartition, DatabusV3Registration> getPartionRegs();
}
