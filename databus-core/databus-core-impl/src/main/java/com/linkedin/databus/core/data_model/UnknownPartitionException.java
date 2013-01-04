package com.linkedin.databus.core.data_model;

public class UnknownPartitionException extends Exception
{
  private static final long serialVersionUID = 1L;

  private final PhysicalPartition _physicalPart;
  public PhysicalPartition getPhysicalPart()
  {
    return _physicalPart;
  }

  public LogicalPartition getLogicalPart()
  {
    return _logicalPart;
  }

  private final LogicalPartition _logicalPart;

  public UnknownPartitionException(PhysicalPartition ppart)
  {
    super("unknown physical partition: " + ppart);
    _physicalPart = ppart;
    _logicalPart = null;
  }

  public UnknownPartitionException(LogicalPartition lpart)
  {
    super("unknown logical partition: " + lpart);
    _physicalPart = null;
    _logicalPart = lpart;
  }
}
