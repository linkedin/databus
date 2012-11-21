package com.linkedin.databus.core;


public class DbusConstantPartitionRouter implements DbusPartitionRouter
{
  private final DbusPartitionId _partitionId;
  
  public DbusConstantPartitionRouter(int constant)
  {
    _partitionId = new DbusPartitionId(constant);
  }

  @Override
  public DbusPartitionId getPartitionId(DbusEvent event)
  {
    return _partitionId;
  }

}
