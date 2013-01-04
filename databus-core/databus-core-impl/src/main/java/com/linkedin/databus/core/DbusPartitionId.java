package com.linkedin.databus.core;

public class DbusPartitionId
{
  public static final int UNKNOWN_PARTITION_ID = -1;
  
  private Integer _partitionId;

  public int getPartitionId()
  {
    return _partitionId;
  }
  
  public DbusPartitionId(int partitionId)
  {
    _partitionId = partitionId;
  }
  
  @Override
  public int hashCode()
  {
	  return _partitionId.hashCode();
  }
  
  @Override
  public boolean equals(Object obj)
  {
	  if ( !( obj instanceof DbusPartitionId))
		  return false;
	  
	  return _partitionId.equals(((DbusPartitionId)obj).getPartitionId());
  }
}
