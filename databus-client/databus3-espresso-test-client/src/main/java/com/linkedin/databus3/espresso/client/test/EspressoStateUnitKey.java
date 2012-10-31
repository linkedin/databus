package com.linkedin.databus3.espresso.client.test;

public class EspressoStateUnitKey
{
  private String _dbName;
  private int _partitionId;

  EspressoStateUnitKey(String stateUnitKey)
  {
    int partitionNumberPos = stateUnitKey.lastIndexOf("_");
    assert(partitionNumberPos > 0);
    
    _dbName = stateUnitKey.substring(0, partitionNumberPos);
    _partitionId = Integer.parseInt(stateUnitKey.substring(partitionNumberPos + 1, stateUnitKey.length()));
  }
  
  public String getDBName()
  {
    return _dbName;
  }
  
  public int getPartitionId()
  {
    return _partitionId;
  }
  
  public String toString()
  {
    return _dbName + "_" + _partitionId;
  }
}
