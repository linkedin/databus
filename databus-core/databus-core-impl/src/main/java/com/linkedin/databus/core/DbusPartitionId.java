package com.linkedin.databus.core;
/*
 *
 * Copyright 2013 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/


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
