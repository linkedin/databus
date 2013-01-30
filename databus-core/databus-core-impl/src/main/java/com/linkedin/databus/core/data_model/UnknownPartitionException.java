package com.linkedin.databus.core.data_model;
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
