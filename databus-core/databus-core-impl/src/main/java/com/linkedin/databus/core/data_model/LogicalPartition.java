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


/**
 * Represents a Databus logical partition
 *
 * @see <a href="https://iwww.corp.linkedin.com/wiki/cf/display/ENGS/Databus+2.0+and+Databus+3.0+Data+Model">Databus 2.0 and Databus 3.0 Data Model</a>
 */
public class LogicalPartition
{
  private final Short _id;

  private static final Short ALL_LOGICAL_PARTITIONS_ID = -1;

  public static final LogicalPartition ALL_LOGICAL_PARTITIONS =
      new LogicalPartition(ALL_LOGICAL_PARTITIONS_ID);

  public LogicalPartition(Short id)
  {
    super();
    if (null == id) throw new NullPointerException("id");
    _id = id;
  }

  public static LogicalPartition createAllPartitionsWildcard()
  {
    return ALL_LOGICAL_PARTITIONS;
  }

  /** The logical partition id */
  public Short getId()
  {
    return _id;
  }

  @Override
  public String toString()
  {
    return toJsonString();
  }

  public String toJsonString()
  {
    StringBuilder sb = new StringBuilder(15);
    sb.append("{\"id\":");
    sb.append(_id.shortValue());
    sb.append("}");

    return sb.toString();
  }

  /** Checks if the object denotes a wildcard */
  public boolean isWildcard()
  {
    return isAllPartitionsWildcard();
  }

  /** Checks if the object denotes a ALL_LOGICAL_SOURCES wildcard */
  public boolean isAllPartitionsWildcard()
  {
    return equalsPartition(ALL_LOGICAL_PARTITIONS);
  }

  public boolean equalsPartition(LogicalPartition other)
  {
    return _id.shortValue() == other._id.shortValue();
  }

  @Override
  public boolean equals(Object other)
  {
    if (null == other || !(other instanceof LogicalPartition)) return false;
    return equalsPartition((LogicalPartition)other);
  }

  @Override
  public int hashCode()
  {
    return _id.hashCode();
  }

}
