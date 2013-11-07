package com.linkedin.databus.core.cmclient;
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


import java.text.ParseException;

public class LogicalPartitionRepresentation
{
	private int _partitionNum;
	private int _schemaVersion;
	private String _logicalPartition;
	
	LogicalPartitionRepresentation(String logicalPartition)
	throws ParseException, NumberFormatException
	{
		/**
		 * Validate that logicalPartition is represented in the format p10_1 ( partition 10, schema 1)
		 */
		if (logicalPartition.charAt(0) != 'p')
		{
			throw new ParseException(logicalPartition, 0);
		}
		String nextStr = logicalPartition.substring(1);
		String[] parts = nextStr.split("_");
		
		if (parts.length != 2)
		{
			throw new ParseException("logical Partition does not have numbers of format p[num1]_[num2]" + logicalPartition, 0);
		}
		int p = Integer.parseInt(parts[0]);
		int s = Integer.parseInt(parts[1]); 
		
		_logicalPartition = logicalPartition;
		_partitionNum = p;
		_schemaVersion = s;			
	}
	
	LogicalPartitionRepresentation(int partitionNum, int schemaVersion)
	{
		_partitionNum = partitionNum;
		_schemaVersion = schemaVersion;
		_logicalPartition = "p" + Integer.toString(partitionNum) + "_" + Integer.toString(schemaVersion);
	}
	
	public int getPartitionNum()
	{
		return _partitionNum;
	}
	
	public int getSchemaVersion()
	{
		return _schemaVersion;
	}

	public String getLogicalPartition()
	{
		return _logicalPartition;
	}

	@Override
	public String toString() {
		return "LogicalPartitionRepresentation [_partitionNum=" + _partitionNum
				+ ", _schemaVersion=" + _schemaVersion + ", _logicalPartition="
				+ _logicalPartition + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime
				* result
				+ ((_logicalPartition == null) ? 0 : _logicalPartition
						.hashCode());
		result = prime * result + _partitionNum;
		result = prime * result + _schemaVersion;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		LogicalPartitionRepresentation other = (LogicalPartitionRepresentation) obj;
		if (_logicalPartition == null) {
			if (other._logicalPartition != null)
				return false;
		} else if (!_logicalPartition.equals(other._logicalPartition))
			return false;
		if (_partitionNum != other._partitionNum)
			return false;
		if (_schemaVersion != other._schemaVersion)
			return false;
		return true;
	}
}
