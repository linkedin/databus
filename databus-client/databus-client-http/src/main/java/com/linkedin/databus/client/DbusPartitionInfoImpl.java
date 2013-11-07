package com.linkedin.databus.client;
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


import com.linkedin.databus.client.pub.DbusPartitionInfo;

public class DbusPartitionInfoImpl implements DbusPartitionInfo {

	private long _id ;
	
	public DbusPartitionInfoImpl(long id)
	{
		_id = id;
	}
	
	@Override
	public long getPartitionId() {
		return _id;
	}

	@Override
	public boolean equalsPartition(DbusPartitionInfo other) 
	{
		if (_id != other.getPartitionId())
			return false;
		return true;
	}

	@Override
	public String toString() {
		return Long.toString(_id);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (_id ^ (_id >>> 32));
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
		return equalsPartition((DbusPartitionInfoImpl) obj);
	}
	
	
}
