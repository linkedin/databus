package com.linkedin.databus.client;

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
