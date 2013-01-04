package com.linkedin.databus.core;

import com.linkedin.databus.core.util.FnvHashFunction;
import com.linkedin.databus.core.util.HashFunction;
import com.linkedin.databus.core.util.JenkinsHashFunction;

public class DbusHashPartitionRouter implements DbusPartitionRouter 
{
	public enum HashType
	{
		FNV,
		JENKINS,
	};
	
	private final HashFunction _hash       ;
	private final int          _numBuckets ;
	public DbusHashPartitionRouter(String hashConf, int numBuckets)
	{
		_numBuckets = numBuckets;
		_hash = getHashFunction(hashConf);
	}
	
	private HashFunction getHashFunction(String hashConf)
	{
		HashType hashType = HashType.valueOf(hashConf);
		HashFunction fun = null;
		switch (hashType)
		{
			case FNV:
				fun = new FnvHashFunction();
				break;
			case JENKINS:
				fun = new JenkinsHashFunction();
				break;
			default:
				throw new RuntimeException("Unknown Hash Type :" + hashConf);
		}
		
		return fun;
	}
	
	@Override
	public DbusPartitionId getPartitionId(DbusEvent event) 
	{
		boolean isKeyNumber = event.isKeyNumber();
		
		long partition = 0;
		
		if ( isKeyNumber)
		{
			long key = event.key();
			partition = _hash.hash(key, _numBuckets);
		} else {
			byte[] bytes = event.keyBytes();
			partition = _hash.hash(bytes,_numBuckets);
		}
		
		int part = (int)partition; //safe since _numBuckets is int
		
		DbusPartitionId partitionId = new DbusPartitionId(part);
		
		return partitionId;
	}

}
