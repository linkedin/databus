package com.linkedin.databus.core.util;
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


import java.nio.ByteBuffer;

public interface HashFunction {
	

	/*
	 * Generates Hash for entire byte buffer
	 * @param buf : ByteBuffer for which hash needs to be computed
	 * @return hash value of buffer
	 */
	public long hash(ByteBuffer buf);
	
	/*
	 * Generates Hash for a section of byte buffer denoted by its
	 * endpoints
	 * 
	 * @param buf : ByteBuffer for which hash needs to be computed
	 * @param off : Starting Offset
	 * @param len : Length of the section for hash computation 
	 * @return the hash value for the section of the buffer
	 */
	public long hash(ByteBuffer buf, int off, int len);
	
	/*
	 * Generates hash for the byte array and bucketize the value to
	 * 0.. (numBuckets - 1)
	 * 
	 * @param key : Array to apply hash and bucketize
	 * @param numBuckets : Number of buckets for bucketization
	 * 
	 * @return Returns the bucket in the range 0..(numBuckets - 1)
	 */
	public long hash(byte[] key, int numBuckets);
	
	/*
	 * Generates hash for the key and bucketize the value to
	 * 0.. (numBuckets - 1)
	 * 
	 * @param key : Input key for which hash needs to be calculated
	 * @param numBuckets : Number of buckets for bucketization
	 * 
	 * @return Returns the bucket in the range 0..(numBuckets - 1)
	 */
	public long hash(long key, int numBuckets);

}
