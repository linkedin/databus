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

/**
 * Hash algorithm by Bob Jenkins, 1996.
 *
 * You may use this code any way you wish, private, educational, or commercial.  It's free.
 * See: http://burtleburtle.net/bob/hash/doobs.html
 *
 * Use for hash table lookup, or anything where one collision in 2^^32
 * is acceptable.  Do NOT use for cryptographic purposes.
 *
 * Java port by Gray Watson http://256.com/gray/
 */
public class JenkinsHashFunction implements HashFunction {

	// max value to limit it to 4 bytes
	private static final long MAX_VALUE = 0xFFFFFFFFL;

	// internal variables used in the various calculations
	long a;
	long b;
	long c;

	/**
	 * Convert a byte into a long value without making it negative.
	 */
	private long byteToLong(byte b) {
		long val = b & 0x7F;
		if ((b & 0x80) != 0) {
			val += 128;
		}
		return val;
	}

	/**
	 * Do addition and turn into 4 bytes.
	 */
	private long add(long val, long add) {
		return (val + add) & MAX_VALUE;
	}

	/**
	 * Do subtraction and turn into 4 bytes.
	 */
	private long subtract(long val, long subtract) {
		return (val - subtract) & MAX_VALUE;
	}

	/**
	 * Left shift val by shift bits and turn in 4 bytes.
	 */
	private long xor(long val, long xor) {
		return (val ^ xor) & MAX_VALUE;
	}

	/**
	 * Left shift val by shift bits.  Cut down to 4 bytes.
	 */
	private long leftShift(long val, int shift) {
		return (val << shift) & MAX_VALUE;
	}

	/**
	 * Convert 4 bytes from the buffer at offset into a long value.
	 */
	private long fourByteToLong(byte[] bytes, int offset) {
		return (byteToLong(bytes[offset + 0])
				+ (byteToLong(bytes[offset + 1]) << 8)
				+ (byteToLong(bytes[offset + 2]) << 16)
				+ (byteToLong(bytes[offset + 3]) << 24));
	}

	/**
	 * Convert 4 bytes from the buffer at offset into a long value.
	 */
	private long fourByteToLong(ByteBuffer buf, int offset) {
		return (byteToLong(buf.get(offset + 0))
				+ (byteToLong(buf.get(offset + 1)) << 8)
				+ (byteToLong(buf.get(offset + 2)) << 16)
				+ (byteToLong(buf.get(offset + 3)) << 24));
	}
	/**
	 * Mix up the values in the hash function.
	 */
	private void hashMix() {
	   a = subtract(a, b); a = subtract(a, c); a = xor(a, c >> 13);
	   b = subtract(b, c); b = subtract(b, a); b = xor(b, leftShift(a, 8));
	   c = subtract(c, a); c = subtract(c, b); c = xor(c, (b >> 13));
	   a = subtract(a, b); a = subtract(a, c); a = xor(a, (c >> 12));
	   b = subtract(b, c); b = subtract(b, a); b = xor(b, leftShift(a, 16));
	   c = subtract(c, a); c = subtract(c, b); c = xor(c, (b >> 5));
	   a = subtract(a, b); a = subtract(a, c); a = xor(a, (c >> 3));
	   b = subtract(b, c); b = subtract(b, a); b = xor(b, leftShift(a, 10));
	   c = subtract(c, a); c = subtract(c, b); c = xor(c, (b >> 15));
	 }

	/**
	 * Hash a "long" key into a 32-bit value.  Every bit of the
	 * key affects every bit of the return value.  Every 1-bit and 2-bit
	 * delta achieves avalanche.  The best hash table sizes are powers of 2.
	 *
	 * @param key          that we are hashing on.
	 * @return Hash value for the buffer.
	 */
	public synchronized long hash(long key)
	{
		// set up the internal state
		// the golden ratio; an arbitrary value
		a = 0x09e3779b9L;
		// the golden ratio; an arbitrary value
		b = 0x09e3779b9L;
        // the previous hash value
		c = 0;

		b = add(b, leftShift(key, 24));
		hashMix();

		return c;
	}

	/**
	 * Hash a variable-length key into a 32-bit value.  Every bit of the
	 * key affects every bit of the return value.  Every 1-bit and 2-bit
	 * delta achieves avalanche.  The best hash table sizes are powers of 2.
	 *
	 * @param buffer Byte array that we are hashing on.
	 * @return Hash value for the buffer.
	 */
	public synchronized long hash(byte[] buffer)
	{
		int len, pos;

		// set up the internal state
		// the golden ratio; an arbitrary value
		a = 0x09e3779b9L;
		// the golden ratio; an arbitrary value
		b = 0x09e3779b9L;
        // the previous hash value
		c = 0;

		// handle most of the key
		pos = 0;
		for (len = buffer.length; len >=12; len -= 12) {
			a = add(a, fourByteToLong(buffer, pos));
			b = add(b, fourByteToLong(buffer, pos + 4));
			c = add(c, fourByteToLong(buffer, pos + 8));
			hashMix();
			pos += 12;
		}

		c += buffer.length;

		// all the case statements fall through to the next on purpose
		switch(len) {
		case 11:
			c = add(c, leftShift(byteToLong(buffer[pos + 10]), 24));
		case 10:
			c = add(c, leftShift(byteToLong(buffer[pos + 9]), 16));
		case 9:
			c = add(c, leftShift(byteToLong(buffer[pos + 8]), 8));
			// the first byte of c is reserved for the length
		case 8:
			b = add(b, leftShift(byteToLong(buffer[pos + 7]), 24));
		case 7:
			b = add(b, leftShift(byteToLong(buffer[pos + 6]), 16));
		case 6:
			b = add(b, leftShift(byteToLong(buffer[pos + 5]), 8));
		case 5:
			b = add(b, byteToLong(buffer[pos + 4]));
		case 4:
			a = add(a, leftShift(byteToLong(buffer[pos + 3]), 24));
		case 3:
			a = add(a, leftShift(byteToLong(buffer[pos + 2]), 16));
		case 2:
			a = add(a, leftShift(byteToLong(buffer[pos + 1]), 8));
		case 1:
			a = add(a, byteToLong(buffer[pos + 0]));
			// case 0: nothing left to add
		}
		hashMix();

		return c;
	}


	@Override
	public synchronized long hash(ByteBuffer buffer, int offset, int length) {
		int len, pos;

		// set up the internal state
		// the golden ratio; an arbitrary value
		a = 0x09e3779b9L;
		// the golden ratio; an arbitrary value
		b = 0x09e3779b9L;
        // the previous hash value
		c = 0;

		// handle most of the key
		pos = 0;
		for (len = length; len >= (offset + 12); len -= 12) {
			a = add(a, fourByteToLong(buffer, pos));
			b = add(b, fourByteToLong(buffer, pos + 4));
			c = add(c, fourByteToLong(buffer, pos + 8));
			hashMix();
			pos += 12;
		}

		c += length;

		// all the case statements fall through to the next on purpose
		switch(len - offset)
		{
			case 11:
				c = add(c, leftShift(byteToLong(buffer.get(pos + 10)), 24));
			case 10:
				c = add(c, leftShift(byteToLong(buffer.get(pos + 9)), 16));
			case 9:
				c = add(c, leftShift(byteToLong(buffer.get(pos + 8)), 8));
				// the first byte of c is reserved for the length
			case 8:
				b = add(b, leftShift(byteToLong(buffer.get(pos + 7)), 24));
			case 7:
				b = add(b, leftShift(byteToLong(buffer.get(pos + 6)), 16));
			case 6:
				b = add(b, leftShift(byteToLong(buffer.get(pos + 5)), 8));
			case 5:
				b = add(b, byteToLong(buffer.get(pos + 4)));
			case 4:
				a = add(a, leftShift(byteToLong(buffer.get(pos + 3)), 24));
			case 3:
				a = add(a, leftShift(byteToLong(buffer.get(pos + 2)), 16));
			case 2:
				a = add(a, leftShift(byteToLong(buffer.get(pos + 1)), 8));
			case 1:
				a = add(a, byteToLong(buffer.get(pos + 0)));
				// case 0: nothing left to add
		}
		hashMix();

		return c;
	}


	@Override
	public long hash(ByteBuffer buf) {
    	int length = buf.position() + buf.remaining();
        return hash(buf, 0, length);
	}



	@Override
	public long hash(byte[] key, int numBuckets) {
		return hash(key)%numBuckets;
	}

	@Override
	public long hash(long key, int numBuckets) {
		return hash(key)%numBuckets;
	}

}
