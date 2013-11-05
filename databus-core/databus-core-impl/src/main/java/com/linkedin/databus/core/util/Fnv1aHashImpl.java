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
package com.linkedin.databus.core.util;

import java.nio.ByteBuffer;

/**
 * Implementation of Fnv-1a hash
 */
public class Fnv1aHashImpl
{
  //Constants sourced from http://www.isthe.com/chongo/tech/comp/fnv/

  /** seed for 32-bit hashes */
  private static final long FNV_BASIS_32 = 2166136261L;
  /** prime for 32-bit hashes */
  private static final long FNV_PRIME_32 = 16777619L;

  /** Initializes the hash accumulator */
  public static final long init32()
  {
    return FNV_BASIS_32;
  }

  /**
   * Adds a byte to a hash accumulator
   * @param     hashAccum       the currently accumulated hash value
   * @param     v               the byte value to add to the hash
   * @return    the new hash accumulator value
   */
  public static final long addByte32(long hashAccum, byte v)
  {
    hashAccum ^= 0xFF & (long)v;
    hashAccum *= FNV_PRIME_32;
    return hashAccum;
  }

  /**
   * Convenience method to add a short to a hash accumulator. It adds both bytes in MSB order.
   * @param     hashAccum       the currently accumulated hash value
   * @param     v               the short value to add to the hash
   * @return    the new hash accumulator value
   */
  public static final long addShort32(long hashAccum, short v)
  {
    return addByte32(addByte32(hashAccum, (byte)((v >> 8) & 0xFF)), (byte)(v & 0xFF));
  }

  /**
   * Convenience method to add an int to a hash accumulator. It adds all 4 bytes in MSB order.
   * @param     hashAccum       the currently accumulated hash value
   * @param     v               the int value to add to the hash
   * @return    the new hash accumulator value
   */
  public static final long addInt32(long hashAccum, int v)
  {
    return addShort32(addShort32(hashAccum, (short)((v >> 16) & 0xFFFF)), (short)(v & 0xFFFF));
  }

  /**
   * Convenience method to add a long to a hash accumulator. It adds all 8 bytes in MSB order.
   * @param     hashAccum       the currently accumulated hash value
   * @param     v               the long value to add to the hash
   * @return    the new hash accumulator value
   */
  public static final long addLong32(long hashAccum, long v)
  {
    return addInt32(addInt32(hashAccum, (int)((v >> 32) & 0xFFFFFFFF)), (int)(v & 0xFFFFFFFF));
  }

  /**
   * Convenience method to add a (subsequence in a) byte array to a hash accumulator.
   * @param     hashAccum       the currently accumulated hash value
   * @param     a               the byte array
   * @param     startIdx        the index of the first byte to hash
   * @param     num             the number of bytes to hash
   * @return    the new hash accumulator value
   */
  public static final long addBytes32(long hashAccum, byte[] a, int startIdx, int num)
  {
    final int n = startIdx + num;
    for (int i = startIdx; i < n; ++i)
    {
      hashAccum = addByte32(hashAccum, a[i]);
    }
    return hashAccum;
  }

  /**
   * Convenience method to add a byte array to a hash accumulator.
   * @param     hashAccum       the currently accumulated hash value
   * @param     a               the byte array
   * @return    the new hash accumulator value
   */
  public static final long addBytes32(long hashAccum, byte[] a)
  {
    return addBytes32(hashAccum, a, 0, a.length);
  }

  /**
   * Convenience method to add a (subsequence in a) ByteBuffer to a hash accumulator.
   * @param     hashAccum       the currently accumulated hash value
   * @param     buf             the byte buffer
   * @param     startIdx        the index of the first byte to hash
   * @param     num             the number of bytes to hash
   * @return    the new hash accumulator value
   */
  public static long addByteBuffer32(long hashAccum, ByteBuffer buf, int startIdx, int num)
  {
    if (buf.hasArray())
    {
      return addBytes32(hashAccum, buf.array(), startIdx, num);
    }

    final int last = Math.min(startIdx + num, buf.limit());
    for (int i=startIdx;  i<last;  i++)
    {
      hashAccum=addByte32(hashAccum, buf.get(i));
    }
   return hashAccum;
  }

  /**
   * Convenience method to add a ByteBuffer to a hash accumulator.
   * @param     hashAccum       the currently accumulated hash value
   * @param     buf               the byte buffer
   * @return    the new hash accumulator value
   */
  public static long addByteBuffer32(long hashAccum, ByteBuffer buf)
  {
   return addByteBuffer32(hashAccum, buf, 0, buf.limit());
  }

  /**
   * Convenience method to add a (substring of a)string
   * @param     hashAccum       the currently accumulated hash value
   * @param     s               the string
   * @param     startIdx        the index of the first byte to hash
   * @param     num             the number of bytes to hash
   * @return    the new hash accumulator value
   */
  public static long addString32(long hashAccum, String s, int startIdx, int num)
  {
    final int endIdx = startIdx + num;
    for (int i = startIdx; i < endIdx; ++i)
    {
      //technically the Java characters are 2 bytes but we use only the lower 8 bits for performance.
      hashAccum = addByte32(hashAccum, (byte)(s.codePointAt(i) & 0xFF));
    }

    return hashAccum;
  }

  /**
   * Convenience method to add a string
   * @param     hashAccum       the currently accumulated hash value
   * @param     s               the string
   * @return    the new hash accumulator value
   */
  public static long addString32(long hashAccum, String s)
  {
    return addString32(hashAccum, s, 0, s.length());
  }

  /**
   * Obtain the 32-bit hash value accumulated so far
   * @param     hashAccum       the hash accumulated so far
   * @return  the hash value as an int
   */
  public static final int getHash32(long hashAccum)
  {
    return (int)(hashAccum & 0xFFFFFFFF);
  }

  /**
   * Obtain the 32-bit hash value accumulated so far for a given bucket
   * @param     hashAccum       the hash accumulated so far
   * @param     numBuckets      the number of buckets
   * @return  the hash value as an int in [0, bucketNum)
   */
  public static final int getHash32(long hashAccum, int numBuckets)
  {
    assert numBuckets > 0;
    //make sure we are protected against negative hash values
    return (int)((hashAccum & 0x7FFFFFFFFFFFFFFFL) % numBuckets);
  }

}
