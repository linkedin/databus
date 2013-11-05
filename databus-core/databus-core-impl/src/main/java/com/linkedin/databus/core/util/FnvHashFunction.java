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

public class FnvHashFunction implements HashFunction
{
  @Override
  public long hash(ByteBuffer buf)
  {
    return hash(buf, 0, buf.limit());
  }

  @Override
  public long hash(ByteBuffer buf, int off, int len)
  {
    long hash = Fnv1aHashImpl.addByteBuffer32(Fnv1aHashImpl.init32(), buf, off, len);
    return hash;
  }

  public long hash(byte[] key)
  {
    long hash = Fnv1aHashImpl.addBytes32(Fnv1aHashImpl.init32(), key, 0, key.length);
    return hash;
  }

  @Override
  public long hash(byte[] key, int numBuckets)
  {
    // make sure we handle negative hash values correctly
    return (hash(key) & 0x7FFFFFFFFFFFFFFFL) % numBuckets;
  }

  private long hash(long val)
  {
    long hashval = Fnv1aHashImpl.addLong32(Fnv1aHashImpl.init32(), val);
    return hashval;
  }

  @Override
  public long hash(long val, int numBuckets)
  {
    // make sure we handle negative hash values correctly
    return (hash(val) & 0x7FFFFFFFFFFFFFFFL) % numBuckets;
  }

  /*
   * public static void main(String[] args) { byte[] b = new byte[1024*1024*100];
   * ByteBuffer buf =
   * ByteBuffer.allocateDirect(1024*1024*100).order(DbusEventFactory.getByteOrder());
   * Random r = new Random(); r.nextBytes(b); buf.put(b);
   *
   * FnvHashFunction fun = new FnvHashFunction(); CRC32 chksum = new CRC32();
   * JenkinsHashFunction jFun = new JenkinsHashFunction();
   *
   * long start = 0; long end = 0; long hash = 0; long diff = 0; long delayMicro = 0;
   *
   * chksum.reset(); chksum.update(b); long prevhash = chksum.getValue(); for (int i = 0;
   * i < 10; i++) { start = System.nanoTime(); chksum.reset(); chksum.update(b); hash =
   * chksum.getValue(); end = System.nanoTime(); assert(prevhash == hash); diff += (end -
   * start); }
   *
   * delayMicro = (diff/1000)/10;
   *
   * System.out.println("Latency of System CRC32 (Micro Seconds) is: " + delayMicro);
   *
   * prevhash = fun.hash(b); for (int i = 0; i < 10; i++) { start = System.nanoTime();
   * hash = fun.hash(b); end = System.nanoTime(); assert(prevhash == hash); diff += (end -
   * start); } delayMicro = (diff/1000)/10;
   * System.out.println("Latency of FNV (Micro Seconds)  is: " + delayMicro);
   *
   * prevhash = jFun.hash(b); for (int i = 0; i < 10; i++) { start = System.nanoTime();
   * hash = jFun.hash(b); end = System.nanoTime(); assert(prevhash == hash); diff += (end
   * - start); } delayMicro = (diff/1000)/10;
   * System.out.println("Latency of Jenkins (Micro Seconds)  is: " + delayMicro);
   *
   * prevhash = ByteBufferCRC32.getChecksum(b); for (int i = 0; i < 10; i++) { start =
   * System.nanoTime(); hash = ByteBufferCRC32.getChecksum(b); end = System.nanoTime();
   * assert(prevhash == hash); diff += (end - start); } delayMicro = (diff/1000)/10;
   * System.out.println("Latency of ByteBufferCRC32 (Micro Seconds)  is: " + delayMicro);
   *
   * //System.out.println("Buffer position-Remaining :" + buf.position() + "-" +
   * buf.remaining());
   *
   * prevhash = fun.hash(buf); for (int i = 0; i < 10; i++) { start = System.nanoTime();
   * hash = fun.hash(buf); end = System.nanoTime(); assert(prevhash == hash); diff += (end
   * - start); } delayMicro = (diff/1000)/10;
   * System.out.println("Latency of FNV (Micro Seconds) for ByteBuffer is: " +
   * delayMicro); //System.out.println("Buffer position-Remaining :" + buf.position() +
   * "-" + buf.remaining());
   *
   * prevhash = fun.hash(buf); for (int i = 0; i < 10; i++) { start = System.nanoTime();
   * hash = fun.hash(buf); end = System.nanoTime(); assert(prevhash == hash); diff += (end
   * - start); } delayMicro = (diff/1000)/10;
   * System.out.println("Latency of Jenkins (Micro Seconds) for ByteBuffer is: " +
   * delayMicro); //System.out.println("Buffer position-Remaining :" + buf.position() +
   * "-" + buf.remaining()); prevhash = ByteBufferCRC32.getChecksum(buf); for (int i = 0;
   * i < 10; i++) { start = System.nanoTime(); hash = ByteBufferCRC32.getChecksum(buf);
   * end = System.nanoTime(); assert(prevhash == hash); diff += (end - start); }
   * delayMicro = (diff/1000)/10;
   * System.out.println("Latency of ByteBufferCRC32 (Micro Seconds)  for ByteBuffer is: "
   * + delayMicro);
   *
   * //System.out.println("Buffer position-Remaining :" + buf.position() + "-" +
   * buf.remaining()); }
   */
}
