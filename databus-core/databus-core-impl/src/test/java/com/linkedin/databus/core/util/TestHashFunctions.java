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


import com.linkedin.databus.core.DbusEventV1;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.zip.CRC32;
import org.testng.annotations.Test;

public class TestHashFunctions
{

	public static int ONE_KB_IN_BYTES = 1024;
	public static int ONE_MB_IN_BYTES = ONE_KB_IN_BYTES * ONE_KB_IN_BYTES;

	@Test
	public void testHashPerf()
	{
		System.out.println("1 KB:");
		testHashPerf(ONE_KB_IN_BYTES);

		System.out.println("10 KB:");
		testHashPerf(ONE_KB_IN_BYTES * 10);

		System.out.println("100 KB:");
		testHashPerf(ONE_KB_IN_BYTES * 100);

		System.out.println("1 MB:");
		testHashPerf(ONE_MB_IN_BYTES);

		System.out.println("10 MB:");
		testHashPerf(ONE_MB_IN_BYTES * 10);

		System.out.println("100 MB:");
		testHashPerf(ONE_MB_IN_BYTES * 100);
	}

    public void testHashPerf(int capacity)
    {
		byte[] b = new byte[capacity];
		ByteBuffer buf = ByteBuffer.allocateDirect(capacity).order(DbusEventV1.byteOrder);
		Random r = new Random();
		r.nextBytes(b);
		buf.put(b);

		FnvHashFunction fun = new FnvHashFunction();
		CRC32 chksum = new CRC32();
		JenkinsHashFunction jFun = new JenkinsHashFunction();

		long start = 0;
		long end = 0;
		long hash = 0;
		long diff = 0;
		long delayMicro = 0;

		chksum.reset();
		chksum.update(b);
		long prevhash = chksum.getValue();
		for (int i = 0; i < 10; i++)
		{
			start = System.nanoTime();
			chksum.reset();
			chksum.update(b);
			hash = chksum.getValue();
			end = System.nanoTime();
			assert(prevhash == hash);
			diff += (end - start);
		}

		delayMicro = (diff/1000)/10;

		System.out.println("Latency of System CRC32 (Micro Seconds) for byte[] is: " + delayMicro);

		prevhash = fun.hash(b);
		for (int i = 0; i < 10; i++)
		{
			start = System.nanoTime();
			hash = fun.hash(b);
			end = System.nanoTime();
			assert(prevhash == hash);
			diff += (end - start);
		}
		delayMicro = (diff/1000)/10;
		System.out.println("Latency of FNV (Micro Seconds) for byte[] is: " + delayMicro);

		prevhash = jFun.hash(b);
		for (int i = 0; i < 10; i++)
		{
			start = System.nanoTime();
			hash = jFun.hash(b);
			end = System.nanoTime();
			assert(prevhash == hash);
			diff += (end - start);
		}
		delayMicro = (diff/1000)/10;
		System.out.println("Latency of Jenkins (Micro Seconds) for byte[]  is: " + delayMicro);

		prevhash = ByteBufferCRC32.getChecksum(b);
		for (int i = 0; i < 10; i++)
		{
			start = System.nanoTime();
			hash = ByteBufferCRC32.getChecksum(b);
			end = System.nanoTime();
			assert(prevhash == hash);
			diff += (end - start);
		}
		delayMicro = (diff/1000)/10;
		System.out.println("Latency of ByteBufferCRC32 (Micro Seconds) for byte[] is: " + delayMicro);

		//System.out.println("Buffer position-Remaining :" + buf.position() + "-" + buf.remaining());

		prevhash = fun.hash(buf);
		for (int i = 0; i < 10; i++)
		{
			start = System.nanoTime();
			hash = fun.hash(buf);
			end = System.nanoTime();
			assert(prevhash == hash);
			diff += (end - start);
		}
		delayMicro = (diff/1000)/10;
		System.out.println("Latency of FNV (Micro Seconds) for ByteBuffer is: " + delayMicro);
		//System.out.println("Buffer position-Remaining :" + buf.position() + "-" + buf.remaining());

		prevhash = fun.hash(buf);
		for (int i = 0; i < 10; i++)
		{
			start = System.nanoTime();
			hash = fun.hash(buf);
			end = System.nanoTime();
			assert(prevhash == hash);
			diff += (end - start);
		}
		delayMicro = (diff/1000)/10;
		System.out.println("Latency of Jenkins (Micro Seconds) for ByteBuffer is: " + delayMicro);
		//System.out.println("Buffer position-Remaining :" + buf.position() + "-" + buf.remaining());
		prevhash = ByteBufferCRC32.getChecksum(buf);
		for (int i = 0; i < 10; i++)
		{
			start = System.nanoTime();
			hash = ByteBufferCRC32.getChecksum(buf);
			end = System.nanoTime();
			assert(prevhash == hash);
			diff += (end - start);
		}
		delayMicro = (diff/1000)/10;
		System.out.println("Latency of ByteBufferCRC32 (Micro Seconds)  for ByteBuffer is: " + delayMicro);

		//System.out.println("Buffer position-Remaining :" + buf.position() + "-" + buf.remaining());
	}
}
