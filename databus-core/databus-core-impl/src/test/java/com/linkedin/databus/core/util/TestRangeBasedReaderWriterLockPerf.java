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


import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.log4j.Level;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.databus.core.util.RangeBasedReaderWriterLock.LockToken;

public class TestRangeBasedReaderWriterLockPerf {

	BufferPositionParser _parser = new BufferPositionParser(Integer.MAX_VALUE, Integer.MAX_VALUE);

	@BeforeMethod
  public void setUp() throws Exception {
	}

	@AfterMethod
  public void tearDown() throws Exception {
	}

	@Test
	/**
	 * Test how fast we can check out read locks from the lock provider
	 * We will keep a maximum of N read locks checked out at any point in time
	 * Once the number of read locks has exceeded N we will release a few K back to the provider
	 * We will also check out a non-overlapping write lock after checking out the read lock
	 */
	public void testReaderInsertWithCooperatingWritesPerformance() throws Exception {

		ArrayList<Double> avgInsertsPerSecond = new ArrayList<Double>();

		for (int numTests = 0; numTests < 20; ++ numTests)
		{
		Queue<LockToken> checkedOutLockTokens = new ArrayBlockingQueue<LockToken>(100);

		RangeBasedReaderWriterLock lockProvider = new RangeBasedReaderWriterLock();
		RangeBasedReaderWriterLock.LOG.setLevel(Level.FATAL);
		long startTime = System.nanoTime();
		int numOperations = 1000000;
		for (int i=numOperations -1; i > 0; --i)
		{
			LockToken token = lockProvider.acquireReaderLock(i, numOperations,_parser, "lock" + i);
			long minStart = lockProvider.getReaderRanges().peek()._id.start;
			lockProvider.acquireWriterLock(0, minStart,_parser);
			lockProvider.releaseWriterLock(_parser);
			if (minStart != i)
			{
				Assert.fail("i should always be equal to minStart");
			}
			//assertEquals(i, minStart);
			while (!checkedOutLockTokens.offer(token))
			{
				for (int j = 0; j < 5; ++j)
				{
					LockToken releaseToken = checkedOutLockTokens.poll();
					lockProvider.releaseReaderLock(releaseToken);
				}
			}

		}

		long endTime = System.nanoTime();

		if (numTests>0)
		{
			double insertsPerSecond = numOperations * 1000000000L / (endTime - startTime);
			avgInsertsPerSecond.add(insertsPerSecond);
			System.out.println("Inserts per second = " + insertsPerSecond);
		}
		}

		double total = 0;
		for (Double d: avgInsertsPerSecond)
		{
			total += d;
		}
		System.out.println("Overall Average Inserts Per Second = " + (total / avgInsertsPerSecond.size()));
	}

}
