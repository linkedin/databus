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


import org.apache.log4j.Level;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus2.test.TestUtil;

public class TestRange {

  @BeforeClass
  public void setUpClass()
  {
    TestUtil.setupLogging(true, null, Level.ERROR);
  }

	@Test
	public void testRange() {
		Range r = new Range(20, 3000);
		AssertJUnit.assertEquals(20, r.start);
		AssertJUnit.assertEquals(3000, r.end);
		r.start = 46;
		AssertJUnit.assertEquals(46, r.start);
	}

	@Test
	public void testRangeContainsSimple()
	{
	  Range r = new Range(1, 100);
	  AssertJUnit.assertFalse(r.contains(-1));
	  AssertJUnit.assertTrue(r.contains(1));
	  AssertJUnit.assertTrue(r.contains(5));
	  AssertJUnit.assertTrue(r.contains(99));
	  AssertJUnit.assertFalse(r.contains(100));
	  AssertJUnit.assertFalse(r.contains(101));
	  AssertJUnit.assertTrue(r.contains(1));
	}

    @Test
    public void testRangeContainsReversed()
    {
      Range r = new Range(100, 1);
      AssertJUnit.assertFalse(r.contains(-1));
      AssertJUnit.assertTrue(r.contains(0));
      AssertJUnit.assertFalse(r.contains(1));
      AssertJUnit.assertFalse(r.contains(5));
      AssertJUnit.assertTrue(r.contains(105));
      AssertJUnit.assertTrue(r.contains(100));
    }

    @Test
    public void testContainsReaderPosition()
    {
      BufferPositionParser parser = new BufferPositionParser(100240000 ,1);
      AssertJUnit.assertFalse(Range.containsReaderPosition(14551, 14551, 109, parser));
      AssertJUnit.assertTrue(Range.containsReaderPosition(parser.setGenId(100, 0), parser.setGenId(200,2), parser.setGenId(100,1), parser));
    }


    @Test
    public void testContainsReaderPositionDiffGenId()
    {
      // start and end differ only in GenId
      BufferPositionParser parser = new BufferPositionParser(100240000 ,1);

      long start = parser.setGenId(0, 1);
      start = parser.setIndex(start, 0);
      start = parser.setOffset(start, 0);
      long end = parser.setGenId(0, 2);
      end = parser.setIndex(end, 0);
      end = parser.setOffset(end, 0);

      // Case when offset != start
      long offset = parser.setGenId(0, 1);
      offset = parser.setIndex(offset, 2);
      offset = parser.setOffset(offset, 10);

      AssertJUnit.assertTrue(Range.containsReaderPosition(start, end, offset, parser));
      AssertJUnit.assertTrue(Range.containsReaderPosition(start, end, start, parser));
      AssertJUnit.assertFalse(Range.containsReaderPosition(start, end, end, parser));
    }

	@Test
	public void testIntersects() {
		{
			Range r1 = new Range(1, 100);
			Range r2 = new Range(1, 100);
			AssertJUnit.assertTrue(r1.intersects(r2));
		}

		{
			// r2 continues where r1 left off : when start < end
			Range r1 = new Range(3773223,3774955);
			Range r2 = new Range(3774955,3775116);
			AssertJUnit.assertTrue(!(r1.intersects(r2)));
		}

		{
          // r2 continues where r1 left off : when start > end
          Range r1 = new Range(3774955,100);
          Range r2 = new Range(100,3774945);
          AssertJUnit.assertTrue(!(r1.intersects(r2)));
		}
	}

	@Test
	public void testCompareTo() {
	  {
	    /*
	     * comparing ranges in the same index
	     */
		Range r1 = new Range(1, 100);
		Range r2 = new Range(2, 100);
		AssertJUnit.assertTrue(r1.compareTo(r2) < 0);
	  }

	  {
	    /*
	     * comparing ranges in different index
	     */
	    Range r1 = new Range(0x100000000L, 0x10000050L);
	    Range r2 = new Range(2, 100);
	    AssertJUnit.assertTrue(r1.compareTo(r2) > 0);
	  }

	  {
		    /*
		     * comparing ranges in the same index
		     */
			Range r1 = new Range(1, 100);
			AssertJUnit.assertTrue(r1.compareTo(r1) == 0);
	  }
	}

	@Test
	public void testToString() {
	     BufferPositionParser parser = new BufferPositionParser(Integer.MAX_VALUE,Integer.MAX_VALUE);
	     Range r1 = new Range(1, 100);

	     String exp = "{start:1:[GenId=0;Index=0;Offset=1] - end:100:[GenId=0;Index=0;Offset=100]}";
	     AssertJUnit.assertEquals("Range ToString", exp,r1.toString(parser));

	     r1 = new Range(0, Integer.MAX_VALUE);
	     exp = "{start:0:[GenId=0;Index=0;Offset=0] - end:2147483647:[GenId=0;Index=0;Offset=2147483647]}";
	     AssertJUnit.assertEquals("Range ToString", exp,r1.toString(parser));

	     r1 = new Range(0, Long.MAX_VALUE);
	     exp = "{start:0:[GenId=0;Index=0;Offset=0] - end:" + Long.MAX_VALUE +
	           ":[GenId=1;Index=2147483647;Offset=2147483647]}";
	     AssertJUnit.assertEquals("Range ToString", exp,r1.toString(parser));
	}
}
