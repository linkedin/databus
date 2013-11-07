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

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.core.DatabusRuntimeException;
import com.linkedin.databus2.test.TestUtil;

public class TestBufferOffset {

	final BufferPositionParser _parser =
	    new BufferPositionParser(Integer.MAX_VALUE, Integer.MAX_VALUE);
	final BufferPositionParser _parser5x100 =
	    new BufferPositionParser(100, 5);
    final BufferPositionParser _parser16x128 =
        new BufferPositionParser(128, 16);

	@BeforeClass
	public void setUp()
	{
	  TestUtil.setupLogging(true, null, Level.OFF);
	}

	@Test
	public void testInit() {
		long position = 0;
		AssertJUnit.assertFalse(_parser.init(position));
		position = -12134L;
		AssertJUnit.assertTrue(_parser.init(position));
	}

	@Test
	public void testSetOffset() {
		long position = Long.MAX_VALUE;
		int offset = Integer.MAX_VALUE;
		position = _parser.setOffset(position, offset);
		AssertJUnit.assertTrue(_parser.bufferOffset(position) == offset);

		position = Long.MIN_VALUE;
		offset = 0;
		position = _parser.setOffset(position, offset);
		AssertJUnit.assertTrue(_parser.bufferOffset(position) == offset);
		AssertJUnit.assertFalse(_parser.init(position));

	}

	@Test
	public void testSetsGets5x100()
	{
	  long pos = _parser5x100.encode(0, 0, 0);
      AssertJUnit.assertEquals(0, _parser5x100.bufferGenId(pos));
      AssertJUnit.assertEquals(0, _parser5x100.bufferIndex(pos));
      AssertJUnit.assertEquals(0, _parser5x100.bufferOffset(pos));

	  pos = _parser5x100.encode(1, 2, 3);
	  AssertJUnit.assertEquals(1, _parser5x100.bufferGenId(pos));
      AssertJUnit.assertEquals(2, _parser5x100.bufferIndex(pos));
      AssertJUnit.assertEquals(3, _parser5x100.bufferOffset(pos));

      pos = _parser5x100.encode(Integer.MAX_VALUE, 4, 99);
      AssertJUnit.assertEquals(Integer.MAX_VALUE, _parser5x100.bufferGenId(pos));
      AssertJUnit.assertEquals(4, _parser5x100.bufferIndex(pos));
      AssertJUnit.assertEquals(99, _parser5x100.bufferOffset(pos));

      pos = _parser5x100.setIndex(pos, 3);
      AssertJUnit.assertEquals(Integer.MAX_VALUE, _parser5x100.bufferGenId(pos));
      AssertJUnit.assertEquals(3, _parser5x100.bufferIndex(pos));
      AssertJUnit.assertEquals(99, _parser5x100.bufferOffset(pos));

      pos = _parser5x100.setOffset(pos, 50);
      AssertJUnit.assertEquals(Integer.MAX_VALUE, _parser5x100.bufferGenId(pos));
      AssertJUnit.assertEquals(3, _parser5x100.bufferIndex(pos));
      AssertJUnit.assertEquals(50, _parser5x100.bufferOffset(pos));

      pos = _parser5x100.setGenId(pos, 31);
      AssertJUnit.assertEquals(31, _parser5x100.bufferGenId(pos));
      AssertJUnit.assertEquals(3, _parser5x100.bufferIndex(pos));
      AssertJUnit.assertEquals(50, _parser5x100.bufferOffset(pos));

      try
      {
        _parser5x100.setOffset(pos, 128);
        AssertJUnit.fail();
      }
      catch (DatabusRuntimeException e)
      {
        //expected
      }

      try
      {
        _parser5x100.setOffset(pos, -1);
        AssertJUnit.fail();
      }
      catch (DatabusRuntimeException e)
      {
        //expected
      }

      try
      {
        _parser5x100.setIndex(pos, 8);
        AssertJUnit.fail();
      }
      catch (DatabusRuntimeException e)
      {
        //expected
      }

      try
      {
        _parser5x100.setIndex(pos, -5);
        AssertJUnit.fail();
      }
      catch (DatabusRuntimeException e)
      {
        //expected
      }

      try
      {
        _parser5x100.setGenId(pos, 0xFFFFFFFFFFFFFFFFL >> _parser.getGenIdShift());
        AssertJUnit.fail();
      }
      catch (DatabusRuntimeException e)
      {
        //expected
      }
	}

    @Test
    public void testSetsGets16x128()
    {
      long pos = _parser16x128.encode(0, 0, 0);
      AssertJUnit.assertEquals(0, _parser16x128.bufferGenId(pos));
      AssertJUnit.assertEquals(0, _parser16x128.bufferIndex(pos));
      AssertJUnit.assertEquals(0, _parser16x128.bufferOffset(pos));

      pos = _parser16x128.encode(5, 6, 7);
      AssertJUnit.assertEquals(5, _parser16x128.bufferGenId(pos));
      AssertJUnit.assertEquals(6, _parser16x128.bufferIndex(pos));
      AssertJUnit.assertEquals(7, _parser16x128.bufferOffset(pos));

      pos = _parser16x128.encode(0, 15, 127);
      AssertJUnit.assertEquals(0, _parser16x128.bufferGenId(pos));
      AssertJUnit.assertEquals(15, _parser16x128.bufferIndex(pos));
      AssertJUnit.assertEquals(127, _parser16x128.bufferOffset(pos));

      pos = _parser16x128.setIndex(pos, 9);
      AssertJUnit.assertEquals(0, _parser16x128.bufferGenId(pos));
      AssertJUnit.assertEquals(9, _parser16x128.bufferIndex(pos));
      AssertJUnit.assertEquals(127, _parser16x128.bufferOffset(pos));

      pos = _parser16x128.setOffset(pos, 111);
      AssertJUnit.assertEquals(0, _parser16x128.bufferGenId(pos));
      AssertJUnit.assertEquals(9, _parser16x128.bufferIndex(pos));
      AssertJUnit.assertEquals(111, _parser16x128.bufferOffset(pos));

      pos = _parser16x128.setGenId(pos, Short.MAX_VALUE);
      AssertJUnit.assertEquals(Short.MAX_VALUE, _parser16x128.bufferGenId(pos));
      AssertJUnit.assertEquals(9, _parser16x128.bufferIndex(pos));
      AssertJUnit.assertEquals(111, _parser16x128.bufferOffset(pos));

      try
      {
        _parser16x128.setOffset(pos, 128);
        AssertJUnit.fail();
      }
      catch (DatabusRuntimeException e)
      {
        //expected
      }

      try
      {
        _parser16x128.setIndex(pos, 16);
        AssertJUnit.fail();
      }
      catch (DatabusRuntimeException e)
      {
        //expected
      }
    }

    @Test
    public void testIncrementGenId()
    {
      long pos = _parser5x100.encode(10, 3, 12);
      pos = _parser5x100.incrementGenId(pos);

      AssertJUnit.assertEquals(11, _parser5x100.bufferGenId(pos));
      AssertJUnit.assertEquals(0, _parser5x100.bufferIndex(pos));
      AssertJUnit.assertEquals(0, _parser5x100.bufferOffset(pos));
    }

	@Test
	public void testSetIndex() {
		long position = Long.MAX_VALUE;

		int index = 200;
		position = _parser.setIndex(position, index);
		AssertJUnit.assertEquals(index, _parser.bufferIndex(position));



		position = Long.MIN_VALUE;
		index = 1;
		position = _parser.setIndex(position, index);
		AssertJUnit.assertEquals(index, _parser.bufferIndex(position));

	}

	@Test
	public void testMasks()
	{
	   {
	      /*
	       * A large number of max size buffers
	       */
	      BufferPositionParser parser = new BufferPositionParser(Integer.MAX_VALUE, Integer.MAX_VALUE);

	      AssertJUnit.assertEquals("Offset Mask",0x7FFFFFFF,parser.getOffsetMask());
	      AssertJUnit.assertEquals("Index Mask",0x3FFFFFFF80000000L,parser.getIndexMask());
	      AssertJUnit.assertEquals("GenID Mask",0x4000000000000000L,parser.getGenIdMask());
          AssertJUnit.assertEquals("Offset Shift", 0, parser.getOffsetShift());
          AssertJUnit.assertEquals("Index Shift", 31, parser.getIndexShift());
          AssertJUnit.assertEquals("GenId Shift", 62, parser.getGenIdShift());
	   }

	   {
	     /*
	      * One small buffer
	      */
	     BufferPositionParser parser = new BufferPositionParser(1024, 1);

	     AssertJUnit.assertEquals("Offset Mask",0x3FF,parser.getOffsetMask());
	     AssertJUnit.assertEquals("Index Mask",0x400L,parser.getIndexMask());
	     AssertJUnit.assertEquals("GenID Mask",0x7FFFFFFFFFFFF800L,parser.getGenIdMask());
	     AssertJUnit.assertEquals("Offset Shift", 0, parser.getOffsetShift());
	     AssertJUnit.assertEquals("Index Shift", 10, parser.getIndexShift());
	     AssertJUnit.assertEquals("GenId Shift", 11, parser.getGenIdShift());
	   }

       {
         /*
          * Small number of very small buffers
          */
         BufferPositionParser parser = new BufferPositionParser(1024 * 1024, 1024);

         AssertJUnit.assertEquals("Offset Mask",0xFFFFF,parser.getOffsetMask());
         AssertJUnit.assertEquals("Index Mask",0x3FF00000L,parser.getIndexMask());
         AssertJUnit.assertEquals("GenID Mask",0x7FFFFFFFC0000000L,parser.getGenIdMask());
         AssertJUnit.assertEquals("Offset Shift", 0, parser.getOffsetShift());
         AssertJUnit.assertEquals("Index Shift", 20, parser.getIndexShift());
         AssertJUnit.assertEquals("GenId Shift", 30, parser.getGenIdShift());
       }
	}

	@Test
	public void testSetMethods()
	{
	  {
        /*
         * A large number of max size buffers
         */
        BufferPositionParser parser = new BufferPositionParser(Integer.MAX_VALUE, Integer.MAX_VALUE);
        AssertJUnit.assertEquals("SetOffset", 100L, parser.setOffset(0, 100));
        AssertJUnit.assertEquals("SetIndex", 0x0000000080000000L, parser.setIndex(0, 1));
        AssertJUnit.assertEquals("SetGenId", 0x4000000000000000L, parser.setGenId(0, 1));
        AssertJUnit.assertEquals("SetOffset", 0x0000000100000400L, parser.setOffset(0x0000000100000000L, 1024));
        AssertJUnit.assertEquals("SetIndex", 0x0000020000000400L, parser.setIndex(0x0000000100000400L, 1024));
        AssertJUnit.assertEquals("SetGenId", 0x4000020000000400L, parser.setGenId(0x0000020000000400L, 1));
	  }

	  {
	    /*
	     * A small number of small-buffers
	     */
	    BufferPositionParser parser = new BufferPositionParser(1024, 1);
        AssertJUnit.assertEquals("SetOffset", 100L, parser.setOffset(0, 100));
        AssertJUnit.assertEquals("SetIndex", 0x400L, parser.setIndex(0, 1));
        AssertJUnit.assertEquals("SetGenId", 0x800L, parser.setGenId(0, 1));
	  }
	}


	@Test
	public void testIt()
	{
      final Logger log = Logger.getLogger("TestBufferOffset.testIt");

	    int individualBufferSize = 40280000;
	    int numBuffers = 1;
        BufferPositionParser parser = new BufferPositionParser(individualBufferSize, numBuffers);
        log.info("Parser:" + parser);
        int l = (0 + 1)%1;
        log.info("L =" + l);
        long start = 0;
        start = parser.setOffset(start, 0);
        start = parser.setGenId(start,48);

        long end = 0;
        end = parser.setOffset(end, 19299);
        end = parser.setGenId(end, 48);

        long point = 0;
        point = parser.setGenId(point, 47);
        point = parser.setOffset(point, 19297);

        boolean v = Range.containsIgnoreGenId(start, end, point, parser);
        AssertJUnit.assertTrue(v);

	}

	@Test
	public void testIncrement()
	{
	  final Logger log = Logger.getLogger("TestBufferOffset.testIncrement");

	  {
	    /*
	     * Many Buffers
	     */

	    int individualBufferSize = 1024*1024;
	    int numBuffers = 8;
        BufferPositionParser parser = new BufferPositionParser(individualBufferSize, numBuffers);

        ByteBuffer[] buffers = new ByteBuffer[numBuffers];
	    for (int i = 0; i <numBuffers; i++)
	    {
	       byte[] b = new byte[individualBufferSize];
	       buffers[i] = ByteBuffer.wrap(b);
	    }

	    AssertJUnit.assertEquals("Increment Offset 1", 0x1, parser.incrementOffset(0, 1, buffers)); //just increment offset
	    AssertJUnit.assertEquals("Increment Offset 2", 0x100000, parser.incrementOffset(0, 1024*1024, buffers)); //index changes
        AssertJUnit.assertEquals("Increment Offset 3", parser.setGenId(0,1), parser.incrementOffset(1024*1024 * 7, 1024*1024, buffers)); //genId changes

	    //incrementing without regress enabled
	    boolean gotException = false;
	    try
	    {
          buffers[0].limit(1024);
	      parser.incrementOffset(0, 1024*1024  + 1, buffers); //index changes
	    } catch ( RuntimeException re) {

	      log.info("Got Exception" + re.getMessage());
	      AssertJUnit.assertEquals("Exception", "Error in _bufferOffset", re.getMessage());
	      gotException = true;
	    } finally {
          buffers[0].clear();
	    }
	    AssertJUnit.assertTrue("Increment without regress", gotException);

	    //incrementing with regress enabled
	    gotException = false;
        try
        {
          buffers[0].limit(1024);
          AssertJUnit.assertEquals("Increment Offset 2", 0x100000, parser.sanitize(1024+1, buffers, true)); //index changes
        } catch ( RuntimeException re) {
          log.info("Got Exception" + re.getMessage());
          gotException = true;
        } finally {
          buffers[0].clear();
        }
        AssertJUnit.assertFalse("Increment without regress", gotException);

        AssertJUnit.assertEquals("Increment Offset 2", 0x100000, parser.incrementIndex(1024, buffers)); //index changes
        AssertJUnit.assertEquals("Increment Offset 2", parser.setGenId(0, 1), parser.incrementGenId(1024)); //index changes
        AssertJUnit.assertEquals("Address", 0xF, parser.address(0x7FFFFFFFFF00000FL));
	  }


	}

}
