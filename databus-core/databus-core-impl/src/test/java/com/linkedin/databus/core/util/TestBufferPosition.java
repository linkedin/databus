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
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus2.test.TestUtil;

public class TestBufferPosition
{
  @BeforeClass
  public void setUp()
  {
    TestUtil.setupLogging(true, null, Level.OFF);
  }

  static ByteBuffer[] allocateByteBuffers(int... sizes)
  {
    ByteBuffer[] res = new ByteBuffer[sizes.length];
    for (int i = 0; i < sizes.length; ++i)
    {
      res[i] = ByteBuffer.allocate(sizes[i]);
    }

    return res;
  }

  static BufferPositionParser createParser(int... sizes)
  {
    int maxSize = 0;
    for (int s: sizes)
    {
      if (s > maxSize) maxSize = s;
    }

    return new BufferPositionParser(maxSize, sizes.length);
  }

  static void adjustLimits(ByteBuffer[] buffers, int... limits)
  {
    Assert.assertEquals(buffers.length, limits.length);
    for (int i = 0; i < limits.length; ++i)
    {
      buffers[i].limit(limits[i]);
    }
  }

  static void assertPosition(BufferPosition pos, long genId, int index, int offset)
  {
    Assert.assertEquals(pos.bufferGenId(), genId);
    Assert.assertEquals(pos.bufferIndex(), index);
    Assert.assertEquals(pos.bufferOffset(), offset);
  }

  @Test
  public void testSkipOverFreeSpace()
  {
    ByteBuffer[] buffers = allocateByteBuffers(128, 128, 50);
    BufferPositionParser parser = createParser(128, 128, 50);
    BufferPosition pos = new BufferPosition(parser, buffers);

    adjustLimits(buffers, 50, 100, 0);

    pos.setPosition(parser.encode(0, 0, 0));
    pos.skipOverFreeSpace();
    //should not change
    assertPosition(pos, 0, 0, 0);

    adjustLimits(buffers, 0, 0, 0);

    pos.setPosition(parser.encode(0, 0, 0));
    pos.skipOverFreeSpace();
    //should wrap around
    assertPosition(pos, 1, 0, 0);

    adjustLimits(buffers, 10, 0, 0);

    pos.setPosition(parser.encode(0, 0, 50));
    pos.skipOverFreeSpace();
    //should wrap around
    assertPosition(pos, 1, 0, 0);

    adjustLimits(buffers, 50, 100, 0);

    pos.setPosition(parser.encode(10, 0, 10));
    //should not change
    pos.skipOverFreeSpace();
    assertPosition(pos, 10, 0, 10);

    pos.setPosition(parser.encode(12, 0, 50));
    //should go to index 1
    pos.skipOverFreeSpace();
    assertPosition(pos, 12, 1, 0);

    pos.setPosition(parser.encode(255, 1, 100));
    //should go to index 0
    pos.skipOverFreeSpace();
    assertPosition(pos, 256, 0, 0);
  }

  @Test
  public void testIncrementIndex()
  {
    ByteBuffer[] buffers = allocateByteBuffers(256, 256, 256, 99);
    BufferPositionParser parser = createParser(256, 256, 256, 99);
    BufferPosition pos = new BufferPosition(parser, buffers);

    pos.setPosition(123, 2, 100);
    pos.incrementIndex();
    assertPosition(pos, 123, 3, 0);

    pos.incrementIndex();
    assertPosition(pos, 124, 0, 0);
  }

  @Test
  public void testIncrementGenId()
  {
    ByteBuffer[] buffers = allocateByteBuffers(256, 256, 256, 99);
    BufferPositionParser parser = createParser(256, 256, 256, 99);
    BufferPosition pos = new BufferPosition(parser, buffers);

    pos.setPosition(123, 2, 100);
    pos.incrementGenId();
    assertPosition(pos, 124, 0, 0);
  }
}
