package com.linkedin.databus.core;
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


import com.linkedin.databus.core.DbusEventBuffer.AllocationPolicy;
import com.linkedin.databus.core.util.BufferPositionParser;
import com.linkedin.databus2.core.AssertLevel;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.easymock.EasyMock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

public class TestScnIndex
{
  public static final Logger LOG = Logger.getLogger(TestScnIndex.class);
  static AssertLevel DEFAULT_ASSERT_LEVEL = AssertLevel.ALL;

  @BeforeMethod
  public void setUp() throws Exception
  {
    Logger.getRootLogger().setLevel(Level.OFF);
    //Logger.getRootLogger().setLevel(Level.INFO);
    //Logger.getRootLogger().setLevel(Level.DEBUG);
  }

  @AfterMethod
  public void tearDown() throws Exception
  {
  }

  static
  {
    BasicConfigurator.configure();
    Logger.getRootLogger().setLevel(Level.ERROR);
  }

/**
  @Test
  public void testScnIndexUtilities() {
    ScnIndex index = new ScnIndex(10240000, 20000000000L, 500 * ByteSizeConstants.ONE_MEGABYTE_IN_BYTES);
    assertTrue("blockNumber = "+index.getBlockNumber(17327387028L), index.getBlockNumber(17327387028L) > 0);
  }
  **/
  @Test
  public void testScnIndexOnEventWithLargeBatch() {

    /**
     * Index is broken up into 3 entries for offsets
     * 0-1000, 1001-2000, 2001-3000
     */

	BufferPositionParser parser = new BufferPositionParser(3000,3);
	ScnIndex index = new ScnIndex(3 * ScnIndex.SIZE_OF_SCN_OFFSET_RECORD, 3000, 10000,
	                              parser, AllocationPolicy.DIRECT_MEMORY, false, null, DEFAULT_ASSERT_LEVEL,
                                true /* enabled */);
	DbusEvent eopEvent = EasyMock.createNiceMock(DbusEvent.class);
	EasyMock.expect(eopEvent.isEndOfPeriodMarker()).andReturn(true).anyTimes();
	EasyMock.expect(eopEvent.isControlMessage()).andReturn(true).anyTimes();
	EasyMock.replay(eopEvent);

	index.onEvent(eopEvent, 0, 76);
	//200
	index.onEvent(createMockDataEvent(200L), 500, 0);   // 1st block
	index.onEvent(eopEvent, 510,0);   // 1st block
	//200
	index.onEvent(createMockDataEvent(250L), 1500,0);  // 2nd block
	index.onEvent(eopEvent, 1510,0);  // 2nd block
	//300
	index.onEvent(createMockDataEvent(300L), 2100,0);  // 3rd block
	index.onEvent(eopEvent, 2210,0);  // 3rd block

    // Index should look like: 200->500, 250->1500, 300->2100
    LOG.info("index=" + index);
	index.printVerboseString(LOG, Level.DEBUG);
	checkSuccess(index, 200, 500,200);
	checkSuccess(index, 200, 500,200);
	checkSuccess(index, 300, 2100,300);
	assertEquals("Head Check", 0, index.getHead());
	assertEquals("Tail Check", 0, index.getTail());

	//400
	index.moveHead(2100);
	long ofs1_450 = index.getPositionParser().setGenId(450, 1);
	index.onEvent(createMockDataEvent(400L), ofs1_450,0); // Back to 1st block, this should erase the existence of 200
    long ofs1_460 = index.getPositionParser().setGenId(460, 1);
	index.onEvent(eopEvent, ofs1_460,0);  // Back to 1st block
    // Index should look like: 400->[1:450], 250->1500, 300->2100
	checkSuccess(index, 300, 2100, 300);
    checkSuccess(index, 500, ofs1_450, 400);
    checkFailure(index, 200);
	assertEquals("Head Check", 32, index.getHead());
	assertEquals("Tail Check", 16, index.getTail());

    //500
    long ofs1_1100 = index.getPositionParser().setGenId(1100, 1);
    index.onEvent(createMockDataEvent(500L), ofs1_1100,0);
    long ofs1_1110 = index.getPositionParser().setGenId(1110, 1);
	index.onEvent(eopEvent, ofs1_1110, 0);  // 3rd block
    // Index should look like: 400->[1:450], 500->[1:1100], 300->2100
    checkFailure(index, 200);
    checkSuccess(index, 400, ofs1_450, 400);
    checkSuccess(index, 500, ofs1_1100, 500 );
    checkSuccess(index, 300, 2100, 300);
    long ofs1_1010 = index.getPositionParser().setGenId(1010, 1);
	assertEquals(2100, index.getLargerOffset(ofs1_1010));
	assertEquals("Head Check", 32, index.getHead());
	assertEquals("Tail Check", 32, index.getTail());

    //600
    long ofs1_1800 = index.getPositionParser().setGenId(1800, 1);
    index.onEvent(createMockDataEvent(600L), ofs1_1800, 0);
    long ofs1_1805 = index.getPositionParser().setGenId(1805, 1);
	index.onEvent(eopEvent, ofs1_1805, 0);  // 3rd block
    // Index should look like: 400->[1:450], 500->[1:1100], 600->[1:1800], 300->2100
    checkSuccess(index, 600, ofs1_1100, 500);
    checkSuccess(index, 400, ofs1_450, 400);
    checkSuccess(index, 500, ofs1_1100, 500);
    checkSuccess(index, 300, 2100, 300);
	assertEquals("Head Check", 32, index.getHead());
	assertEquals("Tail Check", 32, index.getTail());

    //700
    long ofs0_100 = index.getPositionParser().setGenId(100, 0);
	index.moveHead(ofs0_100);
    long ofs1_2010 = index.getPositionParser().setGenId(2010, 1);
    index.onEvent(createMockDataEvent(700L), ofs1_2010, 0);
    long ofs2_5 = index.getPositionParser().setGenId(5, 2);
    index.onEvent(createMockDataEvent(700L), ofs2_5, 0);
    long ofs2_10 = index.getPositionParser().setGenId(10, 0);
	index.onEvent(eopEvent, ofs2_10, 0);  // 3rd block

    // Index should look like: 400->[1:450], 500->[1:1100], 600->[1:1800], 700->[1:2010]
	LOG.info("index=" + index);
	checkSuccess(index, 700, ofs1_2010, 700);
    assertEquals(400, index.getMinScn());
	assertEquals("Head Check", 0, index.getHead());
	assertEquals("Tail Check", 0, index.getTail());

    // let's pretend that the next event will end at 451
    long ofs1_451 = index.getPositionParser().setGenId(451, 1);
    long potentialHead = index.getLargerOffset(ofs1_451);
    LOG.info("Potential Head is:" + potentialHead);
    index.moveHead(potentialHead);
	assertEquals(ofs1_1100, potentialHead);

    long ofs1_2900 = index.getPositionParser().setGenId(2900, 1);
	index.moveHead(ofs1_2900);
	// let's pretend that the next event will end at 2500
    long ofs2_2500 = index.getPositionParser().setGenId(2500, 2);
    potentialHead = index.getLargerOffset(ofs2_2500);
	LOG.info("potentialHead=" + potentialHead);
	assertEquals(-1, potentialHead);

    long ofs2_1100 = index.getPositionParser().setGenId(1100, 2);
	index.onEvent(createMockDataEvent(800L), ofs2_1100, 0);

    LOG.info("index=" + index);
    long ofs2_1500 = index.getPositionParser().setGenId(1500, 2);
	index.onEvent(eopEvent, ofs2_1500, 0);

	// Index should look like: 700->2010, 800->1100, 700->2010
	checkSuccess(index, 800, ofs2_1100, 800);
	checkSuccess(index, 700, ofs1_2900, 700);
    checkFailure(index, 500);
    assertEquals(700, index.getMinScn());
    LOG.info("index=" + index);

	assertEquals("Head Check", 32, index.getHead());
	assertEquals("Tail Check", 32, index.getTail());
  }

private DbusEvent createMockDataEvent(long windowScn)
  {
  DbusEvent event = EasyMock.createNiceMock(DbusEvent.class);
  EasyMock.expect(event.sequence()).andReturn(windowScn).anyTimes();
  EasyMock.expect(event.isEndOfPeriodMarker()).andReturn(false).anyTimes();
  EasyMock.expect(event.isControlMessage()).andReturn(false).anyTimes();
  EasyMock.replay(event);
  return event;
  }

private void checkSuccess(ScnIndex index, long scn, long offset, long expSCN) {
	try {
		ScnIndex.ScnIndexEntry entry = index.getClosestOffset(scn);
		long readOffset = entry.getOffset();
		LOG.info("Offset Check:" + scn + " : " + readOffset);
		LOG.info("SCN Check:" + scn + " : " + entry.getScn());

    	assertEquals(offset, readOffset);
    	assertEquals(expSCN, entry.getScn());
    }
    catch (OffsetNotFoundException e) {
        LOG.error(scn + " : OffsetNotFound");
      	fail("Exception should not have been thrown");

    }
}

private void checkFailure(ScnIndex index, long scn) {
	try {
		ScnIndex.ScnIndexEntry entry = index.getClosestOffset(scn);
		long readOffset = entry.getOffset();
		LOG.info(scn + " : " + readOffset);
    	fail("Exception should have been thrown");
    }
    catch (OffsetNotFoundException e) {
      LOG.info(scn + " : OffsetNotFound");
    }
}




}
