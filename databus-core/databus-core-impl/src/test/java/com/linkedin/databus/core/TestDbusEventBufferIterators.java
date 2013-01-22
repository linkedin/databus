package com.linkedin.databus.core;

import java.util.List;
import java.util.Vector;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.core.DbusEventBuffer.AllocationPolicy;
import com.linkedin.databus.core.DbusEventBuffer.InternalEventIterator;
import com.linkedin.databus.core.DbusEventBuffer.QueuePolicy;
import com.linkedin.databus.core.util.DbusEventAppender;
import com.linkedin.databus.core.util.DbusEventGenerator;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.AssertLevel;
import com.linkedin.databus2.test.ConditionCheck;
import com.linkedin.databus2.test.TestUtil;

/** Tests management of iterators in DbusEventBuffer */
public class TestDbusEventBufferIterators
{
  @BeforeClass
  public void setupClass()
  {
    TestUtil.setupLogging(true, null, Level.ERROR);
  }

  @Test
  /**
   * Verify internal iterators see all current updates and nothing after that for a event buffer with
   * a single ByteBuffer*/
  public void testInternalIteratorHappyPathSingleBuf() throws InvalidConfigException
  {
    final Logger log =
        Logger.getLogger("TestDbusEventBufferIterator.testInternalIteratorHappyPathSingleBuf");
    //log.setLevel(Level.INFO);
    log.info("starting");

    final DbusEventBuffer dbusBuf =
        new DbusEventBuffer(TestDbusEventBuffer.getConfig(
            100000, 100000, 100, 500, AllocationPolicy.HEAP_MEMORY, QueuePolicy.OVERWRITE_ON_WRITE,
            AssertLevel.ALL));

    log.info("verify iterators on empty buffers ");
    final InternalEventIterator iter0 =
        dbusBuf.acquireInternalIterator(dbusBuf.getHead(), dbusBuf.getTail(), null);
    Assert.assertEquals(iter0.getCurrentPosition(), dbusBuf.getHead());
    Assert.assertEquals(iter0._iteratorTail.getPosition(), dbusBuf.getTail());
    Assert.assertNotNull(iter0.getIdentifier());
    Assert.assertTrue(iter0.getIdentifier().startsWith(InternalEventIterator.class.getSimpleName()));
    Assert.assertTrue(!iter0.hasNext());
    Assert.assertEquals(dbusBuf._busyIteratorPool.size(), 1);

    log.info("append a full window of events");
    final DbusEventGenerator generator = new DbusEventGenerator();
    final Vector<DbusEvent> events = new Vector<DbusEvent>();
    generator.generateEvents(5, 5, 120, 39, events);
    injectEventsInBuffer(dbusBuf, events, false);

    log.info("verify we can read all events");
    //old iterator has not changed
    Assert.assertTrue(!iter0.hasNext());

    log.info("verify new iterator");
    InternalEventIterator iter1 =
        dbusBuf.acquireInternalIterator(dbusBuf.getHead(), dbusBuf.getTail(), null);
    Assert.assertEquals(iter1.getCurrentPosition(), dbusBuf.getHead());
    Assert.assertEquals(iter1._iteratorTail.getPosition(), dbusBuf.getTail());
    Assert.assertNotNull(iter1.getIdentifier());
    Assert.assertTrue(iter1.getIdentifier().startsWith(InternalEventIterator.class.getSimpleName()));
    Assert.assertEquals(dbusBuf._busyIteratorPool.size(), 2);
    Assert.assertTrue(iter1.hasNext());

    log.info("make sure we can read all events");
    readAndCompareIteratorEvents(iter1, events, false);

    log.info("add more windows");
    final Vector<DbusEvent> events2 = new Vector<DbusEvent>();
    final DbusEventGenerator generator2 = new DbusEventGenerator(1000);
    generator2.generateEvents(50, 4, 180, 100, events2);
    injectEventsInBuffer(dbusBuf, events2, false);

    log.info("verify old iterators have not changed");
    Assert.assertTrue(!iter0.hasNext());
    Assert.assertTrue(!iter1.hasNext());

    log.info("verify new iterator");
    events.addAll(events2);
    InternalEventIterator iter2 =
        dbusBuf.acquireInternalIterator(dbusBuf.getHead(), dbusBuf.getTail(), null);
    Assert.assertEquals(iter2.getCurrentPosition(), dbusBuf.getHead());
    Assert.assertEquals(iter2._iteratorTail.getPosition(), dbusBuf.getTail());
    Assert.assertNotNull(iter2.getIdentifier());
    Assert.assertTrue(iter2.getIdentifier().startsWith(InternalEventIterator.class.getSimpleName()));
    Assert.assertEquals(dbusBuf._busyIteratorPool.size(), 3);
    Assert.assertTrue(iter2.hasNext());

    log.info("make sure we can read all events");
    readAndCompareIteratorEvents(iter2, events, false);

    iter0.close();
    Assert.assertEquals(dbusBuf._busyIteratorPool.size(), 2);
    iter2.close();
    Assert.assertEquals(dbusBuf._busyIteratorPool.size(), 1);
    iter1.close();
    Assert.assertEquals(dbusBuf._busyIteratorPool.size(), 0);
    log.info("done");
  }

  @Test
  /**
   * Verify internal iterators see all current updates and nothing after that for a event buffer with
   * a single ByteBuffer*/
  public void testInternalIteratorWrapSingleBuf() throws InvalidConfigException
  {
    final Logger log =
        Logger.getLogger("TestDbusEventBufferIterator.testInternalIteratorWrapSingleBuf");
    //log.setLevel(Level.INFO);
    log.info("starting");

    final DbusEventBuffer dbusBuf =
        new DbusEventBuffer(TestDbusEventBuffer.getConfig(
            1000, 1000, 100, 500, AllocationPolicy.HEAP_MEMORY, QueuePolicy.OVERWRITE_ON_WRITE,
            AssertLevel.ALL));

    for (int i = 0; i < 1100; ++i)
    {
      log.info("add first window iteration " + i);
      final DbusEventGenerator generator = new DbusEventGenerator(40 * i);
      final Vector<DbusEvent> events1 = new Vector<DbusEvent>();
      generator.generateEvents(6, 1, 120, 41, events1);
      injectEventsInBuffer(dbusBuf, events1, true);

      log.info("get a new iterator");
      InternalEventIterator iter1 =
          dbusBuf.acquireInternalIterator(dbusBuf.getHead(), dbusBuf.getTail(), null);
      Assert.assertEquals(iter1.getCurrentPosition(), dbusBuf.getHead());
      Assert.assertEquals(iter1._iteratorTail.getPosition(), dbusBuf.getTail());
      Assert.assertTrue(iter1.hasNext());

      log.info("process the last window");
      readAndCompareIteratorEvents(iter1, events1, true);
      iter1.close();
    }

    log.info("done");
  }

  @Test
  /**
   * Verify internal iterators see all current updates and nothing after that for a event buffer with
   * multiple ByteBuffers*/
  public void testInternalIteratorHappyPathMultiBuf() throws InvalidConfigException
  {
    final Logger log =
        Logger.getLogger("TestDbusEventBufferIterator.testInternalIteratorHappyPathMultiBuf");
    //log.setLevel(Level.INFO);
    log.info("starting");

    final DbusEventBuffer dbusBuf =
        new DbusEventBuffer(TestDbusEventBuffer.getConfig(
            100000, 400, 100, 500, AllocationPolicy.HEAP_MEMORY, QueuePolicy.BLOCK_ON_WRITE,
            AssertLevel.ALL));

    log.info("verify iterators on empty buffers ");
    final InternalEventIterator iter0 =
        dbusBuf.acquireInternalIterator(dbusBuf.getHead(), dbusBuf.getTail(), null);
    Assert.assertEquals(iter0.getCurrentPosition(), dbusBuf.getHead());
    Assert.assertEquals(iter0._iteratorTail.getPosition(), dbusBuf.getTail());
    Assert.assertNotNull(iter0.getIdentifier());
    Assert.assertTrue(iter0.getIdentifier().startsWith(InternalEventIterator.class.getSimpleName()));
    Assert.assertTrue(!iter0.hasNext());
    Assert.assertEquals(dbusBuf._busyIteratorPool.size(), 1);

    log.info("append a full window of events");
    final DbusEventGenerator generator = new DbusEventGenerator();
    final Vector<DbusEvent> events = new Vector<DbusEvent>();
    generator.generateEvents(5, 5, 120, 39, events);
    injectEventsInBuffer(dbusBuf, events, false);

    log.info("verify we can read all events");
    //old iterator has not changed
    Assert.assertTrue(!iter0.hasNext());

    log.info("verify new iterator");
    InternalEventIterator iter1 =
        dbusBuf.acquireInternalIterator(dbusBuf.getHead(), dbusBuf.getTail(), null);
    Assert.assertEquals(iter1.getCurrentPosition(), dbusBuf.getHead());
    Assert.assertEquals(iter1._iteratorTail.getPosition(), dbusBuf.getTail());
    Assert.assertNotNull(iter1.getIdentifier());
    Assert.assertTrue(iter1.getIdentifier().startsWith(InternalEventIterator.class.getSimpleName()));
    Assert.assertEquals(dbusBuf._busyIteratorPool.size(), 2);
    Assert.assertTrue(iter1.hasNext());

    log.info("make sure we can read all events");
    readAndCompareIteratorEvents(iter1, events, false);

    log.info("add more windows");
    final Vector<DbusEvent> events2 = new Vector<DbusEvent>();
    final DbusEventGenerator generator2 = new DbusEventGenerator(1000);
    generator2.generateEvents(50, 4, 180, 100, events2);
    injectEventsInBuffer(dbusBuf, events2, false);

    log.info("verify old iterators have not changed");
    Assert.assertTrue(!iter0.hasNext());
    Assert.assertTrue(!iter1.hasNext());

    log.info("verify new iterator");
    events.addAll(events2);
    InternalEventIterator iter2 =
        dbusBuf.acquireInternalIterator(dbusBuf.getHead(), dbusBuf.getTail(), null);
    Assert.assertEquals(iter2.getCurrentPosition(), dbusBuf.getHead());
    Assert.assertEquals(iter2._iteratorTail.getPosition(), dbusBuf.getTail());
    Assert.assertNotNull(iter2.getIdentifier());
    Assert.assertTrue(iter2.getIdentifier().startsWith(InternalEventIterator.class.getSimpleName()));
    Assert.assertEquals(dbusBuf._busyIteratorPool.size(), 3);
    Assert.assertTrue(iter2.hasNext());

    log.info("make sure we can read all events");
    readAndCompareIteratorEvents(iter2, events, false);

    iter0.close();
    Assert.assertEquals(dbusBuf._busyIteratorPool.size(), 2);
    iter2.close();
    Assert.assertEquals(dbusBuf._busyIteratorPool.size(), 1);
    iter1.close();
    Assert.assertEquals(dbusBuf._busyIteratorPool.size(), 0);
    log.info("done");
  }

  @SuppressWarnings("unused")
  @Test
  /** Verify that internal iterators are automatically released during GC */
  public void testInternalIteratorGC() throws InvalidConfigException
  {
    final Logger log =
        Logger.getLogger("TestDbusEventBufferIterator.testInternalIteratorGC");
    //log.setLevel(Level.INFO);
    log.info("starting");

    final DbusEventBuffer dbusBuf =
        new DbusEventBuffer(TestDbusEventBuffer.getConfig(
            100000, 100000, 100, 500, AllocationPolicy.HEAP_MEMORY, QueuePolicy.OVERWRITE_ON_WRITE,
            AssertLevel.ALL));

    log.info("acquire empty iterator on empty buffers ");
    InternalEventIterator iter0 =
        dbusBuf.acquireInternalIterator(dbusBuf.getHead(), dbusBuf.getTail(), null);

    log.info("append a full window of events");
    final DbusEventGenerator generator = new DbusEventGenerator();
    final Vector<DbusEvent> events = new Vector<DbusEvent>();
    generator.generateEvents(15, 5, 120, 39, events);
    injectEventsInBuffer(dbusBuf, events, false);

    log.info("acquire oterh iterator on empty buffers ");
    //never move this one
    InternalEventIterator iter1 =
        dbusBuf.acquireInternalIterator(dbusBuf.getHead(), dbusBuf.getTail(), null);

    //move this one a few events
    InternalEventIterator iter2 =
        dbusBuf.acquireInternalIterator(dbusBuf.getHead(), dbusBuf.getTail(), null);
    iter2.next();
    iter2.next();
    iter2.next();

    //move this one to the end
    InternalEventIterator iter3 =
        dbusBuf.acquireInternalIterator(dbusBuf.getHead(), dbusBuf.getTail(), null);
    while (iter3.hasNext()) iter3.next();

    log.info("clear up iterators and try to GC");
    Assert.assertEquals(dbusBuf._busyIteratorPool.size(), 4);
    Assert.assertEquals(dbusBuf._rwLockProvider.getNumReaders(), 4);
    iter0 = null;
    iter1 = null;
    iter2 = null;
    iter3 = null;

    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check()
      {
        System.gc();
        //force release of GC'ed iterators
        dbusBuf.untrackIterator(null);
        log.debug("number of open iterators: " + dbusBuf._busyIteratorPool.size());
        log.debug("number of read locks: " + dbusBuf._rwLockProvider.getNumReaders());
        return 0 == dbusBuf._busyIteratorPool.size() &&
               0 == dbusBuf._rwLockProvider.getNumReaders();
      }
    }, "waiting for iterators and locks to be gone", 15000, log);
  }

  @Test
  /** Verifies that managed iterators are automatically released*/
  public void testManagedIteratorRelease()
  {

  }

  /** Write the events directly to the ByteBuffer because we want to avoid invoking internal
   * listeners which in turn will try to use iterators. A bit of a Catch-22. */
  private static void injectEventsInBuffer(DbusEventBuffer buf, Vector<DbusEvent> events,
                                           boolean updateScnIndex)
  {
    DbusEventAppender appender = new DbusEventAppender(events, buf, null, 1.0, false, -1,
                                                       updateScnIndex);
    appender.run();
  }

  protected void readAndCompareIteratorEvents(InternalEventIterator iter,
                                              List<DbusEvent> expectedEvents,
                                              boolean prefixMatch)
  {
    readAndCompareIteratorEvents(iter, expectedEvents, 0, expectedEvents.size(), prefixMatch);
  }

  protected void readAndCompareIteratorEvents(InternalEventIterator iter,
                                              List<DbusEvent> expectedEvents,
                                              final int startIdx, final int endIdx,
                                              boolean prefixMatch)
  {
    int i = startIdx;
    long lastScn = -1;
    while (iter.hasNext())
    {
      DbusEvent actualEvent = iter.next();
      if (actualEvent.isEndOfPeriodMarker())
      {
        Assert.assertEquals(actualEvent.sequence(), lastScn);
        lastScn = -1;
        continue;
      }
      if (-1 == lastScn)
        lastScn = actualEvent.sequence();
      else
        Assert.assertEquals(actualEvent.sequence(), lastScn);
      if (i >= endIdx)
      {
        if (prefixMatch) break; //we are good
        Assert.fail("unexpected event:" + actualEvent);
      }

      DbusEvent expectedEvent = expectedEvents.get(i);
      Assert.assertEquals(actualEvent, expectedEvent, "event mismatch for index " + i +
                          ";\n expected:" + expectedEvent +";\n   actual: " + actualEvent);
      i++;
    }
    Assert.assertTrue((endIdx == i) && (prefixMatch || !iter.hasNext()));
  }

}
