package com.linkedin.databus.core.util;

import java.util.Arrays;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;


/**
 * brief: A class that allows readers and writers to check out locks for operating on ranges at a time
 * @author sdas
 *
 */
public class RangeBasedReaderWriterLock {
  public static final String MODULE = RangeBasedReaderWriterLock.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);


  public class LockToken {
    public Range getRange() {
      return _id;
    }
    protected Range _id;
    protected LockToken(Range id) {
      _id = id;
    }
  }

  private final PriorityQueue<Range> readerRanges;
  private final ReentrantLock mutex;
  private final Condition writesPossible;
  private final Condition readsPossible;

  private Range writerRange;
  private boolean writerIn;
  private boolean writerWaiting;


  public RangeBasedReaderWriterLock() {
    readerRanges = new PriorityQueue<Range>(100);
    writerRange = new Range(-1, 0);
    writerIn = false;
    mutex = new ReentrantLock();
    writesPossible = mutex.newCondition();
    readsPossible = mutex.newCondition();

  }

  public LockToken acquireReaderLock(long startOffset, long endOffset, BufferPositionParser parser)
  {
	boolean debug = LOG.isDebugEnabled();
    if (debug)
    {
      LOG.debug("Asked to acquire reader lock from " + parser.toString(startOffset) + " to " + parser.toString(endOffset));
    }

    //LOG.info(Thread.currentThread().getName() + "-Asked to acquire reader lock from " + parser.toString(startOffset) + " to " + parser.toString(endOffset));

    Range readerRange = new Range(startOffset, endOffset);
    mutex.lock();
    try
    {
      while (writerIn && writerRange.intersects(readerRange))
      {
        try {
          if ( debug )
          {
        	  LOG.debug("Waiting for reads to be possible since writer is In. Reader Range is :" + readerRange.toString(parser)
        		  + ". Writer Range is :" + writerRange.toString(parser));
          }
          readsPossible.await();
        } catch (InterruptedException e) {
          LOG.info("interrupted");
          return null;
        }
        if ( debug )
        {
        	LOG.info("Waiting for reads to be possible: coming out of wait");
        }
      }
      readerRanges.add(readerRange);
    }
    finally
    {
      mutex.unlock();
    }
    LockToken returnVal = new LockToken(readerRange);
    if (debug)
    {
      LOG.debug("Returning with reader lock from " + parser.toString(startOffset) + " to " + parser.toString(endOffset));
    }
    return returnVal;

  }

  public void shiftReaderLockStart(LockToken lockId, long newStartOffset, BufferPositionParser parser) {
    if (LOG.isDebugEnabled())
    {
      LOG.debug("Being asked to shift reader lock start to "  + parser.toString(newStartOffset));
    }
    mutex.lock();
    try
    {
      readerRanges.remove(lockId._id);
      lockId._id.start = newStartOffset;
      readerRanges.add(lockId._id);
      writesPossible.signal();
    }
    finally
    {
      mutex.unlock();
    }

  }

  public void shiftReaderLockStartIfWriterWaiting(LockToken lockId, long newStartOffset, BufferPositionParser parser) {
    if (LOG.isDebugEnabled())
    {
      LOG.debug("Being asked to shift reader lock start to "  + parser.toString(newStartOffset) + ";writerWaiting = " + writerWaiting);
    }

    if (writerWaiting) // it is okay to check this value outside the protected section, we don't care if we make a mistake
    {
      mutex.lock();
      try
      {
        readerRanges.remove(lockId._id);
        lockId._id.start = newStartOffset;
        readerRanges.add(lockId._id);
        writesPossible.signal();
      }
      finally
      {
        mutex.unlock();
      }
    }
  }

  public void releaseReaderLock(LockToken lockId) {
    if (LOG.isDebugEnabled())
    {
      LOG.debug("Being asked to release reader lock "  + lockId._id);
    }

    mutex.lock();
    try
    {
      boolean readerLockRemoved = readerRanges.remove(lockId._id);
      assert readerLockRemoved;
      writesPossible.signal();
    }
    finally
    {
      mutex.unlock();
    }
  }

  public void acquireWriterLock(long start, long end, BufferPositionParser parser)
  {
    long startOffset = parser.address(start);
    long endOffset = parser.address(end);

    boolean debug = LOG.isDebugEnabled();

    if (debug)
      LOG.debug("Acquiring writer lock from " + parser.toString(start) + " to " + parser.toString(end));

    mutex.lock();

    try
    {
        while (!readerRanges.isEmpty() && Range.contains(startOffset, endOffset, parser.address(readerRanges.peek().start)))
        {
          if ( debug )
          {
        	  LOG.debug("Entering wait because reader(s) exist: Writer Range: ["
        			  + parser.toString(start) + "(Address:" +  parser.toString(startOffset) + ")-"
        			  + parser.toString(end) + "(Address:" + parser.toString(endOffset) + ")]. Nearest Reader Range :"
        			  + readerRanges.peek().toString(parser));
          }

          Iterator<Range> it = readerRanges.iterator();
          while (it.hasNext())
          {
            LOG.info(it.next().toString(parser));
          }
          try
          {
            writerWaiting = true;
            writesPossible.await();

            if ( debug )
            	LOG.debug("Writer coming out of wait");
          }
          catch (InterruptedException e)
          {
            e.printStackTrace();
          }
        }
      writerWaiting = false;
      writerIn = true;
      writerRange.start = start;
      writerRange.end = end;
    }
    finally
    {
      mutex.unlock();
    }

  }

  public void releaseWriterLock(BufferPositionParser parser) {
    if (LOG.isDebugEnabled())
    {
      LOG.debug("Releasing writer lock from " + parser.toString(writerRange.start) + " to " + parser.toString(writerRange.end));
    }

    mutex.lock();
    try
    {
      writerIn = false;
      readsPossible.signalAll();
    }
    finally
    {
      mutex.unlock();
    }
  }

  public String toString(BufferPositionParser parser, boolean doSort)
  {
    StringBuilder strBuilder = new StringBuilder();

    strBuilder.append("[writerIn:" + writerIn).append(",WriterWaiting:");
    strBuilder.append(writerWaiting).append(",WriterRange:").append(writerRange.toString(parser));
    strBuilder.append("\nReader Ranges:\n");

    if ( !doSort)
    {
      Iterator<Range> it = readerRanges.iterator();
      while (it.hasNext())
      {
        strBuilder.append(it.next().toString(parser)).append("\n");
      }
    } else {
      Range[] ranges = new Range[readerRanges.size()];
      readerRanges.toArray(ranges);
      Arrays.sort(ranges);
      for (int i = 0 ; i < ranges.length; i++)
      {
        strBuilder.append(ranges[i].toString(parser)).append("\n");
      }
    }
    return strBuilder.toString();
  }



  // package private getters for unit-tests
  PriorityQueue<Range> getReaderRanges()
  {
    return readerRanges;
  }

  public boolean isWriterIn()
  {
    return writerIn;
  }

  public boolean isWriterWaiting()
  {
    return writerWaiting;
  }

  public Range getWriterRange()
  {
    return writerRange;
  }
}
