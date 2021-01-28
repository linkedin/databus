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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonParseException;

import com.linkedin.databus.core.DbusEventInternalReadable.EventScanStatus;
import com.linkedin.databus.core.RelayEventTraceOption.RelayEventTraceOptionBuilder;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.util.BufferPosition;
import com.linkedin.databus.core.util.BufferPositionParser;
import com.linkedin.databus.core.util.ByteSizeConstants;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus.core.util.Range;
import com.linkedin.databus.core.util.RangeBasedReaderWriterLock;
import com.linkedin.databus.core.util.RangeBasedReaderWriterLock.LockToken;
import com.linkedin.databus.core.util.StringUtils;
import com.linkedin.databus2.core.AssertLevel;
import com.linkedin.databus2.core.DatabusException;
import sun.nio.ch.DirectBuffer;

// TODO Decide if we really want to provide a writable iterator to classes outside of DbusEventBuffer.
public class DbusEventBuffer implements Iterable<DbusEventInternalWritable>,
DbusEventBufferAppendable, DbusEventBufferStreamAppendable
{
  public static final String MODULE = DbusEventBuffer.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);
  public static final String MMAP_META_INFO_FILE_NAME = "metaFile";
  public static final String SESSION_PREFIX = "session_";
  public static final String MMAP_META_INFO_SUFFIX = ".info";
  public static final String PERF_MODULE = MODULE + "Perf";
  public static final Logger PERF_LOG = Logger.getLogger(PERF_MODULE);

  protected static final AtomicLong ITERATORS_COUNTER = new AtomicLong(0);

  private final long _bufferRemoveWaitPeriodSec; // wait period before buffer is removed (if refCount is 0)
  private final double _nanoSecsInMSec = 1000000.0;
  private final DbusEventFactory _eventFactory;

  private byte _eventSerializationVersion = -1;  // TODO:  nuke this; use _eventFactory.getVersion() instead
  private boolean _scnRegress = false;
  private boolean _dropOldEvents = false;


  public static final int MAX_DEBUG_ON_ERROR_ITERATIONS = 2;

  public enum QueuePolicy
  {
    BLOCK_ON_WRITE,
    OVERWRITE_ON_WRITE
  }

  public enum AllocationPolicy
  {
    HEAP_MEMORY,
    DIRECT_MEMORY,
    MMAPPED_MEMORY
  }

  public static String getMmapMetaInfoFileNamePrefix()
  {
    return MMAP_META_INFO_FILE_NAME;
  }

  public static String getSessionPrefix()
  {
    return SESSION_PREFIX;
  }

  /**
   * A map to keep track of the number of trace files created for particular physical partition.
   * The key is PhysicalPartition.Name + "_" + PhysicalPartition.Id/ The value is the number of
   * trace files created. (see also DDSDBUS-222)
   *
   * Access to this field must be synchronized on DbusEventBuffer.class
   */
  static final HashMap<String, Integer> TRACE_FILES_COUNT_MAP = new HashMap<String, Integer>();

  public void setDropOldEvents(boolean val) {
    _dropOldEvents = val;
  }

  protected static class SessionIdGenerator
  {
    private long _lastSessionIdGenerated = -1;
    synchronized String generateSessionId() {
      // just in case - to guarantee uniqueness
      long sessionId;
      while ((sessionId = System.currentTimeMillis()) <= _lastSessionIdGenerated)
        ;
      _lastSessionIdGenerated = sessionId;
      return SESSION_PREFIX + _lastSessionIdGenerated;
    }

  }
  /**
   * Iterator over a fixed range of events in the buffer and with no locking. The remove() method
   * is not supported.
   */
  protected class BaseEventIterator implements Iterator<DbusEventInternalWritable>
  {
    protected final BufferPosition _currentPosition;
    protected final BufferPosition _iteratorTail;
    protected DbusEventInternalWritable _iteratingEvent;  // TODO Make this readable
    protected String _identifier;

    public BaseEventIterator(long head, long tail, String iteratorName)
    {
      _currentPosition = new BufferPosition(_bufferPositionParser,_buffers);
      _currentPosition.setPosition(head);

      _iteratorTail = new BufferPosition(_bufferPositionParser,_buffers);
      _iteratorTail.setPosition(tail);
      _iteratingEvent = _eventFactory.createWritableDbusEvent();
      reset(head, tail, iteratorName);
      trackIterator(this);
    }

    /**
     * Private constructor called by DbusEventBuffer to initialize iterator. The iterator is
     * defined over the events in the byte range [head, tail).
     *
     * @param head          the start gen-id position for the iterator
     * @param tail          the gen-id position of the first byte not to be read by the iterator
     */
    public BaseEventIterator(long head, long tail)
    {
      this(head, tail, null);
    }

    //PUBLIC ITERATOR METHODS
    @Override
    public boolean hasNext()
    {
      // make sure that currentPosition is wrapped around the buffer limit
      boolean result = false;
      if (_currentPosition.init())
      {
        result = false;
      }
      else
      {
        try
        {
          _currentPosition.sanitize();
          _iteratorTail.sanitize();
          if (_currentPosition.getPosition() > _iteratorTail.getPosition())
          {
            LOG.error("unexpected iterator state: this:" + this + " \nbuf: " + DbusEventBuffer.this);
            throw new DatabusRuntimeException("unexpected iterator state: " +  this);
          }
          result = (_currentPosition.getPosition() != _iteratorTail.getPosition());

          if (LOG.isTraceEnabled())
            LOG.trace(" - hasNext = " + result + " currentPosition = " +
                      _currentPosition + " iteratorTail = " + _iteratorTail
                      + "limit = " + _buffers[0].limit() + "tail = " + _tail);
        }
        catch (DatabusRuntimeException e)
        {
          _log.error("error in hasNext for iterator: " + this);
          _log.error("buffer: " + DbusEventBuffer.this);
          throw e;
        }
      }
      return result;
    }

    /**
     * Returns a cached instance. Do not expect the event returned
     * by next() to have a validity lifetime longer than the next next() call
     */
    @Override
    public DbusEventInternalWritable next()
    {
      try
      {
        return next(false);
      }
      catch (InvalidEventException e)
      {
        throw new RuntimeException(e);
      }
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder(getClass().getSimpleName());
      builder.append(": {");
      printInternalState(builder);
      builder.append("}");
      return builder.toString();
    }

    /**
     * Removes all events that have been consumed so far by the iterator
     * This could in the extreme case clear the buffer
     */
    @Override
    public void remove()
    {
      throw new DatabusRuntimeException("not supported");
    }

    //OTHER PUBLIC METHODS
    public void close()
    {
      untrackIterator(this);
    }

    /**
     * Copy local state into the passed in iterator. If a new iterator is allocated, it is
     * caller's responsibility to release it.
     *
     * Does not change destination iterator name
     * @param destinationIterator : the iterator which will be changed to become a copy of this
     */
    public BaseEventIterator copy(BaseEventIterator destinationIterator, String iteratorName)
    {
      if (null == destinationIterator)
      {
        destinationIterator = acquireLockFreeInternalIterator(
            _currentPosition.getPosition(), _iteratorTail.getPosition(), iteratorName);
      }
      destinationIterator.reset(_currentPosition.getPosition(),
                                _iteratorTail.getPosition(),
                                destinationIterator._identifier);

      return destinationIterator;
    }

    public String printExtendedStateInfo()
    {
      String baseState = toString();
      return baseState + "; buffer.head: " + _head + "; buffer.tail: " + _tail
          + "; buffer.currentWritePosition: " + _currentWritePosition;
    }

    //INTERNAL STATE MANAGEMENT
    @Override
    protected void finalize() throws Throwable
    {
      close();
      super.finalize();
    }

    /**
     * Reset the iterator with a new reality w.r.t start and end points
     */
    protected void reset(long head, long tail, String iteratorName)
    {
      assert head >= 0 : "name:" + iteratorName;
      assert head <= tail
          : "head:" + head + "; tail: " + tail + "; name:" + iteratorName;

      _identifier = null != iteratorName ? iteratorName
          : getClass().getSimpleName() + ITERATORS_COUNTER.getAndIncrement();

      _currentPosition.setPosition(head);
      _iteratorTail.setPosition(tail);

      assertPointers();
      assert _iteratingEvent != null;
    }

    protected DbusEventInternalWritable next(boolean validateEvent) throws InvalidEventException
    {
      final long oldPos = _currentPosition.getPosition();

      if (!hasNext())
      {
        throw new NoSuchElementException();
      }
      DbusEventInternalWritable nextEvent = currentEvent();
      if (validateEvent)
      {
        if (!nextEvent.isValid())
        {
          _log.error("invalid event in iterator detected:" + this);
          //if the event is corrupted print a reasonable amount of bytes
          int dumpLen = Math.min(100, Math.max(nextEvent.size(), 100));
          _log.error("current event bytes:" +
                     hexdumpByteBufferContents(_currentPosition.getPosition(), dumpLen));
          if (oldPos >= 0)
          {
            _log.error("previous event bytes @ " + _bufferPositionParser.toString(oldPos, _buffers) + ": " +
                hexdumpByteBufferContents(oldPos, 100));
          }

          throw new InvalidEventException();
        }
      }

      try
      {
        _currentPosition.incrementOffset(nextEvent.size());
      }
      catch (DatabusRuntimeException e)
      {
        if (oldPos >= 0)
        {
          _log.error("previous event bytes @ " + _bufferPositionParser.toString(oldPos, _buffers) + ": " +
              hexdumpByteBufferContents(oldPos, 100));
        }
        _scnIndex.printVerboseString(LOG, Level.ERROR);
        throw new InvalidEventException("error in incrementOffset: " + e.getMessage(), e);
      }
      return nextEvent;
    }


    /**
     * Get the current event pointed to by the iterator
     */
    protected DbusEventInternalWritable currentEvent()
    {
      _currentPosition.sanitize();
      assert _iteratingEvent != null;
      _iteratingEvent = (DbusEventInternalWritable)_iteratingEvent.reset(_buffers[_currentPosition.bufferIndex()], _currentPosition.bufferOffset());
      return _iteratingEvent;
    }

    /**
     * Get the current position pointed to by the iterator
     * Package private to allow helper classes to access currentPosition
     */
    protected long getCurrentPosition()
    {
      _currentPosition.sanitize();
      return _currentPosition.getPosition();
    }

    protected StringBuilder printInternalState(StringBuilder builder)
    {
      if (null == builder) builder = new StringBuilder();
      builder.append("identifier: ");
      builder.append(_identifier);
      builder.append('-');
      builder.append(System.identityHashCode(this));
      builder.append(", currentPosition: ");
      builder.append(_currentPosition);
      builder.append(", iteratorTail: ");
      builder.append(_iteratorTail);
      assert _iteratingEvent != null;
      if (BaseEventIterator.this.hasNext() && _iteratingEvent.isValid())
      {
        builder.append(", iteratingEvent: ");
        builder.append(_iteratingEvent);
      }
      return builder;
    }

    protected void assertPointers()
    {
      assert (_currentPosition.getPosition() >= _head.getPosition())
          : printExtendedStateInfo();
      assert (_iteratorTail.getPosition() <= _currentWritePosition.getPosition()):
          printExtendedStateInfo();
    }

    // PUBLIC GETTERS
    public String getIdentifier()
    {
      return _identifier;
    }

    public DbusEventBuffer getEventBuffer()
    {
      return DbusEventBuffer.this;
    }
  }

  /**
   * Iterator over a fixed range of events in the buffer with locking.
   */
  protected class InternalEventIterator extends BaseEventIterator
  {
    protected LockToken _lockToken;

    public InternalEventIterator(long head, long tail, String iteratorName)
    {
      super(head, tail, iteratorName);
    }

    /**
     * Private constructor called by DbusEventBuffer to initialize iterator. The iterator is
     * defined over the events in the byte range [head, tail).
     *
     * @param head          the start gen-id position for the iterator
     * @param tail          the gen-id position of the first byte not to be read by the iterator
     */
    public InternalEventIterator(long head, long tail)
    {
      this(head, tail, null);
    }

    //OTHER PUBLIC METHODS
    @Override
    public void close()
    {
      releaseReadLock();
      super.close();
    }

    /**
     * Copy local state into the passed in iterator. If a new iterator is allocated, it is
     * caller's responsibility to release it.
     *
     * Does not change destination iterator name
     * @param destinationIterator : the iterator which will be changed to become a copy of this
     */
    public InternalEventIterator copy(InternalEventIterator destinationIterator,
                                      String iteratorName)
    {
      if (null == destinationIterator)
      {
        destinationIterator = acquireInternalIterator(_currentPosition.getPosition(),
                                                      _iteratorTail.getPosition(), iteratorName);
      }
      else
      {
        destinationIterator.reset(_currentPosition.getPosition(),
                                  _iteratorTail.getPosition(),
                                  destinationIterator._identifier);
      }

      return destinationIterator;
    }

    //INTERNAL STATE MANAGEMENT

    /**
     * Reset the iterator with a new reality w.r.t start and end points
     */
    @Override
    protected void reset(long head, long tail, String iteratorName)
    {
      super.reset(head, tail, iteratorName);
      try
      {
        reacquireReadLock();
      }
      catch (InterruptedException e)
      {
        throw new DatabusRuntimeException(e);
      }
      catch (TimeoutException e)
      {
        throw new DatabusRuntimeException(e);
      }
    }

    @Override
    protected StringBuilder printInternalState(StringBuilder builder)
    {
      builder = super.printInternalState(builder);
      if (null != _lockToken)
      {
        builder.append(", lockToken=");
        builder.append(_lockToken);
      }
      return builder;
    }


    // LOCK MANAGEMENT
    /**
     * re-acquire read lock for the range addressed by the iterator
     * @throws TimeoutException
     * @throws InterruptedException
     */
    protected synchronized void reacquireReadLock() throws InterruptedException, TimeoutException
    {
      if (_lockToken != null)
      {
        _rwLockProvider.releaseReaderLock(_lockToken);
        _lockToken = null;
      }

      if (_currentPosition.getPosition() >= 0)
      {
        _lockToken = _rwLockProvider.acquireReaderLock(
            _currentPosition.getPosition(), _iteratorTail.getPosition(), _bufferPositionParser,
            getIdentifier() + "-" + System.identityHashCode(this));
      }
    }

    protected synchronized void releaseReadLock() {
      if (_lockToken != null)
      {
        _rwLockProvider.releaseReaderLock(_lockToken);
        _lockToken = null;
      }
    }

    @Override
    protected void assertPointers()
    {
      super.assertPointers();
      assert (null == _lockToken || _lockToken.getRange().start <= _currentPosition.getPosition()) :
        printExtendedStateInfo();
      assert (null == _lockToken || _lockToken.getRange().end >= _iteratorTail.getPosition()):
          printExtendedStateInfo();
    }
  }

  /**
   * An iterator that will automatically release any resources once it goes past its
   * last element.
   */
  protected class ManagedEventIterator extends InternalEventIterator
  {

    public ManagedEventIterator(long head, long tail, String iteratorName)
    {
      super(head, tail, iteratorName);
    }

    public ManagedEventIterator(long head, long tail)
    {
      this(head, tail, "ManagedEventIterator" + ITERATORS_COUNTER.getAndIncrement());
    }

    @Override
    public boolean hasNext()
    {
      boolean hasMore = super.hasNext();
      if (!hasMore) close();
      return hasMore;
    }

  }

  /**
   * Iterator over events in the buffer. Unlike {@link InternalEventIterator}, this class will
   * sync its state with the underlying buffer and new events added to the buffer will become
   * visible to the iterator.
   */
  public class DbusEventIterator extends InternalEventIterator
  {
    /**
     * Private constructor called by DbusEventBuffer to initialize iterator
     */
    protected DbusEventIterator(long head, long tail)
    {
      this(head, tail, "DbusEventIterator" + ITERATORS_COUNTER.getAndIncrement());
    }

    protected DbusEventIterator(long head, long tail, String iteratorName)
    {
      super(head, tail, iteratorName);
    }

    /**
     * Copy local state into the passed in iterator. If a new iterator is allocated, it is
     * caller's responsibility to release it.
     *
     * Does not change destination iterator name
     * @param destinationIterator : the iterator which will be changed to become a copy of this
     */
    public DbusEventIterator copy(DbusEventIterator destinationIterator, String iteratorName)
    {
      if (null == destinationIterator)
      {
        destinationIterator = new DbusEventIterator(_currentPosition.getPosition(),
                                                    _iteratorTail.getPosition(),
                                                    iteratorName);
      }
      else
      {
        super.copy(destinationIterator, iteratorName);
      }

      return destinationIterator;
    }

    /**
     * Shrinks the iterator tail to the currentPosition
     *
     */
    public void trim()
    {
      _iteratorTail.copy(_currentPosition);
    }

    /**
     * Synchronizes the state of the iterator with the state of the buffer. In particular, the
     * tail of the iterator may lag behind the tail of the buffer as it is updated only
     * explicitly using this method.
     * @throws TimeoutException
     * @throws InterruptedException
     */
    private void copyBufferEndpoints() throws InterruptedException, TimeoutException
    {
      final boolean debugEnabled = _log.isDebugEnabled();
      _queueLock.lock();
      try
      {
        final long oldPos = _currentPosition.getPosition();
        final long oldTail = _iteratorTail.getPosition();
        try
        {
          _iteratorTail.copy(_tail);

          if (_head.getPosition() < 0)
          {
            _currentPosition.setPosition(-1);
          } else if (_currentPosition.getPosition() < 0) {
            _currentPosition.copy(_head);
          }

          if (empty() || _currentPosition.getPosition() < _head.getPosition())
          {
        	  _currentPosition.copy(_head);
          }
        }
        finally
        {
          if (oldPos != _currentPosition.getPosition() || oldTail != _iteratorTail.getPosition())
          {
            if (debugEnabled)
              _log.debug("refreshing iterator: " + this);

            reacquireReadLock();

            if (debugEnabled)
              _log.debug("done refreshing iterator: " + this);
          }
        }
      }
      finally
      {
        _queueLock.unlock();
      }
    }

    /**
     * Allows a reader to wait on the iterator until there is new data to consume or time elapses
     *
     */
    public boolean await(long time, TimeUnit unit)
    {
      final boolean isDebug = LOG.isDebugEnabled();
      // wait for notification that there is data to consume
      _queueLock.lock();
      try
      {
        try
        {
          copyBufferEndpoints();
          boolean available = hasNext();
          if ( !available)
          {
            if (isDebug)
              LOG.debug(toString() + ": waiting for notEmpty" + this);

            available = _notEmpty.await(time,unit);

            if ( isDebug )
              LOG.debug("_notEmpty coming out of await: " + available);

            if ( available )
              copyBufferEndpoints();
          }
          return available;
        }
        catch (InterruptedException e)
        {
          LOG.warn(toString() + ": await/refresh interrupted");
          return false;
        }
        catch (TimeoutException e)
        {
          _log.error(toString() + ": timeout waiting for a lock", e);
          return false;
        }
      }
      finally
      {
        _queueLock.unlock();
      }
    }


    /**
     * Allows a reader (of the buffer) to wait on the iterator until there is
     * new data to consume, ignoring any InterruptedExceptions.
     */
    public void awaitUninterruptibly()
    {
      try
      {
        await(true);
      } catch (InterruptedException ie) {
        //Not expected to reach here
      }
    }

    /**
     * Allows a reader (of the buffer) to wait on the iterator until there is
     * new data to consume.
     */
    public void await() throws InterruptedException
    {
      await(false);
    }

    /**
     * Allows a reader (of the buffer) to wait on the iterator until there is
     * new data to consume.
     */
    public void await(boolean absorbInterrupt) throws InterruptedException
    {
      boolean debugEnabled = _log.isDebugEnabled();

      // wait for notification that there is data to consume
      _queueLock.lock();
      try
      {
        try
        {
          copyBufferEndpoints();
          while (!hasNext())
          {
            if (debugEnabled)
              _log.debug(_identifier+": waiting for notEmpty" + this);

            _notEmpty.await();
            copyBufferEndpoints();
            if (debugEnabled)
              _log.debug("Iterator " + this + " coming out of await");
          }
        }
        catch (InterruptedException e)
        {
          _log.warn(toString() + ": await/refresh interrupted", e);
          if (!absorbInterrupt)
            throw e;
        }
        catch (TimeoutException e)
        {
          throw new DatabusRuntimeException(toString() + ": refresh timed out", e);
        }
      }
      finally
      {
        _queueLock.unlock();
      }
    }

    /**
     * Allows a reader (of the buffer) to wait on the iterator until there is
     * new data to consume.
     * TODO:  merge this with above await()! (with absorbInterrupt = true and new absorbTimeout = true)
     */
    public void awaitInterruptibly()
    {
      boolean debugEnabled = _log.isDebugEnabled();

      // wait for notification that there is data to consume
      _queueLock.lock();
      try
      {
        try
        {
          copyBufferEndpoints();
          final boolean wait = true;  // never changed => pointless:  ??
          while (wait && !hasNext())
          {
            if (debugEnabled)
              _log.debug(_identifier+": waiting for notEmpty" + this);

            _notEmpty.await();
            copyBufferEndpoints();
            if (debugEnabled)
              _log.debug("Iterator " + this + " coming out of await");
          }
        }
        catch (InterruptedException e)
        {
          _log.warn(toString() + ": lock wait/refresh interrupted", e);
        }
        catch (TimeoutException e)
        {
          _log.error(toString() + ": refresh timed out", e);
        }
      }
      finally
      {
        _queueLock.unlock();
      }

    }

    @Override
    public boolean hasNext()
    {
      boolean result = super.hasNext();
      if (!result)
      {
        //looks like we have reached the end -- give one more try in case _iteratorTail was not
        //up-to-date
        try
        {
          copyBufferEndpoints();
        }
        catch (InterruptedException e)
        {
          _log.warn(toString() + ": refresh interrupted");
          return false;
        }
        catch (TimeoutException e)
        {
          _log.error(toString() + ": refresh timed out");
          return false;
        }
        result = super.hasNext();
      }

      return result;
    }

    /**
     * Removes all events that have been consumed so far by the iterator
     * This could in the extreme case clear the buffer
     */
    @Override
    public void remove()
    {
      boolean debugEnabled = LOG.isDebugEnabled();

      if (debugEnabled)
        LOG.debug("Iterator " + _identifier + " hasNext = " + hasNext() +
                  " being asked to remove stuff" + this);

      _rwLockProvider.shiftReaderLockStart(_lockToken, _currentPosition.getPosition(),
                                           _bufferPositionParser);

      acquireWriteLock();

      try
      {
        if(isClosed()) {
          LOG.warn("canceling remove operation on iterator because the buffer is closed. it=" + this);
          throw new DatabusRuntimeException(toString() + " remove canceled.");
        }
        copyBufferEndpoints();

        long newHead = _currentPosition.getPosition();

        //we need to fetch the scn for the new head to pass to ScnIndex.moveHead()
        //TODO a hack that needs to be fixed
        long newScn = -1;
        long newTs = -1;
        if (0 <= newHead && newHead < _tail.getPosition())
        {
          DbusEvent e = currentEvent();
          assert e.isValid();
          newScn = e.sequence();
          newTs = e.timestampInNanos();
        }
        moveHead(newHead, newScn, newTs, debugEnabled);
      }
      catch (InterruptedException e)
      {
        _log.error("buffer locks: " + _rwLockProvider);
        throw new DatabusRuntimeException(toString() + ": refresh interrupted", e);
      }
      catch (TimeoutException e)
      {
        _log.error("remove timeout for iterator " + this + " in buffer " + DbusEventBuffer.this, e);
        throw new DatabusRuntimeException(toString() + ": refresh timed out", e);
      }
      catch (RuntimeException e)
      {
        _log.error("error removing events for iterator " + this + ":" + e.getMessage());
        _log.error("buffer:" + DbusEventBuffer.this);
        throw e;
      }
      catch (AssertionError e)
      {
        _log.error("error removing events for iterator " + this + ":" + e.getMessage());
        _log.error("buffer:" + DbusEventBuffer.this);
        _log.error(hexdumpByteBufferContents(_currentPosition.getPosition(), 200));
        throw e;
      }
      finally
      {
        releaseWriteLock();
      }
    }

    public boolean equivalent(DbusEventIterator lastSuccessfulIterator)
    {
       return (lastSuccessfulIterator != null)  &&
              lastSuccessfulIterator._currentPosition.equals(_currentPosition);
    }

  } // End of Class DbusEventIterator

  private enum WindowState
  {
    INIT,
    STARTED,
    EVENTS_ADDED,
    IN_READ,   // State when puller is in readEvents call
    ENDED,
  }

  private static int MIN_INITIAL_ITERATORS = 30;


  // Locks and state around locks
  private final ReentrantLock _queueLock = new ReentrantLock();
  private final Lock _readBufferLock = new ReentrantLock();
  private final Condition _notFull = _queueLock.newCondition();
  private final Condition _notEmpty = _queueLock.newCondition();
  protected final RangeBasedReaderWriterLock _rwLockProvider;
  private final AtomicInteger readLocked = new AtomicInteger(0);
  private final PhysicalPartition _physicalPartition;

  /**
   * keeps track of the current write position (may not be equal to
   * tail because tail is moved lazily on endEvents call)
   */
  private final BufferPosition _currentWritePosition;

  /**
   * An index that keeps track of scn -> offset mappings
   */
  private final ScnIndex _scnIndex;


  /**
   * A list of ByteBuffers to allow DbusEventBuffer to grow beyond a single ByteBuffer
   * size limitation (2GB).
   * In the future, we might want to use this to support dynamically resizing the buffer.
   * Invariants for each buffer:
   * (1) if it has been completely filled in, then the limit() points right after the last written
   * byte; otherwise, limit() == capacity()
   */
  private final ByteBuffer[] _buffers;
  // Maximum size of an individual buffer
  private final int _maxBufferSize;

  /**
   * the initial size of the event staging buffer used to validate incoming events
   */
  private final int _initReadBufferSize;
  /**
   * The maximum allowed event size
   */
  private final int _maxEventSize;

  /**
   * head and tail of the whole buffer (abstracting away the fact that there are multiple buffers involved)
   * head points to the first valid (oldest) event in the oldest event window in the buffer
   * tail points to the next writable location in the buffer
   *
   * Initially : head starts off as 0, tail starts off at 0
   */
  private final  BufferPosition _head;
  private final  BufferPosition _tail;

  /** A flag if buffer persistence is enabled */
  final boolean _bufferPersistenceEnabled;

  // When head == tail, use _empty to distinguish between empty and full buffer
  private boolean _empty;

  private boolean _isClosed = false;

  protected boolean isClosed()
  {
    // has to be under lock
    if(!_queueLock.isLocked())
      throw new RuntimeException("checking if buffer is closed should be done under _queueLock");

    return _isClosed;
  }

  protected void setClosed() throws DatabusException
  {
    acquireWriteLock();
    try {
      if(_isClosed) {
        throw new DatabusException("closing already closed buffer");
      }
      _isClosed = true;
    } finally {
      releaseWriteLock();
    }
  }

  /** Allocated memory for the buffer */
  private final long _allocatedSize;

  private final HashSet<InternalDatabusEventsListener> _internalListeners =
      new HashSet<InternalDatabusEventsListener>();
  private final AllocationPolicy _allocationPolicy;
  private final QueuePolicy _queueingPolicy;
  private File _mmapSessionDirectory;
  private File _mmapDirectory;
  private String _sessionId;

  // Cached objects to prevent frequent 'new'-s

  // Pool of iterators
  protected final Set<WeakReference<BaseEventIterator>> _busyIteratorPool =
          new HashSet<WeakReference<BaseEventIterator>>(MIN_INITIAL_ITERATORS);

  /** Should the buffer check asserts and how strict */
  private final AssertLevel _assertLevel;

  // State to support event window "transactions"
  private WindowState _eventState = WindowState.INIT;
  private final BufferPosition _eventStartIndex;
  private int _numEventsInWindow;
  /** last scn written; the max Scn */
  private volatile long _lastWrittenSequence;
  /** the scn for which we've seen the EOW event */
  private volatile long _seenEndOfPeriodScn = -1;
  /** first scn after which stream requests can be served from the buffer
   * See DDS-699 for description*/
  private volatile long _prevScn;
  /** timestamp of first event **/


  private final BufferPositionParser _bufferPositionParser;
  /** timestamp of first data event in buffer; at head **/
  private volatile long _timestampOfFirstEvent;
  /** SCN of first event */
  private volatile long _minScn;

  /** timestamp of latest data event of buffer **/
  private volatile long _timestampOfLatestDataEvent = 0;

  /** The last generated session id; we keep track of those to avoid duplicates */
  private static SessionIdGenerator _sessionIdGenerator = new SessionIdGenerator();

  // Ref counting for the buffer
  private int _refCount = 0;
  private long _tsRefCounterUpdate = Long.MAX_VALUE;

  //instance logger
  final Logger _log;

  public PhysicalPartition getPhysicalPartition() {
	  return _physicalPartition;
  }

  public synchronized void increaseRefCounter() {
    _refCount ++;
    _tsRefCounterUpdate = System.currentTimeMillis();
  }
  public synchronized void decreaseRefCounter() {
    _refCount --;
    _tsRefCounterUpdate = System.currentTimeMillis();
  }
  public synchronized boolean shouldBeRemoved(boolean now) {
    if(_refCount > 0)
      return false;

    if(now)
      return true;

    return (System.currentTimeMillis() - _tsRefCounterUpdate) > _bufferRemoveWaitPeriodSec*1000;
  }
  public synchronized int getRefCount() {
    return _refCount;
  }

  public void forceReleaseDirectMemory()
  {
    clear();
    for (ByteBuffer buf : _buffers)
    {
      if (buf.isDirect())
      {
        ((DirectBuffer)buf).cleaner().clean();
      }
    }
  }

  /**
   * Clears the buffer. Should be only called by consumers who are using the buffer
   * like a "producer - consumer" queue.
   */
  public void clear() {
    clearAndStart(false, -1);
  }

  private void clearAndStart(boolean start, long prevScn)
  {
    acquireWriteLock();

    try {
      if(isClosed()) {
        LOG.warn("canceling clearAndStart because the buffer is being closed");
        return;
      }
      lockFreeClear();
      _scnIndex.clear();
      if (start)
      {
        this.start(prevScn);
      }
      _empty=true;
    } finally {
      releaseWriteLock();
    }
  }

  public void reset(long prevScn)
  {
    clearAndStart(true, prevScn);
  }

  public long getTimestampOfFirstEvent() {
    return _timestampOfFirstEvent;
  }

  /**
   * Clears the buffer, assumes that requisite locks have
   * been obtained outside this method
   *
   */
  private void lockFreeClear() {
    _scnIndex.clear();
    _head.setPosition(0L);
    _tail.setPosition(0L);
    _currentWritePosition.setPosition(0L);
    _prevScn=-1L;
    _empty =true;
    _lastWrittenSequence = -1L;
    _timestampOfFirstEvent = 0;
    // TODO (medium) DDSDBUS-56:
    // what happens to the iterators that might be iterating over this buffer?
    // should we call a notifyClear() on them?
    for (ByteBuffer buf: _buffers)
    {
      buf.clear();
    }
    _notFull.signalAll();
    //		notifyIterators(head, tail);
  }


  // Called by tests
  public DbusEventBuffer(Config config) throws InvalidConfigException
  {
    this(config.build());
  }

  // Called by tests that test the client.  Note that this version cannot be
  // used by Espresso tests since the event factory here defaults to BIG_ENDIAN
  // order, whereas Espresso requires LITTLE_ENDIAN == BinaryProtocol.BYTE_ORDER.
  // (Such tests can use the three-arg constructor with pPartition set to null.)
  public DbusEventBuffer(StaticConfig config)
  {
    this(config.getMaxSize(), config.getMaxIndividualBufferSize(), config.getScnIndexSize(),
         config.getReadBufferSize(), config.getMaxEventSize(),
         config.getAllocationPolicy(), config.getMmapDirectory(),
         config.getQueuePolicy(), config.getTrace(), null, config.getAssertLevel(),
         config.getBufferRemoveWaitPeriod(), config.getRestoreMMappedBuffers(),
         config.getRestoreMMappedBuffersValidateEvents(), config.isEnableScnIndex(), new DbusEventV1Factory());
  }

  public DbusEventBuffer(StaticConfig config, PhysicalPartition pPartition, DbusEventFactory eventFactory)
  {
    this(config.getMaxSize(), config.getMaxIndividualBufferSize(), config.getScnIndexSize(),
         config.getReadBufferSize(), config.getMaxEventSize(),
         config.getAllocationPolicy(), config.getMmapDirectory(),
         config.getQueuePolicy(), config.getTrace(), pPartition, config.getAssertLevel(),
         config.getBufferRemoveWaitPeriod(), config.getRestoreMMappedBuffers(),
         config.getRestoreMMappedBuffersValidateEvents(), config.isEnableScnIndex(), eventFactory);
  }

  /**
   * Fine-grained constructor.
   */
  public DbusEventBuffer(long maxEventBufferSize, int maxIndividualBufferSize, int maxIndexSize,
                         int initReadBufferSize, int maxEventSize,
                         AllocationPolicy allocationPolicy, File mmapDirectory,
                         QueuePolicy queuePolicy, RelayEventTraceOption traceOption,
                         PhysicalPartition physicalPartition, AssertLevel assertLevel, long bufRemovalWaitPeriod,
                         boolean restoreBuffers, boolean validateEventesInRestoredBuffers,
                         boolean enableScnIndex, DbusEventFactory eventFactory)
  {
    //TODO replace all occurrences of LOG with _log so we get partition info
    _log = (null == physicalPartition) ? LOG :
           Logger.getLogger(MODULE + "." + physicalPartition.toSimpleString());

    _assertLevel = assertLevel;
    LOG.info("DbusEventBuffer starting up with " + "eventBufferSize = " + maxEventBufferSize +
             ", maxIndividualBufferSize = " + maxIndividualBufferSize +
             ", maxIndexSize = "+ maxIndexSize +
             ", initReadBufferSize = " + initReadBufferSize + ", maxEventSize=" + maxEventSize +
             ", allocationPolicy = " +
             allocationPolicy.toString() + ", mmapDirectory = " + mmapDirectory.getAbsolutePath() +
             ",queuePolicy = " + queuePolicy +
             ", eventTraceOption = " + traceOption.getOption() + ", needFileSuffix = " +
             traceOption.isNeedFileSuffix() + ", assertLevel=" + _assertLevel +
             ", bufRemovalWaitPeriod=" + bufRemovalWaitPeriod + ", restoreBuffers=" + restoreBuffers);

    _eventFactory = eventFactory;
    _eventSerializationVersion = eventFactory.getVersion();
    _bufferPersistenceEnabled = restoreBuffers;
    _queueingPolicy = queuePolicy;
    _allocationPolicy = allocationPolicy;
    List<ByteBuffer> buffers = new ArrayList<ByteBuffer>();
    _maxBufferSize = maxIndividualBufferSize;
    _empty = true;
    _lastWrittenSequence = -1;
    _prevScn=-1L;
    _timestampOfFirstEvent = 0;
    _timestampOfLatestDataEvent = 0;
    _bufferRemoveWaitPeriodSec = bufRemovalWaitPeriod;
    _physicalPartition = (physicalPartition == null)? new PhysicalPartition(0, "default") : physicalPartition;

    // file to read meta info for saved buffers (if any), MetaInfo constructor doesn't create/open any actual file
    DbusEventBufferMetaInfo mi = null;

    // in case of MMAPED memory - see if there is a meta file, and if there is - read from it
    if(allocationPolicy == AllocationPolicy.MMAPPED_MEMORY) {
      _sessionId = _sessionIdGenerator.generateSessionId(); // new session
      File metaInfoFile = new File(mmapDirectory, metaFileName());
      if(restoreBuffers) {
        if(!metaInfoFile.exists()) {
          LOG.warn("restoreBuffers flag is specified, but the file " + metaInfoFile + " doesn't exist");
        } else if((System.currentTimeMillis() - metaInfoFile.lastModified()) > _bufferRemoveWaitPeriodSec*1000) {
          LOG.warn("restoreBuffers flag is specified, but the file " + metaInfoFile + " is older than " + _bufferRemoveWaitPeriodSec + " secs");
        } else {
          try{
            mi = new DbusEventBufferMetaInfo(metaInfoFile);
            mi.loadMetaInfo();
            if(mi.isValid()) {
              _sessionId = mi.getSessionId(); // figure out what session directory to use for the content of the buffers
              LOG.info("found file " + mi.toString() + "; will reuse session = " + _sessionId);

              validateMetaData(maxEventBufferSize, mi);
            } else {
              LOG.warn("cannot restore from file " + metaInfoFile);
            }

          } catch (DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException e) {
            throw new RuntimeException(e);
          }
        }
      }

      // init the directories.
      if (!mmapDirectory.exists() && !mmapDirectory.mkdirs()) {
        throw new RuntimeException("Could not create directory " + _mmapDirectory.getAbsolutePath());
      }

      _mmapSessionDirectory = new File(mmapDirectory, _sessionId);
      if (!_mmapSessionDirectory.exists() && !_mmapSessionDirectory.mkdirs()) {
        throw new RuntimeException("Could not create directory " + _mmapSessionDirectory.getAbsolutePath());
      } else {
        LOG.info("MMapsessiondir = " + _mmapSessionDirectory.getAbsolutePath());
      }

      _mmapDirectory = mmapDirectory;
      if (!restoreBuffers) {
        LOG.info("restoreBuffers is false => will delete mmap session directory " + _mmapSessionDirectory + " on exit");
        _mmapSessionDirectory.deleteOnExit();
      }
    }

    LOG.debug("Will allocate a total of " + maxEventBufferSize + " bytes");
    long allocatedSize = 0;
    while (allocatedSize < maxEventBufferSize)
    {
      int nextSize = (int) Math.min(_maxBufferSize, (maxEventBufferSize-allocatedSize));
      if (LOG.isDebugEnabled())
        LOG.debug("Will allocate a buffer of size " + nextSize + " bytes with allocationPolicy = " + allocationPolicy.toString());
      ByteBuffer buffer = allocateByteBuffer(nextSize, _eventFactory.getByteOrder(), allocationPolicy,
                                             restoreBuffers, _mmapSessionDirectory,
                                             new File(_mmapSessionDirectory, "writeBuffer_"+ buffers.size()));
      buffers.add(buffer);
      allocatedSize += nextSize;
    }
    LOG.info("Allocated a total of " + allocatedSize + " bytes into " + buffers.size() + " buffers");
    _allocatedSize = allocatedSize;

    _buffers = new ByteBuffer[buffers.size()];
    buffers.toArray(_buffers);
    if(mi != null && mi.isValid()) {
      try {
        setAndValidateMMappedBuffers(mi);
      } catch (DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException e) {
        throw new RuntimeException(e);
      }
    }

    _maxEventSize = maxEventSize;
    if (initReadBufferSize <= getMaxReadBufferCapacity())
    {
      _initReadBufferSize = initReadBufferSize;
    }
    else
    {
      _initReadBufferSize = getMaxReadBufferCapacity();
      _log.warn(String.format("Initial event staging buffer size %d > than max possible %d event size; " +
      		                  "resetting to %d ", _initReadBufferSize, getMaxReadBufferCapacity(),
      		                  getMaxReadBufferCapacity()));
    }
    if (0 >= _initReadBufferSize)
    {
      throw new DatabusRuntimeException("invalid initial event staging buffer size: " + _initReadBufferSize);
    }

    _bufferPositionParser = new BufferPositionParser((int)(Math.min(_maxBufferSize, maxEventBufferSize)), buffers.size());

    _scnIndex = new ScnIndex(maxIndexSize, maxEventBufferSize, _maxBufferSize, _bufferPositionParser,
                             allocationPolicy, restoreBuffers, _mmapSessionDirectory, _assertLevel,
                             enableScnIndex, _eventFactory.getByteOrder());

    _head = new BufferPosition(_bufferPositionParser, _buffers);
    _tail = new BufferPosition(_bufferPositionParser, _buffers);
    _currentWritePosition = new BufferPosition(_bufferPositionParser, _buffers);
    _eventStartIndex = new BufferPosition(_bufferPositionParser, _buffers);

    _rwLockProvider = new RangeBasedReaderWriterLock();

    LOG.info ( "Trace Relay Option : " + traceOption.getOption() + " physicalPartition:" + _physicalPartition.getName() + " pSourceName:" + _physicalPartition );
    if (RelayEventTraceOption.Option.file == traceOption.getOption())
    {
      InternalDatabusEventsListener traceListener = createEventTraceListener(traceOption,
                                      _physicalPartition.getName() + "_" + _physicalPartition.getId());
      _internalListeners.add(traceListener);
    }

    if(mi != null && mi.isValid())
      try {
        initBuffersWithMetaInfo(mi); // init some of the DbusEvent Buffer fields from MetaFile if available
        if(validateEventesInRestoredBuffers)
          validateEventsInBuffer();
      } catch (DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException e) {
        throw new RuntimeException(e);
      }
    else {
      clear(); // clears all stuff
      resetWindowState(); // reset if new buffers only
    }

    // invalidate metainfo to avoid accidental loading of old data
    File metaInfo = new File(mmapDirectory, metaFileName());
    if(metaInfo.exists()) {
      File renameTo = new File(metaInfo.getAbsoluteFile() + "." + System.currentTimeMillis());
      if(metaInfo.renameTo(renameTo))
        LOG.warn("existing metaInfoFile " + metaInfo + " found. moving it to " + renameTo);
      else
        LOG.error("failed to move existing metaInfoFile " + metaInfo + " to " + renameTo + ". This may cause buffer to load this file if it gets restarted!");
    }
    if (enableScnIndex && _scnIndex.isEmpty()) {
      _scnIndex.setUpdateOnNext(true);
    }
    _queueLock.lock();
    updateFirstEventMetadata();
    _queueLock.unlock();

  }

  String metaFileName() {
    return MMAP_META_INFO_FILE_NAME + "." + _physicalPartition.getName() + "_" + _physicalPartition.getId();
  }

  /**
   * go over all the ByteBuffers and validate them
   * @throws DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException
   */
  private void setAndValidateMMappedBuffers(DbusEventBufferMetaInfo mi)
      throws DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException
  {
    // set buffer info - pos and limit
    DbusEventBufferMetaInfo.BufferInfo [] bufsInfo = null;
    bufsInfo = mi.getBuffersInfo();

    int i = 0;
    for (ByteBuffer buffer : _buffers) {
      DbusEventBufferMetaInfo.BufferInfo bi = bufsInfo[i];

      buffer.position(bi.getPos());
      buffer.limit(bi.getLimit());

      // validate
      if (buffer.position() > buffer.limit()  ||
          buffer.limit() > buffer.capacity()  ||
          buffer.capacity() != bi.getCapacity()) {
        String msg = "ByteBuffers don't match: i=" + i + "; pos=" + buffer.position() +
            "; limit=" + buffer.limit() + "; capacity=" + buffer.capacity() + "; miCapacity=" + bi.getCapacity();
        throw new DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException(mi, msg);
      }
      i++;
    }
    _log.info("successfully validated all " + i + " mmapped buffers");
  }

  /**
   * go thru all the events in the buffer and see that they are valid
   * also compare the event at the end of the buffer with _lastWrittenSeq
   * @throws DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException
   */
  public void validateEventsInBuffer() throws DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException {
    // go over all the events and validate them

    DbusEventIterator eventIterator = acquireIterator("validateEventsIterator");

    DbusEvent e = null;
    int num = 0;
    boolean first = true;
    long firstScn = -1;
    long start = System.currentTimeMillis();
    try {
      while (eventIterator.hasNext()) {
        e = eventIterator.next();
        num ++;
        if(e.isValid()) {
          //LOG.info("event " + e + " is valid");
          if(first) {
            firstScn = e.sequence();
            first = false;
          }
        } else {
          LOG.error("event " + e + " is not valid");
          throw new DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException(
                "Buffer validation failed. There are some invalid events");
        }
      }
      long time = System.currentTimeMillis() - start;
      LOG.info("scanned " + num + " events in " + time + " msec. event at the end of the buffer: " + e);
      LOG.info("firstScn = " + firstScn + "; _lastWrittenSequence = " + _lastWrittenSequence +  "; minScn = " + getMinScn());
      if(e != null && _lastWrittenSequence != e.sequence()) {
        throw new DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException(
              "Buffer validation failed. e.sequence=" + e.sequence() + " and _lastWrittenSeq=" + _lastWrittenSequence);
      }
    } finally {
      releaseIterator(eventIterator);
    }
  }

  /**
   * compare and match data between the metaFile and passed in in the constructor
   * @param mi
   * @throws DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException
   */
  private void validateMetaData(long maxTotalEventBufferSize, DbusEventBufferMetaInfo mi) throws DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException {
    // figure out number of buffers we are going to allocate
    long numBuffs = maxTotalEventBufferSize/_maxBufferSize;
    if(maxTotalEventBufferSize % _maxBufferSize > 0) numBuffs++;  // calculate number of ByteBuffers we will have
    long miNumBuffs = mi.getLong(DbusEventBufferMetaInfo.NUM_BYTE_BUFFER);
    if(miNumBuffs != numBuffs) {
      throw new DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException(mi ,
            "Invalid number of ByteBuffers in meta file:" + miNumBuffs + "(expected =" + numBuffs + ")");
    }
    // individual buffer size
    long miBufSize = mi.getLong(DbusEventBufferMetaInfo.MAX_BUFFER_SIZE);
    if(miBufSize != _maxBufferSize)
      throw new DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException(mi,
            "Invalid maxBufferSize in meta file:" + miBufSize + "(expected =" + _maxBufferSize + ")");

    // _allocatedSize - validate against real buffers
     long allocatedSize = mi.getLong(DbusEventBufferMetaInfo.ALLOCATED_SIZE);
     if(maxTotalEventBufferSize != allocatedSize)
       throw new DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException(mi,
             "Invalid maxEventBufferSize in meta file:" + allocatedSize + "(expected =" + maxTotalEventBufferSize + ")");

  }

  public static ByteBuffer allocateByteBuffer(int size, ByteOrder byteOrder,
                                       AllocationPolicy allocationPolicy,
                                       boolean restoreBuffers,
                                       File mmapSessionDir,
                                       File mmapFile)
  {
    ByteBuffer buffer = null;

    switch (allocationPolicy)
    {
    case HEAP_MEMORY:
      buffer = ByteBuffer.allocate(size).order(byteOrder);
      break;
    case DIRECT_MEMORY:
      buffer = ByteBuffer.allocateDirect(size).order(byteOrder);
      break;
    case MMAPPED_MEMORY:
    default:
      // expect that dirs are already created and initialized
      if (!mmapSessionDir.exists()) {
        throw new RuntimeException(mmapSessionDir.getAbsolutePath() + " doesn't exist");
      }

      if (restoreBuffers) {
        if (!mmapFile.exists()) {
          LOG.warn("restoreBuffers is true, but file " + mmapFile + " doesn't exist");
        } else {
          LOG.info("restoring buffer from " + mmapFile);
        }
      } else {
        if (mmapFile.exists()) {
          // this path should never happen (only if the generated session ID accidentally matches a previous one)
          LOG.info("restoreBuffers is false; deleting existing mmap file " + mmapFile);
          if (!mmapFile.delete()) {
            throw new RuntimeException("deletion of file failed: " + mmapFile.getAbsolutePath());
          }
        }
        LOG.info("restoreBuffers is false => will delete new mmap file " + mmapFile + " on exit");
        mmapFile.deleteOnExit(); // in case we don't need files later.
      }

      try {
        FileChannel rwChannel = new RandomAccessFile(mmapFile, "rw").getChannel();
        buffer = rwChannel.map(FileChannel.MapMode.READ_WRITE, 0, size).order(byteOrder);
        rwChannel.close();
      } catch (FileNotFoundException e) {
        throw new RuntimeException("[should never happen!] can't find mmap file/dir " + mmapFile.getAbsolutePath(), e);
      } catch (IOException e) {
        throw new RuntimeException("unable to initialize mmap file " + mmapFile, e);
      }
    }
    return buffer;
  }

  private InternalDatabusEventsListener createEventTraceListener(RelayEventTraceOption traceOption,
                                                                 String pSourceName)
  {
    // for now the trace option is file-based. if needed, we can add one that print events to std out
    String fileName = traceOption.getFilename();
    if(traceOption.isNeedFileSuffix())
    {
      synchronized (DbusEventBuffer.class)
      {
        Integer traceNum = TRACE_FILES_COUNT_MAP.get(pSourceName);

        fileName += "." + pSourceName + (null != traceNum ? "." + traceNum : "");
        if (null == traceNum) TRACE_FILES_COUNT_MAP.put(pSourceName, 1);
        else TRACE_FILES_COUNT_MAP.put(pSourceName, traceNum + 1);
      }
    }
    LOG.info ( "Trace File Name is : " + fileName + "  pSourceName: " + pSourceName );
    FileBasedEventTrackingCallback cbk = new FileBasedEventTrackingCallback(fileName, traceOption.isAppendOnly());
    try
    {
      cbk.init();
    }
    catch (IOException e)
    {
      throw new RuntimeException("unable to initialize FileBasedEventTrackingCallbock: Filename is :" + fileName, e);
    }
    return cbk;
  }

  /**
   * Constructor
   * @param maxEventBufferSize
   * @param maxIndexSize
   * @param policy                   the queueing policy for the buffer:
   * @param statsCollector           the mbean to collect statistics; if null, no stats are
   *                                   collected
   */
  /**
   * public DbusEventBuffer(long maxEventBufferSize, int maxIndexSize, QueuePolicy policy) {
    Config config = new Config();
    config._maxSize = maxEventBufferSize;
    config._scnIndexSize = maxIndexSize;
    config._queuePolicy = policy;

    StaticConfig staticConfig  = config.build();
    LOG.debug("In main constructor with maxEventBufferSize = " + maxEventBufferSize);
    allocateInternals(staticConfig);
  }
   **/

  RangeBasedReaderWriterLock getRwLockProvider()
  {
    return _rwLockProvider;
  }

  /** Set scn immediately preceding the minScn; */
  public void setPrevScn(long scn)
  {
    if (_log.isDebugEnabled())
      _log.info("setting prevScn to: " + scn);
    _prevScn = scn;
  }

  /** get scn immediately preceding the minScn ;
   * - scn=sinceScn ; offset=-1 is equivalent of flexible checkpoints w.r.t. behaviour of /stream
   * - scn=sinceScn; with offset=0 will yield an scn not found; as data in this scn is no longer contained in the buffer
   *  */
  @Override
  public long getPrevScn() {
    return _prevScn;
  }

  /**
   * Get the windowScn for the oldest event window in the buffer
   */
  @Override
  public long getMinScn() {
    return _minScn;
  }

  private void resetWindowState()
  {
    _eventState = WindowState.INIT;
    _numEventsInWindow = 0;
    _eventStartIndex.setPosition(-1);
  }

  /* (non-Javadoc)
   * @see com.linkedin.databus.core.DbusEventBufferAppendable#start(long startScn)
   */
  @Override
  public void start(long startScn)
  {
    assert((_eventState == WindowState.INIT) || (_eventState == WindowState.ENDED));
    startEvents();
    endEvents(startScn);
    this.setPrevScn(startScn);
  }

  /* (non-Javadoc)
   * @see com.linkedin.databus.core.DbusEventBufferAppendable#startEvents()
   */
  @Override
  public void startEvents() {
    assert ((_eventState == WindowState.INIT) || (_eventState == WindowState.ENDED));
    acquireWriteLock();
    try {
      if(isClosed()) {
        throw new DatabusRuntimeException("attempting startEvents for a closed buffer");
      }
      resetWindowState();
      _eventState = WindowState.STARTED;
      // set currentWritePosition to tail.
      // This allows us to silently rollback any writes we did past the tail but never called endEvents() on.
      long tailPosition = _tail.getPosition();
      _currentWritePosition.setPosition( ((tailPosition > 0) ? tailPosition:0));
    } finally {
      releaseWriteLock();
    }
  }



  /* (non-Javadoc)
   * @see com.linkedin.databus.core.DbusEventBufferAppendable#appendEvent(com.linkedin.databus.core.DbusEventKey, long, short, long, short, byte[], byte[], boolean)
   */
  @Override
  public boolean appendEvent(DbusEventKey key, short pPartitionId, short lPartitionId,
                             long timeStamp, short srcId, byte[] schemaId, byte[] value,
                             boolean enableTracing)
  {
    return appendEvent(key, pPartitionId, lPartitionId, timeStamp, srcId, schemaId, value, enableTracing, null);
  }

  @Override
  @Deprecated
  public boolean appendEvent(
                             DbusEventKey key,
                             long sequenceId,
                             short pPartitionId,
                             short logicalPartitionId,
                             long timeStampInNanos,
                             short srcId,
                             byte[] schemaId,
                             byte[] value,
                             boolean enableTracing,
                             DbusEventsStatisticsCollector statsCollector)
  {
    throw new RuntimeException("This method is not implemented!!!");
  }

  /* (non-Javadoc)
   * @see com.linkedin.databus.core.DbusEventBufferAppendable#appendEvent(com.linkedin.databus.core.DbusEventKey, long, short, long, short, byte[], byte[], boolean, com.linkedin.databus.monitoring.mbean.DbusEventsStatisticsCollector)
   */
  @Override
  public boolean appendEvent(DbusEventKey key, short pPartitionId, short lPartitionId,
                             long timeStamp, short srcId, byte[] schemaId, byte[] value,
                             boolean enableTracing, DbusEventsStatisticsCollector statsCollector)
  {
    return appendEvent(key, pPartitionId, lPartitionId, timeStamp, srcId, schemaId, value, enableTracing, false, statsCollector);
  }

  @Override
  public boolean appendEvent(DbusEventKey key, short pPartitionId, short lPartitionId,
                             long timeStamp, short srcId, byte[] schemaId, byte[] value,
                             boolean enableTracing, boolean isReplicated, DbusEventsStatisticsCollector statsCollector)
  {
    DbusEventInfo eventInfo = new DbusEventInfo(null, 0L, pPartitionId, lPartitionId,
                                                timeStamp, srcId, schemaId, value, enableTracing,
                                                false);
    eventInfo.setEventSerializationVersion(DbusEventFactory.DBUS_EVENT_V1);  // make this explicit
    // this single change causes 5 failures in TestDbusEventBufferMult:
    //eventInfo.setEventSerializationVersion(getEventSerializationVersion()); // use _eventFactory version for consistency
    eventInfo.setReplicated(isReplicated);

    return appendEvent(key, eventInfo, statsCollector);
  }

  @Override
  public boolean appendEvent(DbusEventKey key,
                             DbusEventInfo eventInfo,
                             DbusEventsStatisticsCollector statsCollector)
  {
    boolean isDebugEnabled = LOG.isDebugEnabled();
    acquireWriteLock();
    try {
      assert((_eventState == WindowState.STARTED) || (_eventState == WindowState.EVENTS_ADDED));
      try
      {
        _scnIndex.assertHeadPosition(_head.getRealPosition());
        _bufferPositionParser.assertSpan(_head.getPosition(), _currentWritePosition.getPosition(),isDebugEnabled);
      } catch (RuntimeException re) {
        LOG.fatal("Got runtime Exception :", re);
        LOG.fatal("Event Buffer is :" + toString());
        throw re;
      }

      if(isClosed()) {
        throw new DatabusRuntimeException("refusing to append event, because the buffer is closed");
      }

      final int expNumBytesWritten = DbusEventFactory.computeEventLength(key, eventInfo);
      prepareForAppend(expNumBytesWritten);

      if (_eventState == WindowState.STARTED) {
        //We set eventStartIndex here because _currentWritePosition is not finalized before
        //the call to prepareForAppend
        _eventStartIndex.copy(_currentWritePosition);
      }

      if (isDebugEnabled)
      {
        LOG.debug("serializingEvent at position " + _currentWritePosition.toString());
        LOG.debug("PhysicalPartition passed in=" + eventInfo.getpPartitionId() + "; from the buffer = "
                  + _physicalPartition.getId().shortValue());
      }

      eventInfo.setSequenceId(0L);
      eventInfo.setpPartitionId(_physicalPartition.getId().shortValue());
      eventInfo.setAutocommit(false);
      int bytesWritten = DbusEventFactory.serializeEvent(key,
                                                         _buffers[_currentWritePosition.bufferIndex()],
                                                         eventInfo);

      //prepareForAppend makes decision to move Head depending upon expNumBytesWritten
      if ( bytesWritten != expNumBytesWritten)
      {
    	  String msg = "Actual Bytes Written was :" + bytesWritten +
    	               ", Expected to Write :" + expNumBytesWritten;
    	  LOG.fatal(msg);
    	  LOG.fatal("Event Buffer is :" + toString());
    	  throw new DatabusRuntimeException(msg);
      }

      final long newWritePos =
          _bufferPositionParser.incrementOffset(_currentWritePosition.getPosition(), bytesWritten,
                                                _buffers);
      moveCurrentWritePosition(newWritePos);

      _eventState = WindowState.EVENTS_ADDED;
      _numEventsInWindow++;
      _timestampOfLatestDataEvent = Math.max(_timestampOfLatestDataEvent,
                    eventInfo.getTimeStampInNanos());
    }
    catch (KeyTypeNotImplementedException ex)
    {
      if (null != statsCollector)
        statsCollector.registerEventError(DbusEventInternalReadable.EventScanStatus.ERR);
      throw new DatabusRuntimeException(ex);
    }
    finally
    {
      /*
       * Ensuring that any locks that might be held are released safely before
       * returning from this method
       */
      releaseWriteLock();
      finalizeAppend();
    }
    return true;
  }

  /**
   * Sets up the buffer state to prepare for appending an event.
   * This includes
   * a) moving the head far enough so that the new event does not overwrite it.
   *    - this also implies moving the head for the ScnIndex to keep it in lock-step with the buffer
   * b) moving the currentWritePosition to the correct location so that the entire event will fit
   *    into the selected ByteBuffer
   * @param dbusEventSize has the size of the event that will be appended.
   * @throws com.linkedin.databus.core.KeyTypeNotImplementedException
   */
  private void prepareForAppend(final int dbusEventSize)
  throws KeyTypeNotImplementedException
  {
    boolean isDebugEnabled = LOG.isDebugEnabled();

    _queueLock.lock();
    try
    {
      ByteBuffer buffer = _buffers[_currentWritePosition.bufferIndex()];

      //try to find a free ByteBuffer with enough space to fit the event
      //we will make at most three attempts: 1) current, possibly half-full, ByteBuffer
      //2) next, possibly last and smaller, ByteBuffer 3) makes sure at least one max capacity
      //ByteBuffer is check
      //when checking for available space at the end of a ByteBuffer always leave one free byte
      //to distinguish between a finalized ByteBuffer (limit <= capacity - 1) and a ByteBuffer
      //being written to (limit == capacity)
      final int maxFindBufferIter = 3;
      int findBufferIter = 0;
      for (;
           findBufferIter < maxFindBufferIter && buffer.capacity() - 1 -
               _currentWritePosition.bufferOffset() < dbusEventSize;
           ++findBufferIter)
      {
        if (isDebugEnabled)
        	_log.debug("skipping buffer " + _currentWritePosition.bufferIndex() +
        	          ": " + buffer +
        	          ": insufficient capacity " + (buffer.capacity() -
        	              _currentWritePosition.bufferOffset()) + " < " + dbusEventSize);
        final long newWritePos =
            _bufferPositionParser.incrementIndex(_currentWritePosition.getPosition(), _buffers);

        // ensureFreeSpace will call moveHead, which also resets limit to capacity
        ensureFreeSpace(_currentWritePosition.getPosition(), newWritePos, isDebugEnabled);
        moveCurrentWritePosition(newWritePos);

        buffer = _buffers[_currentWritePosition.bufferIndex()];
      }

      if (maxFindBufferIter == findBufferIter)
        throw new DatabusRuntimeException("insufficient buffer capacity for event of size:" +
                                          dbusEventSize);

      // passing true for noLimit, because we just need to make sure we don't go over capacity,
      // limit will be reset in the next call
      final long stopIndex =
          _bufferPositionParser.incrementOffset(_currentWritePosition.getPosition(),
                                                dbusEventSize, _buffers, true);  //no limit true - see DDSDBUS-1515
      ensureFreeSpace(_currentWritePosition.getPosition(), stopIndex, isDebugEnabled);
      buffer.position(_currentWritePosition.bufferOffset());
    }
    finally
    {
      _queueLock.unlock();
    }
  }

  /**
   * Any cleanup work required to finalize the append activity
   *
   *
   */
  private void finalizeAppend() {
    //releaseWriteRangeLock();
  }


  /**
   * @see com.linkedin.databus.core.DbusEventBufferAppendable#rollbackEvents()
   */
  @Override
  public void rollbackEvents() {
    acquireWriteLock();

    try {
      if(isClosed()) {
        LOG.warn("attempt to rollbackEvents for a closed buffer");
        return;
      }
      // do nothing
      // tail should be pointing to eventWindowStartScn
      // reset window local state
      resetWindowState();
      rollbackCurrentWritePosition();
    } finally {
      releaseWriteLock();
    }
  }


  /**
   * Reset _currentWritePosition to _tail; used for rolling back events written to the buffer. We
   * also have to reset the buffer limits of buffers between _tail and _currentWritePosition. We
   * have to watch out for the following special cases (1) [ CWP T ] - we have to reset all limits
   * since it is guaranteed to be [ CWP H T ] unless other invariants are broken
   * (2) [ T CWP H ] or [ T CWP ] [H] we should not reset any limits
   * (3) [ T ][ CWP H ] - we should be careful not to reset the CWP ByteBuffer's limit.
   **/
  private void rollbackCurrentWritePosition()
  {
    final int tailIdx = _tail.bufferIndex();
    final int writePosIdx = _currentWritePosition.bufferIndex();

    for (int i = 0; i < _buffers.length; ++i)
    {
      final int realBufIdx = (tailIdx + i) % _buffers.length;
      if (realBufIdx == tailIdx)
      {
        if (realBufIdx == writePosIdx && _tail.bufferOffset() <= _currentWritePosition.bufferOffset())
        {
          //same ByteBuffer [ T CWP ] - no need to reset limit
          break;
        }
      }

      if (realBufIdx == writePosIdx && realBufIdx != tailIdx)
      {
        //we've reached the _currentWritePosition; stop unless it is the case [ CWP T ]
        break;
      }

      _buffers[realBufIdx].limit(_buffers[realBufIdx].capacity());
    }

    _currentWritePosition.setPosition(_tail.getPosition());
    assert assertBuffersLimits();
  }

  /*
   * @see com.linkedin.databus.core.DbusEventBufferAppendable#endEvents()
   */
  @Override
  public void endEvents(boolean updateWindowScn, long windowScn,
                        boolean updateIndex, boolean callListener,
                        DbusEventsStatisticsCollector statsCollector)
  {
    boolean isDebugEnabled = LOG.isDebugEnabled();

    if (windowScn < _lastWrittenSequence)
    {
      throw new RuntimeException("Trying to add batch of events at sequence: " + windowScn +
                                 " when lastWrittenSequence = " + _lastWrittenSequence);
    }

    if (WindowState.ENDED == _eventState){
      if (isDebugEnabled)
      {
        LOG.debug("Skipping event window as Window is already in ended state" + windowScn);
      }
    }
    acquireWriteLock();

    try
    {
      if(isClosed()) {
        throw new DatabusRuntimeException("refusing to endEvents, because the buffer is closed");
      }

      if (WindowState.STARTED == _eventState && windowScn == _lastWrittenSequence)
      {
        //nothing to finish
        if (isDebugEnabled)
        {
          LOG.debug("Skipping event window that did not move forward:" + windowScn);
        }
        _eventState = WindowState.ENDED;

        return;
      }

      DbusEventInfo eventInfo = new DbusEventInfo(null,
                                                  windowScn,
                                                  _physicalPartition.getId().shortValue(),
                                                  (short)0,
                                                  _timestampOfLatestDataEvent,
                                                  DbusEventInternalWritable.EOPMarkerSrcId,
                                                  DbusEventInternalWritable.emptyMd5,
                                                  DbusEventInternalWritable.EOPMarkerValue,
                                                  false, //enable tracing
                                                  false, // autocommit
                                                  getEventSerializationVersion(),
                                                  (short)0,  // payload schema version, 0 since there is no payload
                                                  null  // DbusEventPart for metadata
                                                  );
      final int expNumBytesWritten = DbusEventFactory.computeEventLength(DbusEventInternalWritable.EOPMarkerKey,
                                                                         eventInfo);
      prepareForAppend(expNumBytesWritten);
      final int numBytesWritten = _eventFactory.serializeLongKeyEndOfPeriodMarker(_buffers[_currentWritePosition.bufferIndex()],
                                                                                  eventInfo);

      if (numBytesWritten != expNumBytesWritten)
      {
        String msg = "Actual Bytes Written was :" + numBytesWritten +
            ", Expected to Write :" + expNumBytesWritten;
        LOG.fatal(msg);
        LOG.fatal("Event Buffer is :" + toString());
        throw new DatabusRuntimeException(msg);
      }
      final long newWritePos =
          _bufferPositionParser.incrementOffset(_currentWritePosition.getPosition(), numBytesWritten,
                                                _buffers);
      moveCurrentWritePosition(newWritePos);

      finalizeAppend();

      _currentWritePosition.sanitize();
      // srcId is being set to 0, since End of Period applies to all sources
      // tracked by the buffer
      boolean updatedIndex = false;

      if ( updateWindowScn || updateIndex || callListener )
      {
        // HACK
        _eventStartIndex.sanitize();
        InternalEventIterator eventIterator =
            acquireInternalIterator(_eventStartIndex.getPosition(),
                                    _currentWritePosition.getPosition(),
                                    "endEventsIterator");

        try {
          LOG.debug("acquired iterator");
          DbusEventInternalWritable e = null;
          while (eventIterator.hasNext()) {
            long eventPosition = eventIterator.getCurrentPosition();
            e = eventIterator.next();
            if ( updateWindowScn)
            {
              e.setSequence(windowScn);
              e.applyCrc();
            }
            if (null != statsCollector) {
              statsCollector.registerDataEvent(e);
            }

            if ( updateIndex)
            {
              _scnIndex.onEvent(e, eventPosition, e.size());

              // ScnIndex updated only if valid event seen
              if (!e.isControlMessage())
                updatedIndex = true;
            }

            // TODO (DDSDBUS-57): per SD's request, scnIndex is kept out of internalListeners.
            // But in theory, scnIndex shall be one of the internalListeners.
            // SD shall address this code when he gets around to it - LG
            // [who the heck are SD and LG??]
            if ((!_internalListeners.isEmpty()) && callListener)
            {
              for (InternalDatabusEventsListener listener : _internalListeners)
              {
                listener.onEvent(e, eventPosition, e.size());
              }
            }

            if ( ! e.isControlMessage())
            {
            	if (_timestampOfFirstEvent == 0 )
            	{
            		_timestampOfFirstEvent = e.timestampInNanos()/(1000*1000);
            	}

            	_timestampOfLatestDataEvent = e.timestampInNanos()/(1000*1000);
            }
          }
        } finally {
          releaseIterator(eventIterator);
        }
      }

      if (updatedIndex && (QueuePolicy.OVERWRITE_ON_WRITE == _queueingPolicy))
      {
    	  try
    	  {
    		 _scnIndex.assertLastWrittenPos(_eventStartIndex);
    	  } catch (RuntimeException re) {
    		  _log.fatal("Got runtime Exception: ", re);
    		  _log.fatal("Event Buffer is: " + toString());
    		  _log.fatal("SCN Index is: ");
    		  _scnIndex.printVerboseString(LOG, org.apache.log4j.Level.FATAL);

    		  throw re;
    	  }
      }


      _eventState = WindowState.ENDED;
      _lastWrittenSequence = windowScn;
      long oldTail = _tail.getPosition();

      assert _currentWritePosition.bufferGenId() - _head.bufferGenId() <= 1 : toString();
      _tail.copy(_currentWritePosition);
      if (_head.getPosition() < 0)
      {
        _head.setPosition(0);
      }

      if (QueuePolicy.OVERWRITE_ON_WRITE == _queueingPolicy)
      {
        try
        {
          _bufferPositionParser.assertSpan(_head.getPosition(), _tail.getPosition(),isDebugEnabled);
          _scnIndex.assertHeadPosition(_head.getRealPosition());
        } catch (RuntimeException re) {
          _log.fatal("Got runtime Exception: ", re);
          _log.info("Old Tail was: " + _bufferPositionParser.toString(oldTail, _buffers) + ", New Tail is: " + _tail.toString());
          _log.fatal("Event Buffer is: " + toString());
          throw re;
        }
      }

      if (_log.isDebugEnabled())
        _log.debug("DbusEventBuffer: head = " + _head.toString() + " tail = " + _tail.toString() + "empty = " + empty());
      if (null != statsCollector) {
        statsCollector.registerBufferMetrics(this.getMinScn(),_lastWrittenSequence,this.getPrevScn(),this.getBufferFreeSpace());
        statsCollector.registerTimestampOfFirstEvent(_timestampOfFirstEvent);
      }
      _empty = false;
      updateFirstEventMetadata();
      _notEmpty.signalAll();


    } catch (KeyTypeNotImplementedException ex)
    {
      if (null != statsCollector)
        statsCollector.registerEventError(DbusEventInternalReadable.EventScanStatus.ERR);
      throw new RuntimeException(ex);
    } finally {
      releaseWriteLock();
    }
  }


  public void endEvents(long sequenceId)
  {
    endEvents(sequenceId,null);
  }

  @Override
  public void endEvents(long eventWindowScn,DbusEventsStatisticsCollector statsCollector)
  {
    if (_eventState != WindowState.EVENTS_ADDED)
    {
      //We set eventStartIndex here because _currentWritePosition is not finalized before the call to prepareForAppend
      _eventStartIndex.copy(_currentWritePosition);
    }
    endEvents(true, eventWindowScn, true, true,statsCollector);
  }

  enum StreamingMode {
    WINDOW_AT_TIME, // return when a WINDOW worth of events is surved
    CONTINUOUS      // as much as fits into buffer
  }


  /**
   * Given a checkPoint, writes events that form the next sequence of events after
   * that checkpoint onto the provided WritableByteChannel.
   *
   * params are actually passed in  {@link StreamEventsArgs}
   * @param checkPoint
   *            : checkpoint to start streaming from
   * @param streamFromLatestScn
   *            : if true will try to stream from the maxScn
   * @param batchFetchSize
   *            : upper bound on number of bytes to send over the wire
   * @param writeChannel
   *            : a writableByteChannel to transfer bytes to
   * @param encoding
   *            : one of JSON / BINARY : currently only supporting BINARY
   * @param filter
   *            : a filter to be applied to the events, events allowed by this
   *            filter will be transferred
   * @throws ScnNotFoundException
   *             // throws when things go wrong : semantics need to be
   *             hardened
   */

  /**
   *
   * @param checkPoint
   * @param writeChannel
   * @param {@link StreamEventsArgs}
   * @throws ScnNotFoundException
   */
  public StreamEventsResult streamEvents(Checkpoint checkPoint,
                                         WritableByteChannel writeChannel,
                                         StreamEventsArgs args
                                         )
  throws ScnNotFoundException, OffsetNotFoundException
  {
    long startTimeTs = System.nanoTime();

    StreamEventsResult result = new StreamEventsResult(0, 0);
    boolean isDebugEnabled = _log.isDebugEnabled();
    boolean oneWindowAtATime = args.getSMode() == StreamingMode.WINDOW_AT_TIME; // window at a time
    int batchFetchSize = args.getBatchFetchSize();
    DbusEventsStatisticsCollector statsCollector = args.getDbusEventsStatisticsCollector();
    int maxClientEventVersion = args.getMaxClientEventVersion();

    //int sleepTimeMs = RngUtils.randomPositiveInt()%3;

    if (isDebugEnabled)
    {
      _log.debug("Stream:begin:" + checkPoint.toString());
    }

    if (empty())
    {
      if (isDebugEnabled)
        _log.debug("Nothing to send out. Buffer is empty");
      return result;
    }

    long offset;
    // TODO (DDSDBUS-58): Put in assertions about checkPoint.getClientMode()
    long sinceScn = checkPoint.getWindowScn();
    long messagesToSkip = checkPoint.getWindowOffset();
    boolean skipWindowScn = messagesToSkip < 0;

    InternalEventIterator eventIterator = null;
    try {

      ScnIndex.ScnIndexEntry entry = null;
      if ( args.isStreamFromLatestScn() )
      {
        // If streamFromLatestScn is set, then the checkpoint is not respected in the current implementation,
        // but streaming starts from the last window available in the buffer. Hence, init() is invoked on
        // checkpoint
        sinceScn = _lastWrittenSequence;
        messagesToSkip = 0;
        skipWindowScn = false;
        checkPoint.init(); // Will no longer be flexible
      }

      if (checkPoint.getFlexible())
      {
        long minScn = 0;
        _queueLock.lock();
        try
        {
          eventIterator =
              acquireInternalIterator(_head.getPosition(),
                                      _bufferPositionParser.sanitize(_tail.getPosition(), _buffers),
                                      "streamEventsIterator");
          minScn = getMinScn();

          if (isDebugEnabled)
          {
            _log.debug("Acquired read iterator from " + _head.toString() + " to " +
                       _bufferPositionParser.toString(_bufferPositionParser.sanitize(_tail.getPosition(), _buffers), _buffers)
                       + " minScn = " + minScn);
          }
        } finally {
          _queueLock.unlock();
        }

        if (minScn < 0)
        {
          if (isDebugEnabled)
          {
            _log.debug("Nothing to send out. Buffer is empty");
          }
          return result;
        }

        sinceScn = minScn;
        messagesToSkip = 0;
        skipWindowScn = false;

      }
      else
      {
        long minScn = getMinScn();
        long prevScn = getPrevScn();
        if ((sinceScn == _lastWrittenSequence && skipWindowScn) || (minScn < 0))
        {
          //DDS-737 : guards against the situation where first window is not completely written but buffer is deemed not empty;
          if (minScn < 0) {
            if (sinceScn >= prevScn) {
              _log.error("Buffer still not fully ready; please wait for new events: sinceScn=" + sinceScn + " Anticipating events from scn=" + prevScn);
            } else {
              _log.error("Buffer still not fully ready; but request will be obsolete sinceScn=" + sinceScn + " Anticipating events from scn=" + prevScn);
              throw new ScnNotFoundException();
            }
          } else {
            _log.debug("No new events for SCN:" + sinceScn);
          }
          return result;
        }

        if (sinceScn < minScn)
        {
          //DDS-699
          prevScn = getPrevScn();
          if ( (sinceScn > prevScn) || ((sinceScn == prevScn) && skipWindowScn==true)) {
            //can serve data from next Scn - whis is minScn
            checkPoint.setWindowScn(minScn);
            checkPoint.setWindowOffset(0);
            checkPoint.setConsumptionMode(DbusClientMode.ONLINE_CONSUMPTION);
            try
            {
              return streamEvents(checkPoint, writeChannel, args);
            }
            catch (ScnNotFoundException e1)
            {
              throw e1;
            }
          } else {
            //either ; sinceScn < prevScn or sinceScn=prevScn with offset >= 0;
            _log.error("sinceScn is less than minScn and prevScn : sinceScn=" + sinceScn + " minScn=" + minScn + " PrevScn= " + prevScn);
            throw new ScnNotFoundException();
          }
        }

        long startTimeTs1 = System.nanoTime();
        entry = _scnIndex.getClosestOffset(sinceScn);
        long endTimeTs1 = System.nanoTime();
        if (PERF_LOG.isDebugEnabled())
        {
          PERF_LOG.debug("getClosestOffset(sinceScn) took: sinceScn=" + sinceScn + " " + (endTimeTs1 - startTimeTs1) / _nanoSecsInMSec + "ms");
        }

        offset = entry.getOffset();
        eventIterator = acquireInternalIterator(offset,
                                                _bufferPositionParser.sanitize(_tail.getPosition(),
                                                                               _buffers),
                                                "streamEventsIterator");
        if (isDebugEnabled)
        {
          _log.debug("Stream:offset:" + _bufferPositionParser.toString(offset, _buffers));
          DbusEvent e = _eventFactory.createReadOnlyDbusEventFromBuffer(_buffers[_bufferPositionParser.bufferIndex(offset)],
                                                                        _bufferPositionParser.bufferOffset(offset));
          _log.debug("Stream:Event@Offset:sequence:"+e.sequence());
        }
      }

      EventScanningState state = EventScanningState.LOOKING_FOR_FIRST_VALID_EVENT; // looking for first valid event
      int skippedMessages = 0;
      // Iterate over the buffer to locate batchFetchSize events
      int batchSize = 0;
      boolean foundWindow = false;
      long currentWindowScn = 0;
      boolean isFirstEvent = true;
      long startTimeTs2 = System.nanoTime();
      boolean done = false;
      // If we encounter the situation (SCN_A < checkpoint_windowSCN < SCN_B), then we end up changing the checkpoint
      // windowScn value to SCN_B. In that case, we save SCN_A in prevWindowScn (for logging).
      long prevWindowScn = 0;
      while (!done && eventIterator.hasNext()) {
        // for the first event, we need to validate that we got a "clean" read lock
        // since we intentionally split the getOffset from the getIterator call
        DbusEventInternalWritable e;
        int eventVersion;

        try
        {
          e = eventIterator.next(isFirstEvent);
          eventVersion = e.getVersion();

          // Convert event to the supported version if possible (DDSDBUS-2063).
          // For now we also assume that clientVersion equals to the eventVersion that the client understands
          // if convertToDifferentVersion() cannot convert - it will throw a runtime exception
          if (eventVersion > maxClientEventVersion) {
            e = DbusEventInternalWritable.convertToDifferentVersion(e, (byte)maxClientEventVersion);
          }

          if ( isFirstEvent)
          {
            if ((entry != null) && (entry.getScn() != e.sequence()))
            {
              String msg = "Concurrent Overwritting of Event. Expected sequence :" + entry.getScn()
                  + ", Got event=" + e.toString();
              _log.warn(msg);
              throw new OffsetNotFoundException(msg);
            }
          }
        }
        catch (InvalidEventException e2)
        {
          _log.warn("Found invalid event on getting iterator. This is not unexpected but should be investigated.");
          _log.warn("RangeBasedLocking :" + _rwLockProvider.toString(_bufferPositionParser, true));
          if (null != statsCollector)
            statsCollector.registerEventError(DbusEventInternalReadable.EventScanStatus.ERR);
          throw new DatabusRuntimeException(e2);
        }

        isFirstEvent = false;

        if (state == EventScanningState.LOOKING_FOR_FIRST_VALID_EVENT)
        {
          if (e.sequence() > sinceScn)
          {
            _log.info("ScnIndex state = " + _scnIndex);
            _log.info("Iterator position = " + eventIterator._currentPosition.toString());
            _log.info("Iterator = " + eventIterator);
            _log.info("Found event " + e + " while looking for sinceScn = " + sinceScn);
            /* while (eventIterator.hasNext())
            {
              e = eventIterator.next();
              _log.error("DbusEventBuffer:dump:" + e);
            } */
            throw new ScnNotFoundException();
          }
          else
          {
            state = EventScanningState.IN_LESS_THAN_EQUALS_SCN_ZONE; // <= sinceScn
          }
        }

        if (state == EventScanningState.IN_LESS_THAN_EQUALS_SCN_ZONE)
        {
          if (skipWindowScn)
          {
            if (e.sequence() < sinceScn)
            {
              currentWindowScn = e.sequence();
              continue;
            }
            else
            {
              if (e.sequence() == sinceScn)
              {
                // we are in the == zone
                foundWindow = true;
                continue;
              }
              else
              {
                if (foundWindow)
                {
                  state = EventScanningState.FOUND_WINDOW_ZONE;
                }
                else
                {
                  // we never found the window but reached a greater
                  // window
                  state = EventScanningState.MISSED_WINDOW_ZONE;
                  prevWindowScn = currentWindowScn;
                  currentWindowScn = e.sequence();
                }
              }
            }
          }
          else
          {
            if (e.sequence() < sinceScn)
            {
              currentWindowScn = e.sequence();
              continue;
            }
            else
            {
              if (e.sequence() == sinceScn)
              {
                foundWindow = true;
                state = EventScanningState.FOUND_WINDOW_ZONE;
              }
              else
              {
                // we never found the window but reached a greater
                // window
                state = EventScanningState.MISSED_WINDOW_ZONE;
                prevWindowScn = currentWindowScn;
                currentWindowScn = e.sequence();
              }
            }
          }
        }

        if (state == EventScanningState.FOUND_WINDOW_ZONE)
        {
          if (skippedMessages < messagesToSkip)
          {
            if (e.isEndOfPeriodMarker() || e.isCheckpointMessage())
            {
              continue;
            }
            ++skippedMessages;
            continue;
          }
          else
          {
            state = EventScanningState.VALID_ZONE;
          }
        }

        if (state == EventScanningState.VALID_ZONE)
        {
          boolean controlMessage = e.isControlMessage();
          boolean filterAllowed = args.getFilter().allow(e);
          if (controlMessage || filterAllowed)
          {
            if (batchSize + e.size() > batchFetchSize)
            {
              // sending this would violate our contract on upper bound of
              // bytes to send
              if (isDebugEnabled)
                _log.debug("streamEvents returning after streaming " + batchSize + " bytes because " + batchSize
                           + " + " + e.size() + " > " + batchFetchSize);
              result.setSizeOfPendingEvent(e.size());
              break;
            }

            long startTimeTs1 = System.nanoTime();
            int bytesWritten = e.writeTo(writeChannel, args.getEncoding());
            long endTimeTs1 = System.nanoTime();
            if (PERF_LOG.isDebugEnabled())
            {
              PERF_LOG.debug("writeTo(sinceScn=" + sinceScn + ", bytes=" + bytesWritten +
                             ") took: " + ((endTimeTs1 - startTimeTs1) / _nanoSecsInMSec) + "ms");
            }

            if (0 >= bytesWritten)
            {
              done = true;
            }
            else
            {
              if (null != statsCollector)
                statsCollector.registerDataEventFiltered(e);

              checkPoint.onEvent(e);

              if (isDebugEnabled)
              {
                if (e.isEndOfPeriodMarker())
                  _log.debug("Stream:sequence:"+e.sequence());
                else
                  _log.debug("Stream:sequence:"+e.sequence()+":headerCrc:"+e.headerCrc());
              }

              /** When batch writing is implemented this becomes
               * written  = EventWriter.writeTo(e, writeChannel, encoding);
               * if (written) // The write actually happened and didn't get buffered
               * {
               *  eventIterator.remove();
               * }
               */
              batchSize += e.size();
              result.incNumEventsStreamed(1);
              if (isDebugEnabled)
                _log.debug("buf.stream: GOT event scn="+e.sequence() + ";srcid=" + e.getSourceId() +
                           ";eow=" + e.isEndOfPeriodMarker() + ";oneWindatTime=" + oneWindowAtATime);
            }
          }
          else
          {
            if (isDebugEnabled)
              _log.debug("Event was valid according to checkpoint, but was filtered out :" + e);
          }

          // register both filtered and non-filtered events
          if (null != statsCollector)
          {
            statsCollector.registerDataEvent(e);
          }
          // end of the window - don't send more
          if(e.isEndOfPeriodMarker() && oneWindowAtATime)
          {
            _log.info("buf.stream: reached end of a window. scn="+e.sequence());
            break;
          }
        }

        if (state == EventScanningState.MISSED_WINDOW_ZONE)
        {
          // did not find the window that we were looking for
          // set checkpoint to the first window that we found > the windowScn
          // being searched for and start streaming from there.
          _log.info("Could not serve target SCN " + checkPoint + ". Setting checkpoint to " + currentWindowScn +
                    " (target+" + (currentWindowScn-checkPoint.getWindowScn()) + "). Previous window SCN=" + prevWindowScn +
                    " (target-" + (checkPoint.getWindowScn()-prevWindowScn) + ")");
          checkPoint.setWindowScn(currentWindowScn);
          //get the currentWindowScn as well; not the next window; that's why the offset is 0 and not -1
          checkPoint.setWindowOffset(0);
          checkPoint.setConsumptionMode(DbusClientMode.ONLINE_CONSUMPTION);
          try
          {
            return streamEvents(checkPoint, writeChannel, args);
          }
          catch (ScnNotFoundException e1)
          {
            throw e1;
          }
        }

      }

      if (batchSize == 0)
      {
        if (isDebugEnabled)
          _log.debug("No events were sent out.");
      }

      long endTimeTs2 = System.nanoTime();
      if (PERF_LOG.isDebugEnabled())
      {
        	PERF_LOG.debug("while loop took:" + (endTimeTs2 - startTimeTs2) / _nanoSecsInMSec + "ms");
      }

    } finally {
      if(eventIterator != null)
        releaseIterator(eventIterator);
    }

    if (isDebugEnabled)
      _log.debug("Stream:events:"+result.getNumEventsStreamed());

    long endTimeTs = System.nanoTime();
    if (PERF_LOG.isDebugEnabled())
    {
      PERF_LOG.debug("streamEvents took:" + (endTimeTs - startTimeTs) / _nanoSecsInMSec + "ms");
    }

    return result;
  }

  /**
   * Batch interface to write events within a range out into a WritableByteChannel
   * @param range
   * @param writeChannel
   * @param encoding
   * @return number of bytes written
   */
  public int batchWrite(Range range, WritableByteChannel writeChannel, Encoding encoding)
  {
    long startOffset = range.start;
    long endOffset = range.end;
    assert (_bufferPositionParser.bufferIndex(startOffset) == _bufferPositionParser.bufferIndex(endOffset));
    ByteBuffer buf = _buffers[_bufferPositionParser.bufferIndex(startOffset)];
    int endBufferOffset = _bufferPositionParser.bufferOffset(endOffset);
    int startBufferOffset = _bufferPositionParser.bufferOffset(startOffset);
    int bytesWritten = 0;
    switch (encoding)
    {
    case BINARY :
    {
      ByteBuffer writeBuf = buf.duplicate().order(_eventFactory.getByteOrder());
      writeBuf.position(startBufferOffset);
      writeBuf.limit(endBufferOffset);
      try
      {
        bytesWritten = writeChannel.write(writeBuf);
      }
      catch (IOException e1)
      {
        LOG.error("batchWrite error: " + e1.getMessage(), e1);
        throw new RuntimeException(e1);
      }
      break;
    }
    case JSON: case JSON_PLAIN_VALUE:
    {
      DbusEventInternalReadable e = _eventFactory.createReadOnlyDbusEventFromBuffer(buf, startBufferOffset);
      int currentBufferOffset = startBufferOffset;
      while (currentBufferOffset != endBufferOffset)
      {
        e = e.reset(buf, currentBufferOffset);
        e.writeTo(writeChannel, encoding);
        currentBufferOffset += e.size();
      }
    }
    }

    return (bytesWritten);
  }


  /* (non-Javadoc)
   * @see com.linkedin.databus.core.DbusEventBufferAppendable#empty()
   */
  @Override
  public boolean empty()
  {
    return (_empty);
//  return (_size.get()==0);

/*
    if (_head < 0)
      return true;
    return false;
 */
  }

  private void releaseWriteLock()
  {
    _queueLock.unlock();
  }

  private void acquireWriteRangeLock(long startOffset, long endOffset)
      throws InterruptedException, TimeoutException
  {
    _rwLockProvider.acquireWriterLock(startOffset, endOffset, _bufferPositionParser);
  }

  private void releaseWriteRangeLock()
  {
    _rwLockProvider.releaseWriterLock(_bufferPositionParser);
  }

  private void acquireWriteLock()
  {
    _queueLock.lock();
  }

  public int getReadStatus()
  {
    return readLocked.get();
  }


  public int readEvents(ReadableByteChannel readChannel, Encoding _encoding) throws InvalidEventException
  {
    switch (_encoding)
    {
    case BINARY:
      return readEvents(readChannel);
    case JSON: case JSON_PLAIN_VALUE:
    {

      BufferedReader in = new BufferedReader(Channels.newReader(readChannel, "UTF-8"));
      try
      {
        return DbusEventSerializable.appendToEventBuffer(in, this, null, false);
      }
      catch (JsonParseException e)
      {
        throw new InvalidEventException(e);
      }
      catch (IOException e)
      {
        throw new InvalidEventException(e);
      }
    }
    }
    return -1;
  }

  /* (non-Javadoc)
   * @see com.linkedin.databus.core.DbusEventBufferAppendable#readEvents(java.nio.channels.ReadableByteChannel)
   */
  public int readEvents(ReadableByteChannel readChannel) throws InvalidEventException
  {
    return readEvents(readChannel, this._internalListeners, null);
  }

  public int readEvents(ReadableByteChannel readChannel,
                        DbusEventsStatisticsCollector statsCollector) throws InvalidEventException
  {
    return readEvents(readChannel, this._internalListeners, statsCollector);
  }

  /* (non-Javadoc)
   * @see com.linkedin.databus.core.DbusEventBufferAppendable#readEvents(java.nio.channels.ReadableByteChannel, java.util.List)
   */
  public int readEvents(ReadableByteChannel readChannel,
                        Iterable<InternalDatabusEventsListener> eventListeners)
  throws InvalidEventException
  {
    return readEvents(readChannel, eventListeners, null);
  }


  /* (non-Javadoc)
   * @see com.linkedin.databus.core.DbusEventBufferAppendable#readEvents(java.nio.channels.ReadableByteChannel, java.util.List, com.linkedin.databus.monitoring.mbean.DbusEventsStatisticsCollector)
   */
  @Override
  public int readEvents(ReadableByteChannel readChannel,
                        Iterable<InternalDatabusEventsListener> eventListeners,
                        DbusEventsStatisticsCollector statsCollector) throws InvalidEventException
  {
    try
    {
      return readEventsInternal(readChannel, eventListeners, statsCollector);
    }
    finally
    {
      //in the absence of data about the previous scn be conservative to avoid
      //holes in the buffer
      if (getMinScn() > 0 && 0 > getPrevScn())
      {
        long newPrevScn = getMinScn() - 1;
        setPrevScn(newPrevScn);
      }
    }
  }

  static enum ReadEventsScanStatus
  {
    OK,
    PARTIAL_EVENT,
    INVALID_EVENT,
    SCN_REGRESSION,
    MISSING_EOP
  }

  /** A class used by readEvents() to keep track of its progress reading from the read buffer (aka
   * the staging buffer) */
  class ReadEventsReadPosition
  {
    /** the start of the data to be copied*/
    int _start;
    /** a pointer to the byte after the last verified event in the staging buffer  */
    int _pos;
    /** a pointer to the byte after the event currently being verified in the staging buffer */
    int _nextPos;
    /** the end of data in the staging buffer */
    int _end;
    /** number of events skipped during the current iteration */
    int _skippedEvents;
    /** number of events processed so far */
    int _readEvents;
    /** the sequence of the last successfully processed event from the staging buffer */
    long _lastProcessedSeq;
    /** the verification status of the current event in the staging buffer */
    ReadEventsScanStatus _scanStatus;
    /** The event currently being processed */
    DbusEventInternalWritable _curEvent;  // TODO Make this DbusEventInternalReadable
    /** The scn of the last successfully processed window */
    long _lastSeenStgWin;
    /** A flag if _scnRegress flag has to be reset after the current event is processed */
    boolean _resetScnRegress;
    /**
     * used for reading bytes into a staging area
     * before inserting into the primary buffer
     */
    private ByteBuffer _readBuffer;

    public ReadEventsReadPosition()
    {
      _lastSeenStgWin = _seenEndOfPeriodScn;
      _readBuffer = ByteBuffer.allocate(_initReadBufferSize).order(_eventFactory.getByteOrder());
    }

    /** This should be called after the data in the buffer changes: reading from network*/
    public void startIteration()
    {
      _start = 0;
      _pos = 0;
      _skippedEvents = 0;
      _end = _readBuffer.position();
      _lastProcessedSeq = _lastWrittenSequence;
      if (_curEvent == null) {
        // TODO Change this call to createReadOnly, but that is a bigger change.
        _curEvent = _eventFactory.createWritableDbusEventFromBuffer(_readBuffer, _pos);
      } else {
        _curEvent = (DbusEventInternalWritable)_curEvent.reset(_readBuffer, _pos);
      }
    }

    public int getReadStart()
    {
      return _start;
    }

    public int getPosition()
    {
      return _pos;
    }

    public int getEnd()
    {
      return _end;
    }

    /** the sequence of the currently processed event in the staging buffer */
    public long getSeq()
    {
      assert null != _curEvent;
      assert isValidEvent() : null != _scanStatus ? _scanStatus.toString() : "null";

      return _curEvent.sequence();
    }

    private boolean isValidEvent()
    {
      return _scanStatus == ReadEventsScanStatus.OK
          || _scanStatus == ReadEventsScanStatus.MISSING_EOP
          || _scanStatus == ReadEventsScanStatus.SCN_REGRESSION;
    }

    public long getLastProcessedSeq()
    {
      return _lastProcessedSeq;
    }

    public int getNumReadEvents()
    {
      return _readEvents;
    }

    public int getSkippedEvents()
    {
      return _skippedEvents;
    }

    public ReadEventsScanStatus getEventScanStatus()
    {
      return _scanStatus;
    }

    public boolean hasNext()
    {
      return _pos < _end;
    }

    public boolean hasEventWithOldScn()
    {
      long seq = getSeq();

      //invariant should be: _seenEndOfPeriodScn <= _lastWrittenSequence <= _lastProcessedSeq
      //an event is considered old if (1) it has an scn < _lastProcessedSeq, i.e. it is from
      //a previous window or (2) scn == _lastProcessedSeq && scn == _seenEndOfPeriodScn, i.e.
      //it is from the las
      boolean res = (seq > 0) && (_lastProcessedSeq > 0) &&
                    ((seq < _lastProcessedSeq) || (seq == _lastSeenStgWin));
      return res;
    }

    public boolean hasMissingEOP()
    {
      long seq = getSeq();
      boolean missingEopMarker = (_lastProcessedSeq > 0) && (seq > _lastProcessedSeq)
          && (_lastSeenStgWin < _lastProcessedSeq);
      return missingEopMarker;
    }

    public int bytesProcessed()
    {
      return _pos - _start;
    }

    public int bytesRemaining()
    {
      return _end - _pos;
    }

    @Override
    public String toString()
    {
      return "readPos: {start:" + _start + ", end:" + _end + ", pos:" + _pos +
          ", read:" + _readEvents + ", skipped:" + _skippedEvents + ", lastSeenStgWin:"
          + _lastSeenStgWin + ", seq:" +
          (null == _curEvent || bytesProcessed() == 0 ? -1 : getSeq()) +
          ", lastProcessedSeq: " + _lastProcessedSeq + "}";
    }

    /**
     * Checks if the current message is an SCN regression event. If the message is an SCN regress
     * event, {@link #_scnRegress} will be set to true (side effect!). Otherwise, the call will
     * determine if it is to be reset to false but it will not clear it! The reason is to allow other
     * code to check the current value until it is cleared.
     */
    private void checkForReadEventsScnRegress()
    {
      if (_curEvent.isSCNRegressMessage())
      {
        _log.info("Seeing SCNRegress Message :" + _curEvent);
        _scnRegress = true;
      } else if ( _scnRegress && _curEvent.isEndOfPeriodMarker()){
        _log.info("Resetting SCNRegress as EOP is seen : EOP :" + _curEvent);
        _resetScnRegress = true;
      }
    }

    public ReadEventsScanStatus startEventProcessing()
    {
      _start = _pos;
      _resetScnRegress = false;
      try
      {
        _curEvent = (DbusEventInternalWritable)_curEvent.reset(_readBuffer, _pos);
        final EventScanStatus eventScanStatus = _curEvent.scanEvent(false);
        switch (eventScanStatus)
        {
          case PARTIAL:
          {
            _scanStatus = ReadEventsScanStatus.PARTIAL_EVENT;
            break;
          }
          case ERR:
          {
            _scanStatus = ReadEventsScanStatus.INVALID_EVENT;
            break;
          }
          case OK:
          {
            _nextPos = _pos + _curEvent.size();
            checkForReadEventsScnRegress();

            if (_log.isDebugEnabled())
            {
              long eventSrcId = _curEvent.getSourceId();
              LOG.debug("scan event:position:"+ getPosition() +
                        ";size:" + _curEvent.size() +
                        ";seq:" + _curEvent.sequence() +
                        ";evSrcId:" + eventSrcId);
            }

            _scanStatus = ReadEventsScanStatus.OK;
            if (_dropOldEvents && (!_scnRegress) && hasEventWithOldScn())
              _scanStatus = ReadEventsScanStatus.SCN_REGRESSION;
            else if (_dropOldEvents && (!_scnRegress) && hasMissingEOP())
              _scanStatus = ReadEventsScanStatus.MISSING_EOP;
            break;
          }
          default:
            throw new IllegalStateException("unknown event scan status: " + eventScanStatus);
        }
      }
      catch (UnsupportedDbusEventVersionRuntimeException e)
      {
        _log.fatal("Unknown dbus event version:", e);
        _scanStatus = ReadEventsScanStatus.INVALID_EVENT;
      }

      return _scanStatus;
    }

    /** Marks that the current event in the staging buffer has passed all checks and can be copied
     * to the main buffer */
    public void eventAccepted()
    {
      if (!isValidEvent()) return;
      if (_resetScnRegress)
        _scnRegress = false;

      _pos = _nextPos;
      final long seq = getSeq();
      if (seq > _lastProcessedSeq)
        _lastProcessedSeq = seq;
      if (_curEvent.isEndOfPeriodMarker() && (_lastProcessedSeq > _lastSeenStgWin))
      {
        _lastSeenStgWin = _lastProcessedSeq;
      }

      if (_log.isDebugEnabled())
        _log.debug("stg events scanned: " + this);
    }

    /** Marks that the current event in the staging buffer has been written to the main buffer. */
    public void eventWritten()
    {
      _start += _curEvent.size();
      ++_readEvents;
    }

    /** Marks that the current event in the staging buffer has an */
    public void eventSkipped()
    {
      if(_log.isDebugEnabled())
        LOG.debug("skipping event " + getSkippedEvents() +
                  " event.seq:" + getSeq() +
                  " _lastWrittenSequence:" + _lastWrittenSequence +
                  toString());
      _pos = _nextPos;
      ++_skippedEvents;
      ++_readEvents;// we need to count them as received (even though we dropped them)
    }

    public DbusEvent getCurEvent()
    {
      return _curEvent;
    }

	public long getLastSeenStgWin() {
		return _lastSeenStgWin;
	}

  /**
   * @return the readBuffer
   */
  public ByteBuffer getReadBuffer()
  {
    return _readBuffer;
  }

  /**
   * Grows the read buffer to a new size. The _curEvent object is reset to null in the process. The method does not
   * enforce an upper-bound of the new size. This is responsibility of the caller.
   */
  public ByteBuffer growReadBuffer(int newSize)
  {
    assert 0 == _readBuffer.position() : "readBuffer position not 0; data before it will be lost!";

    if (newSize <= _readBuffer.capacity())
    {
      throw new DatabusRuntimeException("invalid new event staging buffer size: " + newSize + "; current buffer: "
                                        + _readBuffer);
    }
    if (_log.isDebugEnabled())
    {
      _log.debug("growing event staging buffer from " + _readBuffer.capacity() + " to " + newSize);
    }
    ByteBuffer newBuf = ByteBuffer.allocate(newSize).order(_eventFactory.getByteOrder());
    newBuf.put(_readBuffer);
    _curEvent = null;
    _readBuffer = newBuf;

    return newBuf;
  }

  }

  /** A class used to keep track of the readEvents progress writing to the main buffer */
  class ReadEventsWritePosition
  {
    /** number of bytes remaining*/
    int _numBytesWritten;
    /**
     * The iterator used to write the events. We don't need read locking because of the global
     * write lock */
    BaseEventIterator _writeIter;
    /** The last read event */
    DbusEventInternalWritable _lastEvent;
    /** The current byte buffer being written to */
    ByteBuffer _curBuf;


    public ReadEventsWritePosition()
    {
      _writeIter = acquireLockFreeInternalIterator(_tail.getPosition(), _tail.getPosition(),
                                                   "ReadEventsWritePosition");
    }

    public  DbusEventInternalWritable next()
    {
      if (null != _lastEvent)
      {
        _numBytesWritten += _lastEvent.size();
      }
      _lastEvent = _writeIter.next();
      return _lastEvent;
    }

    /** The gen-id current write start position in the main buffer */
    public long getCurPos()
    {
      return _writeIter.getCurrentPosition();
    }

    public void startNewIteration()
    {
      _lastEvent = null;
      int writeBufIndex = _bufferPositionParser.bufferIndex(getCurPos());
      _curBuf = _buffers[writeBufIndex];
      _numBytesWritten = 0;

      assertPositions();
    }

    public int getNumBytesWritten()
    {
      return _numBytesWritten;
    }

    public void determineWriteEnd(ReadEventsReadPosition readPos)
    {
      int increment = readPos.bytesProcessed();

      //while calculating endPosition, discount the bufferLimit since we will resize the limit
      //to capacity if needed
      setNextFreePos(_bufferPositionParser.incrementOffset(getCurPos(), increment, _buffers));
      if ( _log.isDebugEnabled() )
      {
        _log.debug("readEvents: _empty :" + _empty + ", " + this + ", Head:" + _head +
                   ", Tail:" + _tail);
      }

      assertPositions();
    }

    @Override
    public String toString()
    {
      return "writePos:{iter:" + _writeIter + ", numBytesWritten:" + _numBytesWritten + "}";
    }

    private void assertPositions()
    {
      final int posIdx = _bufferPositionParser.bufferIndex(getCurPos());
      final int nextFreeIdx = _bufferPositionParser.bufferIndex(getNextFreePos());
      assert (posIdx == nextFreeIdx  && getCurOfs() <= getNextFreeOfs()) ||
             ((posIdx + 1) % _buffers.length == nextFreeIdx && getNextFreeOfs() == 0)
             : toString();
    }

    private void setNextFreePos(long nextFreePos)
    {
      if (nextFreePos > _currentWritePosition.getPosition())
        moveCurrentWritePosition(nextFreePos);
      _writeIter.reset(_writeIter.getCurrentPosition(), nextFreePos, _writeIter.getIdentifier());
      //_writeIter._iteratorTail.setPosition(nextFreePos);
    }

    /** The gen-id position of the first free byte after the write */
    public long getNextFreePos()
    {
      return _writeIter._iteratorTail.getPosition();
    }

    public BufferPosition getNextFree()
    {
      return _writeIter._iteratorTail;
    }

    /** the local offset part of _startPos*/
    public int getCurOfs()
    {
      return _bufferPositionParser.bufferOffset(getCurPos());
    }

    /** the local offset part of _nextFreePos  */
    public int getNextFreeOfs()
    {
      return _bufferPositionParser.bufferOffset(getNextFreePos());
    }

    public void moveToNextBuffer()
    {
      final long nextWritePos =
          _bufferPositionParser.incrementIndex(_currentWritePosition.getPosition(), _buffers);
      setNextFreePos(nextWritePos);
      assert 0 == getNextFreeOfs(): toString();
    }

    public ByteBuffer getCurBuf()
    {
      return _curBuf;
    }

    public void close()
    {
      releaseIterator(_writeIter);
    }

  }

  private static final double LN_5 = Math.log(5.0);

  private int readEventsInternal(ReadableByteChannel readChannel,
                                 Iterable<InternalDatabusEventsListener> eventListeners,
                                 DbusEventsStatisticsCollector statsCollector)
          throws InvalidEventException
  {
    final boolean logDebugEnabled = _log.isDebugEnabled();

    ReadEventsReadPosition readPos = new ReadEventsReadPosition();
    ReadEventsWritePosition writePos = new ReadEventsWritePosition();

    _readBufferLock.lock();
    try
    {
      _eventState = WindowState.IN_READ;

      boolean mightHaveMoreData = true;
      //ensuring index is updated correctly if a control event of preceding window doesn't appear
      //first (no start() called)
      if (_scnIndex.isEnabled() && _scnIndex.isEmpty())
      {
        _scnIndex.setUpdateOnNext(true);
      }
      try
      {
        while (mightHaveMoreData)
        {
          final ByteBuffer readBuffer = readPos.getReadBuffer();
          boolean success = readEventsFromChannel(readChannel, readBuffer, logDebugEnabled);
          readPos.startIteration();

          final int numBytesRead = readPos.bytesRemaining();

          //if there is an error we'll try to process whatever was read but stop after that
          mightHaveMoreData = success && (numBytesRead > 0) &&
              (readBuffer.position() == readBuffer.limit());

          if (numBytesRead > 0)
          {
            _queueLock.lock();
            try
            {
              if(isClosed()) {
                LOG.warn("stopping attempt to read more events into a buffer while it is closed. readPos=" + readPos + "; buf=" + this.toString());
                return 0;
              }
              try
              {
                _scnIndex.assertHeadPosition(_head.getRealPosition());
                _bufferPositionParser.assertSpan(_head.getPosition(),
                                                 _currentWritePosition.getPosition(),
                                                 logDebugEnabled);
              } catch (RuntimeException re) {
                _log.fatal("Got runtime Exception :", re);
                _log.fatal("Event Buffer is :" + toString());
                _scnIndex.printVerboseString(_log, Level.DEBUG);
                throw re;
              }

              readBuffer.flip();
              boolean hasMoreInStgBuffer = true;
              boolean preEndPeriodEvent = false;
              while (hasMoreInStgBuffer && readPos.hasNext())
              {
                writePos.startNewIteration();

                //figure out the boundary of events at which we can write
                //leave one byte at the end, to distinguish between a finalized full ByteBuffer
                //(limit <= capacity - 1) and a ByteBuffer that is still being written to
                //(limit == capacity)
                final int contiguousCapacity = writePos.getCurBuf().capacity() -
                    writePos.getCurOfs() - 1;

                final ReadEventsScanStatus eventScanStatus = readPos.startEventProcessing();
                switch (eventScanStatus)
                {
                case OK:
                {
                  if (readPos.getCurEvent().isEndOfPeriodMarker())
                  {
                    if (preEndPeriodEvent)
                    {
                      readPos.eventSkipped();
                      break;
                    }
                    else
                    {
                      preEndPeriodEvent = true;
                    }
                  }
                  else
                  {
                    preEndPeriodEvent = false;
                  }
                  final int curEventSize = readPos.getCurEvent().size();
                  if (readPos.bytesProcessed() + curEventSize > contiguousCapacity)
                  {
                    //not enough space to fit event in the target buffer
                    if (0 == writePos.getCurOfs())
                    {
                      //event bigger than the ByteBuffer capacity
                      throw new InvalidEventException("event too big to fit into buffer" +
                          "; size:" + curEventSize +
                          "; event:" + readPos.getCurEvent() +
                          "; " + readPos +
                          "; buffer.capacity:" + writePos.getCurBuf().capacity());
                    }
                    else
                    {
                      if (logDebugEnabled)
                        _log.debug("unable to fit event with size " + readPos.getCurEvent().size());

                      //if we could not fit all the data in the destination ByteBuffer,
                      //we should ensure that we clear up any remaining data in the
                      //ByteBuffer.
                      long nextBufferPos =
                          _bufferPositionParser.incrementIndex(writePos.getCurPos(), _buffers);
                      boolean interrupted = ensureFreeSpace(writePos.getCurPos(),
                                                            nextBufferPos,
                                                            logDebugEnabled);
                      if (interrupted)
                      {
                        _log.warn("ensureFree space interrupted: " + readPos + " " + writePos);
                        return readPos.getNumReadEvents();
                      }
                      assert assertBuffersLimits();

                      writePos.moveToNextBuffer();
                      _tail.copy(_currentWritePosition);
                      assert assertBuffersLimits();
                      preEndPeriodEvent = false;
                    }
                  }
                  else
                  {
                    //we can fit the event in the target buffer
                    readPos.eventAccepted(); //done with processing in the stg buffer

                    //how are we on free space?
                    boolean interrupted = ensureFreeSpace(
                        writePos.getCurPos(), writePos.getCurPos() + curEventSize,
                        logDebugEnabled);
                    if (interrupted)
                    {
                      _log.warn("ensureFree space interrupted:" + readPos + " " + writePos);
                      return readPos.getNumReadEvents();
                    }

                    writePos.determineWriteEnd(readPos);

                    //we are good on free space, about time to copy the damn data
                    copyReadEventToEventBuffer(readPos, writePos, eventListeners, statsCollector,
                                               logDebugEnabled);
                  }
                  break;
                }
                case PARTIAL_EVENT:
                {
                  final int curCapacity = readBuffer.capacity();
                  if (logDebugEnabled)
                    _log.debug("partial event at " + readPos);
                  if (0 != readPos.getReadStart())
                  {
                    //compact stg buffer and try to read more data from the network
                    compactStgBuffer(readPos, logDebugEnabled);
                    hasMoreInStgBuffer = false;
                  }
                  else if (curCapacity >= getMaxReadBufferCapacity())
                  {
                    //we couldn't read an entire event in the staging buffer and we are already
                    //at max allowed size of the read buffer
                    throw new InvalidEventException("event too big to fit in staging buffer with capacity : " +
                        curCapacity
                        + "; readPos:" + readPos + "; consider increasing connectionDefaults.eventBuffer.maxSize" +
                           " or connectionDefaults.eventBuffer.maxEventSize if set explicitly.");
                  }
                  else
                  {
                    //grow the staging buffer faster for small sizes and slower for big sizes
                    //intuitively: <= 5K - 3x, 25K - 2x, 125K - 1.6x, 625K - 1.5x and so on
                    final double growFactor = curCapacity <= 5 * 1024 ? 3.0
                        : 1.0 + 2.0 * LN_5 / Math.log(curCapacity / 1024.0);
                    final int newSize = Math.min(getMaxReadBufferCapacity(), (int)(growFactor * curCapacity));
                    if (newSize < curCapacity)
                    {
                      throw new DatabusRuntimeException("unexpected readbuffer size: " + newSize +
                                                        "; readBuffer=" + readBuffer + "; readBufferCapacity=" +
                                                        getMaxReadBufferCapacity());
                    }
                    readPos.growReadBuffer(newSize);
                    hasMoreInStgBuffer = false;
                  }
                  break;
                }
                case SCN_REGRESSION:
                {
                  // events should be monotonically increasing
                  // skipping the event and all the events before it (same buffer should have
                  // only increasing events)
                  String errMsg = logSequenceErrorPackets(readPos);
                  _log.warn("got an old event: seq=" + readPos.getSeq() + ", " + errMsg);
                  readPos.eventSkipped();
                  break;
                }
                case INVALID_EVENT:
                {
                  if (null != statsCollector)
                    statsCollector.registerEventError(DbusEventInternalReadable.EventScanStatus.ERR);
                  throw new InvalidEventException();
                }
                case MISSING_EOP:
                {
                  String errMsg = logSequenceErrorPackets(readPos);
                  _log.error("detected missing EOP: " + errMsg);
                  throw new InvalidEventException(errMsg);
                }
                default:
                  throw new IllegalStateException("unknown scan status: " + eventScanStatus);
                }
              }

              if (!readPos.hasNext())
              {
                readBuffer.clear();
              }
            } finally {
              _queueLock.unlock();
            }
          }
        }
      }
      finally
      {
        if (null != statsCollector) {
          statsCollector.registerBufferMetrics(getMinScn(), this.lastWrittenScn(),
                                               this.getPrevScn(),
                                               this.getBufferFreeSpace());
          statsCollector.registerTimestampOfFirstEvent(_timestampOfFirstEvent);
        }
        _eventState = WindowState.ENDED;
      }
    } catch (RuntimeException re) {
      _log.error("Got runtime exception in readEvents: " + re.getMessage(), re);
      _log.error("Buffer State: " + toString());
      throw re;
    } finally {
      _readBufferLock.unlock();
      writePos.close();
    }

    if (logDebugEnabled)
      _log.debug("readEvents result: " + readPos + " " + writePos);

    return readPos.getNumReadEvents();
  }

  /**
   * Read events from a channel for readEvents().
   *
   * @param readChannel      the channel to read from
   * @param readBuffer       the buffer to read into
   * @param logDebugEnabled  if debug logging is enabled
   * @return true the read succeeded
   */
  private boolean readEventsFromChannel(ReadableByteChannel readChannel,
                                        ByteBuffer readBuffer, boolean logDebugEnabled)
  {
    if (logDebugEnabled)
      _log.debug("reading events from channel to " + readBuffer);
    boolean success = true;
    long oneread = 1;
    while (success && oneread > 0)
    {
      try
      {
        oneread = readChannel.read(readBuffer);
        if (logDebugEnabled)
          _log.debug("Read " + oneread + " bytes");
      }
      catch (IOException e)
      {
        _log.error("readEvents error: " + e.getMessage(), e);
        success = false;
      }
    }
    if (logDebugEnabled)
      _log.debug("read events from channel success=" + success + " to " + readBuffer);

    return success;
  }

  /**
   * Used by readEventsInternal to move the partial event at the end to the beginning of the
   * staging buffer so we can try to read more data.
   *
   * @param readPos
   * @param logDebugEnabled
   */
  private void compactStgBuffer(ReadEventsReadPosition readPos, boolean logDebugEnabled)
  {
    final ByteBuffer readBuffer = readPos.getReadBuffer();

    readBuffer.clear();//despite its name, clear() does not remove the data
    if (readPos.hasNext())
    {
      if (logDebugEnabled)
      {
        _log.debug("Copying " + readPos.bytesRemaining() + " bytes to the start of the readBuffer");
      }

      for (int i=0; i < readPos.bytesRemaining(); ++i)
      {
        readBuffer.put(readBuffer.get(readPos.getPosition() + i));
      }
      readPos.startIteration();

      if (logDebugEnabled)
      {
        _log.debug("readBuffer after compaction: " + readBuffer + "; " + readPos);
      }
    }
  }

  /**
   * Makes sure that we have enough space at the destination write buffer. Depending on the
   * queuing policy, we can either wait for space to free up or will overwrite it.
   *
   * @param writeStartPos     the gen-id starting write position
   * @param writeEndPos       the gen-id ending write position (after the last byte written)
   * @param logDebugEnabled   a flag if debug logging messages are enabled
   * @return true if the wait for free space was interrupted prematurely
   */
  private boolean ensureFreeSpace(long writeStartPos, long writeEndPos, boolean logDebugEnabled)
  {
    // Normalize the write end position to make sure that if we have to move
    // the head, it points to real data.  This deals with two code-path
    // variants of the same basic bug.  Specifically, if the head points at the
    // current bytebuffer's limit (i.e., at invalid data at the end of it), and
    // the proposed end position is exactly equal to that, BufferPositionParser's
    // incrementOffset() (a.k.a. sanitize()) advances the position to the start
    // of the next buffer.  Since this is beyond (or "overruns") the head, the
    // subsequent call to setNextFreePos(), which calls moveCurrentWritePosition(),
    // blows up.  By normalizing the end position, we effectively block until the
    // head can advance to the same position (or beyond), i.e., until all iterators
    // have caught up, which allows setNextFreePos()/moveCurrentWritePosition() to
    // succeed.  See DDSDBUS-1816 for even more details.
    final BufferPosition normalizedWriteEndPos = new BufferPosition(writeEndPos,
                                                                    _bufferPositionParser,
                                                                    _buffers);
    normalizedWriteEndPos.skipOverFreeSpace();

    boolean interrupted = false;

    if (!empty())
    {
      if (QueuePolicy.BLOCK_ON_WRITE == _queueingPolicy)
      {
        interrupted = waitForReadEventsFreeSpace(logDebugEnabled, writeStartPos,
                                                 normalizedWriteEndPos.getPosition());
      }
      else
      {
        freeUpSpaceForReadEvents(logDebugEnabled, writeStartPos,
                                 normalizedWriteEndPos.getPosition());
      }
    }
    if (logDebugEnabled)
    {
      _log.debug("ensureFreeSpace: writeStart:" + _bufferPositionParser.toString(writeStartPos, _buffers) +
                 "; writeEnd:" + _bufferPositionParser.toString(writeEndPos, _buffers) +
                 "; normalizedWriteEnd:" + normalizedWriteEndPos +
                 "; head:" + _head + "; tail:" + _tail +
                 "; interrupted:" + interrupted);
    }
    assert interrupted || !overwritesHead(writeStartPos, normalizedWriteEndPos.getPosition());

    return interrupted;
  }

  /**
   * Waits for space to free up in the buffer to be used by readEvents. The caller should specify
   * the buffer range it wants to write to. The beginning and end of the range are genid-based
   * buffer offsets.
   *
   * @param logDebugEnabled       if we should log debug messages
   * @param writeStartPosition    the beginning of the desired write range
   * @param writeEndPosition      the end (after last byte) of the desired write range
   * @return true if the wait succeeded; false if the wait was interrupted
   */
  private boolean waitForReadEventsFreeSpace(boolean logDebugEnabled,
                                             long writeStartPosition,
                                             long writeEndPosition)
  {
    assert _queueLock.isHeldByCurrentThread();

    boolean interrupted = false;
    // If we detect that we are overwriting head, wait till we have space available
    while (!interrupted && overwritesHead(writeStartPosition, writeEndPosition))
    {
      _log.info("Waiting for more space to be available. WriteStart: "
                + _bufferPositionParser.toString(writeStartPosition, _buffers) + " to "
                + _bufferPositionParser.toString(writeEndPosition, _buffers) + " head = " + _head);

      try
      {
        _notFull.await();
      } catch (InterruptedException ie) {
        _log.warn("readEvents interrupted", ie);
        interrupted = true;
      }

      if (logDebugEnabled)
      {
        _log.debug("Coming out of wait for more space. WriteStart: "
                   + _bufferPositionParser.toString(writeStartPosition, _buffers) + " to "
                   + _bufferPositionParser.toString(writeEndPosition, _buffers) + " head = " + _head);
      }
      if(isClosed())
        throw new DatabusRuntimeException("Coming out of wait for more space, but buffer has been closed");

    }
    return interrupted;
  }

  /**
   * Checks if a given range of gen-id positions contains the head. The range is defined by
   * [writeStartPosition,  writeEndPosition). This check is performed to ensure that a
   * write will not overwrite event data.
   * @return true iff the given range contains the head
   */
  // TODO:  add (debug-only?) sanity check that no part of write-range overlaps any active portion of buffer?
  //        (for example, could be contained entirely between head and tail, or overwriting only tail end)
  private boolean overwritesHead(long writeStartPosition, long writeEndPosition)
  {
    return empty()? false : Range.containsReaderPosition(writeStartPosition,  writeEndPosition,
                                                         _head.getPosition(), _bufferPositionParser);
  }

  /**
   * Frees up in the buffer to be used by readEvents. This should be used only with
   * OVERWRITE_ON_WRITE {@link QueuePolicy}. The caller should specify the buffer range it wants
   * to write to. The beginning and end of the range are genid-based buffer offsets.
   *
   * Side effects: (1) the buffer head will be moved. (2) {@link #_timestampOfFirstEvent} is
   * changed
   *
   * @param logDebugEnabled       if we should log debug messages
   * @param writeStartPosition    the beginning of the desired write range
   * @param writeEndPosition      the end (after last byte) of the desired write range
   */
  private void freeUpSpaceForReadEvents(boolean logDebugEnabled, long writeStartPosition,
                                        long writeEndPosition)
  {
    if (logDebugEnabled)
      _log.debug("freeUpSpaceForReadEvents: start:" +
                  _bufferPositionParser.toString(writeStartPosition, _buffers)
                  + "; end:" + _bufferPositionParser.toString(writeEndPosition, _buffers)
                  + "; head:" + _head);
    if (overwritesHead(writeStartPosition, writeEndPosition))
    {
      if (logDebugEnabled)
      {
        _log.debug("free space from " + _bufferPositionParser.toString(writeStartPosition, _buffers)
                   + " to " + _bufferPositionParser.toString(writeEndPosition, _buffers) + " head = "
                   + _head);
      }

      long proposedHead = _scnIndex.getLargerOffset(writeEndPosition);
      if (proposedHead < 0)
      {
        // Unless there is a bug in scn index code, the reason to get here is if
        // the transaction is too big to fit in one buffer.
        String error = "track(ScnIndex.head): failed to get larger window offset:" +
            "nextFreePosition=" + _bufferPositionParser.toString(writeEndPosition, _buffers) +
            " ;Head=" + _head + "; Tail=" + _tail +
            " ;CurrentWritePosition=" + _currentWritePosition +
            " ;MinScn=" + getMinScn();
        _log.error(error);
        _scnIndex.printVerboseString(_log, Level.ERROR);

        throw new DatabusRuntimeException(error);
      }

      //we need to fetch the scn for the new head to pass to ScnIndex.moveHead()
      //TODO a hack that needs to be fixed
      long newScn = -1;
      long newTs = -1;
      if (proposedHead < _tail.getPosition())
      {
        DbusEvent e = eventAtPosition(proposedHead);
        newScn = e.sequence();
        newTs = e.timestampInNanos();
      }

      moveHead(proposedHead, newScn, newTs, logDebugEnabled);
    }
  }

  private void adjustByteBufferLimit(long oldHeadPos)
  {
    final int newHeadIdx = _head.bufferIndex();
    final int newHeadOfs = _head.bufferOffset();
    final long newHeadGenid = _head.bufferGenId();
    final int oldHeadIdx = _bufferPositionParser.bufferIndex(oldHeadPos);
    final int oldHeadOfs = _bufferPositionParser.bufferOffset(oldHeadPos);
    final long oldHeadGenid = _bufferPositionParser.bufferGenId(oldHeadPos);

    assert oldHeadPos <= _head.getPosition() : "oldHeaPos:" + oldHeadPos + " " +  toString();
    assert newHeadGenid - oldHeadGenid <= 1 : "oldHeaPos:" + oldHeadPos + " " +  toString();

    final boolean resetLimit = (newHeadIdx != oldHeadIdx) || /* head moves to a different ByteBuffer */
                               (newHeadOfs < oldHeadOfs);    /* wrap around in the same ByteBuffer */

    if (resetLimit)
    {
      if (_buffers.length == 1)
      {
        //a special case of a wrap-around in a single buffer
        _buffers[0].limit(_buffers[0].capacity());
      }
      else
      {
        final int bufferNumDiff = (oldHeadGenid < newHeadGenid) ?
            _buffers.length + newHeadIdx - oldHeadIdx :
            newHeadIdx - oldHeadIdx;
        assert 0 <= bufferNumDiff && bufferNumDiff <= _buffers.length  :
            "oldHeadPos:" + oldHeadPos + " " +  toString();

        for (int i = 0; i < bufferNumDiff; ++i)
        {
          final int bufIdx = (oldHeadIdx + i) % _buffers.length;
          _buffers[bufIdx].limit(_buffers[bufIdx].capacity());
        }
      }
      assert assertBuffersLimits();
    }
  }

  /**
   * Copies the current event bytes from the staging buffer to the main buffer. Previous calls must
   * ensure that the target write area determined by writePos is already free.
   * @param readPos         determines the region in the staging buffer to copy from
   * @param writePos        determines the region in the main buffer to write to
   */
  private void copyReadEventToEventBuffer(ReadEventsReadPosition readPos,
                                          ReadEventsWritePosition writePos,
                                          Iterable<InternalDatabusEventsListener> eventListeners,
                                          DbusEventsStatisticsCollector statsCollector,
                                          boolean logDebugEnabled)
  {
    final ByteBuffer readBuffer = readPos.getReadBuffer();
    final int numBytesToWrite = readPos.bytesProcessed();
    final int writeStartOfs = writePos.getCurOfs();
    final ByteBuffer curBuf = writePos.getCurBuf();

    assert writePos.getNextFree().bufferGenId() - _head.bufferGenId() <= 1 :
        writePos.toString() + " buf:" + toString();

    assert curBuf.limit() >= writePos.getNextFreeOfs() :
        "curBuf:" + curBuf + "; " + writePos;

    final int oldLimit = readBuffer.limit();
    readBuffer.mark();
    readBuffer.position(readPos.getReadStart());
    readBuffer.limit(readPos.getPosition());

    // Set the limit/position
    curBuf.position(writeStartOfs);
    if (LOG.isDebugEnabled())
    {
        LOG.debug("copying from " + readBuffer + " into " + writePos.getCurBuf() +
                  "head:" + _head + " tail:" + _tail);
    }
    curBuf.put(readBuffer); // copy _readBuffer
    readBuffer.limit(oldLimit);
    readBuffer.reset();

    if (numBytesToWrite > 0)
    {
      // update index and call listeners on each event (may rewrite event)
      updateNewReadEvent(readPos, writePos, statsCollector, eventListeners, logDebugEnabled);
      if(readPos.getLastSeenStgWin() > _seenEndOfPeriodScn)
      {
        _seenEndOfPeriodScn = readPos.getLastSeenStgWin(); // this is end of period for this SCN
      }
    }
    if (logDebugEnabled)
      LOG.debug("Tail is set to :" + _tail + ", Head is at :" + _head);

    assert (_head.bufferIndex() != _tail.bufferIndex() || _head.getPosition() < _tail.getPosition()
           || _head.bufferOffset() < writePos.getCurBuf().limit());
  }

  /**
   * Helper Method For ReadEvents
   * Responsible for validating the new event written to the current buffer of EVB and updating
   * SCNIndex, tail and currentWritePosition
   *
   * Side effects: updates the SCN index, {@link #_tail} and {@link #_currentWritePosition}. May
   * update the {@link #_empty} flag.
   *
   * @return number of events successfully processed
   */
  private int updateNewReadEvent(ReadEventsReadPosition readPos,
                                 ReadEventsWritePosition writePos,
    	  					     DbusEventsStatisticsCollector statsCollector,
    	  					     Iterable<InternalDatabusEventsListener> eventListeners,
    	  					     boolean logDebugEnabled)
  {
    int eventsWritten = 0;

    if ( writePos.getNextFreePos() > writePos.getCurPos())
    {
      // seems like we read "something"
      if (logDebugEnabled)
        LOG.debug("readEvents: acquiring iterator for " + writePos);

      eventsWritten = updateScnIndexWithNewReadEvent(readPos, writePos, eventListeners,
                                                     statsCollector, logDebugEnabled);

      if (eventsWritten >0)
      {
        writePos.setNextFreePos(_currentWritePosition.getPosition());
        _empty = false;
        updateFirstEventMetadata();
        _notEmpty.signalAll();
        assert assertBuffersLimits();
      } else {
        //This should not happen - if validity is true and endPosition > startPosition,
        // there should be atleast one event  and readPos.getCurEvent() should point to that
        LOG.error("Buffer State is :" + toString());
        LOG.error("readPos:" + readPos);
        LOG.error("writePos:" + writePos);
        throw new RuntimeException("Unexpected State in EventBuffer");
      }

      if (logDebugEnabled)
        _log.debug("updateNewReadEvents: eventsWritten:" + eventsWritten + "; " + readPos +
                   " " + writePos + "; tail:" + _tail);
    }

    return eventsWritten;
  }

  /*
   * Helper Method for ReadEvents
   * Iterate new event read to the current buffer in EVB and updates SCNIndex. It also calls out
   * all specified listeners.
   * Assumes no partial/invalid events are in the buffer. The caller owns the eventIterator and
   * should release it.
   *
   * Post conditions:
   * <ul>
   *   <li> eventIterator.hasNext() is false or eventIterator will point to the event whose
   *        processing failed.
   *   <li> readPos.getCurEvent() will contain the last valid Event if at least one is found.
   * </ul>
   */
  private int updateScnIndexWithNewReadEvent(ReadEventsReadPosition readPos,
                                             ReadEventsWritePosition writePos,
                                             Iterable<InternalDatabusEventsListener> eventListeners,
                                             DbusEventsStatisticsCollector statsCollector,
    	  									 boolean logDebugEnabled)
  {
    int eventsWritten = 0;
    long currentPosition = writePos.getCurPos();
    DbusEventInternalWritable e = writePos.next();

    try
    {
      assert e.isValid() : e.toString();

      //commit the data to the buffer before any internal listeners process it so that a
      //RuntimeException does not leave the buffer in an inconsistent state
      if (null != statsCollector)
        statsCollector.registerDataEvent(e);
      readPos.eventWritten();
      ++eventsWritten;
      _tail.setPosition(currentPosition + e.size());
      _currentWritePosition.setPosition(_tail.getPosition());

      if (! e.isControlMessage())
      {
        _timestampOfLatestDataEvent = e.timestampInNanos()/(1000*1000);
        if (_timestampOfFirstEvent == 0 )
        {
          _timestampOfFirstEvent = _timestampOfLatestDataEvent;
        }
      }

      _lastWrittenSequence = e.sequence();

      _scnIndex.onEvent(e, currentPosition, e.size());
      callListeners(e, currentPosition, eventListeners);
      if(eventListeners != _internalListeners) {
        // if this is not the same object (different set of listeners)
        callListeners(e, currentPosition, _internalListeners);
      }
    }
    catch (RuntimeException ex)
    {
      _log.error("error updating scn index " + _scnIndex + " for event " + readPos.getCurEvent()
                 + ": " + ex.getMessage(), ex);
    }

    return eventsWritten;
  }

  private void callListeners(DbusEventInternalWritable event, long currentPosition,
                             Iterable<InternalDatabusEventsListener> eventListeners) {
    if (eventListeners != null)
    {
      for (InternalDatabusEventsListener listener: eventListeners)
      {
        try
        {
            listener.onEvent(event, currentPosition, event.size());
        }
        catch (RuntimeException e)
        {
          _log.warn("internal listener " + listener + " failed for event " + event);
        }
      }
    }
  }

  /**
   * Creates an event at given gen-id position in the buffer. The position must be a valid position.
   * @param  pos     the desired position
   * @return the event object
   */
  private DbusEvent eventAtPosition(long pos)
  {
    final int proposedHeadIdx = _bufferPositionParser.bufferIndex(pos);
    final int proposedHeadOfs = _bufferPositionParser.bufferOffset(pos);
    DbusEvent e = _eventFactory.createReadOnlyDbusEventFromBuffer(_buffers[proposedHeadIdx], proposedHeadOfs);
    assert e.isValid();
    return e;
  }

  /**
   * A helper routine to log error messages related to
   * Old event delivery
   * Missing EOW events
   */
  private String logSequenceErrorPackets(ReadEventsReadPosition readPos)
  {
      String k="UNINITIALIZED";
      if ( readPos.getCurEvent().isKeyString() )
        try {
          k = new String(readPos.getCurEvent().keyBytes(), "UTF-8");
        } catch (UnsupportedEncodingException e){ }
      else {
         k = Long.toString(readPos.getCurEvent().key());
      }
      String errMsg = "" + _physicalPartition.getName()  +
    		  " _lastWrittenSequence=" + _lastWrittenSequence +
    		  " _seenEndOfPeriodScn=" + _seenEndOfPeriodScn + " key=" + k +
    		  " _scnRegress=" + _scnRegress + " " + readPos;
      return errMsg;
  }

  /**
   * Moves the head of the buffer after an erase of events. Caller must hold {@link #_queueLock}
   *
   * @param proposedHead       the new head gen-id position
   * @param newScn             the new head scn (may be -1)
   * @param newHeadTsNs        the new head timestamp (may be -1)
   * @param logDebugEnabled    if debuf logging is neabled
   */
  protected void moveHead(long proposedHead, long newScn, long newHeadTsNs, boolean logDebugEnabled)
  {
    assert _queueLock.isHeldByCurrentThread();

    final long oldHeadPos = _head.getPosition();
    if (logDebugEnabled)
      _log.debug("about to move head to " + _bufferPositionParser.toString(proposedHead, _buffers) +
                 "; scn=" + newScn + "; oldhead=" + _head);

    try
    {
      acquireWriteRangeLock(oldHeadPos, proposedHead);
    }
    catch (InterruptedException e)
    {
      throw new DatabusRuntimeException(e);
    }
    catch (TimeoutException e)
    {
      throw new DatabusRuntimeException(e);
    }

    try
    {
      if (proposedHead > _tail.getPosition())
        throw new DatabusRuntimeException("moveHead assert failure: newHead > tail: newHead:" +
                                          proposedHead + " " + toString());
      if (_tail.bufferGenId() - _bufferPositionParser.bufferGenId(proposedHead) > 1)
        throw new DatabusRuntimeException("moveHead assert failure: gen mismatch: newHead:" +
                                          proposedHead + " " + toString());

      this.setPrevScn(getMinScn());
      _head.setPosition(proposedHead);
      if (_head.equals(_tail))
      {
        _empty = true;
        newScn = -1;
      }

      if (null != _scnIndex) _scnIndex.moveHead(_head.getPosition(), newScn);
      updateFirstEventMetadata();

      //next we make sure we preserve the ByteBuffer limit() invariant -- see the comment
      //to _buffers
      adjustByteBufferLimit(oldHeadPos);

      if (logDebugEnabled)
          _log.debug("moved head to " + _head.toString() + "; scn=" + newScn);
      _notFull.signalAll();
    }
    finally
    {
      releaseWriteRangeLock();
    }
  }

  // We could probably optimize this to update minScn and timetampOfFirstEvent
  // only if they are not set OR if we are moving head.
  private void updateFirstEventMetadata() {
    if (!_queueLock.isHeldByCurrentThread()) {
      throw new RuntimeException("Queue lock not held when updating minScn");
    }
    boolean found = false;
    BaseEventIterator it = null;
    try {
      it = this.acquireLockFreeInternalIterator(
         _head.getPosition(), _tail.getPosition(), "updateFirstEventMetadata");
      while (it.hasNext()) {
        DbusEvent e = it.next();
        if (!e.isControlMessage()) {
          _minScn = e.sequence();
          _timestampOfFirstEvent = e.timestampInNanos()/1000000;
          found = true;
          break;
        }
      }
      if (!found) {
        _minScn = -1;
        _timestampOfFirstEvent = 0;
      }
    } finally {
      if (null != it) it.close();
    }
  }


  /**
   * Moves _currentWritePosition
   * @param  newWritePos   new gen-id position value for  _currentWritePosition
   */
  protected void moveCurrentWritePosition(long newWritePos)
  {
    //no write position regressions
    if (_currentWritePosition.getPosition() >= newWritePos)
    {
      throw new DatabusRuntimeException("moveCurrentWritePosition: assert regression: " +
           " _currentWritePosition:" + _currentWritePosition + "; newWritePos:" +
           _bufferPositionParser.toString(newWritePos, _buffers));
    }
    //make sure head is not overwritten
    if(overwritesHead(_currentWritePosition.getPosition(), newWritePos))
    {
      throw new DatabusRuntimeException("moveCurrentWritePosition: overwritesHead assert:" +
                                        this);
    }

    final int curWriteIdx = _currentWritePosition.bufferIndex();
    final int curWriteOfs = _currentWritePosition.bufferOffset();
    final long curWriteGenid = _currentWritePosition.bufferGenId();
    final int newWriteIdx = _bufferPositionParser.bufferIndex(newWritePos);
    final int newWriteOfs = _bufferPositionParser.bufferOffset(newWritePos);
    final long newWriteGenid = _bufferPositionParser.bufferGenId(newWritePos);

    //don't skip ByteBuffers
    if (!( newWriteIdx == curWriteIdx ||
           ((curWriteIdx + 1) % _buffers.length) == newWriteIdx))
    {
       throw new DatabusRuntimeException("buffer skip: _currentWritePosition:" +
                                         _currentWritePosition +
                                         "; newWritePos:" +
                                         _bufferPositionParser.toString(newWritePos, _buffers));
    }
    //don't skip generations
    if (newWriteGenid - curWriteGenid > 1)
    {
      throw new DatabusRuntimeException("generation skip: _currentWritePosition:" +
                                        _currentWritePosition +
                                        "; newWritePos:" +
                                       _bufferPositionParser.toString(newWritePos, _buffers) +
                                       "; this=" + this);
    }

    if ((newWriteGenid - _head.bufferGenId()) > 1)
    {
      throw new DatabusRuntimeException("new write position too far ahead: " +
                                        _bufferPositionParser.toString(newWritePos, _buffers) +
                                        "; this=" + this);
    }

    // move to a new ByteBuffer or wrap around in current?
    final boolean resetLimit = newWriteIdx != curWriteIdx ||
                               newWriteOfs < curWriteOfs;
    if (resetLimit)
    {
      _buffers[curWriteIdx].limit(curWriteOfs);
    }

    _currentWritePosition.setPosition(newWritePos);

    assert assertBuffersLimits();
  }

  /** Asserts the ByteBuffers limit() invariant. {@see #_buffers} */
  boolean assertBuffersLimits()
  {
    boolean success = _tail.getPosition() <= _currentWritePosition.getPosition();

    if (!success)
    {
      _log.error("tail:" + _tail + "> _currentWritePosition:" + _currentWritePosition );
      return false;
    }


    final int headIdx = _head.bufferIndex();
    final int writeIdx = _currentWritePosition.bufferIndex();

    // Buffers are split into zones depending on their relative position to the _head and
    // _currentWritePosition
    // _head Zone1 _currentWritePosition Zone2
    // Buffers in Zone2 are not full and should have their limit() == capacity()
    int zone = _head.getPosition() == _currentWritePosition.getPosition() ? 2 : 1;
    for (int i = 0; i < _buffers.length; ++i)
    {
      final int bufIdx = (headIdx + i) % _buffers.length;
      if (1 == zone && bufIdx == writeIdx)
      {
        //should we move to Zone 2?
        //just make sure that if the H and CWP are in the same buffer, H is before CWP
        if (bufIdx != headIdx || _head.bufferOffset() < _currentWritePosition.bufferOffset())
        {
          zone = 2;
        }
      }

      if (2 == zone) {  // empty zone, not readable
        if(_buffers[bufIdx].limit() != _buffers[bufIdx].capacity() // in empty zone limit should be equal to capacity
            &&  _head.bufferOffset() != _buffers[bufIdx].limit()    // unless head is at the limit (at the end but wasn't sanitized yet)
            ) {
          success = false;
          _log.error("assertBuffersLimits failure: buf[" + bufIdx + "]=" +
              _buffers[bufIdx] + "; head:" + _head +
              "; _currentWritePosition:" + _currentWritePosition +
              "; _tail:" + _tail);
        }
      }
    }

    return success;
  }

  @Override
  public String toString()
  {
    return "DbusEventBuffer ["
           + ",_rwLockProvider=" + _rwLockProvider + ", readLocked=" + readLocked
           + ",_scnIndex=" + _scnIndex + ", _buffers=" + Arrays.toString(_buffers)
           + ",_maxBufferSize=" + _maxBufferSize
           + "," + bufPositionInfo()
           + ",_allocatedSize=" + _allocatedSize + ", _internalListeners=" + _internalListeners
           + ",_allocationPolicy=" + _allocationPolicy + ", _queueingPolicy=" + _queueingPolicy
           + ",_mmapSessionDirectory=" + _mmapSessionDirectory
           + ",_busyIteratorPool.size=" + _busyIteratorPool.size() + ", _eventState=" + _eventState
           + ",_eventStartIndex=" + _eventStartIndex + ", _numEventsInWindow=" + _numEventsInWindow
           + ",_lastWrittenSequence=" + _lastWrittenSequence + ", _prevScn=" + _prevScn
           + ",_bufferPositionParser=" + _bufferPositionParser
           + "]";
  }

  public String bufPositionInfo()
  {
    return "_head=" + _head + ",_tail=" + _tail + ",_empty=" + _empty
        + ",_currentWritePosition=" + _currentWritePosition;
  }

  public String toShortString() {
    StringBuffer sb = new StringBuffer();
    sb.append("h=" + _head);
    sb.append(";t=" + _tail);
    sb.append(";cwp=" + _currentWritePosition);

    return sb.toString();
  }

  /**
   * Waits uninterruptibly for the buffer read space to rise above a certain threshold
   * @param freeSpaceThreshold
   */
  public void waitForFreeSpaceUninterruptibly(long freeSpaceThreshold)
  {
    try
    {
      waitForFreeSpace(freeSpaceThreshold,false);
    } catch (InterruptedException ie) {
      // Wont happen but to keep compiler happy
      LOG.error("This should not be seen");
    }
  }

  /**
   * Waits for the buffer read space to rise above a certain threshold
   * @throws InterruptedException when interrupted while waiting for more space
   * @param freeSpaceThreshold
   */
  public void waitForFreeSpace(long freeSpaceThreshold)
  throws InterruptedException
  {
    waitForFreeSpace(freeSpaceThreshold,true);
  }

  private void waitForFreeSpace(long freeSpaceThreshold, boolean interruptCaller)
  throws InterruptedException
  {
    _queueLock.lock();
    int blocked = 0;// counts number of minutes we are blocked in the wait,prints message if waited more then a minute
    try
    {
      while ( true )
      {
        int freeReadSpace = getBufferFreeReadSpace();

        if ( freeReadSpace >= freeSpaceThreshold ) {
          if(blocked>1) {
            LOG.info("we have enough space in the buffer in waitForFreeSpace(). freeSpace="
                + freeReadSpace + ",threshold=" + freeSpaceThreshold + ",bl=" + blocked
                + ",bufPosInfo=" + bufPositionInfo());
          }
          return;
        }
        if(blocked>0)
          LOG.warn("waiting for free space in the buffer in waitForFreeSpace(). freeSpace="
            + freeReadSpace + ",threshold=" + freeSpaceThreshold + ",bl=" + blocked
            + ",bufPosInfo=" + bufPositionInfo());

        blocked++;
        try
        {
          _notFull.await(60, TimeUnit.SECONDS); // if blocked will print warning every minute
        } catch (InterruptedException ie) {
          if ( interruptCaller)
            throw ie;
        }
        if(isClosed())
          throw new DatabusRuntimeException("Coming out of wait, and the buffer is closed");
      }
    } finally {
      _queueLock.unlock();
    }
  }


/**
   * Returns the amount of free space left in the event buffer.
   * No guarantees of atomicity.
   */
  public long getBufferFreeSpace()
  {
    long remaining = remaining();
    return remaining;
  }

  /**
   * Returns the amount of space left in the buffer that can be safely read
   * from a channel.
   * No guarantees of atomicity.
   */
  public int getBufferFreeReadSpace()
  {
    // While in readEvents, _readBuffer could be in inconsistent state

    assert(_eventState != WindowState.IN_READ);

    long remaining = remaining();
    return (int)Math.min(remaining, getMaxReadBufferCapacity());
  }

  public int getMaxReadBufferCapacity()
  {
    return _maxEventSize;
  }

  private long remaining()
  {
    if (LOG.isDebugEnabled())
    {
      LOG.debug("Remaining query : head = " + _head.toString() + " tail =" + _tail.toString());
    }
    if (empty()) {
      long space = 0;
      for (ByteBuffer buf : _buffers)
      {
        space += buf.capacity();
      }
      return space;
    }

    if (_head.getRealPosition() < _tail.getRealPosition()) {
      long space = 0;
      for (int i=0; i < _head.bufferIndex(); ++i)
      {
        space += _buffers[i].capacity();
      }
      space += _head.bufferOffset();
      space += _buffers[_tail.bufferIndex()].capacity() - _tail.bufferOffset();
      for (int i= _tail.bufferIndex()+1; i < _buffers.length; ++i)
      {
        space += _buffers[i].capacity();
      }
      return space;
    }

    if (_head.getRealPosition() > _tail.getRealPosition()) {
      if (_head.bufferIndex() == _tail.bufferIndex())
      {
        return (_head.getRealPosition() - _tail.getRealPosition());
      }
      else
      {
        long space = _buffers[_tail.bufferIndex()].capacity() - _tail.bufferOffset();
        space += _head.bufferOffset();

        for (int i=_tail.bufferIndex()+1; i < _head.bufferIndex(); ++i )
        {
          space += _buffers[i].capacity();
        }
        return space;
      }

    }

    return 0;
  }

  //TODO we can add a flag to control if we want to track iterators since it is used only for
  //debugging and it has overhead associated with it
  protected void trackIterator(BaseEventIterator eventIterator)
  {
    synchronized (_busyIteratorPool)
    {
      _busyIteratorPool.add(new WeakReference<BaseEventIterator>(eventIterator));
    }
  }

  protected void untrackIterator(BaseEventIterator eventIterator)
  {
    synchronized (_busyIteratorPool)
    {
      Iterator<WeakReference<BaseEventIterator>> refIter = _busyIteratorPool.iterator();
      //both remove specified iterator and clean up GC'ed references
      while (refIter.hasNext())
      {
        WeakReference<BaseEventIterator> curRef = refIter.next();
        BaseEventIterator iter = curRef.get();
        if (null == iter)
          refIter.remove();
        else if (iter.equals(eventIterator))
          refIter.remove();
      }
    }
  }

  /**
   * Creates a long-lived iterator over events. It has the ability to wait if there are no
   * immediately-available events. It is responsibility of the caller to free the iterator
   * using {@link #releaseIterator(BaseEventIterator)}.
   */
  public DbusEventIterator acquireIterator(String iteratorName) {
    _queueLock.lock();
    try
    {
      DbusEventIterator eventIterator = new DbusEventIterator(_head.getPosition(),
                                                              _tail.getPosition(),
                                                              iteratorName);
      return eventIterator;
    }
    finally
    {
      _queueLock.unlock();
    }
  }

  /**
   * Acquires an iterator over a fixed range of events. This iterator cannot block waiting for more
   * events. It is responsibility of the caller to free the iterator
   * using {@link #releaseIterator(BaseEventIterator)}.
   */
  protected InternalEventIterator acquireInternalIterator(long head, long tail, String iteratorName)
  {
    InternalEventIterator eventIterator = new InternalEventIterator(head, tail, iteratorName);
    return eventIterator;
  }

  /**
   * Acquires an iterator over a fixed range of events with no range-locking (it is responsibility
   * of the caller to ensure this is safe, e.g., by holding another range-lock over the desired
   * iterator range). This iterator cannot block waiting for more
   * events. It is responsibility of the caller to free the iterator
   * using {@link #releaseIterator(BaseEventIterator)}.
   */
  protected BaseEventIterator acquireLockFreeInternalIterator(long head, long tail,
                                                              String iteratorName)
  {
    BaseEventIterator eventIterator = new BaseEventIterator(head, tail, iteratorName);
    return eventIterator;
  }

  /**
   * Creates a "short-lived" iterator. The set of events that it is going to iterate over is
   * pre-determined at the time of the iterator creation. Subsequent additions of events to the
   * buffer will not be visible.
   *
   * <b>Important: any resources associated with the iterator will
   * be released once it goes over all events, i.e., when {@link Iterator#hasNext()} returns
   * false.</b>
   *
   * The iterator is meant to be used mostly in for-each loops.
   */
  @Override
  public Iterator<DbusEventInternalWritable> iterator()
  {
    _queueLock.lock();
    try
    {
      ManagedEventIterator eventIterator = new ManagedEventIterator(_head.getPosition(),
                                                                    _tail.getPosition());
      return eventIterator;
    }
    finally
    {
      _queueLock.unlock();
    }
  }

  public void releaseIterator(BaseEventIterator e)
  {
    e.close();
  }

  public void addInternalListener(InternalDatabusEventsListener listener)
  {
    if (!_internalListeners.contains(listener))
    {
      _internalListeners.add(listener);
    }
  }

  public boolean removeInternalListener(InternalDatabusEventsListener listener)
  {
    return _internalListeners.remove(listener);
  }

  /**
   * package private to allow helper classes to inspect internal details
   */
  long getHead()
  {
    return _head.getPosition();
  }

  /**
   * package private to allow helper classes to inspect internal details
   */
  long getTail()
  {
    return _tail.getPosition();
  }

  /**
   * package private to allow helper classes to inspect internal details
   */
  ByteBuffer[] getBuffer()
  {
      return _buffers;
  }

  /**
   * package private to allow helper classes to inspect internal details
   */
  ScnIndex getScnIndex()
  {
    return _scnIndex;
  }

  /**
   * @return the bufferPositionParser for this event buffer
   */
  public BufferPositionParser getBufferPositionParser()
  {
    return _bufferPositionParser;
  }


  /**
   * package private to allow helper classes to set the head of the buffer
   * internally updates index state as well.
   *
   * <p><b>NOTE: This modifies the event buffer state directly. Use outside unit
   * tests is extremely discouraged</b>
   */
  void setHead(long offset)
  {
    _head.setPosition(offset);
    _scnIndex.moveHead(offset);
  }

  /**
   * package private to allow helper classes to set the tail of the buffer
   * this does not update the scnIndex
   *
   * <p><b>NOTE: This modifies the event buffer state directly. Use outside unit
   * tests is extremely discouraged</b>
   */
  void setTail(long offset)
  {
    _tail.setPosition(offset) ;
    _currentWritePosition.setPosition(offset);
  }

  /**
   * deletes at least the first window in the buffer
   * Useful if you want to align the head of the buffer past an eop marker
   * @return -1 if we couldn't find the next window
   */
  long deleteFirstWindow()
  {
    long proposedHead = _scnIndex.getLargerOffset(_head.getPosition());
    if (proposedHead > 0)
    {
      _head.setPosition(proposedHead);
      _scnIndex.moveHead(proposedHead);
    }
    return proposedHead;
  }

  /*
   * Recreates SCN Index from the current state of EVB
   */
  void recreateIndex()
  {
    DbusEventIterator itr = null;
    try
    {
      _scnIndex.acquireWriteLock();

      // Clear the index
      _scnIndex.clear();

      // Iterate all the events in the buffer
      itr = acquireIterator("scnIndexRecreate");

      while (itr.hasNext())
      {
        long eventPosition = itr.getCurrentPosition();
        DbusEvent e = itr.next();
        _scnIndex.onEvent(e,eventPosition,e.size());
      }
    } finally {
      _scnIndex.releaseWriteLock();
      if ( itr != null)
        releaseIterator(itr);
    }
  }

  /*
  public boolean persist()
  {
    acquireWriteRangeLock(0, _maxBufferSize);

    String indexFileName= "eventBuffer.meta";
    String dataFileName = "eventBuffer.data";
    try
    {
    File indexFile = new File(indexFileName);
    indexFile.createNewFile();
    FileOutputStream fos = new FileOutputStream(indexFile);

    File dataFile = new File(dataFileName);
    dataFile.createNewFile();
    FileOutputStream dos = new FileOutputStream(dataFile);
    Checkpoint cp = new Checkpoint();
    cp.setFlexible();
    this.streamEvents(cp, -1, dos.getChannel(), Encoding.BINARY, null);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    JsonFactory f = new JsonFactory();
    JsonGenerator g;
    try
    {
      g = f.createJsonGenerator(baos, JsonEncoding.UTF8);

      g.writeStartObject();
      g.writeNumberField("head", _head);
      g.writeNumberField("tail", _tail);
      g.writeArrayFieldStart("bufferMeta");
      for (int i=0; i < _buffers.size(); ++i)
      {
        ByteBuffer buf = _buffers.get(i);
        g.writeStartObject();
        g.writeNumberField("capacity", buf.capacity());
        g.writeNumberField("limit", buf.limit());
        g.writeNumberField("position", buf.position());
        g.writeEndObject();
      }
      g.writeEndArray();
      g.writeEndObject();
      g.flush();
      g.close();
    }
    catch (IOException e)
    {
      LOG.error("persist error: " + e.getMessage(), e);
    }
    fos.write(baos.toByteArray());
    fos.close();
    }
    catch (IOException e)
    {
      LOG.error("persist error: " + e.getMessage(), e);
    }
    switch (_allocationPolicy)
    {
      case MMAPPED_MEMORY: {
        for (ByteBuffer buf : _buffers)
        {
          MappedByteBuffer mappedBuf = (MappedByteBuffer) buf;
          mappedBuf.force();
        }
      }
    }
    return true;
  }
  */


  public long getAllocatedSize()
  {
    return _allocatedSize;
  }

  public static class StaticConfig
  {
    private final long _maxSize;
    private final int _maxIndividualBufferSize;
    private final int _readBufferSize;
    private final int _maxEventSize;
    private final int _scnIndexSize;
    private final AllocationPolicy _allocationPolicy;
    private final File  _mmapDirectory;
    private final double _defaultMemUsage;
    private final QueuePolicy _queuePolicy;
    private final DbusEventBuffer _existingBuffer;
    private final RelayEventTraceOption _trace;
    private final AssertLevel _assertLevel;
    private final long _bufferRemoveWaitPeriod;
    private final boolean _restoreMMappedBuffers;
    private final boolean _restoreMMappedBuffersValidateEvents;

    private final boolean _enableScnIndex;

    public StaticConfig(long maxSize,
                        int maxIndividualBufferSize,
                        int readBufferSize,
                        int maxEventSize,
                        int scnIndexSize,
                        AllocationPolicy allocationPolicy,
                        File mmapDirectory,
                        double defaultMemUsage,
                        QueuePolicy queuePolicy,
                        DbusEventBuffer existingBuffer,
                        RelayEventTraceOption trace,
                        AssertLevel assertLevel,
                        long bufferRemoveWaitPeriod,
                        boolean restoreMMappedBuffers,
                        boolean restoreMMappedBuffersValidateEvents,
                        boolean enableScnIndex)
    {
      super();
      _maxSize = maxSize;
      _maxIndividualBufferSize = maxIndividualBufferSize;
      _readBufferSize = readBufferSize;
      _maxEventSize = maxEventSize;
      _scnIndexSize = scnIndexSize;
      _allocationPolicy = allocationPolicy;
      _mmapDirectory = mmapDirectory;
      _defaultMemUsage = defaultMemUsage;
      _queuePolicy = queuePolicy;
      _existingBuffer = existingBuffer;
      _trace = trace;
      _assertLevel = assertLevel;
      _bufferRemoveWaitPeriod = bufferRemoveWaitPeriod;
      _restoreMMappedBuffers = restoreMMappedBuffers;
      _restoreMMappedBuffersValidateEvents = restoreMMappedBuffersValidateEvents;
      _enableScnIndex = enableScnIndex;
    }

    public boolean isEnableScnIndex()
    {
      return _enableScnIndex;
    }


    public boolean getRestoreMMappedBuffersValidateEvents() {
      return _restoreMMappedBuffersValidateEvents;
    }

    public boolean getRestoreMMappedBuffers() {
      return _restoreMMappedBuffers;
    }

    /**
     * wait time before a buffer with ref count 0 is removed
     * from bufferMult
     * @return waitPeriod in sec
     */
    public long getBufferRemoveWaitPeriod() {
      return _bufferRemoveWaitPeriod;
    }
    /**
     * The amount of memory to be used for the event buffer for data.
     *
     *  Default: 80% of the databus.relay.eventBuffer.defaultMemUsage * Runtime.getRuntime().maxMemory()
     * */
    public long getMaxSize()
    {
      return _maxSize;
    }


    /**
     * The maximum size of one single sub-buffer in the event buffer for data.
     */
    public int getMaxIndividualBufferSize()
    {
      return _maxIndividualBufferSize;
    }

    /**
     * The amount of memory to be used for the event buffer for read buffering.
     * The value is the inital size of the readBuffer : min(Config.averageEventSize,Config.maxEventSize)
     * Note that the actual value of readBufferSize at runtime is v, getReadBufferSize()<= v <= Config.maxEventSize
     * Default: 20K
     * */
    public int getReadBufferSize()
    {
      return _readBufferSize;
    }

    /**
     * The amount of memory to be used for the event buffer for the SCN index.
     *
     * Default: 10% of the databus.relay.eventBuffer.defaultMemUsage * Runtime.getRuntime().maxMemory()
     * */
    public int getScnIndexSize()
    {
      return _scnIndexSize;
    }

    /**
     * Allocation policy for the eventBuffer. Controls if the event buffer should be allocated as a
     * JVM heap buffer, direct buffer (not GC-ed) or mmapped buffer
     *
     * Default: MMAPPED_MEMORY if maxSize > 10000; HEAP_MEMORY otherwise
     */
    public AllocationPolicy getAllocationPolicy()
    {
      return _allocationPolicy;
    }

    /**
     * Top-level directory for mmapped files. Session directories are located under the
     * mmapDirectory and cleaned up on jvm exit. Ideally mmapDirectory and eventLogWriter.topLevelDir
     * should be located on different disks for optimum performance.
     *
     * Default: mmapDir
     */
    public File getMmapDirectory()
    {
      return _mmapDirectory;
    }

    /**
     * The fraction of the available memory to be used by the event buffer if maxSize is not
     * specified explicitly.
     *
     * Default: 0.75
     */
    public double getDefaultMemUsage()
    {
      return _defaultMemUsage;
    }

    /**
     * The queueing policy for the event buffer. A flag if the event buffer should overwrite the
     * oldest events if there is no space (OVERWRITE_ON_WRITE) for new events or whether it should
     * block (BLOCK_ON_WRITE)
     *
     * Default: OVERWRITE_ON_WRITE
     */
    public DbusEventBuffer.QueuePolicy getQueuePolicy()
    {
      return _queuePolicy;
    }

    /** Wired in event buffer */
    public DbusEventBuffer getExistingBuffer()
    {
      return _existingBuffer;
    }

    /** Event buffer tracing configuration (for testing purpose) */
    public RelayEventTraceOption getTrace()
    {
      return _trace;
    }

    public DbusEventBuffer getOrCreateEventBuffer(DbusEventFactory eventFactory)
    {
      DbusEventBuffer result = getExistingBuffer();
      if (null == result)
      {
        result = new DbusEventBuffer(this, null /* physicalPartition */, eventFactory);
      }

      return result;
    }

    public DbusEventBuffer getOrCreateEventBufferWithPhyPartition(PhysicalPartition pp, DbusEventFactory eventFactory)
    {
      DbusEventBuffer result = getExistingBuffer();
      if (null == result)
      {
        result = new DbusEventBuffer(this, pp, eventFactory);
      }

      return result;
    }

    /** Which class of asserts should be validated */
    public AssertLevel getAssertLevel()
    {
      return _assertLevel;
    }

    /**
     * @return the maximum allowed event size; if -1, an automatic determination will be done: see
     * {@link Config#setMaxEventSize(int)}.
     */
    public int getMaxEventSize()
    {
      return _maxEventSize;
    }
  }

  public static class Config implements ConfigBuilder<StaticConfig>
  {
    public static final int DEFAULT_MAX_EVENT_SIZE = -1;
    public static final double DEFAULT_DEFAULT_MEMUSAGE = 0.75;
    public static final double DEFAULT_EVENT_BUFFER_MAX_SIZE_QUOTA = 0.8;
    public static final double DEFAULT_EVENT_BUFFER_READ_BUFFER_QUOTA = 0.1;
    public static final QueuePolicy DEFAULT_QUEUE_POLICY = QueuePolicy.OVERWRITE_ON_WRITE;
    // Maximum individual Buffer Size
    private static int FIVE_HUNDRED_MEGABYTES_IN_BYTES = 500 * ByteSizeConstants.ONE_MEGABYTE_IN_BYTES;
    public static final int DEFAULT_INDIVIDUAL_BUFFER_SIZE = FIVE_HUNDRED_MEGABYTES_IN_BYTES;
    public static final String DEFAULT_MMAP_DIRECTORY = "mmappedBuffer";
    private static final long BUFFER_REMOVE_WAIT_PERIOD = 3600*24;
    private static final int DEFAULT_AVERAGE_EVENT_SIZE=20*1024;

    protected long _maxSize;
    protected int _maxIndividualBufferSize;
    protected int _readBufferSize;
    private int _maxEventSize;
    private int _averageEventSize=DEFAULT_AVERAGE_EVENT_SIZE;
    protected int _scnIndexSize;
    protected String _allocationPolicy;
    protected String _mmapDirectory;
    protected double _defaultMemUsage;
    protected String _queuePolicy;
    protected DbusEventBuffer _existingBuffer;
    private RelayEventTraceOptionBuilder _trace;
    private String _assertLevel = AssertLevel.NONE.toString();
    private long _bufferRemoveWaitPeriodSec;
    private boolean _restoreMMappedBuffers = false;
    private boolean _restoreMMappedBuffersValidateEvents = false;

    private boolean _enableScnIndex = true;

    public Config()
    {
      super();
      _defaultMemUsage = DEFAULT_DEFAULT_MEMUSAGE;
      deriveSizesFromMemPct();
      _allocationPolicy = getMaxSize() > 10000 ? "DIRECT_MEMORY":"HEAP_MEMORY";
      _mmapDirectory = DEFAULT_MMAP_DIRECTORY;
      _queuePolicy = DEFAULT_QUEUE_POLICY.toString();
      _trace = new RelayEventTraceOptionBuilder();
      _bufferRemoveWaitPeriodSec = BUFFER_REMOVE_WAIT_PERIOD;
      _restoreMMappedBuffers = false;
      _enableScnIndex = true;
      _maxEventSize = DEFAULT_MAX_EVENT_SIZE;
    }

    public Config(Config other)
    {
      _maxSize = other._maxSize;
      _maxIndividualBufferSize = other._maxIndividualBufferSize;
      _readBufferSize = other._readBufferSize;
      _maxEventSize = other._maxEventSize;
      _averageEventSize=other._averageEventSize;
      _scnIndexSize = other._scnIndexSize;
      _allocationPolicy = other._allocationPolicy;
      _mmapDirectory = other._mmapDirectory;
      _defaultMemUsage = other._defaultMemUsage;
      _queuePolicy = other._queuePolicy;
      _existingBuffer = other._existingBuffer;
      _trace = new RelayEventTraceOptionBuilder(other._trace);
      _bufferRemoveWaitPeriodSec = other._bufferRemoveWaitPeriodSec;
      _restoreMMappedBuffers = other._restoreMMappedBuffers;
      _enableScnIndex = other._enableScnIndex;
    }

    /** Computes the buffer sizes based on the current {@link #getDefaultMemUsage()} percentage */
    private void deriveSizesFromMemPct()
    {
      long maxMem = Runtime.getRuntime().maxMemory();
      long memForEventBuffer = Math.min((long)(_defaultMemUsage * maxMem), 10 * 1024 * 1024);
      _maxSize = (long)(DEFAULT_EVENT_BUFFER_MAX_SIZE_QUOTA * memForEventBuffer);
      _maxIndividualBufferSize = DEFAULT_INDIVIDUAL_BUFFER_SIZE;
      _readBufferSize = (int) (DEFAULT_EVENT_BUFFER_READ_BUFFER_QUOTA * memForEventBuffer);
      _scnIndexSize = (int) ( Math.abs(1.0 - DEFAULT_EVENT_BUFFER_MAX_SIZE_QUOTA -
                                       DEFAULT_EVENT_BUFFER_READ_BUFFER_QUOTA) * memForEventBuffer );
    }

    public boolean isEnableScnIndex()
    {
      return _enableScnIndex;
    }

    public void setEnableScnIndex(boolean enableScnIndex)
    {
      _enableScnIndex = enableScnIndex;
    }

    public void setRestoreMMappedBuffersValidateEvents(boolean restoreMMappedBuffersValidateEventsValidateEvents) {
      _restoreMMappedBuffersValidateEvents = restoreMMappedBuffersValidateEventsValidateEvents;
    }
    public boolean getRestoreMMappedBuffersValidateEvents() {
      return _restoreMMappedBuffersValidateEvents;
    }
    public void setRestoreMMappedBuffers(boolean restoreMMappedBuffers) {
      _restoreMMappedBuffers = restoreMMappedBuffers;
    }
    public boolean getRestoreMMappedBuffers() {
      return _restoreMMappedBuffers;
    }

    public void setBufferRemoveWaitPeriodSec(long waitPeriod) {
      _bufferRemoveWaitPeriodSec = waitPeriod;
    }
    public long getBufferRemoveWaitPeriodSec() {
      return _bufferRemoveWaitPeriodSec;
    }

    public void setMaxSize(long eventBufferMaxSize)
    {
      _maxSize = eventBufferMaxSize;
    }

    public void setMaxIndividualBufferSize(int individualBufferMaxSize)
    {
      _maxIndividualBufferSize = individualBufferMaxSize;
    }

    @Deprecated
    public void setReadBufferSize(int eventBufferReadBufferSize)
    {
      LOG.warn("Unable to set readBufferSize to " + eventBufferReadBufferSize +
            " as this has been deprecated! Use averageEventSize instead!");
    }

    /**
     * Size in bytes of
     * @param averageEventSize
     */
    public void setAverageEventSize(int averageEventSize)
    {
      _averageEventSize = averageEventSize;
    }

    public void setScnIndexSize(int eventBufferScnIndexSize)
    {
      LOG.info("setting scn index size to " + eventBufferScnIndexSize);
      _scnIndexSize = eventBufferScnIndexSize;
    }

    public void setDefaultMemUsage(double eventBufferDefaultMemUsage)
    {
      _defaultMemUsage = eventBufferDefaultMemUsage;
      deriveSizesFromMemPct();
    }

    public void setAllocationPolicy(String allocationPolicy)
    {
      LOG.info("Setting allocation policy to " + allocationPolicy);
      _allocationPolicy = allocationPolicy;
    }

    public String getMmapDirectory()
    {
      return _mmapDirectory;
    }

    public void setMmapDirectory(String mmapDirectory)
    {
      _mmapDirectory = mmapDirectory;
    }

    public void setQueuePolicy(String queuePolicy)
    {
      _queuePolicy = queuePolicy;
    }

    public void setExistingBuffer(DbusEventBuffer existingBuffer)
    {
      _existingBuffer = existingBuffer;
    }

    public long getMaxSize()
    {
      return _maxSize;
    }

    public int getMaxIndividualBufferSize()
    {
      return _maxIndividualBufferSize;
    }

    public int getReadBufferSize()
    {
      return _readBufferSize;
    }

    public int getScnIndexSize()
    {
      return _scnIndexSize;
    }

    public String getAllocationPolicy()
    {
      return _allocationPolicy;
    }

    public double getDefaultMemUsage()
    {
      return _defaultMemUsage;
    }

    public String getQueuePolicy()
    {
      return _queuePolicy;
    }

    public DbusEventBufferAppendable getExistingBuffer()
    {
      return _existingBuffer;
    }

    @Override
    public StaticConfig build() throws InvalidConfigException
    {
      //TODO (DDSDBUS-59) add verification for the config
      LOG.info("Event buffer max size: " + _maxSize);

      if (_readBufferSize > _averageEventSize)
      {
        LOG.warn("Ignoring staging event buffer size because it is deprecated: req size= " + _readBufferSize + " " +
        		"is larger than " + _averageEventSize);
      }
      _readBufferSize=_averageEventSize;
      LOG.info("Staging event buffer size: " + _readBufferSize);

      if (_enableScnIndex)
      {
        LOG.info("Event buffer scn index size: " + _scnIndexSize);
      }
      LOG.info("Event buffer allocation policy: " + _allocationPolicy.toString());
      LOG.info("Using queue policy: " + _queuePolicy.toString());

      AllocationPolicy allocPolicy;

      try
      {
        allocPolicy = AllocationPolicy.valueOf(_allocationPolicy);
      }
      catch (Exception e)
      {
        throw new InvalidConfigException("Invalid Config Value for allocationPolicy: " + _allocationPolicy);
      }

      File mmapDirectory = new File(_mmapDirectory);
      if (allocPolicy.equals(AllocationPolicy.MMAPPED_MEMORY) && !mmapDirectory.exists())
      {
        if (!mmapDirectory.mkdirs())
        {
          throw new InvalidConfigException("Invalid Config Value: Cannot create mmapDirectory: " + _mmapDirectory);
        }

        if (mmapDirectory.exists() && !mmapDirectory.canWrite())
        {
          throw new InvalidConfigException("Invalid Config Value: Cannot write to mmapDirectory: " + _mmapDirectory);
        }
      }

      QueuePolicy queuePolicy = null;
      try
      {
        queuePolicy = QueuePolicy.valueOf(_queuePolicy);
      }
      catch (IllegalArgumentException e)
      {
        throw new InvalidConfigException("Invalid queueing policy:" + _queuePolicy);
      }

      AssertLevel assertLevel = null;
      try
      {
        assertLevel = AssertLevel.valueOf(_assertLevel);
      }
      catch (IllegalArgumentException e)
      {
        throw new InvalidConfigException("Invalid assert level:" + _assertLevel);
      }

      //the biggest event we can process is the one that we can fit in the smallest ByteBuffer in the main event buffer,
      // i.e. the last ByteBuffer
      //int maxMaxEventSize = _maxSize % _maxIndividualBufferSize == 0 ? _maxIndividualBufferSize
      //                                                               : (int)(_maxSize % _maxIndividualBufferSize);
      //Although the above is safer, there are a bunch of unit test with specific sizing that fail due to the
      //automatic _initReadBufferSize adjustment in the constructor. Since small last buffer fragments are rare,
      //it is unlikely that to have to read an event that is > last buffer capacity and < the first buffer capacity.
      int maxMaxEventSize = maxMaxEventSize();

      int realMaxEventsSize = (DEFAULT_MAX_EVENT_SIZE == _maxEventSize)
          ? maxMaxEventSize
          : Math.min(_maxEventSize, maxMaxEventSize);

      int initReadBufferSize = Math.min(_readBufferSize, realMaxEventsSize);
      LOG.info("Initial staging event buffer size: " + initReadBufferSize);
      return new StaticConfig(_maxSize, _maxIndividualBufferSize, initReadBufferSize, realMaxEventsSize,
                              _scnIndexSize, allocPolicy,
                              mmapDirectory, _defaultMemUsage, queuePolicy, _existingBuffer,
                              _trace.build(), assertLevel, _bufferRemoveWaitPeriodSec,
                              _restoreMMappedBuffers, _restoreMMappedBuffersValidateEvents,
                              _enableScnIndex);
    }

    public RelayEventTraceOptionBuilder getTrace()
    {
      return _trace;
    }

    public void setTrace(RelayEventTraceOptionBuilder trace)
    {
      _trace = trace;
    }

    public String getAssertLevel()
    {
      return _assertLevel;
    }

    public void setAssertLevel(String assertLevel)
    {
      _assertLevel = assertLevel;
    }

    /**
     * The maximum event size; -1 if it will be determined automatically (see {@link #setMaxEventSize(int)}.
     */
    public int getMaxEventSize()
    {
      return _maxEventSize;
    }

    /**
     * Change the maximum allowed event size. Set to a value <= 0 for automatic determination based on other parameters:
     * maxEventSize = maxSize * (100 - CheckpointThresholdPct) / 100
     */
    public void setMaxEventSize(int maxEventSize)
    {
      _maxEventSize = maxEventSize > 0 ? maxEventSize : DEFAULT_MAX_EVENT_SIZE;
    }

    public int getAverageEventSize()
    {
      return _averageEventSize;
    }

    /** The maximum possible event size */
    public int maxMaxEventSize()
    {

      //the biggest event we can process is the one that we can fit in the smallest ByteBuffer in the main event buffer,
      // i.e. the last ByteBuffer
      //int maxMaxEventSize = _maxSize % _maxIndividualBufferSize == 0 ? _maxIndividualBufferSize
      //                                                               : (int)(maxSize  % _maxIndividualBufferSize);
      //Although the above is safer, there are a bunch of unit test with specific sizing that fail due to the
      //automatic _initReadBufferSize adjustment in the constructor. Since small last buffer fragments are rare,
      //it is unlikely that to have to read an event that is > last buffer capacity and < the first buffer capacity.
      int maxMaxEventSize = (int)Math.min(_maxSize, _maxIndividualBufferSize);

      //we have to account to the last byte in the ByteBuffer being unused for data
      return maxMaxEventSize - 1;
    }
  }

  @Override
  public long lastWrittenScn()
  {
    return _lastWrittenSequence;
  }

  @Override
  public void setStartSCN(long sinceSCN)
  {
    setPrevScn(sinceSCN);
  }

  /** Use only for testing!! sets _empty state of buffer  */
  void setEmpty(boolean val) {
    _empty = val;
  }

  public long getTimestampOfLatestDataEvent()
  {
    return _timestampOfLatestDataEvent;
  }

  /**
   * perform various closing duties
   * @param persistBuffer - if true save metainfo, if false delete sessionId dir and metainfo file
   */
  public void closeBuffer(boolean persistBuffer) {

    acquireWriteLock();
    try {

      // after this try-catch no new modifications of the buffer should start
      // but we need to check for some modifications in progress
      if(_eventState != WindowState.ENDED && _eventState != WindowState.INIT) { // ongoing append
        rollbackEvents();
      }

      setClosed(); // done under writelock

    } catch (DatabusException e) {
      _log.warn("for buffer " + toString(), e);
      return; // buffer already closed
    } finally {
      releaseWriteLock();
    }

    // some listeners are appenders to a file
    if(_internalListeners != null) {
      for(InternalDatabusEventsListener l : _internalListeners) {
        try {
         l.close();
        } catch (IOException ioe) {
          _log.warn("Couldn't close channel/file for listener=" + l, ioe);
        } catch (RuntimeException re) {
          _log.warn("Couldn't close channel/file for listener=" + l, re);
        }
      }
    }

    if(persistBuffer) {
      try {
        saveBufferMetaInfo(false);
      } catch (IOException e) {
        _log.error("error saving meta info for buffer for partition: " +
            getPhysicalPartition() + ": " + e.getMessage(), e);
      } catch (RuntimeException e) {
        _log.error("error saving meta info for buffer for partition: " +
            getPhysicalPartition() + ": " + e.getMessage(), e);
      }
    } else {
      // remove the content of mmap directory and the directory itself
      cleanUpPersistedBuffers();
    }
  }

  /**
   * Remove memory mapped file for the current session
   * and associated meta info file
   * Usecase : A database is dropped, all buffers associated with that database must be dropped, so also its associated persisted data files
   * @throws DatabusException
   */
  public void removeMMapFiles() {
	  if(_allocationPolicy != AllocationPolicy.MMAPPED_MEMORY) {
		  _log.warn("Skipping removal of MMap files because allocation policy is " + _allocationPolicy);
		  return;
	  }

	  File f = new File(_mmapDirectory, metaFileName());
	  if (f.exists())
		  f.deleteOnExit();

	  if (_mmapSessionDirectory != null && _mmapSessionDirectory.exists())
		  _mmapSessionDirectory.deleteOnExit();
  }

  private void flushMMappedBuffers() {
    _log.info("flushing buffers to disk for partition: " + _physicalPartition + "; allocation_policy=" + _allocationPolicy);
    if(_allocationPolicy == AllocationPolicy.MMAPPED_MEMORY) {
      for (ByteBuffer buf: _buffers)
      {
        if (buf instanceof MappedByteBuffer) ((MappedByteBuffer)buf).force();
      }

      _scnIndex.flushMMappedBuffers();
      _log.info("done flushing buffers to disk for partition: " + _physicalPartition);
    }
  }

  public void cleanUpPersistedBuffers() {
    if(_allocationPolicy != AllocationPolicy.MMAPPED_MEMORY ) {
      _log.info("Not cleaning up buffer mmap directory because allocation policy is " + _allocationPolicy
               + "; bufferPersistenceEnabled:" + _bufferPersistenceEnabled);
      return;
    }

    // remove all the content of the session id for this buffer
    _log.warn("Removing mmap directory for buffer(" + _physicalPartition + "): " + _mmapSessionDirectory);
    if(! _mmapSessionDirectory.exists() ||  !_mmapSessionDirectory.isDirectory()) {
      _log.warn("cannot cleanup _mmap=" + _mmapSessionDirectory + " directory because it doesn't exist or is not a directory");
      return;
    }

    try {
      FileUtils.cleanDirectory(_mmapSessionDirectory);
    } catch (IOException e) {
      _log.error("failed to cleanup buffer session directory " + _mmapSessionDirectory);
    }
    // delete the directory itself
    if(!_mmapSessionDirectory.delete()) {
      _log.error("failed to delete buffer session directory " + _mmapSessionDirectory);
    }

    File metaFile = new File(_mmapDirectory, metaFileName());
    if(metaFile.exists()) {
      _log.warn("Removing meta file " + metaFile);
      if(!metaFile.delete()) {
        _log.error("failed to delete metafile " + metaFile);
      }
    }
  }

  /**
   * save metaInfoFile about the internal buffers + scn index.
   * @param infoOnly - if true, will create a meta file that will NOT be used when loading the buffers
   * @throws IOException
   */

  public void saveBufferMetaInfo(boolean infoOnly) throws IOException {

    acquireWriteLock(); // uses _queue lock, same lock used by readevents()
    try {
      // first make sure to flush all the data including scn index data
      flushMMappedBuffers();

      // save index metadata
      //  scnIndex file will be located in the session directory
      _scnIndex.saveBufferMetaInfo();


      saveDataBufferMetaInfo(infoOnly);
    } finally {
      releaseWriteLock();
    }
  }

  private void saveDataBufferMetaInfo(boolean infoOnly) throws IOException {

    if(_allocationPolicy != AllocationPolicy.MMAPPED_MEMORY || !_bufferPersistenceEnabled) {
      _log.info("Not saving state metaInfoFile, because allocation policy is " + _allocationPolicy
               + "; bufferPersistenceEnabled:" + _bufferPersistenceEnabled);
      return;
    }

    String fileName = metaFileName() + (infoOnly?MMAP_META_INFO_SUFFIX:"");
    DbusEventBufferMetaInfo mi = new DbusEventBufferMetaInfo(new File(_mmapDirectory, fileName));
    _log.info("about to save DbusEventBuffer for PP " + _physicalPartition + " state into " + mi.toString());

    // record session id - to figure out directory for the buffers
    mi.setSessionId(_sessionId);

    // write buffers specific info - num of buffers, pos and limit of each one
    mi.setVal(DbusEventBufferMetaInfo.NUM_BYTE_BUFFER,Integer.toString(_buffers.length));
    StringBuilder bufferInfo = new StringBuilder("");
    for (ByteBuffer b : _buffers) {
      DbusEventBufferMetaInfo.BufferInfo bi = new DbusEventBufferMetaInfo.BufferInfo(b.position() , b.limit(), b.capacity());
      bufferInfo.append(bi.toString());
      bufferInfo.append(" ");
    }
    mi.setVal(DbusEventBufferMetaInfo.BYTE_BUFFER_INFO, bufferInfo.toString());

    String currentWritePosition = Long.toString(_currentWritePosition.getPosition());
    mi.setVal(DbusEventBufferMetaInfo.CURRENT_WRITE_POSITION, currentWritePosition);

    // _maxBufferSize
    mi.setVal(DbusEventBufferMetaInfo.MAX_BUFFER_SIZE, Integer.toString(_maxBufferSize));

    //NOTE. no need to save readBuffer and rwChannel

    String head = Long.toString(_head.getPosition());
    mi.setVal(DbusEventBufferMetaInfo.BUFFER_HEAD, head);

    String tail = Long.toString(_tail.getPosition());
    mi.setVal(DbusEventBufferMetaInfo.BUFFER_TAIL, tail);

    String empty = Boolean.toString(_empty);
    mi.setVal(DbusEventBufferMetaInfo.BUFFER_EMPTY, empty);

    mi.setVal(DbusEventBufferMetaInfo.ALLOCATED_SIZE, Long.toString(_allocatedSize));

    mi.setVal(DbusEventBufferMetaInfo.EVENT_START_INDEX, Long.toString(_eventStartIndex.getPosition()));

    // _numEventsInWindow
    mi.setVal(DbusEventBufferMetaInfo.NUM_EVENTS_IN_WINDOW, Integer.toString(_numEventsInWindow));
    // _lastWrittenSequence
    mi.setVal(DbusEventBufferMetaInfo.LAST_WRITTEN_SEQUENCE, Long.toString(_lastWrittenSequence));

    mi.setVal(DbusEventBufferMetaInfo.SEEN_END_OF_PERIOD_SCN, Long.toString(_seenEndOfPeriodScn));
    // _prevScn
    mi.setVal(DbusEventBufferMetaInfo.PREV_SCN, Long.toString(_prevScn));
    // _timestampOfFirstEvent
    mi.setVal(DbusEventBufferMetaInfo.TIMESTAMP_OF_FIRST_EVENT, Long.toString(_timestampOfFirstEvent));
    // _timestampOfLatestDataEvent
    mi.setVal(DbusEventBufferMetaInfo.TIMESTAMP_OF_LATEST_DATA_EVENT, Long.toString(_timestampOfLatestDataEvent));
    // eventState
    mi.setVal(DbusEventBufferMetaInfo.EVENT_STATE, _eventState.toString());

    mi.saveAndClose();

  }

  public void initBuffersWithMetaInfo(DbusEventBufferMetaInfo mi) throws DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException {

    if(mi.isValid()) {
      _head.setPosition(mi.getLong(DbusEventBufferMetaInfo.BUFFER_HEAD));
      _tail.setPosition(mi.getLong(DbusEventBufferMetaInfo.BUFFER_TAIL));

      _currentWritePosition.setPosition(mi.getLong(DbusEventBufferMetaInfo.CURRENT_WRITE_POSITION));
      _empty = mi.getBool(DbusEventBufferMetaInfo.BUFFER_EMPTY);
      // _eventStartIndex
      _eventStartIndex.setPosition(mi.getLong(DbusEventBufferMetaInfo.EVENT_START_INDEX));
      _numEventsInWindow = mi.getInt(DbusEventBufferMetaInfo.NUM_EVENTS_IN_WINDOW);
      _eventState = DbusEventBuffer.WindowState.valueOf(mi.getVal(DbusEventBufferMetaInfo.EVENT_STATE));

      _lastWrittenSequence = mi.getLong(DbusEventBufferMetaInfo.LAST_WRITTEN_SEQUENCE);
      _seenEndOfPeriodScn = mi.getLong(DbusEventBufferMetaInfo.SEEN_END_OF_PERIOD_SCN);

      _prevScn = mi.getLong(DbusEventBufferMetaInfo.PREV_SCN);
      _timestampOfFirstEvent = mi.getLong(DbusEventBufferMetaInfo.TIMESTAMP_OF_FIRST_EVENT);
      _timestampOfLatestDataEvent = mi.getLong(DbusEventBufferMetaInfo.TIMESTAMP_OF_LATEST_DATA_EVENT);
    }
  }

  public long getSeenEndOfPeriodScn()
  {
	  return _seenEndOfPeriodScn;
  }

  public boolean isSCNRegress()
  {
	  return _scnRegress;
  }

  public Logger getLog()
  {
    return _log;
  }

  /**
   * Dumps as a hex string the contents of a buffer around a position. Useful for debugging
   * purposes.
   * @param pos         gen-id position in the event buffer
   * @param length      number of bytes to print
   * @return the string; if it starts with "!", an error occurred
   */
  public String hexdumpByteBufferContents(long pos, int length)
  {
    try
    {
      if (length < 0)
      {
        return "! invalid length: " + length;
      }

      final int bufIdx = _bufferPositionParser.bufferIndex(pos);
      if (bufIdx >= _buffers.length)
      {
        return "! invalid buffer position: " + pos;
      }

      final int bufOfs = _bufferPositionParser.bufferOffset(pos);
      return StringUtils.hexdumpByteBufferContents(_buffers[bufIdx], bufOfs, length);
    }
    catch (RuntimeException e)
    {
      return "! unable to generate dump for position " + pos + ": " + e;
    }
  }

  //TODO HACK need better API
  /**
   * Injects an event in the regular stream of events
   * @return true iff successful
   * @throws InvalidEventException
   */
  public boolean injectEvent(DbusEventInternalReadable event)
         throws InvalidEventException
  {
    final ByteBuffer eventBuf = event.getRawBytes();
    byte[] cpEventBytes = null;
    if (eventBuf.hasArray())
    {
      cpEventBytes = eventBuf.array();
    }
    else
    {
      cpEventBytes = new byte[event.getRawBytes().limit()];
      eventBuf.get(cpEventBytes);
    }
    ByteArrayInputStream cpIs = new ByteArrayInputStream(cpEventBytes);
    ReadableByteChannel cpRbc = Channels.newChannel(cpIs);
    int ecnt = readEvents(cpRbc);

    return ecnt > 0;
  }

  public synchronized byte getEventSerializationVersion()
  {
    return _eventSerializationVersion;
  }

  // For Testing ONLY
  // TODO:  nuke this; incompatible with _eventFactory var (sole caller is DbusEventBufferForThisTest
  //        in TestSendEventsExecHandler:  nuke that, too)
  protected synchronized void setEventSerializationVersion(byte eventSerializationVersion)
  {
    _eventSerializationVersion = eventSerializationVersion;
  }
}
