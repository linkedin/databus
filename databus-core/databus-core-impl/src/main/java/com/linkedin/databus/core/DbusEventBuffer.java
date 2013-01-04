package com.linkedin.databus.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonParseException;

import com.linkedin.databus.core.DbusEvent.EventScanStatus;
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
import com.linkedin.databus2.core.AssertLevel;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.filter.DbusFilter;

public class DbusEventBuffer implements Iterable<DbusEvent>,
DbusEventBufferAppendable, DbusEventBufferStreamAppendable
{
  public static final String MODULE = DbusEventBuffer.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);
  public static final String MMAP_META_INFO_FILE_NAME = "metaFile";
  public static final String SESSION_PREFIX = "session_";
  public static final String MMAP_META_INFO_SUFFIX = ".info";
  public static final String PERF_MODULE = MODULE + "Perf";
  public static final Logger PERF_LOG = Logger.getLogger(PERF_MODULE);

  // On seeing some runtime exceptions, turn on debug log (useful for errors which repeat) but make sure they are not in debug forever
  private final int _numDebugOnErrorIterations = 0;
  private final Level _oldLogLevel = Level.OFF;
  private final boolean _alreadyDebugonErrorEnabled = false;
  private boolean _dropOldEvents = false;
  private final long _bufferRemoveWaitPeriodSec; // wait period before buffer is removed (if refCount is 0)
  private final double _nanoSecsInMSec = 1000000.0;


  private boolean _scnRegress = false;


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

  public class DbusEventIterator implements Iterator<DbusEvent>
  {

    private final RangeBasedReaderWriterLock _lockProvider;
    private final DbusEventBuffer _dbuf;
    private final BufferPosition _currentPosition;
    private final BufferPosition _iteratorTail;
    private DbusEvent _iteratingEvent;
    private boolean _selfRemoving;
    private String _identifier;
    private LockToken _lockToken;

    /**
     * Private constructor called by DbusEventBuffer to initialize iterator
     * @param dbuf
     * @param lockProvider
     * @param head
     * @param tail
     */
    private DbusEventIterator(DbusEventBuffer dbuf, RangeBasedReaderWriterLock lockProvider, long head, long tail)
    {
      iteratorsAllocated++;
      this._dbuf = dbuf;
      this._lockProvider = lockProvider;
      this._lockToken = null;
      _currentPosition = new BufferPosition(_bufferPositionParser,_buffers);
      _iteratorTail = new BufferPosition(_bufferPositionParser,_buffers);

      reset(head, tail, false, "default");
    }

    private DbusEventIterator(	DbusEventBuffer dbuf,
    							RangeBasedReaderWriterLock lockProvider,
    							long head,
    							long tail,
    							String iteratorName,
    							boolean selfRemoving)
    {
    	_dbuf = dbuf;
        _identifier = iteratorName;
        _lockProvider = lockProvider;
        _lockToken = null;
        _currentPosition = new BufferPosition(_bufferPositionParser,_buffers);
        _currentPosition.setPosition(head);

        _iteratorTail = new BufferPosition(_bufferPositionParser,_buffers);
        _iteratorTail.setPosition(tail);
        _iteratingEvent = new DbusEvent();
        _selfRemoving = selfRemoving;
        reacquireReadLock();
    }

    /**
     * Reset the iterator with a new reality w.r.t start and end points
     * @param head
     * @param tail
     * @param selfRemoving
     * @param iteratorName
     */
    public void reset(long head, long tail, boolean selfRemoving, String iteratorName)
    {
      _identifier = iteratorName;
      _currentPosition.setPosition(head);
      _iteratorTail.setPosition(tail);
      if (_iteratingEvent == null)
      {
        _iteratingEvent = new DbusEvent();
      }
      this._selfRemoving = selfRemoving;
      reacquireReadLock();

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
        destinationIterator = _dbuf.acquireIterator(iteratorName);
      }
      destinationIterator.reset(_currentPosition.getPosition(), _iteratorTail.getPosition(), _selfRemoving, destinationIterator._identifier);

      return destinationIterator;
    }

    /**
     * re-acquire read lock for the range addressed by the iterator
     */
    private void reacquireReadLock()
    {
      if (_lockToken != null)
      {
        _lockProvider.releaseReaderLock(_lockToken);
        _lockToken = null;
      }

      if (_currentPosition.getPosition() >= 0)
      {
        _lockToken = _lockProvider.acquireReaderLock(_currentPosition.getPosition(), _iteratorTail.getPosition(), _bufferPositionParser);
      }
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
     */
    private void copyBufferEndpoints()
    {
      _iteratorTail.copy(_dbuf._tail);

      if (_dbuf._head.getPosition() < 0)
      {
        _currentPosition.setPosition(-1);
      } else if (_currentPosition.getPosition() < 0) {
        _currentPosition.copy(_dbuf._head);
      }

      if ( _currentPosition.getPosition() < _head.getPosition())
      {
    	  _currentPosition.copy(_head);
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
            try
            {
              if (isDebug)
                LOG.debug("" + _identifier + ":waiting for notEmpty" + this);

              available = _notEmpty.await(time,unit);

              if ( isDebug )
                LOG.debug("_notEmpty coming out of await: " + available);

              if ( available )
                copyBufferEndpoints();
            }
            catch (InterruptedException e)
            {
              LOG.warn("await interrupted", e);
            }
          }
          return available;
        }
        finally
        {
          reacquireReadLock();
        }
      }
      finally
      {
        _queueLock.unlock();
      }
    }


    /**
     * Allows a reader to wait on the iterator until there is new data to consume
     *
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
     * Allows a reader to wait on the iterator until there is new data to consume
     *
     */
    public void await()
    	throws InterruptedException
    {
    	await(false);
    }

    public void await(boolean absorbInterrupt)
    	throws InterruptedException
    {
      // wait for notification that there is data to consume
      _queueLock.lock();
      try
      {
        try
        {
          copyBufferEndpoints();
          while (!hasNext())
          {
            try
            {
              if (LOG.isDebugEnabled())
                LOG.debug(_identifier+":waiting for notEmpty" + this);

              _notEmpty.await();
              copyBufferEndpoints();
            }
            catch (InterruptedException e)
            {
              LOG.warn("await interrupted", e);
              if ( ! absorbInterrupt)
              	throw e;
            }
            if (LOG.isDebugEnabled())
              LOG.debug("Iterator " + this + " coming out of await");
          }
        }
        finally
        {
          reacquireReadLock();
        }
      }
      finally
      {
        _queueLock.unlock();
      }
    }

    /**
     * Allows a reader to wait on the iterator until there is new data to consume
     *
     */
    public void awaitInterruptibly()
    {
      boolean debugEnabled = LOG.isDebugEnabled();

      // wait for notification that there is data to consume
      _queueLock.lock();
      try
      {
        try
        {
          copyBufferEndpoints();
          boolean wait = true;
          while (wait && !hasNext())
          {
            try
            {
              if (debugEnabled) LOG.debug(_identifier+":waiting for notEmpty" + this);

              _notEmpty.await();
              copyBufferEndpoints();
            }
            catch (InterruptedException e)
            {
              if (debugEnabled) LOG.debug("Iterator " + _identifier + ": wait interrupted");
              wait = false;
            }
            if (debugEnabled) LOG.debug("Iterator " + this + " coming out of await");
          }
        }
        finally
        {
          reacquireReadLock();
        }
      }
      finally
      {
        _queueLock.unlock();
      }

    }

    /**
     * Get the current event pointed to by the iterator
     */
    private DbusEvent currentEvent()
    {
      _currentPosition.sanitize();
      if (null==_iteratingEvent)
      {
        _iteratingEvent = new DbusEvent();
      }
      _iteratingEvent.reset(_dbuf._buffers[_currentPosition.bufferIndex()], _currentPosition.bufferOffset());
      return _iteratingEvent;
    }

    /**
     * Get the current position pointed to by the iterator
     * Package private to allow helper classes to access currentPosition
     */
    long getCurrentPosition()
    {
      _currentPosition.sanitize();
      return _currentPosition.getPosition();
    }

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
        _currentPosition.sanitize();
        result = (_currentPosition.getPosition() != _bufferPositionParser.sanitize(_iteratorTail.getPosition(), _dbuf._buffers));
        if (LOG.isDebugEnabled())
          LOG.debug(" - hasNext = " + result + " currentPosition = " +
                    _currentPosition +
                    " iteratorTail = " + _iteratorTail
                    + "limit = " + _dbuf._buffers[0].limit() +
                    "tail = " + _dbuf._tail);
      }
      if (!result && _selfRemoving)
      {
        _dbuf.releaseIterator(this);
      }
      return result;
    }

    private DbusEvent next(boolean validateEvent) throws InvalidEventException
    {
      if (!hasNext())
      {
        throw new NoSuchElementException();
      }
      DbusEvent nextEvent = currentEvent();
      if (validateEvent)
      {
        if (!nextEvent.isValid())
        {
          throw new InvalidEventException();
        }
      }
      _currentPosition.incrementOffset(nextEvent.size());
      return nextEvent;
    }

    /**
     * Returns a cached instance. Do not expect the event returned
     * by next() to have a validity lifetime longer than the next next() call
     */
    @Override
    public DbusEvent next()
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
		return "DbusEventIterator [_currentPosition=" + _currentPosition
				+ ", _iteratorTail=" + _iteratorTail + ", _iteratingEvent="
				+ _iteratingEvent + ", _selfRemoving=" + _selfRemoving
				+ ", _identifier=" + _identifier + ", _lockToken=" + _lockToken
				+ "]";
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
        LOG.debug("Iterator " + _identifier + " hasNext = " + hasNext() + " being asked to remove stuff" + this);

      _dbuf.acquireWriteLock();
      try
      {
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
      finally
      {
        reacquireReadLock();
        _dbuf.releaseWriteLock();
      }
    }

    /**
     * TODO (High): Why is this needed DDSDBUS-55
     */
    public void releaseReadLock() {
      if (_lockToken !=null)
      {
        _lockProvider.releaseReaderLock(_lockToken);
        _lockToken = null;
      }

    }

    public String getIdentifier()
    {
      return _identifier;
    }

    public DbusEventBuffer getEventBuffer()
    {
      return _dbuf;
    }

    public boolean equivalent(DbusEventIterator lastSuccessfulIterator)
    {

    	return (lastSuccessfulIterator != null)  && lastSuccessfulIterator._currentPosition.equals(_currentPosition);

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
  private final RangeBasedReaderWriterLock _rwLockProvider;
  private final AtomicInteger readLocked = new AtomicInteger(0);
  private boolean writeLocked = false;
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
   * used for reading bytes into a staging area
   * before inserting into the primary buffer
   */
  private final ByteBuffer _readBuffer;

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
  DbusEvent _writingEvent; // use this event while writing to the buffer

  // Pool of iterators
  private final Queue<DbusEventIterator> _busyIteratorPool = new ArrayDeque<DbusEventIterator>(
      MIN_INITIAL_ITERATORS);

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

  /** timestamp of latest data event of buffer **/
  private volatile long _timestampOfLatestDataEvent = 0;

  /** The last generated session id; we keep track of those to avoid duplicates */
  private volatile long _lastSessionIdGenerated = -1;

  // Stats for monitoring performance
  static int iteratorsAllocated = 0;

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
  public boolean shouldBeRemoved(boolean now) {
    if(_refCount > 0)
      return false;

    if(now)
      return true;

    return (System.currentTimeMillis() - _tsRefCounterUpdate) > _bufferRemoveWaitPeriodSec*1000;
  }
  public int getRefCount() {
    return _refCount;
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
    lockFreeClear();
    _scnIndex.clear();
    if (start)
    {
      this.start(prevScn);
    }
    _empty=true;
    releaseWriteLock();
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
    // what happens to the iterators that might be iterating over this
    // buffer
    // should we call a notifyClear() on them?
    for (ByteBuffer buf: _buffers)
    {
      buf.clear();
    }
    _notFull.signalAll();
    //		notifyIterators(head, tail);
  }




  /**
   * Called when head moves; need to find the new event represented by new head of the buffer and get its timestamp
   */
  long getFirstEventTimestamp()
  {

    long startTimeTs = System.nanoTime();
    ScnIndex.ScnIndexEntry entry=null;
    long ts = 0;
    try
    {
      entry = _scnIndex.getClosestOffset(getMinScn());
    }
    catch (OffsetNotFoundException e1)
    {
      LOG.info("First event not found: at scn = " + getMinScn());
      return 0;
    }
    if (entry != null)
    {
      long offset = entry.getOffset();
      DbusEventIterator eventIterator = acquireIterator(offset, _bufferPositionParser.sanitize(_tail.getPosition(), _buffers), "firstEventIterator");
      if (eventIterator.hasNext()) {
        DbusEvent e = eventIterator.next();
       ts =  e.timestampInNanos()/(1000*1000);
      }
      releaseIterator(eventIterator);
    }

    long endTimeTs = System.nanoTime();
    if (PERF_LOG.isDebugEnabled())
    {
    	PERF_LOG.debug("getFirstEventTimestamp took:" + (endTimeTs - startTimeTs) / _nanoSecsInMSec  + "ms");
    }


    return ts;
  }

  public DbusEventBuffer(Config config) throws InvalidConfigException
  {
    this(config.build());
  }

  public DbusEventBuffer(StaticConfig config, PhysicalPartition pPartition)
  {
    this(config.getMaxSize(), config.getMaxIndividualBufferSize(), config.getScnIndexSize(),
         config.getReadBufferSize(), config.getAllocationPolicy(), config.getMmapDirectory(),
         config.getQueuePolicy(), config.getTrace(), pPartition, config.getAssertLevel(),
         config.getBufferRemoveWaitPeriod(), config.getRestoreMMappedBuffers(), config.getRestoreMMappedBuffersValidateEvents());
  }

  public DbusEventBuffer(StaticConfig config)
  {
    this(config.getMaxSize(), config.getMaxIndividualBufferSize(), config.getScnIndexSize(),
         config.getReadBufferSize(), config.getAllocationPolicy(), config.getMmapDirectory(),
         config.getQueuePolicy(), config.getTrace(), null, config.getAssertLevel(),
         config.getBufferRemoveWaitPeriod(), config.getRestoreMMappedBuffers(), config.getRestoreMMappedBuffersValidateEvents());
  }

  public DbusEventBuffer(long maxEventBufferSize, int maxIndividualBufferSize, int maxIndexSize,
                         int maxReadBufferSize, AllocationPolicy allocationPolicy,
                         File mmapDirectory, QueuePolicy policy,
                         AssertLevel assertLevel, boolean restoreBuffers) {
    this(maxEventBufferSize, maxIndividualBufferSize, maxIndexSize, maxReadBufferSize,
         allocationPolicy, mmapDirectory, policy,
         new RelayEventTraceOption(RelayEventTraceOption.Option.none), null, assertLevel,
         Config.BUFFER_REMOVE_WAIT_PERIOD, restoreBuffers, false);
  }

  public DbusEventBuffer(long maxEventBufferSize, int maxIndividualBufferSize, int maxIndexSize,
                  int maxReadBufferSize, AllocationPolicy allocationPolicy, File mmapDirectory,
                  QueuePolicy queuePolicy, RelayEventTraceOption traceOption,
                  PhysicalPartition physicalPartition, boolean restoreBuffers) {
    this(maxEventBufferSize, maxIndividualBufferSize, maxIndexSize, maxReadBufferSize,
         allocationPolicy, mmapDirectory, queuePolicy, traceOption, physicalPartition,
         AssertLevel.NONE, Config.BUFFER_REMOVE_WAIT_PERIOD, restoreBuffers, false);
  }

  /**
   * Fine-grained constructor.
   */
  public DbusEventBuffer(long maxEventBufferSize, int maxIndividualBufferSize, int maxIndexSize,
                         int maxReadBufferSize, AllocationPolicy allocationPolicy, File mmapDirectory, QueuePolicy queuePolicy,
                         RelayEventTraceOption traceOption, PhysicalPartition physicalPartition,
                         AssertLevel assertLevel, long bufRemovalWaitPeriod,
                         boolean restoreBuffers, boolean validateEventesInRestoredBuffers) {
    //TODO replace all occurrences of LOG with _log so we get partition info
    _log = (null == physicalPartition) ? LOG :
           Logger.getLogger(MODULE + "." + physicalPartition.toSimpleString());

    _assertLevel = assertLevel;
    LOG.info("DbusEventBuffer starting up with " + "maxEventBufferSize = " + maxEventBufferSize + ", maxIndividualBufferSize = " + maxIndividualBufferSize +
             ", maxIndexSize = "+ maxIndexSize +
             ", maxReadBufferSize = " + maxReadBufferSize + ", allocationPolicy = " +
             allocationPolicy.toString() + ", mmapDirectory = " + mmapDirectory.getAbsolutePath() +
             ",queuePolicy = " + queuePolicy +
             ", eventTraceOption = " + traceOption.getOption() + ", needFileSuffix = " +
             traceOption.isNeedFileSuffix() + ", assertLevel=" + _assertLevel +
             ", bufRemovalWaitPeriod=" + bufRemovalWaitPeriod + ", restoreBuffers=" + restoreBuffers);

    _bufferPersistenceEnabled = restoreBuffers;
    _queueingPolicy = queuePolicy;
    _allocationPolicy = allocationPolicy;
    List<ByteBuffer> buffers = new ArrayList<ByteBuffer>();
    _maxBufferSize = maxIndividualBufferSize;
    _writingEvent = new DbusEvent();
    _empty = true;
    _lastWrittenSequence = -1;
    _prevScn=-1L;
    _timestampOfFirstEvent = 0;
    _timestampOfLatestDataEvent = 0;
    _bufferRemoveWaitPeriodSec = bufRemovalWaitPeriod;
    if(physicalPartition == null) {
      physicalPartition = new PhysicalPartition(0, "default");
    }
    _physicalPartition = physicalPartition;

    // file to read meta info for saved buffers (if any), MetaInfo constructor doesn't create/open any actual file
    DbusEventBufferMetaInfo mi = null;

    // in case of MMAPED memory - see if there is a meta file, and if there is - read from it
    if(allocationPolicy == AllocationPolicy.MMAPPED_MEMORY) {
      _sessionId = generateSessionId(); // new session
      File metaInfoFile = new File(mmapDirectory, metaFileName());
      if(restoreBuffers) {
        if(!metaInfoFile.exists()) {
          LOG.warn("RestoreBuffers flag is specified, but the file " + metaInfoFile + " doesn't exist");
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
      if(!restoreBuffers)
        _mmapSessionDirectory.deleteOnExit();
    }
    // when allocating readBuffer we don't need the content - hence 'false' for restore buffers
    _readBuffer = allocateByteBuffer(maxReadBufferSize, DbusEvent.byteOrder, allocationPolicy,
                                     false, _mmapSessionDirectory,
                                     new File(_mmapSessionDirectory, "readBuffer"));

    // NOTE. for restoreBuffers option we don't store metaInfo for readBuffer - no need

    LOG.debug("Will allocate a total of " + maxEventBufferSize + " bytes");
    long allocatedSize = 0;
    while (allocatedSize < maxEventBufferSize)
    {
      int nextSize = (int) Math.min(_maxBufferSize, (maxEventBufferSize-allocatedSize));
      if (LOG.isDebugEnabled())
        LOG.debug("Will allocate a buffer of size " + nextSize + " bytes with allocationPolicy = " + allocationPolicy.toString());
      ByteBuffer buffer = allocateByteBuffer(nextSize, DbusEvent.byteOrder, allocationPolicy,
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

    _bufferPositionParser = new BufferPositionParser((int)(Math.min(_maxBufferSize, maxEventBufferSize)), buffers.size());

    _scnIndex = new ScnIndex(maxIndexSize, maxEventBufferSize, _maxBufferSize, _bufferPositionParser,
        allocationPolicy, restoreBuffers, _mmapSessionDirectory, _assertLevel);

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
    if (_scnIndex.isEmpty()) {
      _scnIndex.setUpdateOnNext(true);
    }
  }

  synchronized String generateSessionId() {
    // just in case - to guarantee uniqueness
    long sessionId;
    while ((sessionId = System.currentTimeMillis()) <= _lastSessionIdGenerated);
    _lastSessionIdGenerated = sessionId;
    return SESSION_PREFIX + _lastSessionIdGenerated;
  }

  String metaFileName() {
    return MMAP_META_INFO_FILE_NAME + "." + _physicalPartition.getName() + "_" + _physicalPartition.getId();
  }

  /**
   * go over all the ByteBuffers and validate them
   * @param bufsInfo
   * @throws DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException
   */
  private void setAndValidateMMappedBuffers(DbusEventBufferMetaInfo mi) throws DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException {
    // set buffer info - pos and limit
    DbusEventBufferMetaInfo.BufferInfo [] bufsInfo = null;
    bufsInfo = mi.getBuffersInfo();

    int i = 0;
    for(ByteBuffer buffer : _buffers) {
      DbusEventBufferMetaInfo.BufferInfo bi = bufsInfo[i];

      buffer.position(bi.getPos());
      buffer.limit(bi.getLimit());

      // validate
      if(buffer.position()>buffer.limit()     ||
          buffer.limit() > buffer.capacity()  ||
          buffer.capacity() != bi.getCapacity()) {
        String msg = "ByteBuffers dont't match: i= " + i + ";pos=" + buffer.position() +
            ";limit=" + buffer.limit() + "; capacity=" + buffer.capacity() + ";miCapacity=" + bi.getCapacity();
        throw new DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException(mi, msg);
      }
      i++;
    }
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
   * @param maxEventBufferSize
   * @param mi
   * @throws DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException
   */
  private void validateMetaData(long maxTotalEventBufferSize, DbusEventBufferMetaInfo mi) throws DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException {
    // figure out number of buffers we are going to allocate
    long numBuffs = maxTotalEventBufferSize/_maxBufferSize;
    if(maxTotalEventBufferSize % _maxBufferSize > 0) numBuffs++;  // calculate number of ByteBuffers we will have
    long miNumBuffs = mi.getLong("ByteBufferNum");
    if(miNumBuffs != numBuffs) {
      throw new DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException(mi ,
            "Invalid number of ByteBuffers in meta file:" + miNumBuffs + "(expected =" + numBuffs + ")");
    }
    // individual buffer size
    long miBufSize = mi.getLong("maxBufferSize");
    if(miBufSize != _maxBufferSize)
      throw new DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException(mi,
            "Invalid maxBufferSize in meta file:" + miBufSize + "(expected =" + _maxBufferSize + ")");

    // _allocatedSize - validate against real buffers
     long allocatedSize = mi.getLong("allocatedSize");
     if(maxTotalEventBufferSize != allocatedSize)
       throw new DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException(mi,
             "Invalid maxEventBufferSize in meta file:" + allocatedSize + "(expected =" + maxTotalEventBufferSize + ")");

  }

  public static ByteBuffer allocateByteBuffer(int size, ByteOrder byteOrder,
                                       AllocationPolicy allocationPolicy,
                                       boolean restoreBuffers,
                                       File mmapSessionDir,
                                       File mmapFile) {
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
    {
      if(restoreBuffers && !mmapFile.exists()) {
        LOG.warn("restoreBuffers is set to true, but file " + mmapFile + " doesn't exist");
      }
      if(restoreBuffers) {
        LOG.info("restoring buffer from " +mmapFile);
      }
      // dirs are already created and initialized
      if(!mmapSessionDir.exists())
        throw new RuntimeException(mmapSessionDir.getAbsolutePath() + " doesn't exist");

      if (mmapFile.exists() && !restoreBuffers && !mmapFile.delete()) {
        throw new RuntimeException("deletion of file failed: " + mmapFile.getAbsolutePath());
      }
      if(!restoreBuffers)
        mmapFile.deleteOnExit(); // in case we don't need files later.

      FileChannel rwChannel;
      try {
        rwChannel = new RandomAccessFile(mmapFile, "rw").getChannel();
        buffer = rwChannel.map(FileChannel.MapMode.READ_WRITE, 0, size).order(byteOrder);
        rwChannel.close();
      } catch (FileNotFoundException e){
        throw new RuntimeException(e);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
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
  public void setPrevScn(long scn) {
    if (_log.isDebugEnabled()) _log.info("setting prevScn to: " + scn);
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
    /**DbusEvent e = new DbusEvent(_buffers.get(0), 0);
    LOG.info("MinEvent: getMinScn: " + e);
     **/
    /*
     * Get the min scn in the index
     */

    long indexScn = _scnIndex.getMinScn();

    /*
     *  Since all searches happen through the index anyways, it is safer to return
     *  the indexScn as opposed to the first event in the buffer
     */

    return indexScn;

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
    resetWindowState();
    _eventState = WindowState.STARTED;
    // set currentWritePosition to tail.
    // This allows us to silently rollback any writes we did past the tail but never called endEvents() on.
    long tailPosition = _tail.getPosition();
    _currentWritePosition.setPosition( ((tailPosition > 0) ? tailPosition:0));
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
                             boolean enableTracing, DbusEventsStatisticsCollector statsCollector)  {
    DbusEventInfo eventInfo = new DbusEventInfo(null, 0L, pPartitionId, lPartitionId,
                                                timeStamp, srcId, schemaId, value, enableTracing,
                                                false);

    return appendEvent(key, eventInfo, statsCollector);
  }

  @Override
  public boolean appendEvent(DbusEventKey key,
                             DbusEventInfo eventInfo,
                             DbusEventsStatisticsCollector statsCollector)
  {
	boolean isDebugEnabled = LOG.isDebugEnabled();
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

      prepareForAppend(key, eventInfo.getValue());

      if (_eventState == WindowState.STARTED) {
        //We set eventStartIndex here because _currentWritePosition is not finalized before
        //the call to prepareForAppend
        _eventStartIndex.copy(_currentWritePosition);
      }

      long preWritePosition = _currentWritePosition.getPosition();
      if (isDebugEnabled)
      {
    	  LOG.debug("serializingEvent at position " + _currentWritePosition.toString());

    	  LOG.debug("PhysicalPartition passed in=" + eventInfo.getpPartitionId() + "; from the buffer = "
    			  			+ _physicalPartition.getId().shortValue());
      }

      eventInfo.setSequenceId(0L);
      eventInfo.setpPartitionId(_physicalPartition.getId().shortValue());
      eventInfo.setAutocommit(false);
      int bytesWritten = DbusEvent.serializeEvent(key, _buffers[_currentWritePosition.bufferIndex()], eventInfo);

      long expNumBytesWritten = DbusEvent.length(key, eventInfo.getValue());

      //prepareForAppend makes decision to move Head depending upon expNumBytesWritten
      if ( bytesWritten != expNumBytesWritten)
      {
    	  String msg = "Actual Bytes Written was :" + bytesWritten + ", Expected to Write :" + expNumBytesWritten;
    	  LOG.fatal(msg);
    	  LOG.fatal("Event Buffer is :" + toString());
    	  throw new RuntimeException(msg);
      }

      _currentWritePosition.incrementOffset(bytesWritten);

      assert _currentWritePosition.bufferGenId() - _head.bufferGenId() <= 1 : toString();

      _eventState = WindowState.EVENTS_ADDED;
      _numEventsInWindow++;
      _timestampOfLatestDataEvent = Math.max(_timestampOfLatestDataEvent,
                    eventInfo.getTimeStampInNanos());

      if (_writingEvent == null)
      {
        _writingEvent = new DbusEvent();
      }

      preWritePosition = _bufferPositionParser.sanitize(preWritePosition, _buffers);
      _writingEvent.reset(_buffers[_bufferPositionParser.bufferIndex(preWritePosition)], _bufferPositionParser.bufferOffset(preWritePosition));
    }
    catch (KeyTypeNotImplementedException ex)
    {
      if (null != statsCollector)
        statsCollector.registerEventError(EventScanStatus.ERR);
      throw new RuntimeException(ex);
    }
    finally
    {
      /*
       * Ensuring that any locks that might be held are released safely before
       * returning from this method
       */
      finalizeAppend();
    }
    return true;
  }

  /**
   * Sets up the buffer state to prepare for appending an event.
   * This includes
   * a) moving the head far enough so that the new event does not overwrite it.
   *    - this also implies moving the head for the ScnIndex to keep it in lock-step with the buffer
   * b) moving the currentWritePosition to the correct location so that the entire event will fit into the selected ByteBuffer
   * @param key
   * @param scn
   * @param value
   * @throws com.linkedin.databus.core.KeyTypeNotImplementedException
   */
  private void prepareForAppend(DbusEventKey key, byte[] value) throws com.linkedin.databus.core.KeyTypeNotImplementedException
  {
	boolean isDebugEnabled = LOG.isDebugEnabled();
    int dbusEventSize = DbusEvent.length(key, value);
    _queueLock.lock();
    try
    {
      long initialWriteStartPos = _currentWritePosition.getPosition();
      // LOG.debug("currentWritePosition = " + _bufferPositionParser.toString(currentWritePosition));
      ByteBuffer buffer = _buffers[_currentWritePosition.bufferIndex()];
      buffer.position(_currentWritePosition.bufferOffset());

      while ((buffer.capacity() - buffer.position()) < dbusEventSize)
      {
        // buffer does not have enough capacity to write this event to
        // set the limit at the current point and move to the next buffer
        buffer.limit(buffer.position());
        //System.out.println("Set limit for buffer at " + _bufferPositionParser.toString(currentWritePosition) + " to " + buffer.position());

        _currentWritePosition.sanitize();
        buffer = _buffers[_currentWritePosition.bufferIndex()];
        buffer.position(_currentWritePosition.bufferOffset());
      }

      if (buffer.position() + dbusEventSize > buffer.limit())
      {
        buffer.limit(buffer.capacity());
      }

      long startIndex = _currentWritePosition.getPosition();

      long stopIndex = _bufferPositionParser.incrementOffset(startIndex, dbusEventSize, _buffers);

      long startPositionCopy = _head.getPosition() < 0 ? 0 :_head.getPosition();
      _writingEvent.unsetInited();


      boolean moveHead = (!empty()) &&
      			Range.containsReaderPosition(initialWriteStartPos, stopIndex, _head.getPosition(), _bufferPositionParser);

      if (_bufferPositionParser.init(startPositionCopy))
        startPositionCopy = 0;

      acquireWriteRangeLock(startIndex, stopIndex);
      if (_bufferPositionParser.bufferOffset(stopIndex) > _buffers[_bufferPositionParser.bufferIndex(stopIndex)].limit())
      {
        throw new RuntimeException("I don't ever expect to be here");
        //buffers.get(_bufferPositionParser.bufferIndex(stopIndex)).limit(_bufferPositionParser.bufferOffset(stopIndex));
      }

      if (moveHead)
      {
        long proposedHead = _scnIndex.getLargerOffset(stopIndex);
        if (isDebugEnabled)
          LOG.debug("Move Head: CurrentHead = " + _head + " CurrentOffset = " + _bufferPositionParser.toString(stopIndex) +
                    " ProposedHead = " + _bufferPositionParser.toString(proposedHead));

       if (0 > proposedHead)
       {
         if ( QueuePolicy.OVERWRITE_ON_WRITE == _queueingPolicy)
         {
           try
           {
             _scnIndex.assertHeadPosition(_head.getRealPosition());
             _bufferPositionParser.assertSpan(_head.getPosition(), _tail.getPosition(),isDebugEnabled);
           } catch (RuntimeException re) {
             LOG.error("prepareForAppend: Got runtime Exception :", re);
           }
         }

         LOG.warn("track(ScnIndex.head): prepareForAppend: failed to find larger offset");
         LOG.warn("Head=" + _head + " Tail=" + _tail + " CurrentOffset=" + stopIndex +
                  " CurrentWritePosition=" + _currentWritePosition +
                  " ProposedHead=" + proposedHead + " PreviousOffset=" + startIndex
                  + " EventSize=" + dbusEventSize + " MinScn=" + getMinScn());
         LOG.warn("Event Buffer is :" + toString());
         LOG.warn("SCN Index is :");
         _scnIndex.printVerboseString(LOG, org.apache.log4j.Level.WARN);
       }

       moveHead(proposedHead, -1, -1, isDebugEnabled);
      }
      else
      {
        try
        {
          _head.setPosition(_bufferPositionParser.sanitize(startPositionCopy, _buffers));
        }
        catch (RuntimeException re)
        {
          LOG.error("Initial Start Write Position = " + _bufferPositionParser.toString(initialWriteStartPos));
          LOG.error("startPositionCopy = " + _bufferPositionParser.toString(startPositionCopy));
          LOG.error("_head = " + _head.toString());
          LOG.error("Range.contains = " + Range.containsReaderPosition(startIndex, stopIndex, _head.getPosition(),_bufferPositionParser));

          throw re;
        }
      }
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
    releaseWriteRangeLock();
  }


  /* (non-Javadoc)
   * @see com.linkedin.databus.core.DbusEventBufferAppendable#rollbackEvents()
   */
  @Override
  public void rollbackEvents() {
    // do nothing
    // tail should be pointing to eventWindowStartScn
    // reset window local state
    resetWindowState();

  }


  /* (non-Javadoc)
   * @see com.linkedin.databus.core.DbusEventBufferAppendable#endEvents(boolean, long, boolean, boolean)
   */
  @Override
  public void endEvents(boolean updateWindowScn, long windowScn,
                        boolean updateIndex, boolean callListener, DbusEventsStatisticsCollector statsCollector)
  {
    boolean isDebugEnabled = LOG.isDebugEnabled();

    if (windowScn < _lastWrittenSequence)
    {
      throw new RuntimeException("Trying to add batch of events at sequence: " + windowScn + " when lastWrittenSequence = " + _lastWrittenSequence);
    }

    if (WindowState.ENDED == _eventState){
        if (isDebugEnabled)
        {
          LOG.debug("Skipping event window as Window is already in ended state" + windowScn);
        }
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

    try
    {
      prepareForAppend(DbusEvent.EOPMarkerKey, DbusEvent.EOPMarkerValue);
      long preWritePosition = _currentWritePosition.getPosition();
      DbusEventInfo eventInfo = new DbusEventInfo(null,
                                                  windowScn,
                                                  _physicalPartition.getId().shortValue(),
                                                  (short)0,
                                                  _timestampOfLatestDataEvent,
                                                  DbusEvent.EOPMarkerSrcId,
                                                  DbusEvent.emptymd5,
                                                  DbusEvent.EOPMarkerValue,
                                                  false, //enable tracing
                                                  false // autocommit
                                                  );
      int bytesWritten =
        DbusEvent.serializeEndOfPeriodMarker(_buffers[_currentWritePosition.bufferIndex()], eventInfo);

      _currentWritePosition.incrementOffset(bytesWritten);
      assert _currentWritePosition.bufferGenId() - _head.bufferGenId() <= 1 : toString();
      //System.out.println("pre-sanitize-end-events currentWritePosition = " + _bufferPositionParser.toString(currentWritePosition));

      preWritePosition =  _bufferPositionParser.sanitize(preWritePosition, _buffers);
      _writingEvent.reset(_buffers[_bufferPositionParser.bufferIndex(preWritePosition)], _bufferPositionParser.bufferOffset(preWritePosition));
      //System.out.println("EOP Marker = " + writingEvent);

      finalizeAppend();

      _currentWritePosition.sanitize();
      //System.out.println("post-sanitize-end-events currentWritePosition = " + _bufferPositionParser.toString(currentWritePosition));
      // srcId is being set to 0, since End of Period applies to all sources
      // tracked by the buffer
      boolean updatedIndex = false;

      if ( updateWindowScn || updateIndex || callListener )
      {
        // HACK
        _eventStartIndex.sanitize();
        DbusEventIterator eventIterator = acquireIterator(_eventStartIndex.getPosition(),
                                                          _currentWritePosition.getPosition(), "endEventsIterator");

        try {
          LOG.debug("acquired iterator");
          //System.out.println("Acquiring iterator from " + _bufferPositionParser.toString(eventStartIndex) + " to " + _bufferPositionParser.toString(currentWritePosition));
          DbusEvent e = null;
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
    		  LOG.fatal("Got runtime Exception :", re);
    		  LOG.fatal("Event Buffer is :" + toString());
    		  LOG.fatal("SCN Index is :");
    		  _scnIndex.printVerboseString(LOG, org.apache.log4j.Level.FATAL);

    		  throw re;
    	  }
      }

      //Update Tail
      acquireWriteLock();
      try
      {
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
      	  LOG.fatal("Got runtime Exception :", re);
      	  LOG.info("Old Tail was :" + _bufferPositionParser.toString(oldTail) + ", New Tail is :" + _tail.toString());
      	  LOG.fatal("Event Buffer is :" + toString());
      	  throw re;
          }
        }

        if (LOG.isDebugEnabled())
          LOG.debug("DbusEventBuffer: head = " + _head.toString() + " tail = " + _tail.toString() + "empty = " + empty());
        if (null != statsCollector) {
          statsCollector.registerBufferMetrics(this.getMinScn(),_lastWrittenSequence,this.getPrevScn(),this.getBufferFreeSpace());
          statsCollector.registerTimestampOfFirstEvent(_timestampOfFirstEvent);
        }
        _empty = false;
        _notEmpty.signalAll();
      }
      finally
      {
        releaseWriteLock();
      }

    }
    catch (KeyTypeNotImplementedException ex)
    {
      if (null != statsCollector)
        statsCollector.registerEventError(EventScanStatus.ERR);
      throw new RuntimeException(ex);
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
  public int streamEvents(Checkpoint checkPoint,  boolean streamFromLatestScn, int batchFetchSize,
          WritableByteChannel writeChannel, Encoding encoding,
          DbusFilter filter) throws ScnNotFoundException, OffsetNotFoundException {
	  return streamEvents(checkPoint, streamFromLatestScn, batchFetchSize, writeChannel, encoding, filter, StreamingMode.CONTINUOUS, null);
  }

  public int streamEvents(Checkpoint checkPoint, int batchFetchSize,
                          WritableByteChannel writeChannel, Encoding encoding,
                          DbusFilter filter,
                          DbusEventsStatisticsCollector statsCollector) throws ScnNotFoundException, OffsetNotFoundException
  {
    return streamEvents(checkPoint, false, batchFetchSize, writeChannel, encoding, filter,
                        StreamingMode.CONTINUOUS, statsCollector);
  }


  public int streamEvents(Checkpoint checkPoint, boolean streamFromLatestScn, int batchFetchSize,
          WritableByteChannel writeChannel, Encoding encoding,
          DbusFilter filter,
          DbusEventsStatisticsCollector statsCollector) throws ScnNotFoundException, OffsetNotFoundException
  {
	  return streamEvents(checkPoint, streamFromLatestScn, batchFetchSize, writeChannel, encoding, filter,
			  StreamingMode.CONTINUOUS, statsCollector);
  }

  /**
   *
   * @param checkPoint
   * @param batchFetchSize
   * @param writeChannel
   * @param encoding
   * @param filter
   * @param statsCollector
   * @throws ScnNotFoundException
   */
  public int streamEvents(Checkpoint checkPoint, boolean streamFromLatestScn, int batchFetchSize,
                          WritableByteChannel writeChannel, Encoding encoding,
                          DbusFilter filter, StreamingMode sMode,
                          DbusEventsStatisticsCollector statsCollector)
  throws ScnNotFoundException, OffsetNotFoundException
  {
    long startTimeTs = System.nanoTime();

    int numEventsStreamed = 0;
    boolean isDebugEnabled = LOG.isDebugEnabled();
    boolean oneWindowAtATime = sMode == StreamingMode.WINDOW_AT_TIME; // window at a time

    //int sleepTimeMs = RngUtils.randomPositiveInt()%3;

    if (isDebugEnabled) LOG.debug("Stream:begin:" + checkPoint.toString());

    if (empty())
    {
      if (isDebugEnabled) LOG.debug("Nothing to send out. Buffer is empty");
      return numEventsStreamed;
    }

    long offset;
    // TODO (DDSDBUS-58): Put in assertions about checkPoint.getClientMode()
    long sinceScn = checkPoint.getWindowScn();
    long messagesToSkip = checkPoint.getWindowOffset();
    boolean skipWindowScn = messagesToSkip < 0;

    DbusEventIterator eventIterator = null;
    try {

      ScnIndex.ScnIndexEntry entry = null;
      if ( streamFromLatestScn )
      {
        sinceScn = _lastWrittenSequence;
        messagesToSkip = 0;
        skipWindowScn = false;
        checkPoint.init(); // Will no longer be flexible
      }

      if (checkPoint.getFlexible())
      {
        long minScn = 0;
        try
        {
          _queueLock.lock();
          eventIterator = acquireIterator(_head.getPosition(), _bufferPositionParser.sanitize(_tail.getPosition(), _buffers), "streamEventsIterator");
          minScn = getMinScn();

          if (isDebugEnabled)
            LOG.debug("Acquired read iterator from " + _head.toString() +
                      " to " + _bufferPositionParser.toString(_bufferPositionParser.sanitize(_tail.getPosition(), _buffers))
                      +  " minScn = " + minScn);
        } finally {
          _queueLock.unlock();
        }

        if (minScn < 0)
        {
          if (isDebugEnabled)
          {
            LOG.debug("Nothing to send out. Buffer is empty");
          }
          return numEventsStreamed;
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
              LOG.error("Buffer still not fully ready; please wait for new events: sinceScn=" + sinceScn + " Anticipating events from scn=" + prevScn);
            } else {
              LOG.error("Buffer still not fully ready; but request will be obsolete sinceScn=" + sinceScn + " Anticipating events from scn=" + prevScn);
              throw new ScnNotFoundException();
            }
          } else {
            LOG.debug("No new events for SCN:" + sinceScn);
          }
          return 0;
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
              return streamEvents(checkPoint, streamFromLatestScn, batchFetchSize, writeChannel,encoding,
                                  filter, statsCollector);
            }
            catch (ScnNotFoundException e1)
            {
              throw e1;
            }
          } else {
            //either ; sinceScn < prevScn or sinceScn=prevScn with offset >= 0;
            LOG.error("sinceScn is less than minScn and prevScn : sinceScn=" + sinceScn + " minScn=" + minScn + " PrevScn= " + prevScn);
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
        eventIterator = acquireIterator(offset, _bufferPositionParser.sanitize(_tail.getPosition(), _buffers), "streamEventsIterator");
        if (isDebugEnabled)
        {
          LOG.debug("Stream:offset:" + _bufferPositionParser.toString(offset));
          DbusEvent e = new DbusEvent();
          e.reset(_buffers[_bufferPositionParser.bufferIndex(offset)],
                  _bufferPositionParser.bufferOffset(offset));
          LOG.debug("Stream:Event@Offset:sequence:"+e.sequence());
        }
      }

      EventScanningState state = EventScanningState.LOOKING_FOR_FIRST_VALID_EVENT; // looking for first valid event
      int skippedMessages = 0;
      // Iterate over the buffer to locate batchFetchSize events
      int batchSize = 0;
      boolean foundWindow = false;
      long lastWindowScn = 0;
      boolean isFirstEvent = true;
      long startTimeTs2 = System.nanoTime();
      boolean done = false;
      while (!done && eventIterator.hasNext()) {
        // for the first event, we need to validate that we got a "clean" read lock
        // since we intentionally split the getOffset from the getIterator call
        DbusEvent e;

        try
        {
          e = eventIterator.next(isFirstEvent);
          if ( isFirstEvent)
          {
            if ((entry != null) && (entry.getScn() != e.sequence()))
            {
              String msg = "Concurrent Overwritting of Event. Expected sequence :" + entry.getScn()
                  + ", Got event=" + e.toString();
              LOG.warn(msg);
              throw new ScnNotFoundException(msg);
            }
          }
        }
        catch (InvalidEventException e2)
        {
          LOG.warn("Found invalid event on getting iterator. This is not unexpected but should be investigated.");
          LOG.warn("RangeBasedLocking :" + _rwLockProvider.toString(_bufferPositionParser, true));
          if (null != statsCollector)
            statsCollector.registerEventError(EventScanStatus.ERR);
          throw new ScnNotFoundException(e2);
        }

        isFirstEvent = false;

        if (state == EventScanningState.LOOKING_FOR_FIRST_VALID_EVENT)
        {
          if (e.sequence() > sinceScn)
          {
            LOG.info("ScnIndex state = " + _scnIndex);
            LOG.info("Iterator position = " + eventIterator._currentPosition.toString());
            LOG.info("Iterator = " + eventIterator);
            LOG.info("Found event " + e + " while looking for sinceScn = " + sinceScn);
            /**while (eventIterator.hasNext())
          {
            e = eventIterator.next();
            LOG.error("DbusEventBuffer:dump:" + e);
          }**/
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
              lastWindowScn = e.sequence();
              continue;
            }
            else
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
                  lastWindowScn = e.sequence();
                }
              }

          }
          else
          {
            if (e.sequence() < sinceScn)
            {
              lastWindowScn = e.sequence();
              continue;
            }
            else
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
                lastWindowScn = e.sequence();

              }
          }
        }

        if (state == EventScanningState.FOUND_WINDOW_ZONE)
        {
          if (skippedMessages < messagesToSkip)
          {
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
          // paydirt !!
          boolean controlMessage = e.isControlMessage();
          boolean filterAllowed = filter.allow(e);
          if (controlMessage || filterAllowed)
          {
            if (batchSize + e.size() > batchFetchSize)
            {
              // sending this would violate our contract on upper bound of
              // bytes to send
              if (isDebugEnabled)
                LOG.debug("streamEvents returning after streaming " + batchSize + " bytes because " + batchSize
                          + " + " + e.size() + " > " + batchFetchSize);
              break;
            }

            long startTimeTs1 = System.nanoTime();
            int bytesWritten = e.writeTo(writeChannel, encoding);
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
                  LOG.debug("Stream:sequence:"+e.sequence());
                else
                  LOG.debug("Stream:sequence:"+e.sequence()+":headerCrc:"+e.headerCrc());
              }

              /** When batch writing is implemented this becomes
               * written  = EventWriter.writeTo(e, writeChannel, encoding);
               * if (written) // The write actually happened and didn't get buffered
               * {
               *  eventIterator.remove();
               * }
               */
              batchSize += e.size();
              numEventsStreamed += 1;
              if(isDebugEnabled)
                LOG.debug("buf.stream: GOT event scn="+e.sequence() + ";srcid=" + e.srcId() +
                          ";eow=" + e.isEndOfPeriodMarker() + ";oneWindatTime=" + oneWindowAtATime);
            }

          }
          else
          {
            if (isDebugEnabled)
              LOG.debug("Event was valid according to checkpoint, but was filtered out :" + e);
          }

          // register both filtered and non-filtered events
          if (null != statsCollector)
          {
            statsCollector.registerDataEvent(e);
          }
          // end of the window - don't send more
          if(e.isEndOfPeriodMarker() && oneWindowAtATime)
          {
            LOG.info("buf.stream: reached end of a window. scn="+e.sequence());
            break;
          }
        }

        if (state == EventScanningState.MISSED_WINDOW_ZONE)
        {
          // did not find the window that we were looking for
          // set checkpoint to the lastWindowScn that is < the windowScn
          // being searched for
          // and start streaming from there.
          LOG.info("Setting checkpoint to " + lastWindowScn
                   + " could not serve " + checkPoint);
          checkPoint.setWindowScn(lastWindowScn);
          //get the lastWindowScn as well; not the next window; that's why the offset is 0 and not -1
          checkPoint.setWindowOffset(0);
          checkPoint.setConsumptionMode(DbusClientMode.ONLINE_CONSUMPTION);
          try
          {
            return streamEvents(checkPoint, streamFromLatestScn, batchFetchSize, writeChannel,encoding,
                                filter, statsCollector);
          }
          catch (ScnNotFoundException e1)
          {
            throw e1;
          }
        }

      }

      if (batchSize == 0)
      {
        if (LOG.isDebugEnabled())
          LOG.debug("No events were sent out.");
      }

      /**  DbusEvent e = new DbusEvent(_buffers.get(0), 0);
    LOG.info("MinEvent:beforeRelease " + e);
       **/
      long endTimeTs2 = System.nanoTime();
      if (PERF_LOG.isDebugEnabled())
      {
        	PERF_LOG.debug("while loop took:" + (endTimeTs2 - startTimeTs2) / _nanoSecsInMSec + "ms");
      }

    } finally {
      if(eventIterator != null)
        releaseIterator(eventIterator);
    }

    /**e.reset(_buffers.get(0), 0);
    LOG.info("MinEvent:afterRelease " + e);
     **/
    if (LOG.isDebugEnabled())
      LOG.debug("Stream:events:"+numEventsStreamed);

    long endTimeTs = System.nanoTime();
    if (PERF_LOG.isDebugEnabled())
    {
      	PERF_LOG.debug("streamEvents took:" + (endTimeTs - startTimeTs) / _nanoSecsInMSec + "ms");
    }


    return numEventsStreamed;

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
      ByteBuffer writeBuf = buf.duplicate().order(DbusEvent.byteOrder);
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
      DbusEvent e = new DbusEvent(buf, startBufferOffset);
      int currentBufferOffset = startBufferOffset;
      while (currentBufferOffset != endBufferOffset)
      {
        e.reset(buf, currentBufferOffset);

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
    //    return (_size.get()==0);

    /**    if (_head < 0)
      return true;

    return false;
     **/
  }

  private void releaseWriteLock()
  {
    writeLocked = false;
    _queueLock.unlock();
  }

  private void acquireWriteRangeLock(long startOffset, long endOffset)
  {
    _rwLockProvider.acquireWriterLock(startOffset, endOffset, _bufferPositionParser);
  }

  private void releaseWriteRangeLock()
  {
    _rwLockProvider.releaseWriterLock(_bufferPositionParser);
  }

  private void acquireWriteLock() {
    _queueLock.lock();
    writeLocked = true;
  }

  public boolean getWriteStatus() {
    return writeLocked;
  }

  public int getReadStatus() {
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
        return DbusEvent.appendToEventBuffer(in , this, null, false);
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
    DbusEvent _curEvent;
    /** The scn of the last successfully processed window */
    long _lastSeenStgWin;
    /** A flag if _scnRegress flag has to be reset after the current event is processed */
    boolean _resetScnRegress;

    public ReadEventsReadPosition()
    {
      _lastSeenStgWin = _seenEndOfPeriodScn;
    }

    /** This should be called after the data in the buffer changes: reading from network*/
    public void startIteration()
    {
      _start = 0;
      _pos = 0;
      _skippedEvents = 0;
      _end = _readBuffer.position();
      _lastProcessedSeq = _lastWrittenSequence;
      if (null == _curEvent) _curEvent = new DbusEvent(_readBuffer, _pos);
      else _curEvent.reset(_readBuffer, _pos);
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
      _curEvent.reset(_readBuffer, _pos);
      final EventScanStatus eventScanStatus = _curEvent.scanEvent(true);
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

          if (_log.isDebugEnabled()) {
            long eventSrcId = _curEvent.srcId();
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

  }

  /** A class used to keep track of the readEvents progress writing to the main buffer */
  class ReadEventsWritePosition
  {
    /** number of bytes remaining*/
    int _numBytesWritten;
    /** The iterator used to write the events */
    DbusEventIterator _writeIter;
    /** The last read event */
    DbusEvent _lastEvent;
    /** The current byte buffer being written to */
    ByteBuffer _curBuf;


    public ReadEventsWritePosition()
    {
      _writeIter = acquireIterator(_tail.getPosition(), _tail.getPosition(),
                                   "ReadEventsWritePosition");
    }

    public boolean hasNext()
    {
      return _writeIter.hasNext();
    }

    public  DbusEvent next()
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
      setNextFreePos(_bufferPositionParser.incrementOffset(getCurPos(), increment, _buffers, false));
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
      _writeIter._iteratorTail.setPosition(nextFreePos);
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
      getCurBuf().limit(getNextFreeOfs());
      setNextFreePos(_bufferPositionParser.sanitize(getNextFreePos(), _buffers));
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

  private int readEventsInternal(ReadableByteChannel readChannel,
                                 Iterable<InternalDatabusEventsListener> eventListeners,
                                 DbusEventsStatisticsCollector statsCollector)
          throws InvalidEventException
  {
    final boolean logDebugEnabled = LOG.isDebugEnabled();

    ReadEventsReadPosition readPos = new ReadEventsReadPosition();
    ReadEventsWritePosition writePos = new ReadEventsWritePosition();

    try
    {
      _readBufferLock.lock();
      _eventState = WindowState.IN_READ;

      boolean mightHaveMoreData = true;
      //ensuring index is updated correctly if a control event of preceding window doesn't appear
      //first (no start() called)
      if (_scnIndex.isEmpty())
      {
        _scnIndex.setUpdateOnNext(true);
      }
      try
      {
        while (mightHaveMoreData)
        {
          boolean success = readEventsFromChannel(readChannel, logDebugEnabled);
          readPos.startIteration();

          final int numBytesRead = readPos.bytesRemaining();

          //if there is an error we'll try to process whatever was read but stop after that
          mightHaveMoreData = success && (numBytesRead > 0) &&
              (_readBuffer.position() == _readBuffer.limit());

          if (numBytesRead > 0)
          {
            try
            {
              _queueLock.lock();

              try
              {
                _scnIndex.assertHeadPosition(_head.getRealPosition());
                _bufferPositionParser.assertSpan(_head.getPosition(),
                                                 _currentWritePosition.getPosition(),
                                                 logDebugEnabled);
              } catch (RuntimeException re) {
                LOG.fatal("Got runtime Exception :", re);
                LOG.fatal("Event Buffer is :" + toString());
                _scnIndex.printVerboseString(LOG, Level.DEBUG);
                throw re;
              }

              _readBuffer.flip();
              boolean hasMoreInStgBuffer = true;
              while (hasMoreInStgBuffer && readPos.hasNext())
              {
                writePos.startNewIteration();

                //figure out the boundary of events at which we can write
                final int contiguousCapacity = writePos.getCurBuf().capacity() -
                    writePos.getCurOfs();

                final ReadEventsScanStatus eventScanStatus = readPos.startEventProcessing();
                switch (eventScanStatus)
                {
                case OK:
                {
                  final int curEventSize = readPos.getCurEvent().size();
                  if (readPos.bytesProcessed()+ curEventSize > contiguousCapacity)
                  {
                    //not enough space to fit event in the target buffer
                    if (0 == writePos.getCurOfs())
                    {
                      //event bigger than the ByteBuffer capacity
                      throw new InvalidEventException("event too big to fit into buffer;"
                          + "size:" + curEventSize +
                          "; event:" + readPos.getCurEvent() +
                          "; buffer.capacity:" + writePos.getCurBuf().capacity());
                    }
                    else
                    {
                      if (logDebugEnabled)
                        LOG.debug("unable to fit event with size " + readPos.getCurEvent().size());

                      //if we could not fit all the data in the destination ByteBuffer,
                      //we should ensure that we clear up any remaining data in the
                      //ByteBuffer.
                      long bufferLimitWithGenId  =
                          _bufferPositionParser.setOffset(writePos.getCurPos(),
                                                          writePos.getCurBuf().limit());
                      boolean interrupted = ensureFreeSpace(writePos.getCurPos(),
                                                            bufferLimitWithGenId,
                                                            logDebugEnabled);
                      if (interrupted)
                      {
                        _log.warn("ensureFree space interrupted:" + readPos + " " + writePos);
                        return readPos.getNumReadEvents();
                      }
                      assert writePos.getCurBuf().limit() == writePos.getCurBuf().capacity() :
                          writePos.getCurBuf().toString();

                      writePos.moveToNextBuffer();
                      _tail.setPosition(writePos.getNextFreePos());
                      _currentWritePosition.copy(_tail);
                      assert assertBuffersLimits();
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
                  if (logDebugEnabled)
                    LOG.debug("partial event at " + readPos);
                  if (0 == readPos.getReadStart())
                  {
                    //we couldn't read an entire event in the staging buffer and we are already
                    //at the beginning so we cannot compact
                    //just give up
                    throw new InvalidEventException("event too big to fit in staging buffer: "
                        + "size:" + readPos.getCurEvent().size() +
                        "; event:" + readPos.getCurEvent() +
                        "; buffer.capacity:" + _readBuffer.capacity());
                  }
                  else
                  {
                    //compact stg buffer and try to read more data from the network
                    compactStgBuffer(readPos, logDebugEnabled);
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
                  LOG.warn("got an old event: " + errMsg);
                  readPos.eventSkipped();
                  break;
                }
                case INVALID_EVENT:
                {
                  if (null != statsCollector)
                    statsCollector.registerEventError(EventScanStatus.ERR);
                  throw new InvalidEventException();
                }
                case MISSING_EOP:
                {
                  String errMsg = logSequenceErrorPackets(readPos);
                  LOG.error("detected missing EOP: " + errMsg);
                  throw new InvalidEventException(errMsg);
                }
                default:
                    throw new IllegalStateException("unknow scan status: " + eventScanStatus);
                }

                if (!hasMoreInStgBuffer) break;
              }

              if (!readPos.hasNext()) _readBuffer.clear();
            } finally {
              _queueLock.unlock();
            }
          }
        }
      }
      finally
      {
        if (_readBuffer.position() > 0)
        {
          LOG.error("Clearing partial event left in buffer: " + _readBuffer.position());
        }
        _readBuffer.clear();

        if (null != statsCollector) {
          statsCollector.registerBufferMetrics(getMinScn(), this.lastWrittenScn(),
                                               this.getPrevScn(),
                                               this.getBufferFreeSpace());
          statsCollector.registerTimestampOfFirstEvent(_timestampOfFirstEvent);
        }
        _eventState = WindowState.ENDED;
      }
    } catch ( RuntimeException re) {
      LOG.error("Got runtime exception in readEvents. Might set Logging to debug !!: " +
                re.getMessage(), re);
      LOG.error("Buffer State :" + toString());
      throw re;
    } finally {
      _readBufferLock.unlock();
      writePos.close();
    }

    if (logDebugEnabled)
      _log.debug("readEvents result:" + readPos + " " + writePos);

    return readPos.getNumReadEvents();
  }

  /**
   * Read events from a channel for readEvents().
   *
   * @param readChannel      the channel to read from
   * @param logDebugEnabled  if debug logging is enabled
   * @return true the read succeeded
   */
  private boolean readEventsFromChannel(ReadableByteChannel readChannel,
                                        boolean logDebugEnabled)
  {
    if (logDebugEnabled)
      _log.debug("reading events from channel to " + _readBuffer);
    boolean success = true;
    long oneread = 1;
    while (oneread > 0)
    {
      try
      {
        oneread = readChannel.read(_readBuffer);
        if (logDebugEnabled)
          LOG.debug("Read " + oneread + " bytes");
      }
      catch (IOException e)
      {
        LOG.error("readEvents error: " + e.getMessage(), e);
        success = false;
      }
    }
    if (logDebugEnabled)
      _log.debug("read events from channel success=" + success + " to " + _readBuffer);

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
    _readBuffer.clear();//despite its name, clear() does not remove the data
    if (readPos.hasNext())
    {
      if (logDebugEnabled)
      {
        LOG.debug("Copying " + readPos.bytesRemaining() + " bytes to the start of the readBuffer");
      }

      for (int i=0; i < readPos.bytesRemaining(); ++i)
      {
        _readBuffer.put(_readBuffer.get(readPos.getPosition() + i));
      }
      readPos.startIteration();

      if (logDebugEnabled)
      {
        _log.debug("readBuffer after compaction: " + _readBuffer + "; " + readPos);
      }
    }
  }

  /**
   * Makes sure that we have enough space at the destination write buffer. Depending on the
   * queuing policy, we can either wait for space to free up or will overwrite it.
   *
   * @param writePosStart       the gen-id starting write position
   * @param writeEndStart       the gen-id ending write position (after the last byte written)
   * @param logDebugEnabled     a flag if debug logging messages are enabled
   * @return true if the wait for free space was interrupted prematurely
   */
  private boolean ensureFreeSpace(long writeStartPos, long writeEndPos, boolean logDebugEnabled)
  {
    boolean interrupted = false;
    if (QueuePolicy.BLOCK_ON_WRITE == _queueingPolicy)
    {
      interrupted = waitForReadEventsFreeSpace(logDebugEnabled, writeStartPos, writeEndPos);
    } else {
      freeUpSpaceForReadEvents(logDebugEnabled, writeStartPos, writeEndPos);
    }
    if (logDebugEnabled)
      _log.debug("ensureFreeSpace: writeStart:" + _bufferPositionParser.toString(writeStartPos) +
                 " writeEnd:" + _bufferPositionParser.toString(writeEndPos) +
                 "; head:" + _head + "; tail:" + _tail +
                 "; interrupted:" + interrupted);
    assert interrupted || !Range.containsReaderPosition(writeStartPos,  writeEndPos,
                                                        _head.getPosition(), _bufferPositionParser);

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
    while (!interrupted &&
           Range.containsReaderPosition(writeStartPosition,  writeEndPosition, _head.getPosition(),
                                        _bufferPositionParser))
    {
      LOG.warn("Waiting for more space to be available to avoid overwriting head. WriteStart :"
               + _bufferPositionParser.toString(writeStartPosition) + " to "
               + _bufferPositionParser.toString(writeEndPosition) + " head = "
               + _head.toString());

      try
      {
        _notFull.await();
      } catch (InterruptedException ie) {
        LOG.warn("readEvents interrupted", ie);
        interrupted = true;
      }

      if (logDebugEnabled)
      {
        LOG.debug("Coming out of Wait for more space. WriteStart :"
                  + _bufferPositionParser.toString(writeStartPosition) + " to "
                  + _bufferPositionParser.toString(writeEndPosition) + " head = " +
                  _head.toString());
      }

    }
    return interrupted;
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
                 _bufferPositionParser.toString(writeStartPosition)
                 + "; end:" + _bufferPositionParser.toString(writeEndPosition)
                 + "; head:" + _head);
    if (Range.containsReaderPosition(writeStartPosition, writeEndPosition, _head.getPosition(),
                                     _bufferPositionParser))
    {
      if (logDebugEnabled)
      {
        LOG.debug("free space from " + _bufferPositionParser.toString(writeStartPosition)
                  + " to " + _bufferPositionParser.toString(writeEndPosition) + " head = "
                  + _head);
      }

      long proposedHead = _scnIndex.getLargerOffset(writeEndPosition);
      if (proposedHead < 0)
      {
        String error = "track(ScnIndex.head): readEvents: failed to get larger window offset:" +
            "nextFreePosition=" + _bufferPositionParser.toString(writeEndPosition) +
            " ;Head=" + _head + "; Tail=" + _tail +
            " ;CurrentWritePosition=" + _currentWritePosition +
            " ;MinScn=" + getMinScn();
        LOG.error(error);
        _scnIndex.printVerboseString(LOG, Level.ERROR);

        throw new RuntimeException(error);
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

  /**
   * @param newHeadPos
   */
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

    boolean resetLimit = (newHeadIdx != oldHeadIdx) /* head moves to a different ByteBuffer*/
          || (newHeadOfs < oldHeadOfs) /* wrap around in the same ByteBuffer */;

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
            "oldHeaPos:" + oldHeadPos + " " +  toString();

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
   * @param writeBuffer     the destination buffer
   */
  private void copyReadEventToEventBuffer(ReadEventsReadPosition readPos,
                                          ReadEventsWritePosition writePos,
                                          Iterable<InternalDatabusEventsListener> eventListeners,
                                          DbusEventsStatisticsCollector statsCollector,
                                          boolean logDebugEnabled)
  {
    final int numBytesToWrite = readPos.bytesProcessed();
    final int writeStartOfs = writePos.getCurOfs();
    final ByteBuffer curBuf = writePos.getCurBuf();

    assert writePos.getNextFree().bufferGenId() - _head.bufferGenId() <= 1 :
        writePos.toString() + " buf:" + toString();

    assert curBuf.limit() >= writePos.getNextFreeOfs() :
        "curBuf:" + curBuf + "; " + writePos;

    final int oldLimit = _readBuffer.limit();
    _readBuffer.mark();
    _readBuffer.position(readPos.getReadStart());
    _readBuffer.limit(readPos.getPosition());

    // Set the limit/position
    curBuf.position(writeStartOfs);
    if (LOG.isDebugEnabled())
    {
        LOG.debug("copying from " + _readBuffer + " into " + writePos.getCurBuf() +
                  "head:" + _head + " tail:" + _tail);
    }
    curBuf.put(_readBuffer); // copy _readBuffer
    _readBuffer.limit(oldLimit);
    _readBuffer.reset();

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
    DbusEvent e = writePos.next();

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

  private void callListeners(DbusEvent event, long currentPosition,
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
   * @param  pos     the deisred position
   * @return the event object
   */
  private DbusEvent eventAtPosition(long pos)
  {
    final int proposedHeadIdx = _bufferPositionParser.bufferIndex(pos);
    final int proposedHeadOfs = _bufferPositionParser.bufferOffset(pos);
    DbusEvent e = new DbusEvent(_buffers[proposedHeadIdx], proposedHeadOfs);
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
      String k;
      if ( readPos.getCurEvent().isKeyString() )
    	 k = new String(readPos.getCurEvent().keyBytes());
      else
         k = Long.toString(readPos.getCurEvent().key());
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
      LOG.debug("about to move head to " + _bufferPositionParser.toString(proposedHead) +
                "; scn=" + newScn + "; oldhead=" + _head);

    assert proposedHead <= _tail.getPosition() :
        "newHead:" + proposedHead + " " + toString();
    assert _tail.bufferGenId() - _bufferPositionParser.bufferGenId(proposedHead) <= 1 :
        "newHead:" + proposedHead + " " + toString();

    this.setPrevScn(getMinScn());
    _head.setPosition(proposedHead);
    if (null != _scnIndex) _scnIndex.moveHead(proposedHead, newScn);

    if (_head.equals(_tail))
    {
      _empty = true;
      newScn = -1;
    }
    if (null != _scnIndex) _scnIndex.moveHead(_head.getPosition(), newScn);

    _timestampOfFirstEvent = newHeadTsNs > 0 ? newHeadTsNs / 1000000 : getFirstEventTimestamp();

    //next we make sure we preserve the ByteBuffer limit() invariant -- see the comment
    //to _buffers
    adjustByteBufferLimit(oldHeadPos);

    if (logDebugEnabled)
        LOG.debug("moved head to " + _head.toString() + "; scn=" + newScn);
    _notFull.signalAll();
  }

  /** Asserts the ByteBuffers limit() invariant. {@see #_buffers} */
  protected boolean assertBuffersLimits()
  {
    boolean success = true;
    final int headIdx = empty() ? 0 : _head.bufferIndex();
    final int tailIdx = empty() ? 0 : _tail.bufferIndex();

    // Buffers are split into zones depending on their relative position to the head and tail
    // Head Zone1 Tail Zone2
    // Buffers in Zone2 are not full and should have their limit() == capacity()
    int zone = empty() ? 2 : 1;
    for (int i = 0; i < _buffers.length; ++i)
    {
      final int bufIdx = (headIdx + i) % _buffers.length;
      if (1 == zone && bufIdx == tailIdx)
      {
        //should we move to Zone 2?
        //just make sure that if the H and T are in the same buffer, H is before T
        if (bufIdx != headIdx || _head.bufferOffset() < _tail.bufferOffset())
        {
          zone = 2;
        }
      }

      if (2 == zone && _buffers[bufIdx].limit() != _buffers[bufIdx].capacity())
      {
        success = false;
        _log.error("assertBuffersLimits failure: buf[" + bufIdx + "]=" +
                   _buffers[bufIdx] + "; head=" + _head +  "; tail=" + _tail);
      }
    }

    return success;
  }

  @Override
  public String toString()
  {
	return "DbusEventBuffer [_numDebugOnErrorIterations="
			+ _numDebugOnErrorIterations + ", _oldLogLevel=" + _oldLogLevel
			+ ", _alreadyDebugonErrorEnabled=" + _alreadyDebugonErrorEnabled
			+ ", _queueLock=" + _queueLock + ", _readBufferLock="
			+ _readBufferLock + ", _notFull=" + _notFull + ", _notEmpty="
			+ _notEmpty + ", _rwLockProvider=" + _rwLockProvider
			+ ", readLocked=" + readLocked + ", writeLocked=" + writeLocked
			+ ", _currentWritePosition=" + _currentWritePosition
			+ ", _scnIndex=" + _scnIndex + ", _buffers="
			+ Arrays.toString(_buffers) + ", _maxBufferSize=" + _maxBufferSize
			+ ", _readBuffer=" + _readBuffer + ", _head=" + _head + ", _tail="
			+ _tail + ", _empty=" + _empty + ", _allocatedSize="
			+ _allocatedSize + ", _internalListeners=" + _internalListeners
			+ ", _allocationPolicy=" + _allocationPolicy + ", _queueingPolicy="
			+ _queueingPolicy + ", _mmapSessionDirectory="
			+ _mmapSessionDirectory + ", _writingEvent=" + _writingEvent
			+ ", _busyIteratorPool=" + _busyIteratorPool + ", _eventState="
			+ _eventState + ", _eventStartIndex=" + _eventStartIndex
			+ ", _numEventsInWindow=" + _numEventsInWindow
			+ ", _lastWrittenSequence=" + _lastWrittenSequence + ", _prevScn="
			+ _prevScn + ", _bufferPositionParser=" + _bufferPositionParser
			+ "]";
  }

/**
   * Returns the amount of free space left in the event buffer.
   * No guarantees of atomicity.
   */
  public long getBufferFreeSpace() {
    long remaining = remaining();
    return remaining;
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
    try
    {
      _queueLock.lock();

      while ( true )
      {
        int freeReadSpace = getBufferFreeReadSpace();

        if ( freeReadSpace >= freeSpaceThreshold )
          return;

        try
        {
          _notFull.await();
        } catch (InterruptedException ie) {
          if ( interruptCaller)
            throw ie;
        }
      }
    } finally {
      _queueLock.unlock();
    }
  }


  /**
   * Returns the amount of space left in the buffer that can be safely read
   * from a channel.
   * No guarantees of atomicity.
   */
  public int getBufferFreeReadSpace() {

    // While in readEvents, _readBuffer could be in inconsistent state

    assert(_eventState != WindowState.IN_READ);

    long remaining = remaining();
    int readRemaining = _readBuffer.remaining();
    return (int) Math.min(remaining, readRemaining);
  }


  private long remaining() {
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

  public DbusEventIterator acquireIterator(String iteratorName) {
    return acquireIterator(_head.getPosition(), _tail.getPosition(), false, iteratorName);
  }

  private DbusEventIterator acquireIterator(long head, long tail,
                                            boolean selfRemoving, String iteratorName) {

    DbusEventIterator eventIterator = new DbusEventIterator(this,_rwLockProvider, head, tail,iteratorName, selfRemoving);

    synchronized (_busyIteratorPool) {
      _busyIteratorPool.add(eventIterator);
    }
    return eventIterator;
  }

  /**
   * If you acquire an iterator using this interface, you are required to call
   * releaseIterator to return the iterator back to the pool. You can operate
   * on the iterator using the hasNext(), next() and await() calls before you
   * call releaseIterator(). Expected to be used by core databus code that
   * knows what it is doing.
   *
   * @param head
   * @param tail
   */
  public DbusEventIterator acquireIterator(long head, long tail, String iteratorName)
  {
    return acquireIterator(head, tail, false, iteratorName);
  }

  /**
   * Copies a passed in iterator and hands the copy back out
   * The same acquire/release rules apply to this copied iterator :
   * if you acquired the srcIterator with acquireIterator, then you have to call releaseIterator on the copied iterator as well
   * @param srcIterator
   * @param iteratorName
   */
  public DbusEventIterator copyIterator(DbusEventIterator srcIterator, String iteratorName)
  {
    DbusEventIterator destinationIterator = acquireIterator(-1L, _tail.getPosition(), srcIterator._selfRemoving, iteratorName);
    srcIterator.copy(destinationIterator, iteratorName);
    return destinationIterator;
  }

  public void releaseIterator(DbusEventIterator e) {
    e.releaseReadLock();

    synchronized (_busyIteratorPool) {
      _busyIteratorPool.remove(e);
    }
  }

  /**
   * If you acquire an iterator using this interface, the iterator will be
   * returned back to the pool automatically after it stops providing new
   * elements Use this iterator only for for-each loops, debugging etc.
   */
  @Override
  public Iterator<DbusEvent> iterator() {
    return acquireIterator(_head.getPosition(), _tail.getPosition(), true, "default");
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
  ScnIndex getScnIndex() {
	return _scnIndex;
  }

/*
   * @return the bufferPositionParser for this eventBuffer
   */
  public BufferPositionParser getBufferPositionParser()
  {
	  return _bufferPositionParser;
  }


  /**
   * package private to allow helper classes to set the head of the buffer
   * internally updates index state as well
   * @param offset
   */
  void setHead(long offset)
  {
    _head.setPosition(offset);
    _scnIndex.moveHead(offset);
  }

  /**
   * package private to allow helper classes to set the tail of the buffer
   * this does not update the scnIndex
   * @param offset
   */
  void setTail(long offset)
  {
    _tail.setPosition(offset) ;
  }

  /**
   * package private to allow helper/tester classes to modify event formats
   * this does not update the scnIndex
   * return dbusEvent
   */
  DbusEvent getWritingEvent()
  {
	return _writingEvent;
  }

  /**
   * package private to allow tester classes to set the writingEventObject
   * this does not update the scnIndex
   * @param writingEvent DbusEvent
   */
  void setWritingEvent(DbusEvent writingEvent)
  {
	this._writingEvent = writingEvent;
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

  /**
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

   **/

  public long getAllocatedSize()
  {
    return _allocatedSize;
  }

  public static class StaticConfig
  {
    private final long _maxSize;
    private final int _maxIndividualBufferSize;
    private final int _readBufferSize;
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

    public StaticConfig(long maxSize,
                        int maxIndividualBufferSize,
                        int readBufferSize,
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
                        boolean restoreMMappedBuffersValidateEvents)
    {
      super();
      _maxSize = maxSize;
      _maxIndividualBufferSize = maxIndividualBufferSize;
      _readBufferSize = readBufferSize;
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
     *
     * Default: 10% of the databus.relay.eventBuffer.defaultMemUsage * Runtime.getRuntime().maxMemory()
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

    public DbusEventBuffer getOrCreateEventBuffer()
    {
      DbusEventBuffer result = getExistingBuffer();
      if (null == result)
      {
        result = new DbusEventBuffer(this);
      }

      return result;
    }

    public DbusEventBuffer getOrCreateEventBufferWithPhyPartition(PhysicalPartition pp)
    {
      DbusEventBuffer result = getExistingBuffer();
      if (null == result)
      {
        result = new DbusEventBuffer(this, pp);
      }

      return result;
    }

    /** Which class of asserts should be validated */
    public AssertLevel getAssertLevel()
    {
      return _assertLevel;
    }
  }

  public static class Config implements ConfigBuilder<StaticConfig>
  {

    public static final double DEFAULT_DEFAULT_MEMUSAGE = 0.75;
    public static final double DEFAULT_EVENT_BUFFER_MAX_SIZE_QUOTA = 0.8;
    public static final double DEFAULT_EVENT_BUFFER_READ_BUFFER_QUOTA = 0.1;
    public static final QueuePolicy DEFAULT_QUEUE_POLICY = QueuePolicy.OVERWRITE_ON_WRITE;
    // Maximum individual Buffer Size
    private static int FIVE_HUNDRED_MEGABYTES_IN_BYTES = 500 * ByteSizeConstants.ONE_MEGABYTE_IN_BYTES;
    public static final int DEFAULT_INDIVIDUAL_BUFFER_SIZE = FIVE_HUNDRED_MEGABYTES_IN_BYTES;
    public static final String DEFAULT_MMAP_DIRECTORY = "mmappedBuffer";
    private static final long BUFFER_REMOVE_WAIT_PERIOD = 3600*24;

    protected long _maxSize;
    protected int _maxIndividualBufferSize;
    protected int _readBufferSize;
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

    public Config()
    {
      super();
      _defaultMemUsage = DEFAULT_DEFAULT_MEMUSAGE;

      deriveSizesFromMemPct();
      _allocationPolicy = getMaxSize() > 10000 ? "MMAPPED_MEMORY":"HEAP_MEMORY";
      _mmapDirectory = DEFAULT_MMAP_DIRECTORY;
      _queuePolicy = DEFAULT_QUEUE_POLICY.toString();
      _trace = new RelayEventTraceOptionBuilder();
      _bufferRemoveWaitPeriodSec = BUFFER_REMOVE_WAIT_PERIOD;
      _restoreMMappedBuffers = false;
    }

    public Config(Config other)
    {
      _maxSize = other._maxSize;
      _maxIndividualBufferSize = other._maxIndividualBufferSize;
      _readBufferSize = other._readBufferSize;
      _scnIndexSize = other._scnIndexSize;
      _allocationPolicy = other._allocationPolicy;
      _mmapDirectory = other._mmapDirectory;
      _defaultMemUsage = other._defaultMemUsage;
      _queuePolicy = other._queuePolicy;
      _existingBuffer = other._existingBuffer;
      _trace = new RelayEventTraceOptionBuilder(other._trace);
      _bufferRemoveWaitPeriodSec = other._bufferRemoveWaitPeriodSec;
      _restoreMMappedBuffers = other._restoreMMappedBuffers;
    }

    /** Computes the buffer sizes based on a the curent {@link #getDefaultMemUsage()} percentage*/
    private void deriveSizesFromMemPct()
    {
      long maxMem = Runtime.getRuntime().maxMemory();
      long memForEventBuffer = (long)(_defaultMemUsage * maxMem);
      _maxSize = (long)(DEFAULT_EVENT_BUFFER_MAX_SIZE_QUOTA * memForEventBuffer);
      _maxIndividualBufferSize = DEFAULT_INDIVIDUAL_BUFFER_SIZE;
      _readBufferSize = (int) (DEFAULT_EVENT_BUFFER_READ_BUFFER_QUOTA * memForEventBuffer);
      _scnIndexSize = (int) ( Math.abs(1.0 - DEFAULT_EVENT_BUFFER_MAX_SIZE_QUOTA -
                                       DEFAULT_EVENT_BUFFER_READ_BUFFER_QUOTA) * memForEventBuffer );
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

    public void setReadBufferSize(int eventBufferReadBufferSize)
    {
      _readBufferSize = eventBufferReadBufferSize;
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
      LOG.info("Event buffer individual buffer size: " + _maxIndividualBufferSize);
      LOG.info("Event buffer read buffer size: " + _readBufferSize);
      LOG.info("Event buffer scn index size: " + _scnIndexSize);
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

      return new StaticConfig(_maxSize, _maxIndividualBufferSize, _readBufferSize, _scnIndexSize, allocPolicy,
                              mmapDirectory, _defaultMemUsage, queuePolicy, _existingBuffer,
                              _trace.build(), assertLevel, _bufferRemoveWaitPeriodSec,
                              _restoreMMappedBuffers, _restoreMMappedBuffersValidateEvents);
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
   */
  public void closeBuffer() {
    // in case it is a MMAPED buffer
    if(_isClosed) {
      LOG.warn("calling close on already closed buffer " + toString());
      return;
    }

    // some listeners are appenders to a file
    if(_internalListeners != null) {
      for(InternalDatabusEventsListener l : _internalListeners) {
        try {
         l.close();
        } catch (IOException ioe) {
          LOG.warn("Couldn't close channel/file for listener=" + l, ioe);
        } catch (RuntimeException re) {
          LOG.warn("Couldn't close channel/file for listener=" + l, re);
        }
      }
    }

    try {
      saveBufferMetaInfo(false);
    } catch (IOException e) {
      LOG.error("error saving meta info for buffer for partition: " +
                getPhysicalPartition() + ": " + e.getMessage(), e);
    } catch (RuntimeException e) {
      LOG.error("error saving meta info for buffer for partition: " +
                getPhysicalPartition() + ": " + e.getMessage(), e);
    }


    _isClosed = true;
  }

  /**
   * Remove memory mapped file for the current session
   * and associated meta info file
   * Usecase : A database is dropped, all buffers associated with that database must be dropped, so also its associated persisted data files
   * @throws DatabusException
   */
  public void removeMMapFiles() {
	  if(_allocationPolicy != AllocationPolicy.MMAPPED_MEMORY) {
		  LOG.warn("Skipping removal of MMap files because allocation policy is " + _allocationPolicy);
		  return;
	  }

	  File f = new File(_mmapDirectory, metaFileName());
	  if (f.exists())
		  f.deleteOnExit();

	  if (_mmapSessionDirectory != null && _mmapSessionDirectory.exists())
		  _mmapSessionDirectory.deleteOnExit();
  }

  private void flushMMappedBuffers() {
    LOG.info("flushing buffers to disk for partition: " + _physicalPartition + "; allocation_policy=" + _allocationPolicy);
    if(_allocationPolicy == AllocationPolicy.MMAPPED_MEMORY) {
      for (ByteBuffer buf: _buffers)
      {
        if (buf instanceof MappedByteBuffer) ((MappedByteBuffer)buf).force();
      }
      if (_readBuffer instanceof MappedByteBuffer) ((MappedByteBuffer)_readBuffer).force();

      _scnIndex.flushMMappedBuffers();
      LOG.info("done flushing buffers to disk for partition: " + _physicalPartition);
    }
  }

  /**
   * save metaInfoFile about the internal buffers + scn index.
   * @param infoOnly - if true, will create a meta file that will NOT be used when loading the buffers
   * @throws IOException
   */
  public void saveBufferMetaInfo(boolean infoOnly) throws IOException {

    if(_allocationPolicy != AllocationPolicy.MMAPPED_MEMORY || !_bufferPersistenceEnabled) {
      LOG.info("Not saving state metaInfoFile, because allocation policy is " + _allocationPolicy
               + "; bufferPersistenceEnabled:" + _bufferPersistenceEnabled);
      return;
    }

    acquireWriteLock(); // uses _queue lock, same lock used by readevents()
    try {
      // first make sure to flush all the data
      flushMMappedBuffers();

      String fileName = metaFileName() + (infoOnly?MMAP_META_INFO_SUFFIX:"");
      DbusEventBufferMetaInfo mi = new DbusEventBufferMetaInfo(new File(_mmapDirectory, fileName));
      LOG.info("about to save DbusEventBuffer for PP " + _physicalPartition + " state into " + mi.toString());

      // record session id - to figure out directory for the buffers
      mi.setSessionId(_sessionId);

      // write buffers specific info - num of buffers, pos and limit of each one
      mi.setVal("ByteBufferNum",Integer.toString(_buffers.length));
      StringBuilder bufferInfo = new StringBuilder("");
      for (ByteBuffer b : _buffers) {
        DbusEventBufferMetaInfo.BufferInfo bi = new DbusEventBufferMetaInfo.BufferInfo(b.position() , b.limit(), b.capacity());
        bufferInfo.append(bi.toString());
        bufferInfo.append(" ");
      }
      mi.setVal("ByteBufferInfo", bufferInfo.toString());

      String currentWritePosition = Long.toString(_currentWritePosition.getPosition());
      mi.setVal("currentWritePosition", currentWritePosition);

      //  scnIndex file will be located in the session directory
      _scnIndex.saveBufferMetaInfo();

      // _maxBufferSize
      mi.setVal("maxBufferSize", Integer.toString(_maxBufferSize));

      //NOTE. no need to save readBuffer and rwChannel

      String head = Long.toString(_head.getPosition());
      mi.setVal("head", head);

      String tail = Long.toString(_tail.getPosition());
      mi.setVal("tail", tail);

      String empty = Boolean.toString(_empty);
      mi.setVal("empty", empty);

      mi.setVal("allocatedSize", Long.toString(_allocatedSize));

      mi.setVal("eventStartIndex", Long.toString(_eventStartIndex.getPosition()));

      // _numEventsInWindow
      mi.setVal("numEventsInWindow", Integer.toString(_numEventsInWindow));
      // _lastWrittenSequence
      mi.setVal("lastWrittenSequence", Long.toString(_lastWrittenSequence));

      mi.setVal("seenEndOfPeriodScn", Long.toString(_seenEndOfPeriodScn));
      // _prevScn
      mi.setVal("prevScn", Long.toString(_prevScn));
      // _timestampOfFirstEvent
      mi.setVal("timestampOfFirstEvent", Long.toString(_timestampOfFirstEvent));
      // _timestampOfLatestDataEvent
      mi.setVal("timestampOfLatestDataEvent", Long.toString(_timestampOfLatestDataEvent));
      // eventState
      mi.setVal("eventState", _eventState.toString());

      mi.saveAndClose();
    } finally {
      releaseWriteLock();
    }
  }

  public void initBuffersWithMetaInfo(DbusEventBufferMetaInfo mi) throws DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException {

    if(mi.isValid()) {
      _head.setPosition(mi.getLong("head"));
      _tail.setPosition(mi.getLong("tail"));

      _currentWritePosition.setPosition(mi.getLong("currentWritePosition"));
      _empty = mi.getBool("empty");
      // _eventStartIndex
      _eventStartIndex.setPosition(mi.getLong("eventStartIndex"));
      _numEventsInWindow = mi.getInt("numEventsInWindow");
      _eventState = DbusEventBuffer.WindowState.valueOf(mi.getVal("eventState"));

      _lastWrittenSequence = mi.getLong("lastWrittenSequence");
      _seenEndOfPeriodScn = mi.getLong("seenEndOfPeriodScn");

      _prevScn = mi.getLong("prevScn");
      _timestampOfFirstEvent = mi.getLong("timestampOfFirstEvent");
      _timestampOfLatestDataEvent = mi.getLong("timestampOfLatestDataEvent");
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
}
