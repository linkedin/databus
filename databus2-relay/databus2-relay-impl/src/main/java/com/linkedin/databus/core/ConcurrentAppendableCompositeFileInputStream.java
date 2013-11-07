package com.linkedin.databus.core;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.linkedin.databus.core.ConcurrentAppendableCompositeFileInputStream.InputStreamEnumerator.ParserLag;
import com.linkedin.databus.core.TrailFileNotifier.TrailFileListener;
import com.linkedin.databus.core.TrailFileNotifier.TrailFileManager;
import com.linkedin.databus.core.util.NamedThreadFactory;
import com.linkedin.databus.core.util.RateMonitor;
import com.linkedin.databus.monitoring.mbean.GGParserStatistics;
import com.linkedin.databus2.core.DatabusException;

/**
 * A composite InputStream that sequentially reads from an ordered and filtered list of
 * files found inside the input directory and takes care of reading files that are concurrently appended
 * in a defined order.
 *
 * <pre>
 *           __________________________________________________
 *        1 |                                                  | 1
 *  ______________             _____________           ______ ______           _______________
 * |              | 1       1 |             | 1     1 |             |1      *  |              |
 * | CompositeFIS | --------->| SequenceFIS |-------->| Enumerator  |--------> |  SingleFIS   |
 * |______________|           |_____________|         |_____________|          |______________|
 *         |1
 *         |
 *         |
 *         |
 *         |
 *         |1
 * _________________
 * |               |
 * |    TrailFile  |
 * |     Notifier  |
 * |_______________|
 *
 *
 * CompositeFIS : ConcurrentAppendableCompositeFileInputStream
 * SingleFIS : ConcurrentAppendableSingleFileInputStream
 * </pre>
 */
public class ConcurrentAppendableCompositeFileInputStream
  extends InputStream
  implements TrailFileListener
{
  public static final String MODULE = ConcurrentAppendableCompositeFileInputStream.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  /** Directory under which files need to be streamed **/
  private final File _dir;

  /** Underlying Sequence File InputStream **/
  private SequenceInputStream _seqStream = null;

  /** Agent to poll for new files, order them, find deltas and notify this class */
  private final TrailFileNotifier _trailFileLocator;

  /** Client Specified ranker and filter callback **/
  private final TrailFileManager _trailFileManager;

  /** Low Watermark file. All files equal to and greater than this in the ordering defined by FileNameRankFilter will be streamed **/
  private File _latestFile;

  /** Offset in the startFile from which streaming has to start **/
  private final long _initialFileOffset;

  /** Number of byte read calls for this stream */
  private volatile long _numReadCalls = 0;

  /** Number of byte read calls that resulted in a valid byte being sent **/
  private volatile long _numReadCallsWithData = 0;

  /**
   * Enumerator which provides the never-ending stream of files
   *   and implements interface needed for SequenceInputStream
   **/
  private InputStreamEnumerator _streamEnumerator = null;

  /**
   * Lazy initialization is done on streams. This flag is used to denote if initialization is done.
   */
  private boolean _initDone = false;

  /**
   * Rate Monitor to track read throughput

   */
  private final RateMonitor _rateMonitor = new RateMonitor("ConcurrentAppendableCompositeFileInputStream");

  /** Are files in the directory be treated like static (not written to concurrently */
  private final boolean _staticStream;

  private boolean _closed = false;

  /** First Trail File Flag for setting offsets **/
  private boolean _firstTrailFile = true;

  /** executor for stats updating task */
  private ScheduledThreadPoolExecutor _executor;
  private final long STATS_COLLECTION_PERIOD = 5000; //5 seconds

  private GGParserStatistics _ggParserStats = null;
  public void setGGParserStats(GGParserStatistics st) {
    _ggParserStats = st;
  }

  /**
   *
   * Construct a trail directory input stream.
   * @param dir : directory from which trail files will be picked
   * @param filter : Filter and Sorted callback provided to filter and rank the files by trail order
   * @param staticStream : Are files in the directory be treated like static files. Parser should set this to false
   */
  public ConcurrentAppendableCompositeFileInputStream(String dir,
		  											  TrailFileManager filter,
                                                      boolean staticStream)
                      throws IOException
  {
    this(dir,null,-1L,filter,staticStream);
  }

  @Override
  public String toString()
  {
    return "ConcurrentAppendableCompositeFileInputStream{" +
        "_dir=" + _dir +
        ", _seqStream=" + _seqStream +
        ", _trailFileLocator=" + _trailFileLocator +
        ", _trailFileManager=" + _trailFileManager +
        ", _latestFile=" + _latestFile +
        ", _initialFileOffset=" + _initialFileOffset +
        ", _numReadCalls=" + _numReadCalls +
        ", _numReadCallsWithData=" + _numReadCallsWithData +
        ", _streamEnumerator=" + _streamEnumerator +
        ", _initDone=" + _initDone +
        ", _rateMonitor=" + _rateMonitor +
        ", _staticStream=" + _staticStream +
        ", _closed=" + _closed +
        ", _firstTrailFile=" + _firstTrailFile +
        '}';
  }

  /**
   *
   * Construct a trail directory input stream.
   * @param dir : directory from which trail files will be picked
   * @param startFile : current file from which data has to be recorded.
   * @param offset : Offset to the startFile from which streaming should start. If offset <= 0, then the entire file is read...
   * @param filter : Filter and Sorted callback provided to filter and rank the files by trail order
   * @param staticStream : Are files in the directory be treated like static files. Parser should set this to false
   * @throws IOException
   */
  public ConcurrentAppendableCompositeFileInputStream(String dir,
                                          String startFile,
                                          long offset,
                                          TrailFileManager filter,
                                          boolean staticStream)
          throws IOException
  {
    validateDir(dir);

    _dir = new File(dir);
    _trailFileManager = filter;
    _latestFile = (startFile == null) ? null : new File(_dir.getAbsolutePath() + "/" + startFile);
    _initialFileOffset = offset;
    _staticStream = staticStream;

    _trailFileLocator = new TrailFileNotifier(_dir, _trailFileManager, _latestFile, 10, this);
    _rateMonitor.start();
    _rateMonitor.suspend();

    // start timer to collect stats
    _executor = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("StatsTime for " + dir, true)); // add directory to the thread name
    _executor.scheduleWithFixedDelay( new Runnable() {
      @Override
      public void run() {
        updateParserStats();
      }
    }, STATS_COLLECTION_PERIOD, STATS_COLLECTION_PERIOD, TimeUnit.MILLISECONDS);

  }

  /**
   * Validate directory
   * @param dir
   * @throws IOException
   */
  private void validateDir(String dir)
      throws IOException
  {
    File d = new File(dir);

    if (! d.isDirectory())
      throw new IOException("Path (" + dir +") does not exist or is not a directory !!");
  }


  /**
   * Sets up the sequence stream from the trail files located.
   * @throws IOException
   */
  public void initializeStream()
      throws IOException
  {
    _streamEnumerator = new InputStreamEnumerator(_staticStream, _trailFileManager, _ggParserStats);
    boolean success = _trailFileLocator.fetchOneTime();

    if ( ! success )
    	return ;

    _seqStream = new SequenceInputStream(_streamEnumerator);

    if ( ! _staticStream)
      _trailFileLocator.start(); // Note: TrailFileLocator needs to be started only after seqStream is constructed.
    _initDone = true;
  }


  public RateMonitor getRateMonitor()
  {
    return _rateMonitor;
  }

  public enum ReadCall
  {
	  READ_ONE_BYTE,
	  READ_BULK,
	  READ_BULK_WITH_OFFSETS
  };

  private int readData(byte[] b, int off, int len, ReadCall readCall) throws IOException
  {
    // Initialize stream first time
    if ( ! _initDone)
    {
      initializeStream();
    }
    int ret = ConcurrentAppendableSingleFileInputStream.EOF;
    int numBytesRead = 0;

    _rateMonitor.resume();
    if ( null != _seqStream)
    {
      switch(readCall)
      {
        case READ_BULK: ret = _seqStream.read(b);
                        numBytesRead = ret;
                        break;
        case READ_BULK_WITH_OFFSETS: ret = _seqStream.read(b, off, len);
                                     numBytesRead = ret;
                                     break;
        case READ_ONE_BYTE: ret = _seqStream.read();
                                  numBytesRead = 1;
      }
    }

    if (ret != ConcurrentAppendableSingleFileInputStream.EOF)
    {
      _numReadCallsWithData++;
      _rateMonitor.ticks(numBytesRead);

      if(_ggParserStats != null)
        _ggParserStats.addBytesParsed(_rateMonitor.getNumTicks());
    }
    _rateMonitor.suspend();

    _numReadCalls++;
    return ret;
  }

  @Override
  public int read() throws IOException
  {
    return readData(null, -1, -1, ReadCall.READ_ONE_BYTE);
  }

  @Override
  public int read(byte[] b) throws IOException
  {
    return readData(b, -1, -1, ReadCall.READ_BULK);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException
  {
    return readData(b, off, len, ReadCall.READ_BULK_WITH_OFFSETS);
  }

  @Override
  public void close() throws IOException
  {
    if(_executor != null)
      _executor.shutdownNow();

    if ( (null != _trailFileLocator) && (_trailFileLocator.isAlive()))
      _trailFileLocator.shutdown();

    closeStream();
  }

  private void closeStream() throws IOException
  {
    if (_closed)
	  return;

    LOG.info("Closing Stream !!");

    if(_streamEnumerator != null)
      _streamEnumerator.close();

    if (null != _seqStream)
      _seqStream.close();

    LOG.info("Stream closed !!");
    _closed = true;
  }

  @Override
  public void onNewTrailFile(File file) throws DatabusException
  {
    long offset = 0;

    if ( _firstTrailFile)
    {
      offset = _initialFileOffset;
      // Defensive Check to ensure we are getting the file we are expecting
      if ((null != _latestFile) && (! file.equals(_latestFile)) )
      {
        throw new DatabusException("Expected to get " + _latestFile + " but got " + file);
      }
      _firstTrailFile = false;
    } else {
      // Defensive Check to ensure we are not seeing old files.
      if (!_trailFileManager.isNextFileInSequence(_latestFile, file))
      {
        throw new DatabusException("Expected to get next file to " + _latestFile + " but got " + file);
      }
    }
    _latestFile = file;
    _streamEnumerator.enqueueNewTrailFile(new FilePosition(file, offset));
  }

  @Override
  public void onError(Throwable ex)
  {
	  LOG.error("Closing the inputStream because of error", ex);

	  try {
	    closeStream();
	  } catch (IOException e) {
	    LOG.error("Unable to close the inputStream", e);
	  }
  }

  protected TrailFileNotifier getTrailFileNotifier()
  {
	  return _trailFileLocator;
  }

  public long getNumReadCalls()
  {
    return _numReadCalls;
  }

  public long getNumReadCallsWithData()
  {
    return _numReadCallsWithData;
  }

  public File getCurrentFile()
  {
    ConcurrentAppendableSingleFileInputStream s = _streamEnumerator.getCurrStream();

    if ( null == s)
      return null;

    return s.getFile();
  }

  public long getCurrentPosition()
  {
    ConcurrentAppendableSingleFileInputStream s = _streamEnumerator.getCurrStream();

    if ( null == s)
      return -1;

    return s.getCurrPosition();
  }

  public boolean isClosed()
  {
	  return _closed;
  }

  /**
   *
   * Stream which returns EOF for any read. Used as start and end stream(on shutdown).
   */
  public static class EOFStream
    extends InputStream
  {
    @Override
    public int read() throws IOException {
      return ConcurrentAppendableSingleFileInputStream.EOF;
    }

  }

  private static class FilePosition
  {
	@Override
    public String toString()
    {
      return "FilePosition [file=" + file + ", offset=" + offset + "]";
    }

    private final File file;
	private final long offset;

	public FilePosition(File file, long offset) {
		super();
		this.file = file;
		this.offset = offset;
	}

	public File getFile() {
		return file;
	}

	public long getOffset() {
		return offset;
	}
  }


  /**
   *
   * Enumerator for sequencing input-stream from trail files used by SequenceInputStream.
   * This enumerator never gets exhausted (hasMoreElements() always true (unless closed explicitly) and nextElement() blocks if no more files !!)
   *
   */
  public static class InputStreamEnumerator
    implements Enumeration<InputStream>
  {
    private ConcurrentAppendableSingleFileInputStream _currStream = null;
    private final Queue<FilePosition> _newTrailFiles = new ConcurrentLinkedQueue<FilePosition>();
    private final boolean _isStaticStream;

    private final Lock _lock = new ReentrantLock();
    private final Condition _notEmpty = _lock.newCondition();

    private boolean _firstStream = true;
    private volatile boolean _closed = false;

    private final InputStream _beginStream = new EOFStream();
    private final InputStream _endStream = new EOFStream();

    private final Map<String, Long> _inputFileSizes = Collections.synchronizedMap(new HashMap<String, Long>());
    private final Map<String, Long> _inputFileModTimes = Collections.synchronizedMap(new HashMap<String, Long>());

    private final TrailFileManager _trailFileFilter;
    private final GGParserStatistics _ggParserStats;

    // to avoid updates for the same file
    private File _mostRecentParsedFile;
    private File _mostRecentFileAdded = null;
    private long _mostRecentFileAddedSize=0;


    public InputStreamEnumerator(boolean staticStream, TrailFileManager trailFileFilter, GGParserStatistics ggParserStats)
        throws IOException
    {
      _isStaticStream = staticStream;
      _trailFileFilter = trailFileFilter;
      _ggParserStats = ggParserStats;
    }

    /**
     * describes the lag between the file currently being processed and latest file available
     * bytesLag - between position in the currentStream file and end of the latest trail file
     * timeLag - timeStamp_of_the_current_file - timeStamp_of_the_latest_file
     * filesLag - number of files between current file and the latest file (including latest, excluding current)
     */
    public static class ParserLag {
      private final long _bytesLag;
      private final long _tsBegin;
      private final long _tsEnd;
      private final int _filesLag;
      public ParserLag(long byteLag, long tsBegin, long tsEnd, int fileLag) {
        _bytesLag = byteLag;
        _tsBegin = tsBegin;
        _tsEnd = tsEnd;
        _filesLag = fileLag;
      }

      public long getBytesLag() { return _bytesLag; }
      public long getTSBegin() { return _tsBegin; }
      public long getTSEnd() { return _tsEnd; }
      public int getFilesLag() { return _filesLag; }

      @Override
      public String toString() { return "LAG:bytes="+_bytesLag+";files="+_filesLag+"; tsBegin="+_tsBegin + ";tsEnd=" + _tsEnd; }
    }

    /**
     *
     * @return number of bytes/files/seconds between current position and the end of latest file
     */
    public ParserLag calculateLag() {
      File trailFile = null;
      int totalBytes = 0;
      int totalFiles = 0;
      long modTSFirst = 0, modTSLast = 0; // mod times of the files in the queue
      Long size=null;
      Long modTime;

      _lock.lock();
      try {
        // check the current file being parsed (it is not in the queue anymore)
        if(_currStream != null) {
          File fc = _currStream.getFile();
          size = _inputFileSizes.get(fc.getAbsolutePath());
          if(size == null || _newTrailFiles.size()==0) { //if no new files, this file may have been appended
            size = Long.valueOf(fc.length());
            _inputFileSizes.put(fc.getAbsolutePath(), size);
          }
          // adjust the offset
          totalBytes += size - _currStream.getCurrPosition();

          // get the mod time
          modTime = _inputFileModTimes.get(fc.getAbsolutePath());
          if(modTime == null || _newTrailFiles.size()==0) { //if no new files, this file may have been appended
            modTime = Long.valueOf(fc.lastModified());
            _inputFileModTimes.put(fc.getAbsolutePath(), modTime);
          }
          modTSFirst = modTime;
          modTSLast = modTime;  //only for the current file
        }


        // add-up all the files in the queue
        for(FilePosition fp : _newTrailFiles) {
          trailFile = fp.getFile();
          size = _inputFileSizes.get(trailFile.getAbsolutePath()); // cache values to avoid extra disk op
          if(size == null) {
            size = Long.valueOf(trailFile.length());
            _inputFileSizes.put(trailFile.getAbsolutePath(), size);
          }
          totalBytes += size.longValue();
          totalFiles ++;

          // mod time - we just want it for the first file
          if(modTSFirst == 0) {
            modTime = _inputFileModTimes.get(trailFile.getAbsolutePath());
            if(modTime == null) {
              modTime = Long.valueOf(trailFile.lastModified());
              _inputFileModTimes.put(trailFile.getAbsolutePath(), modTime);
            }
            modTSFirst = modTime;
          }
        }
        // Since last file may be appended - we need to re-check it every time
        // 'trailFile' is the file reference from the loop above.
        // if it is not null - this is the file that may be appended
        if(trailFile != null) {
          // get current size, totalBytes already includes its size from the previous loop and it may be incorrect
          totalBytes -= size.longValue(); // undo prev size of this file and get a new one
          size = Long.valueOf(trailFile.length());
          _inputFileSizes.remove(trailFile.getAbsolutePath()); // since file may be appended do not cache its size
          totalBytes += size.longValue();

          // get current modTime
          modTime = Long.valueOf(trailFile.lastModified());
          _inputFileModTimes.remove(trailFile.getAbsolutePath());

          modTSLast = modTime;
        }
      } finally {
        _lock.unlock();
      }
      return new ParserLag(totalBytes, modTSFirst, modTSLast, totalFiles);
    }

    @Override
    public String toString()
    {
      StringBuffer toString = new StringBuffer("InputStreamEnumerator{" +
          "_currStream=" + _currStream +
          ", numberOfNewTrailFiles=" + _newTrailFiles.size() +
          ", _isStaticStream=" + _isStaticStream +
          ", _lock=" + _lock +
          ", _notEmpty=" + _notEmpty +
          ", _firstStream=" + _firstStream +
          ", _closed=" + _closed +
          ", _beginStream=" + _beginStream +
          ", _endStream=" + _endStream);

      if(LOG.isDebugEnabled())
        toString.append(", _newTrailFiles =  " + _newTrailFiles);

      return toString.append('}').toString();
    }

    @Override
    public  boolean hasMoreElements()
    {
      _lock.lock();
      try
      {
        if ( _closed )
          return false;

        // Always return true for non-static stream unless closed.
        if ( !_isStaticStream)
          return true;

        return !_newTrailFiles.isEmpty();
      } finally {
        _lock.unlock();
      }
    }

    @Override
    public  InputStream nextElement()
    {
      if(LOG.isDebugEnabled())
        LOG.debug("Moving to next InputStream !!");
      InputStream stream = null;
      _lock.lock();
      try
      {
        if ( _firstStream)
        {
          /**
           *  First Stream has to be set because SequenceInputStream constructor calls nextElement() directly.
           *  Without this, the the thread could block if no files available at that time.
           */
          stream = _beginStream;
          _firstStream = false;
          return stream;
        }

        if ( _closed )
          return _endStream;

        while ( (_newTrailFiles.isEmpty()) && (!_closed))
          _notEmpty.await();

        if ( _closed )
          return null;

        if(_currStream != null) {
          LOG.info("about to switch to the next file from " + _currStream.getFile());

          // file is done. remove its info from the caches
          _inputFileSizes.remove(_currStream.getFile().getAbsolutePath());
          _inputFileModTimes.remove(_currStream.getFile().getAbsolutePath());

        }

        FilePosition newFilePos = _newTrailFiles.poll();

        try {
          if(_isStaticStream || (!_newTrailFiles.isEmpty()))
            _currStream = ConcurrentAppendableSingleFileInputStream.createStaticFileInputStream(
                                               newFilePos.getFile().getAbsolutePath(), newFilePos.getOffset());
          else
            _currStream =  ConcurrentAppendableSingleFileInputStream.createAppendingFileInputStream(
                                               newFilePos.getFile().getAbsolutePath(), newFilePos.getOffset(),100);

          stream = _currStream;

          // update the stats
          parserFileStarted(newFilePos.getFile());

        } catch (Exception e) {
          LOG.error("Unable to construct stream for trail filePos (" + newFilePos + ")",e);
          throw new RuntimeException("Unable to construct stream for trail filePos (" + newFilePos + ")",e);
        }
      } catch (InterruptedException ie ) {
        LOG.error("Got interrupted while waiting for new trail files !!", ie);
      } finally {
        _lock.unlock();
      }
      return stream;
    }

    /**
     * Called by TrailFileNotifier thread if it finds new file in the order defined by the FileNameRanker object.
     * @param file
     */
    public void enqueueNewTrailFile(FilePosition filePos)
    {
      _lock.lock();

      try
      {
        if (LOG.isDebugEnabled())
          LOG.debug("Enqueueing new trail file Position: " + filePos);

        if ( _closed)
        {
          LOG.error("Trying to insert new filePos (" + filePos + ") when in closed state. Skipping !!");
          return;
        }

        // for metrics collection
        parserAddNewFile(filePos.getFile());

        if ( _newTrailFiles.isEmpty())
        {
          if ( null != _currStream)
            _currStream.appendDone();
        }
        _newTrailFiles.offer(filePos);
        _notEmpty.signal();
      } finally {
        _lock.unlock();
      }
    }

    public void close()
    {
      _lock.lock();
      try
      {
        _closed = true;
        _notEmpty.signal(); //to wakeup thread waiting on this
      } finally {
        _lock.unlock();
      }
    }

    public ConcurrentAppendableSingleFileInputStream getCurrStream() {
      return _currStream;
    }


    /**
     * new trail file has been added.
     * @param f
     */
    public void parserAddNewFile(File f)
    {
      if(_mostRecentFileAdded != null && _trailFileFilter.compareFileName(f, _mostRecentFileAdded) <= 0) {
        LOG.warn("adding file that has been added before=" + f + ". ignoring.");
        return;
      }
      if(_ggParserStats != null)
        _ggParserStats.addNewFile(1); //update stats

    }

    /**
     * parsing of next file started
     * @param f
     */
    public void parserFileStarted(File f)
    {
      if(_mostRecentParsedFile != null && _trailFileFilter.compareFileName(f, _mostRecentParsedFile) <= 0) {
        LOG.warn("skipping update for the file("+f+") because it has been recorded already. most recent file=" + _mostRecentParsedFile);
        return;  // avoid updates for the same file
      }

      if(_ggParserStats != null)
        _ggParserStats.addParsedFile(1);

      _mostRecentParsedFile = f;
    }
  }


  private void updateParserStats() {
    if(_streamEnumerator == null || (_ggParserStats == null))
      return;


    ParserLag pLag = _streamEnumerator.calculateLag();
    if(LOG.isDebugEnabled())
      LOG.debug("running updateParserStats. lag=" + pLag);

    _ggParserStats.setBytesLag(pLag.getBytesLag());
    _ggParserStats.setFilesLag(pLag.getFilesLag());
    _ggParserStats.setModTimeLag(pLag.getTSBegin(), pLag.getTSEnd());
  }

}
