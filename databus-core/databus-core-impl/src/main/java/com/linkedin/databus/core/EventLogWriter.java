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


import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.linkedin.databus.core.util.ByteSizeConstants;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus.core.util.RangeBasedReaderWriterLock;
import com.linkedin.databus.core.util.RangeBasedReaderWriterLock.LockToken;

/**
 * A continuous journal writer of the events as they flow into the DbusEventBuffer
 * This journal can be used for refilling the buffer on startup using the EventLogReader class.
 * @author sdas
 * @deprecated
 */
@Deprecated
public class EventLogWriter extends InternalDatabusEventsListenerAbstract implements Runnable
{
  public static final String MODULE = EventLogWriter.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private enum BatchState
  {
    INIT,
    STARTED,
    ENDED
  }

  public long getTotalBytesWritten()
  {
    return _totalBytesWritten;
  }

  private final DbusEventBuffer _eventBuffer;
  private final File _writeDir;
  private final RangeBasedReaderWriterLock _lockProvider;
  private final Encoding _encoding;
  private final AtomicBoolean _stopRunning;
  private final ArrayBlockingQueue<LockToken> _contiguousRanges;
  private final long _batchLimit;
  private final boolean _blockOnWrite;
  private final long _individualFileMaxBytes;

  private BatchState _batchState;
  private long _batchStartOffset;
  private long _batchNextOffset;
  private long _totalBytesWritten;
  private long _currentFileBytesWritten;
  private FileChannel _currentWritableByteChannel;
  private int _currentWriteFileIndex;
  private final ArrayBlockingQueue<File> _writeFileHandles;
  private final boolean _enabled;
  private final File _writeSessionDir;
  private final ReentrantLock _writeLock;
  private final long _fsyncIntervalInMillis;
  private long _lastFsyncTime;


  /**
   *
   * @param eventBuffer
   * @param writeDir
   * @param encoding
   * @param writeBatchSizeInBytes
   * @param maxPendingWritesBeforeAbort
   * @param individualFileMaxBytes
   */
  public EventLogWriter(boolean enabled, DbusEventBuffer eventBuffer,
                      File writeDir, File writeSessionDir, Encoding encoding,
                      boolean blockOnWrite, long writeBatchSizeInBytes,
                      int maxPendingWritesBeforeAbort,
                      long individualFileMaxBytes, int maxFiles, long fsyncIntervalInMillis)
  {
    _enabled = enabled;
    _eventBuffer = eventBuffer;
    _lockProvider = eventBuffer.getRwLockProvider();
    _encoding = encoding;
    _batchState = BatchState.INIT;
    _stopRunning = new AtomicBoolean(false);
    _writeLock = new ReentrantLock();
    _fsyncIntervalInMillis = fsyncIntervalInMillis;
    _blockOnWrite = blockOnWrite;
    if (_blockOnWrite)
    {
      _contiguousRanges = new ArrayBlockingQueue<LockToken>(10);
    }
    else
    {
      _contiguousRanges = new ArrayBlockingQueue<LockToken>(maxPendingWritesBeforeAbort);
    }

    _batchLimit = writeBatchSizeInBytes;
    _individualFileMaxBytes = individualFileMaxBytes;
    _totalBytesWritten = 0;
    _currentFileBytesWritten = 0;


    if (!writeDir.isDirectory())
    {
      if (!writeDir.mkdir())
      {
        throw new RuntimeException(writeDir + "is not a directory, and I could not create one");
      }
    }
    if (!writeDir.canWrite())
    {
      throw new RuntimeException("Do not have write privileges on " + writeDir);
    }
    _writeDir = writeDir;
    _writeSessionDir = writeSessionDir;

    _writeFileHandles = new ArrayBlockingQueue<File>(maxFiles);
    LOG.info("Configured with writeDirectory :" + _writeDir.getAbsolutePath());
    LOG.info("Configured with writeSession Directory :" + _writeSessionDir.getAbsolutePath());
    _eventBuffer.addInternalListener(this);
  }

  public EventLogWriter(StaticConfig staticConfig)
  {
    this(staticConfig.isEnabled(), staticConfig.getEventBuffer(), staticConfig.getTopLevelLogDir(),
         staticConfig.getWriteSessionDir(), staticConfig.getEncoding(), staticConfig.getBlockOnWrite(),
         staticConfig.getWriteBatchSizeInBytes(),staticConfig.getMaxPendingWrites(),
         staticConfig.getIndividualFileMaxBytes(), staticConfig.getMaxFiles(),
         staticConfig.getFsyncIntervalInMillis());
  }

  @Override
  public void run()
  {
    _stopRunning.set(!_enabled);
    LockToken readRangeLock = null;

    // Init : delete existing files in this directory first
    // We don't do this as part of the constructor since we want the EventLogReader
    // to be able to read files if they are present.
    if (_writeSessionDir.exists())
    {
      for (File f: _writeSessionDir.listFiles())
      {
        LOG.info("deleting file " + f.getAbsolutePath() + " in directory");
        if (!f.delete())
        {
          LOG.error("deleting failed: " + f.getAbsolutePath());
          _stopRunning.set(true);
        }
      }
    }
    else
    {
      if (!_writeSessionDir.mkdir())
      {
        LOG.error("directory creation failed: " + _writeSessionDir);
        _stopRunning.set(true);
      }
    }

    while (!_stopRunning.get())
    {
      try
      {
        readRangeLock = _contiguousRanges.poll(_fsyncIntervalInMillis, TimeUnit.MILLISECONDS);
      }
      catch (InterruptedException e)
      {
        LOG.info("interrupted");
      }
      if (readRangeLock !=null)
      {
        try
        {
          performWrite(readRangeLock);
        }
        catch (IOException e)
        {
          LOG.error("error writing events:" + e.getMessage(), e);
          _stopRunning.set(true);
        }
        finally
        {
          _lockProvider.releaseReaderLock(readRangeLock);
        }
      }
      else
      {
        // Timeout with no batches to write, create a write request if there were any events
        _writeLock.lock();
        try
        {
          if (_batchState == BatchState.STARTED)
          {
            if (LOG.isDebugEnabled())
              LOG.debug("Timeout: will queue write request");

            queueWriteRequest();
            _batchState = BatchState.INIT;
          }
        }
        finally
        {
          _writeLock.unlock();
        }

      }
    }
    flush();
  }

  @Override
  public void onEvent(DbusEvent event, long offset, int size)
  {
    if (_stopRunning.get())
    {
      return;
    }

    _writeLock.lock();
    try
    {
      if (LOG.isDebugEnabled())
        LOG.debug("EventLogWriter:onEvent:" +  event);
      if (_batchState == BatchState.INIT)
      {
        _batchStartOffset = offset;
        _batchNextOffset = offset + size;
        _batchState = BatchState.STARTED;
        return;
      }
      if (_batchState == BatchState.STARTED)
      {
        if ((_batchNextOffset != offset) || ((_batchNextOffset - _batchStartOffset) > _batchLimit))
        {
          queueWriteRequest();
          _batchStartOffset = offset;
        }
        _batchNextOffset = offset + size;

      }
    }
    finally
    {
      _writeLock.unlock();
    }

  }

  private void queueWriteRequest()
  {
    // non-contiguous write
    // signal write ready
    LockToken readRangeLockToken = null;
    try
    {
      readRangeLockToken = _lockProvider.acquireReaderLock(_batchStartOffset, _batchNextOffset,
                                      _eventBuffer.getBufferPositionParser(),
                                      "EventLogWriter.queueWriteRequest");
    }
    catch (InterruptedException e1)
    {
      LOG.warn("queueWriteRequest read lock wait interrupted", e1);
      _stopRunning.set(true);
    }
    catch (TimeoutException e1)
    {
      LOG.error("queueWriteRequest read lock wait timed out", e1);
      _stopRunning.set(true);
    }

    if (_blockOnWrite)
    {
      try
      {
        _contiguousRanges.put(readRangeLockToken);
      }
      catch (InterruptedException e)
      {
        LOG.info("interrupted");
      }
    }
    else
    {
      boolean addRange = _contiguousRanges.offer(readRangeLockToken);
      if (!addRange)
      {
        // Not able to keep up with event load
        // Abandon persisting?
        LOG.error("Not able to keep up with event load, abandoning attempts to persist the buffer");
        _stopRunning.set(true);

        // Release all the read locks
        for (LockToken readRangeToken: _contiguousRanges)
        {
          _lockProvider.releaseReaderLock(readRangeToken);
        }
        _lockProvider.releaseReaderLock(readRangeLockToken);

        // Delete all the files generated so far
        if (_writeSessionDir.exists())
        {
          for (File f: _writeSessionDir.listFiles())
          {
            LOG.info("deleting file " + f.getAbsolutePath() + " in directory");
            if (!f.delete())
            {
              LOG.error("deleting failed: " + f.getAbsolutePath());
            }
          }
        }
      }
    }
  }

  public void stop()
  {
    _stopRunning.set(true);
  }

  private void performWrite(LockToken readRangeLock) throws IOException
  {
    if (_enabled)
    {
      FileChannel writeChannel = getCurrentWriteChannel();
      assert writeChannel != null;
      if (LOG.isDebugEnabled())
        LOG.debug("BatchWrite:"+readRangeLock.getRange().start + ":" + readRangeLock.getRange().end);

      int bytesWritten = _eventBuffer.batchWrite(readRangeLock.getRange(), writeChannel, _encoding);
      _totalBytesWritten += bytesWritten;
      _currentFileBytesWritten += bytesWritten;
      if ((System.currentTimeMillis() - _lastFsyncTime) > _fsyncIntervalInMillis)
      {
        writeChannel.force(true);
      }
    }
  }


  private void flush()
  {
    LockToken readToken;
    while ((readToken = _contiguousRanges.poll()) != null)
    {
      try
      {
        performWrite(readToken);
      }
      catch (IOException e)
      {
        LOG.error("unable to flush" + e.getMessage(), e);
        _stopRunning.set(true);
      }
      finally
      {
        _lockProvider.releaseReaderLock(readToken);
      }
    }
    if (_batchState == BatchState.STARTED)
    {
      LockToken readRangeLockToken = null;
      try
      {
        readRangeLockToken = _lockProvider.acquireReaderLock(_batchStartOffset, _batchNextOffset,
                                                             _eventBuffer.getBufferPositionParser(),
                                                             "EventLogWriter.flush");
        performWrite(readRangeLockToken);
      }
      catch (InterruptedException e)
      {
        LOG.error("flush read lock wait interrupted", e);
        _stopRunning.set(true);
      }
      catch (TimeoutException e)
      {
        LOG.error("flush read lock wait timed out", e);
        _stopRunning.set(true);
      }
      catch (IOException e)
      {
        LOG.error("unable to flush" + e.getMessage(), e);
        _stopRunning.set(true);
      }
      finally
      {
        if (null != readRangeLockToken) _lockProvider.releaseReaderLock(readRangeLockToken);
      }
    }

    if (_currentWritableByteChannel != null)
    {
      try
      {
        _currentWritableByteChannel.force(true);
        _currentWritableByteChannel.close();
      }
      catch (IOException e)
      {
        LOG.error("flush error:" + e.getMessage(), e);
      }
    }
  }

  private FileChannel getCurrentWriteChannel()
  {

    if (_currentWritableByteChannel != null)
    {
      if (_currentFileBytesWritten >= _individualFileMaxBytes)
      {
        try
        {
          _currentWritableByteChannel.force(true);
          _lastFsyncTime = System.currentTimeMillis();
          _currentWritableByteChannel.close();
        }
        catch (IOException e)
        {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
        _currentWritableByteChannel = null;
      }
    }

    if (_currentWritableByteChannel == null)
    {
      File writeFile = getNextWriteFile();
      try
      {
        _currentWritableByteChannel = new FileOutputStream(writeFile).getChannel();
      }
      catch (FileNotFoundException e)
      {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      _currentFileBytesWritten = 0;
    }
    // TODO Auto-generated method stub
    return _currentWritableByteChannel;
  }


  private File getNextWriteFile()
  {


    _currentWriteFileIndex++;

    String fileName = _writeSessionDir.getAbsolutePath() + File.separator + "eventBuffer_" + _currentWriteFileIndex + ".";
    switch (_encoding)
    {
    case BINARY:
      fileName += "bin";
      break;
    case JSON: case JSON_PLAIN_VALUE:
    {
      fileName += "json";
    }

    }
    LOG.info("Creating new file " + fileName);

    File writeFile = new File(fileName);
    try
    {
      writeFile.createNewFile();
    }
    catch (IOException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    assert writeFile.canWrite();

    while (_writeFileHandles.offer(writeFile) == false)
    {
      File deleteHandle = _writeFileHandles.poll();
      if (deleteHandle != null)
      {
        if (!deleteHandle.delete())
        {
          LOG.error("deleting failed:" + deleteHandle.getAbsolutePath());
        }
      }
    }
    return writeFile;
  }


public static class StaticConfig
{
  protected DbusEventBuffer _eventBuffer;
  private final boolean _enabled;
  private final File _topLevelLogDir;
  private final File _writeSessionDir;
  private final Encoding _encoding;
  private final long _writeBatchSizeInBytes;
  private final boolean _blockOnWrite;
  private final long _individualFileMaxBytes;
  private final int _maxFiles;
  private final int _maxPendingWrites;
  private final long _fsyncIntervalInMillis;
  private EventLogWriter _existingLogWriter;

  public StaticConfig(DbusEventBuffer eventBuffer,
                      boolean enabled,
                      String topLevelLogsDir,
                      String writeSessionDir,
                      Encoding encoding,
                      long writeBatchSizeInBytes,
                      boolean blockOnWrite,
                      long individualFileMaxBytes,
                      int maxFiles,
                      int maxPendingWrites,
                      long fsyncIntervalInMillis)
  {
    super();
    _eventBuffer = eventBuffer;
    _enabled = enabled;
    _topLevelLogDir = new File(topLevelLogsDir);
    _writeSessionDir = new File(writeSessionDir);
    _encoding = encoding;
    _writeBatchSizeInBytes = writeBatchSizeInBytes;
    _blockOnWrite = blockOnWrite;
    _individualFileMaxBytes = individualFileMaxBytes;
    _maxFiles = maxFiles;
    _maxPendingWrites = maxPendingWrites;
    _fsyncIntervalInMillis = fsyncIntervalInMillis;
  }

  /** The event buffer to read the events from */
  public DbusEventBuffer getEventBuffer()
  {
    return _eventBuffer;
  }

  /** A flag if the writer is enabled and it will try to keep log of the events */
  public boolean isEnabled()
  {
    return _enabled;
  }

  /** The parent directory for all write session directories */
  public File getTopLevelLogDir()
  {
    return _topLevelLogDir;
  }

  /** The directory where the current write session logs will be written */
  public File getWriteSessionDir()
  {
    return _writeSessionDir;
  }

  /** The encoding of the events: JSON or BINARY */
  public Encoding getEncoding()
  {
    return _encoding;
  }

  /** The number of bytes to buffer before writing the logs */
  public long getWriteBatchSizeInBytes()
  {
    return _writeBatchSizeInBytes;
  }

  /** A flag if the log writer should hold locks in the buffer until logs are written to disk */
  public boolean getBlockOnWrite()
  {
    return _blockOnWrite;
  }

  /** The max size of a file with event logs in the write session directory */
  public long getIndividualFileMaxBytes()
  {
    return _individualFileMaxBytes;
  }

  /** Max number of files in the write session directory */
  public int getMaxFiles()
  {
    return _maxFiles;
  }

  /**
   * Max number of events that can be buffered before the event log writer gives up trying to
   * keep up with the event stream.
   */
  public int getMaxPendingWrites()
  {
    return _maxPendingWrites;
  }

  /** The number of milliseconds between fsyncs to make sure logs are securely written to disk */
  public long getFsyncIntervalInMillis()
  {
    return _fsyncIntervalInMillis;
  }

  public EventLogWriter getOrCreateEventLogWriter(DbusEventBuffer eventBuffer)
  {
    _eventBuffer = eventBuffer;
    _existingLogWriter = new EventLogWriter(this);
    return _existingLogWriter;
  }


}

public static class Config implements ConfigBuilder<StaticConfig>
{


  public static final boolean DEFAULT_ENABLED = false;
  public static final String DEFAULT_TOP_LEVEL_LOG_DIR = "eventLog";
  public static final String DEFAULT_WRITE_SESSION_DIR = null;
  public static final String DEFAULT_ENCODING = Encoding.BINARY.name();
  public static final long DEFAULT_WRITE_BATCH_SIZE_IN_BYTES = 64 * ByteSizeConstants.ONE_KILOBYTE_IN_BYTES;
  public static final boolean DEFAULT_BLOCK_ON_WRITE = false;
  private static int FIVE_HUNDRED_MEGABYTES_IN_BYTES = 500 * ByteSizeConstants.ONE_MEGABYTE_IN_BYTES;
  public static final int DEFAULT_INDIVIDUAL_FILE_MAX_SIZE = FIVE_HUNDRED_MEGABYTES_IN_BYTES;
  public static final int DEFAULT_MAX_FILES = 5;
  public static final long DEFAULT_FSYNC_INTERVAL_IN_MILLIS = 10000;
  public static final int DEFAULT_MAX_PENDING_WRITES = 20;

  protected DbusEventBuffer _eventBuffer;
  protected boolean _enabled;

  protected String _topLevelLogDir;
  protected String _writeSessionDir;
  protected long _individualFileMaxBytes;
  protected int _maxFiles;

  protected String _encoding;

  protected boolean _blockOnWrite;
  protected long _writeBatchSizeInBytes;
  protected int _maxPendingWrites;
  protected long _fsyncIntervalInMillis;

  public Config()
  {
    super();
    _enabled = DEFAULT_ENABLED;
    _topLevelLogDir = DEFAULT_TOP_LEVEL_LOG_DIR;
    _encoding = DEFAULT_ENCODING;
    _writeBatchSizeInBytes = DEFAULT_WRITE_BATCH_SIZE_IN_BYTES;
    _blockOnWrite = DEFAULT_BLOCK_ON_WRITE;
    _individualFileMaxBytes = DEFAULT_INDIVIDUAL_FILE_MAX_SIZE;
    _maxFiles = DEFAULT_MAX_FILES;
    _fsyncIntervalInMillis = DEFAULT_FSYNC_INTERVAL_IN_MILLIS;
    _maxPendingWrites = DEFAULT_MAX_PENDING_WRITES;
  }

  public Config(Config other)
  {
    _enabled = other._enabled;
    _topLevelLogDir = other._topLevelLogDir;
    _encoding = other._encoding;
    _writeBatchSizeInBytes = other._writeBatchSizeInBytes;
    _blockOnWrite = other._blockOnWrite;
    _individualFileMaxBytes = other._individualFileMaxBytes;
    _eventBuffer = other._eventBuffer;
    _maxFiles = other._maxFiles;
    _fsyncIntervalInMillis = other._fsyncIntervalInMillis;
    _maxPendingWrites = other._maxPendingWrites;
  }

  @Override
  public StaticConfig build() throws InvalidConfigException
  {
    //TODO add verification for the config
    LOG.info("Event Log Writer enabled: " + _enabled);

    File writeDir = new File(_topLevelLogDir);
    if (writeDir.exists() && !writeDir.canWrite())
    {
      throw new InvalidConfigException("Invalid Config value : Cannot write to writeDir: " + _topLevelLogDir);
    }

    LOG.info("Event Log Writer writeDir: " + writeDir.getAbsolutePath());

    if (_writeSessionDir == null)
    {
      File writeSessionDir = new File(writeDir.getAbsolutePath() + File.separator + "session" + "_" + System.currentTimeMillis());
      _writeSessionDir = writeSessionDir.getAbsolutePath();
    }

    LOG.info("Event Log Writer writeSessionDir: " + _writeSessionDir);


    Encoding encoding = null;
    try
    {
      encoding = Encoding.valueOf(_encoding);
    }
    catch (Exception e)
    {
      throw new InvalidConfigException("Invalid Config Value for encoding: " + _encoding);
    }


    if (_maxFiles <=0)
    {
      throw new InvalidConfigException("EventLogWriter: maxFiles configured <= 0 : " + _maxFiles);
    }

    if (_maxPendingWrites <=0)
    {
      throw new InvalidConfigException("EventLogWriter: maxPendingWrites configured <=0 : " + _maxPendingWrites);
    }


    LOG.info("Event Log Writer encoding: " + _encoding);
    LOG.info("Event Log Writer writeBatchSizeInBytes: " + _writeBatchSizeInBytes);
    LOG.info("Event Log Writer blockOnWrite: " + _blockOnWrite);
    LOG.info("Event Log Writer individualFileMaxBytes: " + _individualFileMaxBytes);
    LOG.info("Event Log Writer maxFiles: " + _maxFiles);
    LOG.info("Event Log Writer maxPendingWrites: " + _maxPendingWrites);
    LOG.info("Event Log Writer fsyncIntervalInMillis: " + _fsyncIntervalInMillis);


    return new StaticConfig(_eventBuffer, _enabled, _topLevelLogDir, _writeSessionDir, encoding,
                            _writeBatchSizeInBytes,
                            _blockOnWrite, _individualFileMaxBytes, _maxFiles, _maxPendingWrites,
                            _fsyncIntervalInMillis);
  }

  public boolean isEnabled()
  {
    return _enabled;
  }

  public void setEnabled(boolean enabled)
  {
    _enabled = enabled;
  }

  public String getTopLevelLogDir()
  {
    return _topLevelLogDir;
  }

  public void setTopLevelDir(String topLevelLogDir)
  {
    _topLevelLogDir = topLevelLogDir;
  }

  public String getWriteSessionDir()
  {
    return _writeSessionDir;
  }

  public void setWriteSessionDir(String writeSessionDir)
  {
    _writeSessionDir = writeSessionDir;
  }

  public String getEncoding()
  {
    return _encoding;
  }

  public void setEncoding(String encoding)
  {
    _encoding = encoding;
  }

  public long getWriteBatchSizeInBytes()
  {
    return _writeBatchSizeInBytes;
  }

  public void setWriteBatchSizeInBytes(long writeBatchSizeInBytes)
  {
    _writeBatchSizeInBytes = writeBatchSizeInBytes;
  }

  public boolean getBlockOnWrite()
  {
    return _blockOnWrite;
  }

  public void setBlockOnWrite(boolean blockOnWrite)
  {
    _blockOnWrite = blockOnWrite;
  }

  public long getIndividualFileMaxBytes()
  {
    return _individualFileMaxBytes;
  }

  public void setIndividualFileMaxBytes(long individualFileMaxBytes)
  {
    _individualFileMaxBytes = individualFileMaxBytes;
  }

  public int getMaxFiles()
  {
    return _maxFiles;
  }

  public void setMaxFiles(int maxFiles)
  {
    _maxFiles = maxFiles;
  }

  public int getMaxPendingWrites()
  {
    return _maxPendingWrites;
  }

  public void setMaxPendingWrites(int maxPendingWrites)
  {
    _maxPendingWrites = maxPendingWrites;
  }

  public long getFsyncIntervalInMillis()
  {
    return _fsyncIntervalInMillis;
  }

  public void setFsyncIntervalInMillis(long fsyncIntervalInMillis)
  {
    _fsyncIntervalInMillis = fsyncIntervalInMillis;
  }


}


}

