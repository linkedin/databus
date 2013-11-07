package com.linkedin.databus.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.log4j.Logger;

/**
 *
 * A single File InputStream implementation which is capable of reading a file which is getting concurrently appended.
 *
 * For static files, this would be a regular fileInputStream
 * For concurrently appended files, this acts like a stream only returning EOF when the concurrent writer completes and notifies.
 *
 * Concurrent update (non-append type) is not supported and behavior is undefined
 */
public class ConcurrentAppendableSingleFileInputStream
  extends InputStream
 {

  public static final String MODULE = ConcurrentAppendableSingleFileInputStream.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  public static final int EOF = -1;

  public static final int NULL_CHAR = 0x0;

  /**
   *
   * Behavior when EOF is received from underlying stream
   */
  public static enum EOFSyncBehavior
  {
    NO_SYNC,  // Return EOF if received from underlying stream without blocking
    SYNC_ONCE, // Sync once to check for more data. If EOF again, return EOF
    SYNC_TILL_NEW_DATA // Block and peridically sync till new appended data is seen in the stream
  };

  /** File representation */
  private final File _file;

  /** ReSync interval in Ms */
  private final long _resyncIntervalMs;

  /** Byte Offset from which to start reading **/
  private final long _byteOffset;

  /** Underlying File Input Stream */
  private FileInputStream _inputStream = null;

  /** Behavior for sync on EOF */
  private volatile EOFSyncBehavior _eofSync = null;

  /** Flag to indicate the channel is closed **/
  private volatile boolean _closed = false;

  /** Cached last Modified timestamp */
  private long _lastModifiedTimestamp;

  /** File Position needed for resync */
  private long _currOffset = 0;

  /** Previous File Position needed for resync */
  private long _prevOffset = 0;

  /** Number of byte read calls for this stream */
  private volatile long _numReadCalls = 0;

  /** Number of byte read calls that resulted in a valid byte being sent **/
  private volatile long _numReadCallsWithData = 0;


  /**
   *
   * Factory to create an inputStream from a file that is static (do not wait for more input if EOF is reached)
   *
   * @param fileName : Absolute path to the file
   * @param byteOffset : Byte Offset within the file where the read has to start
   * @return InputStream corresponding to the file
   *
   * @throws IOException if unable to access the file
   */
  public static ConcurrentAppendableSingleFileInputStream createStaticFileInputStream(String fileName,
                                                                                      long byteOffset)
      throws IOException
  {
    return new ConcurrentAppendableSingleFileInputStream(fileName, EOFSyncBehavior.NO_SYNC, byteOffset, 0);
  }

  /**
   *
   * Factory to create an inputStream from a file that is concurrently getting appended. The client is responsible for
   * calling the API {@link appendDone()} for the stream to detect end of the stream and return EOF.
   *
   * @param fileName : Absolute path to the file
   * @param byteOffset : Byte Offset within the file where the read has to start
   * @param syncInterval : interval between syncing when EOF is detected by the underlying stream
   *
   * @return InputStream corresponding to the file
   *
   * @throws IOException if unable to access the file
   */
  public static ConcurrentAppendableSingleFileInputStream createAppendingFileInputStream(String fileName, long byteOffset, long syncInterval)
      throws IOException
  {
    return new ConcurrentAppendableSingleFileInputStream(fileName, EOFSyncBehavior.SYNC_TILL_NEW_DATA, byteOffset, syncInterval);
  }

  /**
   *
   * Construct a single File input-stream
   *
   * @param fileName : Absolute file path
   * @param eofSyncBehavior :EOF Sync behavior
   * @param byteOffset : ByteOffset from which read has to start
   * @param resyncIntervalMs  : Resync Interval on reaching EOF
   * @throws IOException
   */
  private ConcurrentAppendableSingleFileInputStream(String fileName,
                                                    EOFSyncBehavior eofSyncBehavior,
                                                    long byteOffset,
                                                    long resyncIntervalMs)
      throws IOException
  {
    _byteOffset = byteOffset;
    _file = new File(fileName);
    _eofSync = eofSyncBehavior;
    _resyncIntervalMs = resyncIntervalMs;
    _lastModifiedTimestamp = _file.lastModified();

    syncStreamOnce(true);

    if ( _byteOffset > 0 )
      _inputStream.getChannel().position(_byteOffset);

    _currOffset = _inputStream.getChannel().position();
    _prevOffset = _currOffset;
  }

  @Override
  public synchronized int read()
      throws IOException
  {
    int retVal = _inputStream.read();
    _prevOffset = _currOffset;
    _currOffset = _inputStream.getChannel().position();

    _numReadCalls++;
    boolean isDebugEnabled = LOG.isDebugEnabled();

    // if valid byte, return immediately
    if ( (retVal != EOF) && (retVal != NULL_CHAR))
    {
      _numReadCallsWithData++;
      if (isDebugEnabled)
        LOG.debug("Byte returned (non-EOF) is :" + retVal + ", State is :" + toString());

      return retVal;
    }

    boolean done = false;
    while ( ((retVal == EOF) || (retVal == NULL_CHAR))
    		&& (!_closed) && (!done))
    {
      if ( retVal == NULL_CHAR)
      {
        // Force sync using prevOffset
        syncOnceAndSleep();
        retVal = _inputStream.read();
        _prevOffset = _currOffset;
        _currOffset = _inputStream.getChannel().position();
        continue;
      }

      switch (_eofSync)
      {
        case NO_SYNC :
        {
          retVal = _inputStream.read();
          _prevOffset = _currOffset;
          _currOffset = _inputStream.getChannel().position();

          if (isDebugEnabled)
            LOG.debug("Byte returned (NO_SYNC) is :" + retVal + ", State is :" + toString());
          done = true;
          break;
        }

        case SYNC_ONCE :
        {
          // sync and set behavior to NO_SYNC
          syncStreamOnce(true);
          _eofSync = EOFSyncBehavior.NO_SYNC;
          break;
        }

        case SYNC_TILL_NEW_DATA :
        {
          // sync and read the next byte
          syncOnceAndSleep();
          retVal = _inputStream.read();
          _prevOffset = _currOffset;
          _currOffset = _inputStream.getChannel().position();
          break;
        }
      }
    }

    if (retVal != EOF)
      _numReadCallsWithData++;

    if (isDebugEnabled)
      LOG.debug("Byte returned is :" + retVal + ", State is :" + toString());

    return retVal;
  }

  /**
   *
   * Verifies if the byte-array contains NULL character between offset and len
   *
   * @param b Data buffer
   * @param offset start location for scanning
   * @param len number of bytes to scan
   * @return true if NULL character is found, otherwise false
   */
  private boolean isNullByteSeen(byte[]b, int offset, int len)
  {
    int limit = offset + len;
    for (int i = offset; i < limit; i++)
    {
      if ( b[i] == NULL_CHAR)
      {
        LOG.warn("Found NULL character from data read from underlying stream. Offset :" + offset + ", Length :" + len + " Re-Syncing");
        return true;
      }
    }
    return false;
  }

  @Override
  public synchronized int read(byte[] b)
      throws IOException
  {
    int retVal = _inputStream.read(b);
    _prevOffset = _currOffset;
    _currOffset = _inputStream.getChannel().position();

    _numReadCalls++;

    boolean nullBytesSeen = false;
    boolean isDebugEnabled = LOG.isDebugEnabled();

    // if not EOF or NULL, return immediately
    if ( retVal != EOF )
    {
      nullBytesSeen = isNullByteSeen(b, 0, retVal);

      if ( ! nullBytesSeen )
      {
        _numReadCallsWithData++;

        if (isDebugEnabled)
          LOG.debug("Num Bytes returned (non-EOF) is :" + retVal + ", State is :" + toString());

        return retVal;
      }
    }

    boolean done = false;
    while ( ((retVal == EOF) || nullBytesSeen)
    		&& (!_closed) && (!done))
    {
     if ( nullBytesSeen )
     {
       // Force sync using prevOffset
       syncOnceAndSleep();
       retVal = _inputStream.read(b);
       _prevOffset = _currOffset;
       _currOffset = _inputStream.getChannel().position();
     } else {
      switch (_eofSync)
      {
        case NO_SYNC :
        {
          retVal = _inputStream.read(b);
          _prevOffset = _currOffset;
          _currOffset = _inputStream.getChannel().position();

          if (isDebugEnabled)
            LOG.debug("Num bytes returned (NO_SYNC) is :" + retVal + ", State is :" + toString());
          done = true;
          break;
        }

        case SYNC_ONCE :
        {
          // sync and set behavior to NO_SYNC
          syncStreamOnce(true);
          _eofSync = EOFSyncBehavior.NO_SYNC;
          break;
        }

        case SYNC_TILL_NEW_DATA :
        {
          // sync and read the next byte
          syncOnceAndSleep();
          retVal = _inputStream.read(b);
          _prevOffset = _currOffset;
          _currOffset = _inputStream.getChannel().position();
          break;
        }
       }
     }
     nullBytesSeen = isNullByteSeen(b, 0, retVal);
    }

    if (retVal != EOF)
      _numReadCallsWithData++;

    if (isDebugEnabled)
      LOG.debug("Num Bytes returned is :" + retVal + ", State is :" + toString());

    return retVal;
  }

  @Override
public synchronized int read(byte b[], int off, int len)
      throws IOException
  {
    int retVal = _inputStream.read(b, off, len);
    _prevOffset = _currOffset;
    _currOffset = _inputStream.getChannel().position();

    _numReadCalls++;

    boolean isDebugEnabled = LOG.isDebugEnabled();
    boolean nullBytesSeen = false;

    // if not EOF or NULL, return immediately
    if ( retVal != EOF )
    {
      nullBytesSeen = isNullByteSeen(b, off, retVal);

      if ( ! nullBytesSeen)
      {
        _numReadCallsWithData++;

        if (isDebugEnabled)
          LOG.debug("Num Bytes returned (non-EOF) is :" + retVal + ", State is :" + toString());

        return retVal;
      }
    }

    boolean done = false;
    while ( ((retVal == EOF) || nullBytesSeen)
    		&& (!_closed) && (!done))
    {
      if ( nullBytesSeen )
      {
        // Force sync using prevOffset
        syncOnceAndSleep();
        retVal = _inputStream.read(b, off, len);
        _prevOffset = _currOffset;
        _currOffset = _inputStream.getChannel().position();
      } else {
       switch (_eofSync)
       {
        case NO_SYNC :
        {
          retVal = _inputStream.read(b, off, len);
          _prevOffset = _currOffset;
          _currOffset = _inputStream.getChannel().position();

          if (isDebugEnabled)
            LOG.debug("Num bytes returned (NO_SYNC) is :" + retVal + ", State is :" + toString());

          done = true;
        }
        case SYNC_ONCE :
        {
          // sync and set behavior to NO_SYNC
          syncStreamOnce(true);
          _eofSync = EOFSyncBehavior.NO_SYNC;
          break;
        }
        case SYNC_TILL_NEW_DATA :
        {
          // sync and read the next byte
          syncOnceAndSleep();
          if ( ! _closed)
          {
        	retVal = _inputStream.read(b, off, len);
            _prevOffset = _currOffset;
        	_currOffset = _inputStream.getChannel().position();
          }
          break;
        }
       }
      }
      nullBytesSeen = isNullByteSeen(b, off, retVal);
    }

    if (retVal != EOF)
      _numReadCallsWithData++;

    if (isDebugEnabled)
      LOG.debug("Num Bytes returned is :" + retVal + ", State is :" + toString());

    return retVal;
  }

  /**
   *
   * Decide if stream is to be synced. If not,then sleep
   *
   * @throws IOException
   */
  private synchronized void syncOnceAndSleep()
      throws IOException
  {
	// Ensure eofSync is Sync_TILL_NEW_DATA once inside the monitor
	if(_eofSync == EOFSyncBehavior.SYNC_TILL_NEW_DATA)
	{
		boolean refreshed = syncStreamOnce(false);

		if ( ! refreshed )
		{
			try
			{
				wait(_resyncIntervalMs);
			} catch (InterruptedException ie) {

			}
		}
	}
  }


  /**
   *
   * Sync by reopening underlying stream.
   * If force == false, syncing is done only if lastModifiedTimestamp got updated otherwise, syncing is done unconditionally
   *
   * @param force
   * @throws IOException
   */
  private synchronized boolean syncStreamOnce(boolean force)
      throws IOException
  {
    boolean doSync = force;

    long newLastModifiedTs = _file.lastModified();
    if ( !force )
    {
      if (_lastModifiedTimestamp < newLastModifiedTs)
        doSync = true;
    }

    if ( doSync)
    {
      if(LOG.isDebugEnabled())
        LOG.debug("Syncing from file :" + _file);

      closeStream();
      _inputStream = new FileInputStream(_file);
      _inputStream.getChannel().position(_prevOffset);
      _lastModifiedTimestamp = newLastModifiedTs;
    } else {
      // Even if sync did not happen, we should align channel position with the offset
      _inputStream.getChannel().position(_prevOffset);
    }

    //Re-align the curOffset
    _currOffset = _inputStream.getChannel().position();
    return doSync;
  }

  @Override
  public synchronized void close() throws IOException
  {
    LOG.info("Closing ConcurrentAppendableSingleFileInputStream for file :" + _file);
    _closed = true;
    closeStream();
    notifyAll();
  }

  public void closeStream() throws IOException
  {
    if ( null != _inputStream)
      _inputStream.close();
  }

  public EOFSyncBehavior getEOFSyncBehavior() {
    return _eofSync;
  }

  /**
   * Notification by the client that concurrent append had been done and safe for this stream to return EOF if detected.
   */
  public synchronized void appendDone()
  {
    LOG.info("Marking file appending done for inputStream :" + toString());

    // Safe to sync one more time when append is done !!
    this._eofSync = EOFSyncBehavior.SYNC_ONCE;

    notifyAll();
  }


  @Override
  public String toString() {
    return "ConcurrentAppendableSingleFileInputStream [_file=" + _file
        + ", _resyncIntervalMs=" + _resyncIntervalMs
        + ", _inputStream=" + _inputStream + ", _eofSync=" + _eofSync
        + ", _closed=" + _closed + ", _lastModifiedTimestamp="
        + _lastModifiedTimestamp + ", _currOffset=" + _currOffset
        + ", _numReadCalls=" + _numReadCalls
        + ", _numReadCallsWithData=" + _numReadCallsWithData + "]";
  }

  public long getNumReadCalls()
  {
    return _numReadCalls;
  }

  public long geNumReadCallsWithData()
  {
    return _numReadCallsWithData;
  }

  public File getFile()
  {
    return _file;
  }

  public synchronized long getCurrPosition()
  {

    long offset = -1;
    try
    {
      offset = _inputStream.getChannel().position();
    } catch (Exception ex) {
      LOG.error("Got exception when getting the current position. State :" + toString(), ex);
    }
    return offset;
  }



}
