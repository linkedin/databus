/*
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
 */

package com.linkedin.databus.core;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.linkedin.databus.core.TrailFileNotifier.TrailFileManager;
import com.linkedin.databus.core.TrailFilePositionSetter.FilePositionResult.Status;
import com.linkedin.databus2.core.DatabusException;

/**
 *
 * Helper Class to find the position (filename, fileByteOffset) where a transaction with a requested
 * SCN (or the next monotonically increasing SCN)starts.
 * The requested SCN can have 2 special values:
 * 1. -1 : This SCN would mean the client is requesting to position to the last completely written transaction in the trail file.
 * 2. -2 : This SCN would mean the client is requesting to position to the first available transaction in the trail File.
 *
 * The main API to us is the {@link getFilePosition()} which will locate the transaction given a SCN
 */
public class TrailFilePositionSetter
{
  public Logger _log;

  /** Use latest SCN in the trail file **/
  public static final long USE_LATEST_SCN  = -1;

  /** Use earliest SCN in the trail file **/
  public static final long USE_EARLIEST_SCN = -2;

  private final File _dir;
  private final String _filePrefix;
  private final FileFilter _filter;
  private String _phySourceName;

  public static final String NEW_LINE_PATTERN_STR = "\r?\n|\r";
  public static final String DOUBLE_CHAR_NEWLINE = "\r\n";

  // Flag to indicate there is no new line
  public static final int NO_NEWLINE_LEN = -1;

  public TrailFilePositionSetter(String dir, String filePrefix)
  throws IOException
  {
    // Append file prefix name towards the end. Error check because validateDir has
    // not yet been run yet.
    final String MODULE = TrailFilePositionSetter.class.getName();
    _log = Logger.getLogger(MODULE);

    validateDir(dir);
    _dir = new File(dir);
    _filePrefix = filePrefix;
    _filter = new FileFilter(_dir, _filePrefix);
    _phySourceName = "";
  }

  // Constructor to set optional parameter phySourceName
  public TrailFilePositionSetter(String dir, String filePrefix, String phySourceName)
  throws IOException
  {
    this(dir, filePrefix);
    _phySourceName = (phySourceName != null) ? phySourceName : "";

    // Append file prefix name towards the end. Error check because validateDir has
    // not yet been run yet.
    final String MODULE = TrailFilePositionSetter.class.getName();
    _log = Logger.getLogger(MODULE + ":" + _phySourceName);
  }

  /**
   * Validate directory
   * @param dir
   * @throws IOException
   */
  private static void validateDir(String dir)
      throws IOException
  {
    File d = new File(dir);

    if (! d.isDirectory())
      throw new IOException("Path (" + dir +") does not exist or is not a directory !!");
  }

  /**
   *
   * This is an optimized version of searching the trail file position. It does a lookup from latest file quickly skipping trail files whose first transaction has
   * SCN newer than the requested SCN.
   *
   * Steps:
   * 1. Get all the list of trail files
   * 2. Iterate the trail files from the latest to earliest trail file until exhausted or successful, do
   *    a) FindResult = Call FindPosition() on the current file
   *    b) If FindResult was successful( FOUND (exact scn is present) or EXACT_SCN_NOT_FOUND (scn with both lower and higher SCn is seen but not exact), then return
   *    c) Otherwise on the first transaction, reset and look at the previous file
   *
   * This method is quick because if the currentTrailFile's SCN has higher than requested SCN, then it fails fast (after first transaction) so that lookup happens on the next file.
   * In EI/Prod, each trail file is in the order of 50 MB and this jumping should save a lot of time.
   *
   * @param scn : SCN to locate
   * @param callback TransactionSCNFinderCallback to parse and store offsets.
   * @return FilePositionResult of the locate operation
   * @throws IOException if issues with File operations.
   */
  public synchronized FilePositionResult locateFilePosition(long scn, TransactionSCNFinderCallback callback)
      throws IOException
  {
    TrailFileNotifier notifier = new TrailFileNotifier(_dir, _filter, null, 0, null);
    List<File> orderedTrailFiles = notifier.getCandidateTrailFiles();

    _log.info("Initial set of Trail Files :" + orderedTrailFiles);

    if ( (null == orderedTrailFiles) || orderedTrailFiles.isEmpty())
    {
      return FilePositionResult.createNoTxnsFoundResult();
    }

    FilePositionResult res = null;

    if ( scn == USE_EARLIEST_SCN)
    {
      res = getFilePosition(scn, callback);
    } else {
      for (int i = orderedTrailFiles.size() - 1; i >= 0; i--)
      {
        callback.reset();

        File startFile = orderedTrailFiles.get(i);
        _log.info("Locating the SCN (" + scn + ") starting from the trail file :" + startFile);
        res = getFilePosition(scn, callback,startFile.getName());

        _log.info("Result of the location operation for SCN (" + scn + ") starting from trail file (" + startFile + ") is : " + res);

        if ((res.getStatus() == Status.EXACT_SCN_NOT_FOUND) || (res.getStatus() == Status.FOUND))
          break;
      }
    }
    return res;
  }

  /**
   *
   * Linear search for the Transaction position in the trail file that is first in the trail file order with SCN
   * equal or more than requested SCN. This search starts from the first transaction available in the
   * trail files.
   * The requested SCN can have 2 special values:
   *    1. -1 : This SCN would mean the client is requesting to position to the last completely written
   *            transaction in the trail directory.
   *    2. -2 : This SCN would mean the client is requesting to position to the first available transaction
   *            in the trail directory.
   *
   *
   * @param scn : SCN to locate
   * @param callback TransactionSCNFinderCallback to parse and store offsets.
   * @return FilePositionResult of the locate operation
   * @throws IOException if issues with File operations.
   */
  protected synchronized FilePositionResult getFilePosition(long scn, TransactionSCNFinderCallback callback)
    throws IOException
  {
    return getFilePosition(scn,callback,null);
  }

  /**
   *
   * Linear search for the Transaction position in the trail file that is first in the trail file order with SCN
   * equal or more than requested SCN. This search starts from the first transaction available in the
   *  rail file passed.
   *
   * The requested SCN can have 2 special values:
   *    1. -1 : This SCN would mean the client is requesting to position to the last completely written
   *            transaction in the trail directory.
   *    2. -2 : This SCN would mean the client is requesting to position to the first available transaction
   *            in the trail directory.
   *
   *
   * @param scn : SCN to locate
   * @param callback TransactionSCNFinderCallback to parse and store offsets.
   * @param startFile : Start Looking for transaction from this trail file
   * @return FilePositionResult of the locate operation
   * @throws IOException if issues with File operations.
   */
  protected synchronized FilePositionResult getFilePosition(long scn, TransactionSCNFinderCallback callback, String startFile)
      throws IOException
  {
    ConcurrentAppendableCompositeFileInputStream stream = null;
    FilePositionResult result = null;
    try
    {
      stream = new ConcurrentAppendableCompositeFileInputStream(_dir.getAbsolutePath(), startFile, -1L, _filter, true);
      result = findTxnScn(stream, scn, callback);

      _log.info("File Position result for scn (" + scn + ") is :" + result);
      _log.info("Input Stream Rate Monitor - " + stream.getRateMonitor());
      _log.info("Callback RM - " + callback.getPerfStats());
    } catch (IOException io) {
      _log.error("Unable to fetch the file position and offsets");
      result = FilePositionResult.createErrorResult(io);
    } finally {
      if(stream != null)
        stream.close();
    }
    return result;
  }


  private FilePositionResult findTxnScn(ConcurrentAppendableCompositeFileInputStream stream,
          long expScn,
          TransactionSCNFinderCallback callback)
       throws IOException
  {
    FilePositionResult result = null;
    //File prevFile = null;
    ScnTxnPos pos = null;

    callback.begin(expScn);
    byte[] bArr = new byte[4*1024];
    File prevFile = null;
    File currFile = null;
    long currPosition = -1;

    List<String> lines = new ArrayList<String>();
    List<Integer> lineEndPos = new ArrayList<Integer>();

    String prevLine = null;
    boolean done = false;
    while(!done)
    {
      prevFile = currFile;
      int numBytes = stream.read(bArr);

      if (numBytes <= 0)
        break;

      currFile = stream.getCurrentFile();
      currPosition  = stream.getCurrentPosition();

      boolean spanFile = false;
      int endOffset = 0;
      if ( (currFile != null) && (prevFile != null) && ( !currFile.equals(prevFile)))
      {
        // Crossed File boundary while reading this block. Track the endOffset where the file ends
        spanFile = true;
        endOffset = (int)(numBytes - currPosition);
      }

      prevLine = splitBytesByNewLines(bArr, numBytes, spanFile, endOffset, prevLine, lines, lineEndPos);

      // On First Read, call the beginFileProcessing callback
      if ( prevFile == null)
        callback.beginFileProcessing(currFile.getName());

      int currOffset = 0;
      for (int i = 0; i < lines.size(); i++)
      {
        String l = lines.get(i);

        //newLineLen can be one of (-1) File Boundary, (1) "\n" or "\r" , (2) "\r\n"
        int newLineLen = lineEndPos.get(i) - currOffset - l.length();

        try
        {
          done = callback.processLine(l, newLineLen);
        }
        catch (DatabusException e)
        {
          _log.error("Got Exception when processing line (" + l + ").",e);
          result = FilePositionResult.createErrorResult(e);
          return result;
        }

        if ( done )
          break;

        // when File boundary on this line
        if (lineEndPos.get(i) == -1)
        {
          callback.endFileProcessing(prevFile.getName());
          callback.beginFileProcessing(currFile.getName());
        }
        currOffset = ((lineEndPos.get(i) < 0 ) ? currOffset + l.length() : lineEndPos.get(i));
      }
      lines.clear();
      lineEndPos.clear();
    }

    // There could last transaction which would be complete with the prevLine added.
    if (!done && (prevLine != null))
    {
      try
      {
        callback.processLine(prevLine, NO_NEWLINE_LEN);
      }
      catch (DatabusException e)
      {
        if(_log.isDebugEnabled())
          _log.debug("Got Exception when processing line (" + prevLine + ").",e);
        result = FilePositionResult.createErrorResult(e);
        return result;
      }
    }

    pos = callback.getTxnPos();

    if (callback.getNumTxnsSeen() <= 0)
    {
      result = FilePositionResult.createNoTxnsFoundResult();
    } else if (expScn == USE_LATEST_SCN) {
      result = FilePositionResult.createFoundResult(pos);
    } else if (expScn == USE_EARLIEST_SCN) {
      result = FilePositionResult.createFoundResult(pos);
    } else {
      // Normal SCN
      if ( pos.getMaxScn() == expScn)
        result = FilePositionResult.createFoundResult(pos);
      else
        result = FilePositionResult.createExactScnNotFoundResult(pos);
    }

    return result;
  }


  /**
   * Split the buffer by newLines where newLine can be one of "\n", "\r" or "\r\n"
   * Also splits a line that spans files to two lines.
   *
   * @param buf Buffer Read from input stream
   * @param numBytes numBytes read from the buffer
   * @param spanFile Did the file boundary cross in this read
   * @param endOffset End Offset in the buf
   * @param prevLine Any Previous partial line created from the previous split
   * @param lines Output list containing splitted lines
   * @param lineEndPos Output list containing actual end offsets including newlines (Used for calculating the length of newline)
   * @return last partial line or null if no such line
   */
  protected static String splitBytesByNewLines(byte[] buf,
                                               int numBytes,
                                               boolean spanFile,
                                               int endOffset,
                                               String prevLine,
                                               List<String> lines,
                                               List<Integer> lineEndPos)
  {
    String s = (prevLine != null) ? prevLine + new String(buf,0,numBytes) : new String(buf,0,numBytes);

    if ( s.isEmpty())
      return null;

    // endOffset should be updated with prepended string
    if ( prevLine != null)
      endOffset += prevLine.length();

    String[] strArr = s.split(NEW_LINE_PATTERN_STR); //Newline can be one \n, \r,\r\n

    // if buf contains only newlines, a zero sized array is returned. Massage it so it works with below logic
    if (strArr.length == 0)
    {
      strArr = new String[1];
      strArr[0] = "";
    }

    int pos = 0;
    for (int i = 0 ; i < strArr.length - 1; i++)
    {
      String l = strArr[i];

      // If this line spans file, split it
      if ( spanFile && (pos <= endOffset) && ((pos + l.length()) > endOffset))
      {
        int pivot = endOffset - pos;
        lines.add(l.substring(0,pivot));
        pos += pivot;
        l = l.substring(pivot);
        lineEndPos.add(-1);
        spanFile = false;
      }

      lines.add(l);
      pos += l.length();
      int newLineLen = 0;

      if (s.startsWith(DOUBLE_CHAR_NEWLINE, pos))
        newLineLen = 2;
      else
        newLineLen = 1;

      pos += newLineLen;
      lineEndPos.add(pos);
    }

    char c = s.charAt(s.length() -1);

    String lastLine = null;

    String l = strArr[strArr.length - 1];
    if ( spanFile && (pos <= endOffset) && ((pos + l.length()) > endOffset))
    {
      int pivot = endOffset - pos;
      lines.add(l.substring(0,pivot));
      pos += pivot;
      l = l.substring(pivot);
      lineEndPos.add(-1);
    }

    /**
     * If last character of the block read is '\r', then we need to read next block before deciding
     * if '\r' is newline or it is followed by '\n'
     */
    if ((c == '\r'))
    {
       lastLine = l + '\r';
    } else if (c != '\n') {
      lastLine = l;
    } else {
      lines.add(l);
      pos += l.length();
      if (s.startsWith(DOUBLE_CHAR_NEWLINE, pos))
        pos += 2;
      else
        pos += 1;
      lineEndPos.add(pos);
    }

    return lastLine;
  }


  public static class FilePositionResult
  {
    public static enum Status
    {
      /**
       * Requested SCN was found.  Clients can proceed by pointing an iterator at the file
       * position and reading.
       */
      FOUND,

      /**
       * Valid state; exact requested SCN doesn't match any "transaction-SCN" (== max SCN in
       * a transaction), but both lower and higher transaction-SCNs were found.  The exact
       * SCN may or may not actually be present as a non-maximal SCN in some transaction.
       * A special corner case occurs when the requested SCN is older than the oldest
       * transaction-SCN but is NOT older than the min SCN in the oldest transaction.  That
       * case also triggers this value (i.e., the min SCN is treated as a stand-in for the
       * missing "previous transaction-SCN").
       *
       * As with FOUND, clients can safely proceed to read from the indicated file position.
       */
      EXACT_SCN_NOT_FOUND,

      /**
       * Trail files are missing or empty; could be a transient state.  Clients are expected
       * to sleep a bit (e.g., with exponential backoff) and retry.
       */
      NO_TXNS_FOUND,

      /**
       * Fatal error; clients are expected to fail.  For example, the requested SCN may be
       * older than the oldest SCN present in the trail files.
       */
      ERROR
    };

    private final Status _status;
    private final ScnTxnPos _txnPos;

    private final Throwable _error;

    public static FilePositionResult createFoundResult(ScnTxnPos scnTxnPos)
    {
      return new FilePositionResult(Status.FOUND,scnTxnPos);
    }

    public static FilePositionResult createExactScnNotFoundResult(ScnTxnPos pos)
    {
      return new FilePositionResult(Status.EXACT_SCN_NOT_FOUND,pos);
    }

    public static FilePositionResult createNoTxnsFoundResult()
    {
      return new FilePositionResult(Status.NO_TXNS_FOUND,null);
    }

    public static FilePositionResult createErrorResult(Throwable ex)
    {
      return new FilePositionResult(ex);
    }

    private FilePositionResult(Status status,
                              ScnTxnPos txnPos)
    {
      super();
      this._status = status;
      this._txnPos = txnPos;
      this._error = null;
    }

    private FilePositionResult(Throwable err)
    {
      super();
      this._status = Status.ERROR;
      this._txnPos = null;
      this._error = err;
    }

    public Throwable getError()
    {
      return _error;
    }

    public Status getStatus()
    {
      return _status;
    }

    public ScnTxnPos getTxnPos()
    {
      return _txnPos;
    }

    @Override
    public String toString()
    {
      return "FilePositionResult [_status=" + _status + ", _txnPos=" + _txnPos + "]";
    }

  }


  public static class FileFilter
    implements TrailFileManager
  {
    private final String _prefix;
    private final Pattern _trailFileNamePattern;

    public FileFilter(File dir,
                      String prefix)
    {
      _prefix = prefix;
      String trailFileNameRegex = prefix + "[\\d]+";
      _trailFileNamePattern = Pattern.compile(trailFileNameRegex);
    }

    @Override
    public int compareFileName(File file1, File file2)
    {
      String f1 = file1.getName().substring(_prefix.length());
      String f2 = file2.getName().substring(_prefix.length());

      Long num1 = Long.parseLong(f1);
      Long num2 = Long.parseLong(f2);
      return num1.compareTo(num2);

    }

    @Override
    public boolean isTrailFile(File file)
    {
      if ((null != file) && (_trailFileNamePattern.matcher(file.getName()).matches()))
        return true;

      return false;
    }

	@Override
	public boolean isNextFileInSequence(File file1, File file2)
	{
	  String f1 = file1.getName().substring(_prefix.length());
	  String f2 = file2.getName().substring(_prefix.length());

	  long num1 = Long.parseLong(f1);
	  long num2 = Long.parseLong(f2);

	  if ((num2 - num1) == 1)
	  {
		return true;
	  }
	  return false;
	}
  }


  public static interface TransactionSCNFinderCallback
  {
    /**
     * Initialization callback. TargetSCN is passed.
     * targetSCN can be one of (USE_LATEST_SCN, USE_EARLIEST_SCN, <any_valid_scn>)
     * @param targetScn
     */
    public void begin(long targetScn);

     /**
      * Callback before starting to stream new file in the directory
      * @param file Absolute Path to the file
      */
     public void beginFileProcessing(String file);

     /**
      * Callback to let client parse a line and extract offsets and SCN. For the last line which spans files is split
      * so that the callback sees them as 2 lines with aligned file boundaries
      *
      *
      * @param line Trail data
      * @param newLineLen : NewLines can be 1 or 2 ("\n" vs "\r\n"). In case of fileBoundary, the newLineLen will be negative
      * @return true if expScn is found, otherwise false. For USE_EARLIEST_SCN, this method should return true
      * as soon as it completely sees the first transaction
      */
     public boolean processLine(String line, int newLineLen) throws DatabusException;

     /**
      * Callback after ending streaming the file in the directory
      * @param file Absolute Path to the file
      */
     public void endFileProcessing(String file);

     /**
      *
      * Return latest completely seen Txn position or null if no such txn
      *
      * @return
      */
     public ScnTxnPos getTxnPos();

     /**
      * Return the number of transactions seen by the callback
      * @return
      */
     public long getNumTxnsSeen();

     /**
      * Return the current offset as tracked by the callback
      * @return
      */
     public long getCurrentFileOffset();

     /**
      * Reset the state of Transaction Callback for new read.
      */
     public void reset();

     /**
      * Return any Perf stats that are monitored  by the callback
      * @return
      */
     public String getPerfStats();
  }
}
