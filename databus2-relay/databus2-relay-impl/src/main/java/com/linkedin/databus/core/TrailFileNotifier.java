package com.linkedin.databus.core;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.log4j.Logger;

import com.linkedin.databus2.core.DatabusException;

/**
 *
 * Thread that polls for new files in trail directory, filters and ranks
 * using the filter callback and notifies the listener through callback
 * of new file in the trail order.
 */
public class TrailFileNotifier
  extends DatabusThreadBase
{
  public static final String MODULE = TrailFileNotifier.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private final File _dir;
  private final long _pollIntervalMs;
  private final TrailFileManager _fileFilter;
  private final TrailFileListener _trailFileListener;

  // Trail files starting from "_initialFileToBeginProcessing" will be processed. Any trail file that is older than _initialFileToBeginProcessing in trail file order will not be read.
  private final File _initialFileToBeginProcessing;

  // High WaterMark File that has been processed. Every poll cycle, only trail files that are later (in trail file order) than lastSeenFile will be processed.
  private File _lastSeenFile;

  private boolean _shutdownOnError = false;
  public TrailFileNotifier(
                           File dir,
                           TrailFileManager fileFilter,
                           File startFile,
                           long pollIntervalMs,
                           TrailFileListener trailFileListener)
  {
    super(MODULE);
    _dir = dir;
    _initialFileToBeginProcessing = startFile;
    _pollIntervalMs = pollIntervalMs;
    _trailFileListener = trailFileListener;
    _fileFilter = fileFilter;
  }

  @Override
  public boolean runOnce()
  {
    LOG.debug("TrailFileNotifier running one cycle !!");

    boolean success = fetchOneTime();

    if ( ! success)
    {
    	_shutdownOnError = true;
    	return false;
    }

    try
    {
      Thread.sleep(_pollIntervalMs);
    } catch (InterruptedException ie) {
      LOG.info("Got interrupted while sleeping for :" + _pollIntervalMs + " ms");
    }

    return true;
  }


  @Override
  public void beforeRun()
  {}

  @Override
  public void afterRun()
  {}

  /**
   * One cycle of fetching trail files
   */
  public synchronized boolean fetchOneTime()
  {
    List<File> candidateTrailFiles = getCandidateTrailFiles();

    if (LOG.isDebugEnabled())
      LOG.debug("Final List of Files in Directory :" + candidateTrailFiles);

    try
    {
      for (File t : candidateTrailFiles)
      {
        if (LOG.isDebugEnabled())
          LOG.debug("Adding trail file :" + t);
        _trailFileListener.onNewTrailFile(t);
        _lastSeenFile = t;
      }
    } catch (Exception ex) {
    	LOG.error("Got exception while enqueuing trail file", ex);
    	_trailFileListener.onError(ex);
    	return false;
    }

    return true;
  }

  public synchronized List<File> getCandidateTrailFiles()
  {
    File[] filesInDirArray = _dir.listFiles();

    if (LOG.isDebugEnabled())
      LOG.debug("Files in Directory :" + Arrays.toString(filesInDirArray));

    List<File> candidateTrailFiles = new ArrayList<File>();

    // filtering out old trail files
    if ( null != filesInDirArray) // filesInDirArray could be null if path doesn't exist or I/O error
    {
      for (File t : filesInDirArray)
      {
        if ( (null == t) || (! _fileFilter.isTrailFile(t)))
          continue;

        if ((null != _initialFileToBeginProcessing) && (_fileFilter.compareFileName(t, _initialFileToBeginProcessing) < 0))
          continue;

        if ( (null != _lastSeenFile) && ( _fileFilter.compareFileName(t, _lastSeenFile) <= 0 ))
          continue;

        candidateTrailFiles.add(t);
      }
    }

    if (LOG.isDebugEnabled())
      LOG.debug("Candidate Files in Directory :" + candidateTrailFiles);

    // Order the trail files
    Collections.sort(candidateTrailFiles, new Comparator<File>() {
      @Override
      public int compare(File o1, File o2) {
        return _fileFilter.compareFileName(o1, o2);
      }});

    return candidateTrailFiles;
  }

  public boolean isShutdownOnError()
  {
     return _shutdownOnError;
  }

  /**
   *
   * Interface called by ChunkedInputStream to filter and order the trail files
   *
   */
  public static interface TrailFileManager
  {
    /**
     *
     * Callback for sorting the files in trail file order
     *
     * @param file1 : file  in trail directory
     * @param file2  : another file in trail directory
     * @return 0 if file1 == file2, -1 if file1 appears before file2 in trail file order, 1 otherwise
     */
    public int compareFileName(File file1, File file2);

    /**
     * If the file is a trail file, return true else return false
     *
     * @param file
     * @return
     */
    public boolean isTrailFile(File file);

    /**
     * Returns true if file2 is the logical next file to file1.
     * If this cannot be determined with the file objects alone, return true
     *
     * @param file1 File1
     * @param file2 File2
     * @return
     */
    public boolean isNextFileInSequence(File file1, File file2);
  }

  /**
   *
   * Callback Interface to notify clients about new trail file presence
   */
  public static interface TrailFileListener
  {
    /**
     * Callback to notify new trail file in the directory.
     *
     * Contract:
     *  (a) The file path is guaranteed to be absolute.
     *  (b) This callback is called in the order of files that are generated.
     * @param  New trail file
     * @throws DatabusException if validation fails on the callback.
     */
    public void onNewTrailFile(File file) throws DatabusException;

    /**
     * Allows callback to cleanup if TrailFileNotifier is shutting-down because of error
     * @param ex
     */
    public void onError(Throwable ex);
  }
}
