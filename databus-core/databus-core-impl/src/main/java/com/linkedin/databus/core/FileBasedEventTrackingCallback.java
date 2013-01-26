package com.linkedin.databus.core;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

import org.apache.log4j.Logger;

public class FileBasedEventTrackingCallback  extends InternalDatabusEventsListenerAbstract
{
  public final static String MODULE = FileBasedEventTrackingCallback.class.getName();
  public final static Logger LOG = Logger.getLogger(MODULE);

  private String _filename;
  protected FileChannel _writeChannel;
  private boolean _append;
  private FileOutputStream _file;

  public FileBasedEventTrackingCallback(String filename, boolean append)
  {
    this(filename, append, 0);
  }

  public FileBasedEventTrackingCallback(String filename, boolean append, int numEventsPerBatch)
  {
    _append = append;
    _filename = filename;
    LOG.info(MODULE + " using " + filename + " for event tracking");
    LOG.info(MODULE + " append-only mode is set to " + append);
  }

  public void init() throws IOException
  {
    openFile(_filename);
  }

  public void openFile(String fileName) throws IOException
  {
    File file = new File(fileName);
    if (!_append)
    {
      //file.deleteOnExit();
      file.createNewFile();
    }
    _file = new FileOutputStream(file, _append);
    _writeChannel = _file.getChannel();
  }

  @Override
  public void close() throws IOException {
    LOG.info("closing " + _filename);

    _writeChannel.force(true);
    _writeChannel.close();
    _file.close();
  }

  public void endEvents()
  {
    try {
      close();
    } catch (IOException ioe) {
      LOG.warn("Couldn't endEvents for " + _file, ioe);
    }
  }

  @Override
  public void onEvent(DbusEvent event, long offset, int size)
  {
    onEvent(event);
  }

  public void onEvent(DbusEvent event)
  {
    if (!event.isEndOfPeriodMarker())
    {
      DbusEventInternalReadable dBusEvent = (DbusEventInternalReadable)event;
      dBusEvent.writeTo(_writeChannel, Encoding.JSON);
    }
  }

}


