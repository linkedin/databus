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
  public void onEvent(DataChangeEvent event, long offset, int size)
  {
    onEvent(event);
  }

  public void onEvent(DataChangeEvent event)
  {
    if (!event.isEndOfPeriodMarker())
    {
      DbusEvent dBusEvent = (DbusEvent)event;
      dBusEvent.writeTo(_writeChannel, Encoding.JSON);
    }
  }

}


