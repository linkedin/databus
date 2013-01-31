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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;

import com.linkedin.databus.core.DbusEventBuffer.DbusEventIterator;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;

/**
 * A journal reader of the event log as dumped by the EventLogWriter.
 * @author sdas
 *
 */
@Deprecated
public class EventLogReader
extends InternalDatabusEventsListenerAbstract
{
  public static final String MODULE = EventLogReader.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private final boolean _enabled;
  private final DbusEventBuffer _eventBuffer;
  private final File _topLevelLogDir;
  private final File _readSessionDir;
  private final ArrayList<File> _filesToRead;


  private boolean _eventSeen;
  private String _firstEventContent;
  private long _lastEopOffset;
  long _firstEventSequence;

  private class LastModifiedSorter implements Comparator<File>
  {

    @Override
    public int compare(File o1, File o2)
    {
      return (int) (o1.lastModified() - o2.lastModified());
    }

  }
  /**
   * A journal reader of the event log as dumped by the EventLogWriter.
   * The EventLogReader must be constructed before the EventLogWriter
   * This allows the EventLogReader to deduce the directory to use for reading log files correctly
   * The organization of the log files under the top-level writeDir is :
   * writeDir/session_<timestamp>/eventBuffer_*.[bin|json]
   * @param enabled
   * @param eventBuffer
   * @param topLevelLogDir
   * @param readSessionDir
   */
  public EventLogReader(boolean enabled, DbusEventBuffer eventBuffer,
                        File topLevelLogDir, File readSessionDir)
  {
    _enabled = enabled;
    _eventBuffer = eventBuffer;
    _topLevelLogDir = topLevelLogDir;
    _readSessionDir = readSessionDir;

    _filesToRead = new ArrayList<File>();
    if (_readSessionDir == null)
    {
      LOG.warn("EventLogReader did not find any pre-existing logs to read from, no directories under writeDir:" + _topLevelLogDir);
    }
    else
    {
      if (_readSessionDir.listFiles().length > 0)
      {
        Encoding encoding = getEncoding(_readSessionDir.listFiles()[0]);
        for (File f: _readSessionDir.listFiles())
        {
          LOG.info(f.getAbsolutePath());
          if (getEncoding(f) != encoding)
          {
            LOG.error("Encoding " + encoding + " did not match encoding for File " + f.getAbsolutePath());
          }
          else
          {
            _filesToRead.add(f);
          }
        }
      }
      else
      {
      }

      Collections.sort(_filesToRead,new LastModifiedSorter());

      _eventSeen = false;
      _firstEventContent = null;
      _lastEopOffset = -1;
      LOG.info("Configured with top-level log Directory :" + _topLevelLogDir.getAbsolutePath());
      LOG.info("Inferred read-session directory :" + _readSessionDir.getAbsolutePath());
      if (_filesToRead.size()== 0)
      {
        LOG.warn("EventLogReader did not find any pre-existing logs to read from, no files under read session Dir:" + _readSessionDir);
      }
    }
  }

  public EventLogReader(StaticConfig staticConfig)
  {
    this(staticConfig.isEnabled(), staticConfig.getEventBuffer(), staticConfig.getTopLevelLogDir(),
         staticConfig.getReadSessionDir());
  }

  private Encoding getEncoding(File f)
  {
    if (f.getAbsolutePath().endsWith(".bin"))
    {
      return Encoding.BINARY;
    }

    if (f.getAbsolutePath().endsWith(".json"))
    {
      return Encoding.JSON;
    }

    return null;
  }

  public Checkpoint read()
  {

    Checkpoint checkPoint = new Checkpoint();
    checkPoint.setFlexible();
    if (_enabled)
    {
      // ArrayList<InternalDatabusEventsListener> eventListeners = new ArrayList<InternalDatabusEventsListener>();
      _eventBuffer.addInternalListener(checkPoint);
      _eventBuffer.addInternalListener(this);
      if (_filesToRead != null)
      {
        for (File f: _filesToRead)
        {
          FileChannel readChannel = null;
          try
          {
            readChannel = new FileInputStream(f).getChannel();
          }
          catch (FileNotFoundException e)
          {
            throw new RuntimeException(e);
          }

          int numEvents=0;
          try
          {
            numEvents = _eventBuffer.readEvents(readChannel, getEncoding(f));
          }
          catch (InvalidEventException e)
          {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
          LOG.info("Read " + numEvents + " events");
        }

        _eventBuffer.removeInternalListener(checkPoint);
        _eventBuffer.removeInternalListener(this);

        LOG.info("Checkpoint = " + checkPoint);


        if (_eventSeen)
        {
          DbusEventIterator iter = _eventBuffer.acquireIterator("EventLogReader:firstEvent");

          try
          {
            DbusEvent event = iter.next();
            String firstEventContent = event.toString();
            // if we didn't wrap the buffer, and the first event is not an eop marker, delete the first window
            if (_firstEventContent.equalsIgnoreCase(firstEventContent) && !event.isEndOfPeriodMarker())
            {
              long result = _eventBuffer.deleteFirstWindow();
              if (result < 0)
              {
                throw new RuntimeException("eventBuffer could not delete first window");
              }
            }
          }
          finally
          {
            _eventBuffer.releaseIterator(iter);
          }

          if (_lastEopOffset >=0)
          {
            iter = _eventBuffer.new DbusEventIterator(this._lastEopOffset, _eventBuffer.getTail(),
                                                      "EventLogReader:lastEOP");
            try
            {
              DbusEvent event = iter.next();
              if (!event.isEndOfPeriodMarker())
              {
                throw new RuntimeException("Tried reading end of period marker, but failed");

              }

              if (iter.hasNext())
              {
                _eventBuffer.setTail(iter.getCurrentPosition());
              }


            }
            catch (NoSuchElementException e)
            {
              LOG.error("NoSuchElementException at : " + _eventBuffer.getBufferPositionParser().toString(_lastEopOffset));
              throw e;
            }
            finally
            {
              _eventBuffer.releaseIterator(iter);
            }
          }
        }
      }
    }
    return checkPoint;

  }

  @Override
  public void onEvent(DbusEvent event, long offset, int size)
  {
    if (!_eventSeen)
    {
      _firstEventContent = event.toString();
      _eventSeen = true;
      _firstEventSequence = event.sequence();
    }

    if (event.isEndOfPeriodMarker())
    {
      _lastEopOffset = offset;
    }

  }

  public static class StaticConfig
  {
    protected DbusEventBuffer _eventBuffer;
    private final boolean _enabled;
    private final File  _topLevelLogDir;
    private final File _readSessionDir;
    private EventLogReader _existingLogReader;



    public StaticConfig(DbusEventBuffer eventBuffer,
                        boolean enabled,
                        String topLevelLogDir,
                        String readSessionDir)
    {
      super();
      _eventBuffer = eventBuffer;
      _enabled = enabled;
      _topLevelLogDir = new File(topLevelLogDir);
      if (readSessionDir != null)
      {
        _readSessionDir = new File(readSessionDir);
      }
      else
      {
        _readSessionDir = null;
      }
    }

    /** The event buffer where the log reader will store the events */
    public DbusEventBuffer getEventBuffer()
    {
      return _eventBuffer;
    }

    /** A flag if the event log reader is to be invoked on relay startup */
    public boolean isEnabled()
    {
      return _enabled;
    }

    /** The parent directory for all session directories */
    public File getTopLevelLogDir()
    {
      return _topLevelLogDir;
    }

    /** The directory where to read the event logs for the current session*/
    public File getReadSessionDir()
    {
      return _readSessionDir;
    }

    public EventLogReader getOrCreateEventLogReader(DbusEventBuffer eventBuffer)
    {
      _eventBuffer = eventBuffer;
      _existingLogReader = new EventLogReader(this);
      return _existingLogReader;
    }
  }

  public static class Config implements ConfigBuilder<StaticConfig>
  {

    public static final boolean DEFAULT_ENABLED = false;
    public static final String DEFAULT_TOP_LEVEL_LOG_DIR = "eventLog";
    public static final String DEFAULT_READ_SESSION_DIR = null;

    protected DbusEventBuffer _eventBuffer;
    protected boolean _enabled;
    protected String _topLevelLogDir;
    protected String _readSessionDir;

    public Config()
    {
      super();
      _enabled = DEFAULT_ENABLED;
      _topLevelLogDir = DEFAULT_TOP_LEVEL_LOG_DIR;
      _readSessionDir = DEFAULT_READ_SESSION_DIR;
    }

    public Config(Config other)
    {
      _eventBuffer = other._eventBuffer;
      _enabled = other._enabled;
      _topLevelLogDir = other._topLevelLogDir;
      _readSessionDir = other._readSessionDir;
    }


    public DbusEventBuffer getEventBuffer()
    {
      return _eventBuffer;
    }




    public void setEventBuffer(DbusEventBuffer eventBuffer)
    {
      _eventBuffer = eventBuffer;
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




    public void setTopLevelLogDir(String topLevelLogDir)
    {
      _topLevelLogDir = topLevelLogDir;
    }




    public String getReadSessionDir()
    {
      return _readSessionDir;
    }




    public void setReadSessionDir(String readSessionDir)
    {
      _readSessionDir = readSessionDir;
    }




    @Override
    public com.linkedin.databus.core.EventLogReader.StaticConfig build() throws InvalidConfigException
    {

      LOG.info("Event Log Reader enabled : " + _enabled);
      File topLevelLogDir = new File(_topLevelLogDir);
      if (topLevelLogDir.exists() && !topLevelLogDir.canRead())
      {
        throw new InvalidConfigException("Invalid Config value : Cannot read from top level log dir: " + _topLevelLogDir);
      }

      if (_readSessionDir == null)
      {
        File[] allFiles = topLevelLogDir.listFiles();
        long newestTime = 0;
        File readSessionDir = null;
        if (allFiles != null)
        {
          for (File f: allFiles)
          {
            if (f.isDirectory())
            {
              if (f.lastModified() > newestTime)
              {
                newestTime = f.lastModified();
                readSessionDir = f;
              }
            }
          }
        }

        if (readSessionDir != null)
        {
          _readSessionDir = readSessionDir.getAbsolutePath();
        }
      }

      return new StaticConfig(_eventBuffer, _enabled, _topLevelLogDir, _readSessionDir);

    }
  }
}
