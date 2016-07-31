package com.linkedin.databus.client;
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


import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.log4j.Logger;

import com.linkedin.databus.client.pub.SCN;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusLogAccumulator;
import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus2.core.container.request.RegisterResponseEntry;
import com.linkedin.databus2.core.container.request.RegisterResponseMetadataEntry;
import com.linkedin.databus2.schemas.SchemaId;
import com.linkedin.databus2.schemas.SchemaRegistryService;
import com.linkedin.databus2.schemas.VersionedSchemaSet;

public class DispatcherState
{
  public static final String MODULE = DispatcherState.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  public enum StateId
  {
    //Initial state when is the dispatcher is started
    INITIAL,
    //Start reading from event buffer and dispatch events.
    START_DISPATCH_EVENTS,
    //First event of a new window (end of window for the previous window has been seen)
    EXPECT_EVENT_WINDOW,
    //Start of a window (which will be followed by data events)
    START_STREAM_EVENT_WINDOW,
    //End of events for a source within a window
    START_STREAM_SOURCE,
    //Beginning of data events to be streamed
    EXPECT_STREAM_DATA_EVENTS,
    //End of events for a source within a window (which maybe followed by more sources)
    END_STREAM_SOURCE,
    //End of a window
    END_STREAM_EVENT_WINDOW,
    //Rollback is triggered (after error in the consumer)
    ROLLBACK,
    //After rollback, events are replayed to the consumer
    REPLAY_DATA_EVENTS,
    //Not used (???)
    STOP_DISPATCH_EVENTS,
    //Shutdown the dispatcher
    CLOSED
  }

  private StateId _stateId;


  //START_DISPATCH_EVENTS
  private final Map<Long, IdNamePair> _sources = new HashMap<Long, IdNamePair>();

  //payload schemas
  private final VersionedSchemaSet _schemaSet = new VersionedSchemaSet(true);
  //metadata schemas
  private final VersionedSchemaSet _metadataSchemasSet = new VersionedSchemaSet();



  private DbusEventBuffer.DbusEventIterator _eventsIterator;
  private DbusEventBuffer.DbusEventIterator _lastSuccessfulIterator;
  // Looks like _payloadSchemaMap is a member variable purely for testing purposes. Keeping it thus can
  // introduce bugs like DDSDBUS-3271. Need to remove the member variable.
  private final Map<Long, List<RegisterResponseEntry>> _payloadSchemaMap = new HashMap<Long, List<RegisterResponseEntry>>();
  private DbusEventBuffer _buffer;

  //EXPECT_EVENT_WINDOW extends START_DISPATCH_EVENTS

  //START_STREAM_EVENT_WINDOW extends DISPATCH_EVENTS
  private volatile SCN _startWinScn;
  private boolean _eventsSeen;

  //START_STREAM_SOURCE extends START_STREAM_EVENT_WINDOW
  private IdNamePair _currentSource;
  private Schema _currentSourceSchema;

  //EXPECT_STREAM_DATA_EVENTS extends START_STREAM_SOURCE

  //END_STREAM_SOURCE extends START_STREAM_SOURCE
  private volatile SCN _endWinScn;

  //END_STREAM_EVENT_WINDOW extends START_STREAM_EVENT_WINDOW

  //CHECKPOINT extends END_STREAM_EVENT_WINDOW
  private Checkpoint _lastSuccessfulCheckpoint;
  private SCN _lastSuccessfulScn;

  //ROLLBACK extends DISPATCH_EVENTS

  //REPLAY_DATA_EVENTS extends ROLLBACK

  //STOP_DISPATCH_EVENTS

  private DbusEventAvroDecoder _eventDecoder;


  private boolean _scnRegress = false;

  private DispatcherState()
  {
    super();
    _stateId = StateId.INITIAL;
  }

  private DispatcherState(DbusEventBuffer buffer)
  {
    super();
    _stateId = StateId.INITIAL;
    _buffer = buffer;
  }

  public static DispatcherState create()
  {
    return new DispatcherState();
  }

  public static DispatcherState create(DbusEventBuffer eventBuffer, String iteratorName)
  {
    DispatcherState result = new DispatcherState(eventBuffer);
    result.createEventsIterator(iteratorName);
    return result;
  }

  private void setLastSuccessfulIterator(DbusEventBuffer.DbusEventIterator newValue)
  {
    final boolean isDebugEnabled = LOG.isDebugEnabled();
    if (isDebugEnabled)
    {
      DbusLogAccumulator.addLog("Changing _lastSuccessfulIterator from: " + _lastSuccessfulIterator, LOG);
    }
    if (null != _lastSuccessfulIterator)
    {
      _lastSuccessfulIterator.close();
    }
    if (null == newValue)
    {
      _lastSuccessfulIterator = null;
    }
    else
    {
      _lastSuccessfulIterator = newValue.copy(_lastSuccessfulIterator, newValue.getIdentifier() + ".save");
    }
    if (isDebugEnabled)
    {
      DbusLogAccumulator.addLog("Changing _lastSuccessfulIterator to: " + _lastSuccessfulIterator, LOG);
    }
  }

  public DispatcherState switchToStartDispatchEvents()
  {
    _stateId = StateId.START_DISPATCH_EVENTS;
    return this;
  }

  private void createEventsIterator(String iteratorName)
  {
    if (null == _eventsIterator)
    {
      _stateId = StateId.START_DISPATCH_EVENTS;
      _lastSuccessfulCheckpoint = null;
      _lastSuccessfulScn = null;
      resetSourceInfo();
      _eventsIterator = _buffer.acquireIterator(iteratorName);
      LOG.info("start dispatch from: " + _eventsIterator);
      setLastSuccessfulIterator(_eventsIterator);
    }
  }

  private void refreshSchemas(List<RegisterResponseMetadataEntry> metadataSchemaList)
  {
    final boolean isDebugEnabled = LOG.isDebugEnabled();
    try
    {
      for (Map.Entry<Long, List<RegisterResponseEntry>> e: _payloadSchemaMap.entrySet())
      {
        for (RegisterResponseEntry r : e.getValue())
        {
          final long id = r.getId();
          String schemaName = null;
          if (_sources.containsKey(id))
          {
            schemaName = _sources.get(r.getId()).getName();
          }
          else
          {
            LOG.error("Obtained a RegisterResponseEntry with schema that has no sourceId set. id = " + id);
            continue;
          }
          String schema = r.getSchema();
          if (_schemaSet.add(schemaName, r.getVersion(), schema))
          {
            LOG.info("Registering schema name=" + schemaName + " id=" + e.getKey().toString() +
                " version=" + r.getVersion());

            if (isDebugEnabled)
            {
              String msg = "Registering schema name=" + schemaName + " id=" + e.getKey().toString() +
                  " version=" + r.getVersion() + ": " + schema;
              DbusLogAccumulator.addLog(msg, LOG);
            }
          }
          else
          {
            if (isDebugEnabled)
            {
              String msg = "Schema already known: " + schemaName + " version " +  r.getId();
              DbusLogAccumulator.addLog(msg, LOG);
            }
          }
        }
      }

      //Refresh metadata schema map
      if ((metadataSchemaList != null) && !metadataSchemaList.isEmpty())
      {
        for (RegisterResponseMetadataEntry e: metadataSchemaList)
        {
          SchemaId id = new SchemaId(e.getCrc32());
          if (_metadataSchemasSet.add(SchemaRegistryService.DEFAULT_METADATA_SCHEMA_SOURCE,e.getVersion(),id,e.getSchema()))
          {
            LOG.info("Added metadata schema version " + e.getVersion() + ",schemaID=0x" + id);
          }
          else
          {
            if (isDebugEnabled)
            {
              String msg = "Metadata schema version " + e.getVersion() + ",schemaId=0x" + id + " already exists";
              DbusLogAccumulator.addLog(msg, LOG);
            }
          }
        }
      }
      else
      {
        if (isDebugEnabled)
        {
          String msg = "Metadata schema is empty";
          DbusLogAccumulator.addLog(msg, LOG);
        }
      }

      _eventDecoder = new DbusEventAvroDecoder(_schemaSet,_metadataSchemasSet);
    }
    catch (Exception e)
    {
      LOG.error("Error adding schema", e);
    }
  }

  public void resetIterators()
  {
    if (null != _lastSuccessfulIterator)
    {
      setLastSuccessfulIterator(null);
      _lastSuccessfulScn = null;
      _lastSuccessfulCheckpoint = null;
    }

    if (null != _eventsIterator)
    {
      DbusEventBuffer eventBuffer = _eventsIterator.getEventBuffer();
      String iteratorName = _eventsIterator.getIdentifier();
      _eventsIterator.close();
      _eventsIterator = eventBuffer.acquireIterator(iteratorName);
      if (LOG.isDebugEnabled())
      {
        String msg = "Reset event iterator to: " + _eventsIterator;
        DbusLogAccumulator.addLog(msg, LOG);
      }
      resetSourceInfo();
    }
  }

  public void switchToExpectEventWindow()
  {
    _stateId = StateId.EXPECT_EVENT_WINDOW;
  }

  public DispatcherState switchToStopDispatch()
  {
    _stateId = StateId.STOP_DISPATCH_EVENTS;
    setEventsIterator(null);
    return this;
  }

  public DispatcherState switchToClosed()
  {
    _stateId = StateId.CLOSED;
    setLastSuccessfulIterator(null);
    setEventsIterator(null);
    return this;
  }

  public synchronized void switchToStartStreamEventWindow(SCN startWinScn)
  {
    _stateId = StateId.START_STREAM_EVENT_WINDOW;
    _startWinScn = startWinScn;
    _eventsSeen = true;
    resetSourceInfo();
  }

  public synchronized void switchToEndStreamEventWindow(SCN endWinScn)
  {
    _stateId = StateId.END_STREAM_EVENT_WINDOW;
    _endWinScn = endWinScn;
    _eventsSeen = false;
  }

  public void switchToStartStreamSource(IdNamePair source, Schema sourceSchema)
  {
    _stateId = StateId.START_STREAM_SOURCE;
    _currentSource = source;
    _currentSourceSchema = sourceSchema;
  }

  public void switchToEndStreamSource()
  {
    _stateId = StateId.END_STREAM_SOURCE;
  }

  public void switchToExpectStreamDataEvents()
  {
    _stateId = StateId.EXPECT_STREAM_DATA_EVENTS;
  }

  public void storeCheckpoint(Checkpoint cp, SCN scn)
  {
    _lastSuccessfulCheckpoint = cp;
    _lastSuccessfulScn = scn;
    setLastSuccessfulIterator(_eventsIterator);
  }

  public void switchToRollback()
  {
    _stateId = StateId.ROLLBACK;
    _eventsSeen = false;
  }

  private void setEventsIterator(DbusEventBuffer.DbusEventIterator newValue)
  {
    String iterName = null == newValue ? "dispatcher iterator" :
      newValue.getIdentifier();
    if (null != _eventsIterator)
    {
      iterName = _eventsIterator.getIdentifier();
      if (LOG.isDebugEnabled())
      {
        String msg = "Closing dispatcher iterator: " + _eventsIterator;
        DbusLogAccumulator.addLog(msg, LOG);
      }
      _eventsIterator.close();
    }
    if (null == newValue)
    {
      _eventsIterator = null;
      if (LOG.isDebugEnabled())
      {
        String msg = "Dispatcher iterator set to null";
        DbusLogAccumulator.addLog(msg, LOG);
      }
    }
    else
    {
      _eventsIterator = newValue.copy(_eventsIterator, iterName);
      if (LOG.isDebugEnabled())
      {
        String msg = "New dispatcher iterator: " + _eventsIterator;
        DbusLogAccumulator.addLog(msg, LOG);
      }
    }
  }

  public void switchToReplayDataEvents()
  {
    _stateId = StateId.REPLAY_DATA_EVENTS;
    resetSourceInfo();
    setEventsIterator(_lastSuccessfulIterator);
  }

  public void resetSourceInfo()
  {
    _currentSource = null;
    _currentSourceSchema = null;
  }

  public StateId getStateId()
  {
    return _stateId;
  }

  public DbusEventBuffer.DbusEventIterator getEventsIterator()
  {
    return _eventsIterator;
  }

  public Map<Long, IdNamePair> getSources()
  {
    return _sources;
  }

  public DbusEventAvroDecoder getEventDecoder()
  {
    return _eventDecoder;
  }

  public synchronized SCN getStartWinScn()
  {
    return _startWinScn;
  }

  public IdNamePair getCurrentSource()
  {
    return _currentSource;
  }

  public Schema getCurrentSourceSchema()
  {
    return _currentSourceSchema;
  }

  public synchronized SCN getEndWinScn()
  {
    return _endWinScn;
  }

  public Checkpoint getLastSuccessfulCheckpoint()
  {
    return _lastSuccessfulCheckpoint;
  }

  public VersionedSchemaSet getSchemaSet()
  {
    return _schemaSet;
  }

  public SCN getLastSuccessfulScn()
  {
    return _lastSuccessfulScn;
  }

  public DbusEventBuffer.DbusEventIterator getLastSuccessfulIterator()
  {
    return _lastSuccessfulIterator;
  }

  @Override
  public String toString()
  {
    return "DispatcherState:" + _stateId.toString();
  }

  public Map<Long, List<RegisterResponseEntry>> getSchemaMap()
  {
    return _payloadSchemaMap;
  }

  public void removeEvents()
  {
    DbusEventBuffer.DbusEventIterator iter = getEventsIterator();
    if (!iter.equivalent(_lastSuccessfulIterator))
    {
      if (_lastSuccessfulIterator == null)
      {
        LOG.warn("Last Successful Iterator was null. Rollback will not be possible!");
      }
      else
      {
        LOG.info("Invalidating last successful iterator " + "last = " +
            _lastSuccessfulIterator +  " this iterator= " + iter);
        setLastSuccessfulIterator(null);
      }
    }
    iter.remove();
  }

  public boolean isSCNRegress()
  {
    return _scnRegress;
  }

  public void setSCNRegress(boolean scnRegress)
  {
    _scnRegress = scnRegress;
  }

  public boolean isEventsSeen()
  {
    return _eventsSeen;
  }

  public void setEventsSeen(boolean hasSeenDataEvents)
  {
    _eventsSeen = hasSeenDataEvents;
  }

  protected DispatcherState addSources(Collection<IdNamePair> sources)
  {
    for (IdNamePair source: sources)
    {
      _sources.put(source.getId(), source);
    }

    return this;
  }

  protected DispatcherState addSchemas(Map<Long, List<RegisterResponseEntry>> schemaMap)
  {
    return addSchemas(schemaMap,null);
  }


  protected DispatcherState addSchemas(Map<Long, List<RegisterResponseEntry>> schemaMap,
      List<RegisterResponseMetadataEntry> metadataSchemaList)
  {
    _payloadSchemaMap.putAll(schemaMap);
    refreshSchemas(metadataSchemaList);
    return this;
  }
}
