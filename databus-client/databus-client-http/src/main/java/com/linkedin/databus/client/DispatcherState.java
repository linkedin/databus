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


import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.log4j.Logger;

import com.linkedin.databus.client.pub.SCN;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus2.core.container.request.RegisterResponseEntry;
import com.linkedin.databus2.schemas.SchemaId;
import com.linkedin.databus2.schemas.VersionedSchemaSet;

public class DispatcherState
{
  public static final String MODULE = DispatcherState.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  public enum StateId
  {
    INITIAL,
    START_DISPATCH_EVENTS,
    EXPECT_EVENT_WINDOW,
    START_STREAM_EVENT_WINDOW,
    START_STREAM_SOURCE,
    EXPECT_STREAM_DATA_EVENTS,
    END_STREAM_SOURCE,
    END_STREAM_EVENT_WINDOW,
    CHECKPOINT,
    ROLLBACK,
    REPLAY_DATA_EVENTS,
    STOP_DISPATCH_EVENTS,
    CLOSED
  }

  private StateId _stateId;

  //INIITIAL

  //START_DISPATCH_EVENTS
  private Map<Long, IdNamePair> _sources;
  private static final VersionedSchemaSet _schemaSet = new VersionedSchemaSet();
  private DbusEventBuffer.DbusEventIterator _eventsIterator;
  private DbusEventBuffer.DbusEventIterator _lastSuccessfulIterator;
  private Map<Long, List<RegisterResponseEntry>> _schemaMap;
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

  public static DispatcherState create()
  {
    return new DispatcherState();
  }

  private void setLastSuccessfulIterator(DbusEventBuffer.DbusEventIterator newValue)
  {
    final boolean debugEnabled = LOG.isDebugEnabled();
    if (debugEnabled)
      LOG.debug("changing _lastSuccessfulIterator from: " + _lastSuccessfulIterator);
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
      _lastSuccessfulIterator = newValue.copy(_lastSuccessfulIterator,
                                              newValue.getIdentifier() + ".save");
    }
    if (debugEnabled)
      LOG.debug("changing _lastSuccessfulIterator to: " + _lastSuccessfulIterator);
  }

  public DispatcherState switchToStartDispatchEvents(Map<Long, IdNamePair> sourcesIdMap,
                                                     Map<Long, List<RegisterResponseEntry>> schemaMap,
                                                     DbusEventBuffer buffer)
  {
    _stateId = StateId.START_DISPATCH_EVENTS;
    _buffer = buffer;
    _sources = sourcesIdMap;
    _schemaMap = schemaMap;
    return this;
  }

  public DispatcherState switchToStartDispatchEventsInternal(DispatcherState msg,
                                                             String iteratorName)
  {
    assert null != msg;
    assert null != msg._buffer;
    assert null != msg.getSources();
    assert null != msg.getSchemaMap();

    _buffer = msg._buffer;
    _sources = msg.getSources();
    _schemaMap = msg.getSchemaMap();
    refreshSchemas();

    /*
     * Except during the startup case, the event iterator is purely owned by the dispatcher
     * and its current state determined by the dispatcher and consumer callback behavior.
     * Puller should not dictate this state.
     * But, Puller used to set this iterator whenever it goes to Register_Response_Success state
     * which can happen many times during the single run. The below condition is to protect against this.
     */
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

    return this;
  }

  private void refreshSchemas()
  {
    boolean debugEnabled = LOG.isDebugEnabled();
    try
    {
    	for (Map.Entry<Long, List<RegisterResponseEntry>> e: _schemaMap.entrySet())
    	{
    		for (RegisterResponseEntry r : e.getValue())
    		{
    			String schema = r.getSchema();
    			Schema s = Schema.parse(schema);
    			String schemaName = _sources.get(r.getId()).getName();
    			if (_schemaSet.add(schemaName, r.getVersion(), schema))
    			{
                  SchemaId schemaHash = SchemaId.forSchema(schema);
    			  LOG.info("Registering schema with id " + e.getKey().toString() +
    			           " version " + r.getVersion() + "[" + schemaHash.toString()+
    			           "]: " + r.getId()  + " " + s);
    			}
    			else
    			{
    			  if (debugEnabled)
    			    LOG.debug("schema already known: " + schemaName + " version " +  r.getId());
    			}
    		}
    	}
    	_eventDecoder = new DbusEventAvroDecoder(_schemaSet);
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
      LOG.info("reset event iterator to: " + _eventsIterator);
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

  public void switchToCheckpoint(Checkpoint cp, SCN scn)
  {
    _stateId = StateId.CHECKPOINT;
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
      LOG.info("closing dispatcher iterator: " + _eventsIterator);
      _eventsIterator.close();
    }
    if (null == newValue)
    {
      _eventsIterator = null;
      LOG.info("dispatcher iterator set to null ");
    }
    else
    {
      _eventsIterator = newValue.copy(_eventsIterator, iterName);
      LOG.info("new dispatcher iterator: " + _eventsIterator);
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
    return _schemaMap;
  }

  public void removeEvents()
  {
      DbusEventBuffer.DbusEventIterator iter = getEventsIterator();
      if (!iter.equivalent(_lastSuccessfulIterator))
      {
    	  if (_lastSuccessfulIterator == null)
    	  {
    		  LOG.warn("last Successful Iterator was null!");
    	  }
    	  else
    	  {
    		  LOG.warn("Invalidating last successful iterator! " + "last = " +
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
}
