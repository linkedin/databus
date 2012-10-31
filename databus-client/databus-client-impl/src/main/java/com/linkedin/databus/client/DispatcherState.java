package com.linkedin.databus.client;

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


  public DispatcherState switchToStartDispatchEvents(Map<Long, IdNamePair> sourcesIdMap,
                                                     Map<Long, List<RegisterResponseEntry>> schemaMap,
                                                     DbusEventBuffer.DbusEventIterator eventIterator)
  {
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
		_eventsIterator = eventIterator;
		_lastSuccessfulIterator = (null != _eventsIterator) ? _eventsIterator.copy(_lastSuccessfulIterator, _eventsIterator.getIdentifier() + ".save") : null;
	}

	_sources = sourcesIdMap;
	_schemaMap = schemaMap;
	boolean debugEnabled = LOG.isDebugEnabled();
	try
	{
		for (Map.Entry<Long, List<RegisterResponseEntry>> e: schemaMap.entrySet())
		{
			for (RegisterResponseEntry r : e.getValue())
			{
				String schema = r.getSchema();
				Schema s = Schema.parse(schema);
				String schemaName = sourcesIdMap.get(r.getId()).getName();
				if (_schemaSet.add(schemaName, r.getVersion(), schema))
				{
	              SchemaId schemaHash = SchemaId.forSchema(schema);
				  LOG.info("Registering schema with id " + e.getKey().toString() +
				           " version " + r.getVersion() + "[" + schemaHash.toString()+
				           "]: " + r.getId()  + " " + s);
				}
				else
				{
				  if (debugEnabled) LOG.debug("schema already known: " + schemaName + " version " + r.getId());
				}
			}
		}
		_eventDecoder = new DbusEventAvroDecoder(_schemaSet);
	}
	catch (Exception e)
	{
		LOG.error("Error adding schema", e);
	}
    return this;
  }

  public void resetIterators()
  {
    if (null != _lastSuccessfulIterator)
    {
      _lastSuccessfulIterator.getEventBuffer().releaseIterator(_lastSuccessfulIterator);
      _lastSuccessfulIterator = null;
      _lastSuccessfulScn = null;
      _lastSuccessfulCheckpoint = null;
    }

    if (null != _eventsIterator)
    {
      DbusEventBuffer eventBuffer = _eventsIterator.getEventBuffer();
      String iteratorName = _eventsIterator.getIdentifier();
      eventBuffer.releaseIterator(_eventsIterator);
      _eventsIterator = eventBuffer.acquireIterator(iteratorName);
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
    _eventsIterator = null;
    return this;
  }

  public DispatcherState switchToClosed()
  {
    _stateId = StateId.CLOSED;
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
    _lastSuccessfulIterator = null != _eventsIterator ?
        _eventsIterator.copy(_lastSuccessfulIterator, _eventsIterator.getIdentifier() + ".save") :
        null;
  }

  public void switchToRollback()
  {
    _stateId = StateId.ROLLBACK;
    _eventsSeen = false;
  }

  public void switchToReplayDataEvents(DbusEventBuffer.DbusEventIterator eventsIter)
  {
    _stateId = StateId.REPLAY_DATA_EVENTS;
    resetSourceInfo();
    if (null != _eventsIterator)
    {
      _eventsIterator.getEventBuffer().releaseIterator(_eventsIterator);
    }
    _eventsIterator = eventsIter;
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
      iter.remove();
      if (!iter.equivalent(_lastSuccessfulIterator))
      {
    	  if (_lastSuccessfulIterator == null)
    	  {
    		  LOG.warn("last Successful Iterator was null!");
    	  }
    	  else
    	  {
    		  LOG.warn("Invalidating last successful iterator! " + "last = " + _lastSuccessfulIterator +  " this iterator= " + iter);
   	         _lastSuccessfulIterator = null;

    	  }
      }

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
