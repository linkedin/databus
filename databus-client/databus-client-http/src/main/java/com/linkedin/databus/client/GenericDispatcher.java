/**
 *
 */
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


import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.management.MBeanServer;

import org.apache.log4j.Logger;

import com.linkedin.databus.client.DispatcherState.StateId;
import com.linkedin.databus.client.consumer.MultiConsumerCallback;
import com.linkedin.databus.client.pub.CheckpointPersistenceProvider;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.RegistrationId;
import com.linkedin.databus.client.pub.SCN;
import com.linkedin.databus.client.pub.SharedCheckpointPersistenceProvider;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DatabusComponentStatus;
import com.linkedin.databus.core.DbusClientMode;
import com.linkedin.databus.core.DbusErrorEvent;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventInternalReadable;
import com.linkedin.databus.core.DbusEventSerializable;
import com.linkedin.databus.core.DispatcherRetriesExhaustedException;
import com.linkedin.databus.core.async.AbstractActorMessageQueue;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus2.core.BackoffTimerStaticConfig;
import com.linkedin.databus2.core.mbean.DatabusReadOnlyStatus;
import com.linkedin.databus2.schemas.SchemaId;
import com.linkedin.databus2.schemas.VersionedSchema;


public abstract class GenericDispatcher<C> extends AbstractActorMessageQueue
{
  public static class SharedCheckpointException extends Exception
  {
    private static final long serialVersionUID = 1L;
    public SharedCheckpointException(Exception cause)
    {
      super(cause);
    }

  }

  public static final String MODULE = GenericDispatcher.class.getName();

  private final AtomicBoolean _stopDispatch = new AtomicBoolean(false);
  private final List<DatabusSubscription> _subsList;
  private final CheckpointPersistenceProvider _checkpointPersistor;
  private final DbusEventBuffer _dataEventsBuffer;
  private final MultiConsumerCallback _asyncCallback;
  private final DispatcherState _internalState;
  private final DatabusReadOnlyStatus _statusMbean;
  private final MBeanServer _mbeanServer;
  private final DatabusSourcesConnection.StaticConfig _connConfig;
  private boolean _inInternalLoop;
  private DatabusHttpClientImpl _serverHandle = null;
  private Logger _log;
  private long _currentWindowSizeInBytes = 0;
  private long _numCheckPoints = 0;
  //sequence num (event.sequence()) of last complete window seen
  protected long _lastWindowScn = -1;
  protected long _lastEowTsNsecs = -1;
  private RegistrationId _registrationId;
  protected boolean _schemaIdCheck=true;

  public GenericDispatcher(String name,
                           DatabusSourcesConnection.StaticConfig connConfig,
                           List<DatabusSubscription> subsList,
                           CheckpointPersistenceProvider checkpointPersistor,
                           DbusEventBuffer dataEventsBuffer,
                           MultiConsumerCallback asyncCallback
                           )
  {
    this(name,connConfig,subsList,checkpointPersistor,dataEventsBuffer,asyncCallback,null,null, null, connConfig.getDispatcherRetries());
  }


  public GenericDispatcher(String name,
                           DatabusSourcesConnection.StaticConfig connConfig,
                           List<DatabusSubscription> subsList,
                           CheckpointPersistenceProvider checkpointPersistor,
                           DbusEventBuffer dataEventsBuffer,
                           MultiConsumerCallback asyncCallback,
                           MBeanServer mbeanServer,
                           DatabusHttpClientImpl serverHandle,
                           RegistrationId registrationId,
                           BackoffTimerStaticConfig dispatcherRetries)
  {
    super(name, dispatcherRetries);
    _log = Logger.getLogger(MODULE + "." + name);
    _subsList = subsList;
    _checkpointPersistor = checkpointPersistor;
    _dataEventsBuffer = dataEventsBuffer;
    _asyncCallback = asyncCallback;
    _internalState = DispatcherState.create(dataEventsBuffer, getName() + ".iter");
    _inInternalLoop = false;
    _serverHandle = serverHandle;
    _mbeanServer = mbeanServer;
    _statusMbean = new DatabusReadOnlyStatus(getName(), getStatus(), -1);
    _statusMbean.registerAsMbean(_mbeanServer);
    _connConfig = connConfig;
    _currentWindowSizeInBytes = 0;
    _registrationId = registrationId;

    _internalState.switchToStartDispatchEvents();
    enqueueMessage(_internalState);
  }

  public Logger getLog()
  {
    return _log;
  }

  //disable schemaId checks for unit tests
  protected void setSchemaIdCheck(boolean schemaIdCheck)
  {
      _schemaIdCheck=schemaIdCheck;
  }

  @Override
  protected boolean executeAndChangeState(Object message)
  {
    boolean success = true;

    if (message instanceof DispatcherState)
    {
      DispatcherState newState = (DispatcherState)message;
      if (newState != _internalState)
      {
        switch (newState.getStateId())
        {
          case CLOSED: _internalState.switchToClosed(); shutdown(); break;
          case STOP_DISPATCH_EVENTS: _internalState.switchToStopDispatch();  break;
          default:
          {
            _log.error("Unknown dispatcher message: " + _internalState.getStateId());
            success = false;
            break;
          }
        }
      }
      else
      {
        _inInternalLoop = false;
        switch (_internalState.getStateId())
        {
          case INITIAL: break;
          case CLOSED: doStopDispatch(_internalState); shutdown(); break;
          case STOP_DISPATCH_EVENTS: doStopDispatch(_internalState);  break;
          case START_DISPATCH_EVENTS: doStartDispatchEvents(); break;
          case EXPECT_EVENT_WINDOW:
          case REPLAY_DATA_EVENTS:
          case EXPECT_STREAM_DATA_EVENTS: doDispatchEvents(); break;
          default:
          {
            _log.error("Unknown internal: " + _internalState.getStateId());
            success = false;
            break;
          }
        }
      }
    }
    else if (message instanceof SourcesMessage)
    {
      SourcesMessage srcMsg = (SourcesMessage)message;
      switch (srcMsg.getTypeId())
      {
      case SET_SOURCES_IDS: _internalState.addSources(srcMsg.getSources()); break;
      case SET_SOURCES_SCHEMAS: _internalState.addSchemas(srcMsg.getSourcesSchemas(),
          srcMsg.getMetadataSchemas()) ;
                              break;
      default:
      {
        _log.error("Unknown sources message type: " + srcMsg);
      }
      }
    }
    else
    {
      success = super.executeAndChangeState(message);
    }

    return success;
  }

  protected void doStopDispatch(DispatcherState curState)
  {
    boolean debugEnabled = _log.isDebugEnabled();
    if (debugEnabled) _log.debug("Entered stopConsumption");

    if (null != curState.getEventsIterator())
    {
      curState.getEventsIterator().close();
    }
    if (null != curState.getLastSuccessfulIterator())
    {
      curState.getLastSuccessfulIterator().close();
    }

    ConsumerCallbackResult stopSuccess = ConsumerCallbackResult.ERROR;
    try
    {
      stopSuccess = _asyncCallback.onStopConsumption();
    }
    catch (RuntimeException e)
    {
      _log.error("internal stopConsumption error: " + e.getMessage(), e);
    }
    if (ConsumerCallbackResult.SUCCESS == stopSuccess || ConsumerCallbackResult.CHECKPOINT == stopSuccess)
    {
      if (debugEnabled) _log.debug("stopConsumption succeeded.");
    }
    else
    {

      getStatus().suspendOnError(new RuntimeException("stop failed"));
      _log.error("stopConsumption failed.");
    }
    getStatus().suspendOnError(new RuntimeException("dispatched stopped"));
    _stopDispatch.set(true);
    if ((_serverHandle != null) && _serverHandle.isClusterEnabled())
    {
  	  _log.error("Suspend while in clusterMode: shutting down");
  	  _serverHandle.shutdownAsynchronously();
  	  return;
    }
  }

  protected void doRollback(DispatcherState curState)
  {
	  doRollback(curState, curState.getLastSuccessfulScn(), true, true);
  }

  protected void doRollback(DispatcherState curState, SCN rollbackScn, boolean checkRetries, boolean regressItr)
  {
    boolean success = true;

    boolean debugEnabled = _log.isDebugEnabled();

    if (!curState.getStateId().equals(StateId.ROLLBACK))
    {
      success = false;
      _log.error("ROLLBACK state expected but was :" + curState.getStateId());
    }

    int retriesLeft = Integer.MAX_VALUE;

    if (checkRetries)
    {
    	retriesLeft = getStatus().getRetriesLeft();
    	_log.info("rolling back; retries left: " + retriesLeft);
    	success = success && (retriesLeft > 0);

    	if (success)
    	{
    		if (0 == getStatus().getRetriesNum()) getStatus().retryOnError("rollback");
    		else getStatus().retryOnLastError();
    	}
    }

    if (success)
    {
      Checkpoint lastCp = curState.getLastSuccessfulCheckpoint();

      if ((null != lastCp) || (!regressItr))
      {
        ConsumerCallbackResult callbackResult = ConsumerCallbackResult.ERROR;
        try
        {
          _log.warn("Rollback to SCN : " + rollbackScn);
          callbackResult = getAsyncCallback().onRollback(rollbackScn);
        }
        catch (RuntimeException e)
        {
          _log.error("internal onRollback error: " + e.getMessage(), e);
        }

        success = ConsumerCallbackResult.isSuccess(callbackResult);
        if (success)
        {
          if (debugEnabled) _log.debug("rollback callback succeeded");
        }
        else
        {
          _log.error("rollback callback failed");
        }
      }

      if ( regressItr)
      {
    	  if (null != curState.getLastSuccessfulIterator())
    	  {
    		  _log.info("rollback to last successful iterator!" +
    		            curState.getLastSuccessfulIterator());
    		  _currentWindowSizeInBytes=0;
    		  curState.switchToReplayDataEvents();
    	  }
    	  else
    	  {
    		  _log.fatal("No iterator found to rollback,  last checkpoint found: \n" + lastCp);
    		  curState.switchToClosed();
    	  }
      }
    }
    else
    {
      DispatcherRetriesExhaustedException exp = new DispatcherRetriesExhaustedException();
      _log.info("Invoke onError callback as dispatcher retries have exhausted");
      getAsyncCallback().onError(exp);
      curState.switchToClosed();
    }

  }


  protected boolean doStartStreamEventWindow(DispatcherState curState)
  {
    boolean success = true;

    boolean debugEnabled = _log.isDebugEnabled();
    if (debugEnabled) _log.debug("Entered startDataEventSequence");


    if (!curState.getStateId().equals(StateId.START_STREAM_EVENT_WINDOW))
    {
      success = false;
      _log.error("START_STREAM_EVENT_WINDOW state expected");
    }
    else
    {
      ConsumerCallbackResult callbackResult = ConsumerCallbackResult.ERROR;
      try
      {
        callbackResult = getAsyncCallback().onStartDataEventSequence(curState.getStartWinScn());
      }
      catch (RuntimeException e)
      {
        _log.error("internal onStartDataEventSequence error: " + e.getMessage(), e);
      }
      success = ConsumerCallbackResult.isSuccess(callbackResult);
      if (success)
      {
        if (debugEnabled) _log.debug("startDataEventSequence succeeded:" + curState.getStartWinScn());
      }
      else
      {
        _log.error("startDataEventSequence failed:" + curState.getStartWinScn());
      }
    }

    return success;
  }

  protected boolean doEndStreamEventWindow(DispatcherState curState)
  {
    boolean success = true;

    boolean debugEnabled = _log.isDebugEnabled();
    if (debugEnabled) _log.debug("Entered endDataEventSequence");
    if (!curState.getStateId().equals(StateId.END_STREAM_EVENT_WINDOW))
    {
      success = false;
      _log.error("END_STREAM_EVENT_WINDOW state expected");
    }
    else
    {
      ConsumerCallbackResult callbackResult = ConsumerCallbackResult.ERROR;
      try
      {
        callbackResult = getAsyncCallback().onEndDataEventSequence(curState.getEndWinScn());
      }
      catch (RuntimeException e)
      {
        _log.error("internal onEndDataEventSequence error: " + e.getMessage(), e);
      }
      success = ConsumerCallbackResult.isSuccess(callbackResult);
      if (success)
      {
        if (debugEnabled) _log.debug("endDataEventSequence callback succeeded:" + curState.getEndWinScn());
      }
      else
      {
        _log.error("endDataEventSequence callback failed:" + curState.getEndWinScn());
      }
    }

    return success;
  }

  protected boolean doStartStreamSource(DispatcherState curState)
  {
    boolean success = true;

    boolean debugEnabled = _log.isDebugEnabled();
    if (debugEnabled) _log.debug("Entered startSource");

    if (!curState.getStateId().equals(StateId.START_STREAM_SOURCE))
    {
      success = false;
      _log.error("START_STREAM_SOURCE state expected");
    }
    else
    {
      String sourceName = curState.getCurrentSource().getName();
      ConsumerCallbackResult callbackResult = ConsumerCallbackResult.ERROR;
      try
      {
        callbackResult = getAsyncCallback().onStartSource(sourceName, curState.getCurrentSourceSchema());
      }
      catch (RuntimeException e)
      {
        _log.error("internal onStartSource error: " + e.getMessage(), e);
      }
      success = ConsumerCallbackResult.isSuccess(callbackResult);
      if (!success)
      {
        _log.error("startSource failed:" + sourceName);
      }
      else
      {
        if (debugEnabled) _log.debug("startSource succeeded: " + sourceName);
        curState.switchToExpectStreamDataEvents();
      }
    }

    return success;
  }

  protected boolean doEndStreamSource(DispatcherState curState)
  {
    boolean success = true;

    boolean debugEnabled = _log.isDebugEnabled();
    if (debugEnabled) _log.debug("Entered endSource");
    if (!curState.getStateId().equals(StateId.END_STREAM_SOURCE))
    {
      success = false;
      _log.error("END_STREAM_SOURCE state expected");
    }
    else if (null == curState.getCurrentSource())
    {
      success = false;
      _log.error("missing source");
    }
    else if (curState.getCurrentSource().getId() >= 0)
    {
      String sourceName = curState.getCurrentSource().getName();
      ConsumerCallbackResult callbackResult = ConsumerCallbackResult.ERROR;
      try
      {
        callbackResult = getAsyncCallback().onEndSource(sourceName, curState.getCurrentSourceSchema());
      }
      catch (RuntimeException e)
      {
        _log.error("internal onEndSource error:" + e.getMessage(), e);
      }
      success = ConsumerCallbackResult.isSuccess(callbackResult);
      if (!success)
      {
        _log.error("endSource() failed:" + sourceName);
      }
      else
      {
        if (debugEnabled) _log.debug("endSource succeeded:" + sourceName);
        curState.resetSourceInfo();
        curState.switchToExpectStreamDataEvents();
      }
    }

    return success;
  }


  protected boolean doCheckStartSource(DispatcherState curState, Long eventSrcId,SchemaId schemaId)
  {
    boolean success = true;

    if (eventSrcId >= 0)
    {
      IdNamePair source = curState.getSources().get(eventSrcId);
      if (null == source)
      {
        _log.error("Unable to find source: srcid=" + eventSrcId);
        success = false;
      }
      else
      {
        VersionedSchema verSchema = curState.getSchemaSet().getLatestVersionByName(source.getName());
        VersionedSchema exactSchema = _schemaIdCheck ? curState.getSchemaSet().getById(schemaId):null;
        if (null == verSchema)
        {
          _log.error("Unable to find schema: srcid=" + source.getId() + " name=" + source.getName());
          success = false;
        }
        else if (_schemaIdCheck && null==exactSchema)
        {
          _log.error("Unable to find schema: srcid=" + source.getId() + " name=" + source.getName() + " schemaId=" + schemaId);
          success = false;
        }
        else if (verSchema.getSchema() != curState.getCurrentSourceSchema())
        {
          curState.switchToStartStreamSource(source, verSchema.getSchema());
          success = doStartStreamSource(curState);
        }
      }
    }

    return success;
  }

  protected boolean storeCheckpoint(DispatcherState curState, Checkpoint cp, SCN winScn) throws IOException
  {
    boolean debugEnabled = _log.isDebugEnabled();

    if (debugEnabled) _log.debug("About to store checkpoint");

    boolean success = true;

    //processBatch - returns false ; then
    ConsumerCallbackResult callbackResult =
    		getAsyncCallback().onCheckpoint(winScn);
    boolean persistCheckpoint = !ConsumerCallbackResult.isSkipCheckpoint(callbackResult) && ConsumerCallbackResult.isSuccess(callbackResult);
    if (persistCheckpoint)
    {
      if (null != getCheckpointPersistor())
      {
    	getCheckpointPersistor().storeCheckpointV3(getSubsciptionsList(), cp, _registrationId);
        ++_numCheckPoints;
      }
      curState.storeCheckpoint(cp, winScn);
      removeEvents(curState);
      if (debugEnabled) _log.debug("checkpoint saved: " + cp.toString());
    }
    else
    {
      if (debugEnabled)
    	_log.debug("checkpoint " + cp + " not saved as callback returned " + callbackResult);
    }
    return success;
  }

  protected boolean processSysEvent(DispatcherState curState, DbusEvent event)
  {
    _log.warn("Unknown system event: srcid=" + event.srcId());
    return true;
  }

  protected boolean processDataEvent(DispatcherState curState, DbusEvent event)
  {
    ConsumerCallbackResult callbackResult =
        getAsyncCallback().onDataEvent(event, curState.getEventDecoder());
    boolean success = ConsumerCallbackResult.isSuccess(callbackResult);

    if (!success)
    {
      _log.error("dataEvent failed.");
    }
    else
    {
      _log.debug("Event queued: " + event.toString());
    }

    return success;
  }

  protected boolean processDataEventsBatch(DispatcherState curState)
  {
    _log.debug("Flushing batched events");

    ConsumerCallbackResult callbackResult = getAsyncCallback().flushCallQueue(-1);
    boolean success = ConsumerCallbackResult.isSuccess(callbackResult);
    if (! success) _log.error("Error dispatching events");

    return success;
  }

  protected void updateDSCTimestamp(long timestampInMillis)
  {
	  if ((_serverHandle != null) && (_serverHandle.getDSCUpdater() != null))
      {
    	  DatabusClientDSCUpdater updater = _serverHandle.getDSCUpdater();
    	  updater.writeTimestamp(timestampInMillis);
      }
  }

  protected void doStartDispatchEvents()
  {
    boolean debugEnabled = _log.isDebugEnabled();

    if (debugEnabled) _log.debug("Entered startDispatch");

    _asyncCallback.setSourceMap(_internalState.getSources());
    getStatus().start();

    ConsumerCallbackResult callbackResult = ConsumerCallbackResult.ERROR;
    try
    {
      callbackResult = _asyncCallback.onStartConsumption();
    }
    catch (RuntimeException e)
    {
      _log.error("internal startConsumption error: " + e.getMessage(), e);
    }
    Boolean callSuccess = ConsumerCallbackResult.isSuccess(callbackResult);
    if (!callSuccess)
    {
      getStatus().suspendOnError(new RuntimeException("start failed"));
      _log.error("StartConsumption failed.");
      _internalState.switchToStopDispatch();
      doStopDispatch(_internalState);
    }
    else
    {
      _stopDispatch.set(false);
      _internalState.switchToExpectEventWindow();
      doDispatchEvents();
    }
  }

  boolean hasCheckpointThresholdBeenExceeded()
  {
      //account for cases where control events are sent ; without windows being removed
      long maxWindowSizeInBytes = (long)(_dataEventsBuffer.getAllocatedSize() * (_connConfig.getCheckpointThresholdPct()) / 100.00);
      boolean exceeded = getCurrentWindowSizeInBytes() > maxWindowSizeInBytes;
      if (_log.isDebugEnabled())
      {
          _log.debug("Threshold check : CurrentWindowSize=" + getCurrentWindowSizeInBytes() +
                  " MaxWindowSize=" + maxWindowSizeInBytes + " bufferFreeSpace=" + _dataEventsBuffer.getBufferFreeSpace());
      }
      if (exceeded)
      {
          _log.info("Exceeded threshold check: CurrentWindowSize=" + getCurrentWindowSizeInBytes() +
              " MaxWindowSize=" + maxWindowSizeInBytes + " bufferFreeSpace=" + _dataEventsBuffer.getBufferFreeSpace());
      }
      return exceeded;
  }

  protected void doDispatchEvents()
  {
    boolean debugEnabled = _log.isDebugEnabled();
    boolean traceEnabled = _log.isTraceEnabled();

    //need to remove eventually but for now I want to avoid a nasty diff
    final DispatcherState curState = _internalState;

    //DbusEventIterator eventIter = curState.getEventsIterator();

    if (! _stopDispatch.get() && !curState.getEventsIterator().hasNext() && !checkForShutdownRequest())
    {
      if (debugEnabled) _log.debug("waiting for events");
      curState.getEventsIterator().await(50, TimeUnit.MILLISECONDS);
    }
    boolean success = true;
    boolean hasQueuedEvents = false;
    while (success && !_stopDispatch.get() &&
        curState.getStateId() != DispatcherState.StateId.STOP_DISPATCH_EVENTS &&
        null != curState.getEventsIterator() &&
        curState.getEventsIterator().hasNext() &&
        !checkForShutdownRequest() &&
        //exit the event processing loop if there are other queued notifications
        !hasMessages())
    {
      DbusEventInternalReadable nextEvent = curState.getEventsIterator().next();
      _currentWindowSizeInBytes += nextEvent.size();
      if (traceEnabled) _log.trace("got event:" + nextEvent);
      Long eventSrcId = (long)nextEvent.srcId();
      if (curState.isSCNRegress())
      {
    	  SingleSourceSCN scn = new SingleSourceSCN(nextEvent.physicalPartitionId(),
    	                                            nextEvent.sequence());
    	  _log.warn("Regress to SCN :" + scn);
    	  curState.switchToRollback();
    	  doRollback(curState, scn, false, false);
    	  curState.setSCNRegress(false);
    	  curState.switchToExpectEventWindow();
      }

      if (null != getAsyncCallback().getStats())
        getAsyncCallback().getStats().registerWindowSeen(nextEvent.timestampInNanos(),
                                                         nextEvent.sequence());

      if (nextEvent.isControlMessage())
      {
    	//control event
        if (nextEvent.isEndOfPeriodMarker())
        {
          if (curState.isEventsSeen())
          {
            if (null != curState.getCurrentSource())
            {
              curState.switchToEndStreamSource();
              success = doEndStreamSource(curState);
            }

            SCN endWinScn = null;
            if (success)
            {
              _lastWindowScn = nextEvent.sequence();
              _lastEowTsNsecs = nextEvent.timestampInNanos();
              endWinScn = new SingleSourceSCN(nextEvent.physicalPartitionId(),
                                              _lastWindowScn);
              curState.switchToEndStreamEventWindow(endWinScn);
              success = doEndStreamEventWindow(curState);
            }
            if (success)
            {
          	  try
              {
          	      //end of period event
                  Checkpoint cp = createCheckpoint(curState, nextEvent);
          	      success = doStoreCheckpoint(curState, nextEvent, cp, endWinScn);
              }
              catch (SharedCheckpointException e)
              {
                //shutdown
                return;
              }
            }
          }
          else
          {
            //empty window
            success = true;
            if (LOG.isDebugEnabled())
            {
              LOG.debug("skipping empty window: " + nextEvent.sequence());
            }

            //write a checkpoint; takes care of slow sources ; but skip storing the first control eop with 0 scn
            if (nextEvent.sequence() > 0)
            {
                _lastWindowScn = nextEvent.sequence();
                //the first window (startEvents()) can have a eop whose sequence() is non-zero but timestamp 0 e.g. in chained relay .
                //The reason is that the eop's timestamp is the max timestamp of all data events seen so far.
                if (nextEvent.timestampInNanos() > 0)
                {
                  _lastEowTsNsecs = nextEvent.timestampInNanos();
                }
                Checkpoint ckpt = createCheckpoint(curState, nextEvent);
                try
                {
                     success = doStoreCheckpoint(curState, nextEvent, ckpt,
                            new SingleSourceSCN(nextEvent.physicalPartitionId(),nextEvent.sequence()));
                }
                catch (SharedCheckpointException e)
                {
                    //shutdown
                    return;
                }
            }
            else
            {
                LOG.warn("EOP with scn=" + nextEvent.sequence());
            }
          }
          if (success)
          {
            curState.switchToExpectEventWindow();
            //we have recovered from the error  and it's not the dummy window
            if (nextEvent.sequence() > 0)
            {
            	if (! getStatus().isRunningStatus()) getStatus().resume();
            }
          }
        }
        else if (nextEvent.isErrorEvent())
        {
          LOG.info("Error event: " + nextEvent.sequence());
          success = processErrorEvent(curState, nextEvent);
        }
        else
        {
          //control event
          success = processSysEvent(curState, nextEvent);
          if (success)
          {

              if (nextEvent.isCheckpointMessage())
              {
                  Checkpoint sysCheckpt = createCheckpoint(curState, nextEvent);
                  try
                  {
                      long scn = sysCheckpt.getConsumptionMode() == DbusClientMode.ONLINE_CONSUMPTION ?
                              nextEvent.sequence() : sysCheckpt.getBootstrapSinceScn();
                      //ensure that control event with 0 scn doesn't get saved unless it is during snapshot of bootstrap
                      if (scn > 0 || sysCheckpt.getConsumptionMode()==DbusClientMode.BOOTSTRAP_SNAPSHOT)
                      {
                          success = doStoreCheckpoint(curState, nextEvent, sysCheckpt,
                                  new SingleSourceSCN(nextEvent.physicalPartitionId(),scn));
                      }
                  }
                  catch (SharedCheckpointException e)
                  {
                      //shutdown
                      return;
                  }
              }
          }
        }
      }
      else
      {
        curState.setEventsSeen(true);

    	//not a control event
        if (curState.getStateId().equals(StateId.EXPECT_EVENT_WINDOW) ||
            curState.getStateId().equals(StateId.REPLAY_DATA_EVENTS))
        {
          SCN startScn = new SingleSourceSCN(nextEvent.physicalPartitionId(),
                                             nextEvent.sequence());
          curState.switchToStartStreamEventWindow(startScn);
          success = doStartStreamEventWindow(curState);

          if (success && (eventSrcId.longValue() >= 0))
          {
            success = doCheckStartSource(curState, eventSrcId,new SchemaId(nextEvent.schemaId()));
          }
        }
        else
        {
          if (null != curState.getCurrentSource() &&
              !eventSrcId.equals(curState.getCurrentSource().getId()))
          {
            curState.switchToEndStreamSource();
            success = doEndStreamSource(curState);
          }

          if (success)
          {
            //Check if schemas of the source exist.
            //Also check if the exact schema id present in event exists in the client. This is worthwhile if there's a
            //guarantee that the entire window is written with the same schemaId, which is the case if the relay does not use a new schema
            //mid-window
            success = doCheckStartSource(curState, eventSrcId,new SchemaId(nextEvent.schemaId()));
          }

        }

        if (success)
        {
          //finally: process data event
          success = processDataEvent(curState, nextEvent);
          if (success)
          {

        	hasQueuedEvents = true;
            if (hasCheckpointThresholdBeenExceeded())
            {
              _log.info("Flushing events because of " + getCurrentWindowSizeInBytes() + " bytes reached without checkpoint ");
              success = processDataEventsBatch(curState);
              if (success)
              {
                  hasQueuedEvents = false;
                  //checkpoint: for bootstrap it's the right checkpoint; that has been lazily created by a checkpoint event
                  // checkpoint: for relay: create a checkpoint that has the prevScn
                  Checkpoint cp = createCheckpoint(curState, nextEvent);
                  // DDSDBUS-1889 : scn for bootstrap is bootstrapSinceSCN
                  // scn for online consumption is : currentWindow
                  SCN lastScn = cp.getConsumptionMode() == DbusClientMode.ONLINE_CONSUMPTION ?
                          curState.getStartWinScn()
                          : new SingleSourceSCN(
                                  nextEvent.physicalPartitionId(),
                                  cp.getBootstrapSinceScn());
                  try
                  {
                      // Even if storeCheckpoint fails, we
                      // should continue (hoping for the best)
                      success = doStoreCheckpoint(curState,
                              nextEvent, cp, lastScn);
                  }
                  catch (SharedCheckpointException e)
                  {
                      // shutdown
                      return;
                  }
                  curState.switchToExpectStreamDataEvents();
                  if (!getStatus().isRunningStatus()) getStatus().resume();
              }
            }
          }
        }
      }
      if (success)
      {
          // check if threshold has been exceeded for control events;
          // DDSDBUS-1776
          // this condition will take care of cases where checkpoint
          // persistence failed or onCheckpoint returned false
          // and the buffer was still left with events, at this juncture
          // we clear the buffer to make progress at the risk
          // of not being able to rollback should an error be encountered
          // before next successful checkpoint
          if (hasCheckpointThresholdBeenExceeded())
          {
              //drain events just in case it hasn't been drained before; mainly control events that are not checkpoint events
              success = processDataEventsBatch(curState);
              if (success)
              {
                  _log.warn("Checkpoint not saved, but removing events to guarantee progress. Triggered on control-event=" + nextEvent.isControlMessage());
                  // guarantee progress: risk being unable to rollback by
                  // removing events, but hope for the best
                  removeEvents(curState);
              }
          }
      }

    }


    if (!_stopDispatch.get() && !checkForShutdownRequest())
    {
      if (success)
      {
          if (hasQueuedEvents)
          {
              success = processDataEventsBatch(curState);
              if (!success)
              {
                _log.error("unable to flush partial window");
              }
          }
          if (debugEnabled) _log.debug("doDispatchEvents to " + curState.toString() );
      }

      if (!success)
      {
        curState.switchToRollback();
        doRollback(curState);
      }

      enqueueMessage(curState); //loop around -- let any other messages be processed
    }

  }

  protected boolean doStoreCheckpoint(DispatcherState curState,
                                 DbusEvent nextEvent,
                                 Checkpoint cp,
                                 SCN endWinScn) throws SharedCheckpointException
  {
    //drain all the callbacks ; if there are errors; then return false; prepare to rollback
    boolean success = processDataEventsBatch(curState);
    if (!success)
    {
        LOG.error("Outstanding callbacks failed. This event: checkpoint= " + nextEvent.isCheckpointMessage()
            + " eop=" + nextEvent.isEndOfPeriodMarker() );
    }
    else
    {
        try
        {
            //try to store checkpoint; if this doesn't succeed then still return true
            storeCheckpoint(curState, cp, endWinScn);
        }
        catch (IOException e)
        {
            _log.error("checkpoint persisting failed: " + cp);
            if (isSharedCheckpoint())
            {
                handleErrStoringSharedCheckpoint();
                throw new SharedCheckpointException(e);
            }
        }
    }
    return success;
  }

  protected boolean isSharedCheckpoint()
  {
      return getCheckpointPersistor() instanceof SharedCheckpointPersistenceProvider;
  }

  protected void handleErrStoringSharedCheckpoint()
  {
      if (_serverHandle != null)
      {
          _log.info("Server should be shutdown! \n");
          //asynch shutdown; server is waiting on condition signalled by shutdown
          _serverHandle.shutdownAsynchronously();
      }
  }

  private boolean processErrorEvent(DispatcherState curState, DbusEventInternalReadable nextEvent)
  {
    boolean success = false;
    DbusErrorEvent errorEvent = null;

    if (nextEvent.isErrorEvent())
    {
      errorEvent = DbusEventSerializable.getErrorEventFromDbusEvent(nextEvent);

      if (null == errorEvent)
      {
        _log.error("Null error event received at dispatcher");
      }
      else
      {
        _log.info("Delivering error event to consumers: " + errorEvent);
        ConsumerCallbackResult callbackResult = ConsumerCallbackResult.ERROR;
        try
        {
          callbackResult = _asyncCallback.onError(errorEvent.returnActualException());
        }
        catch (RuntimeException e)
        {
          _log.error("internal onError error: " + e.getMessage(), e);
        }
        success = ConsumerCallbackResult.isSuccess(callbackResult);
      }
    }
    else
    {
      _log.error("Unexcpected event received while DbusErrorEvent is expected! " + nextEvent);
    }

    return success;
  }

  public List<DatabusSubscription> getSubsciptionsList()
  {
    return _subsList;
  }

  public CheckpointPersistenceProvider getCheckpointPersistor()
  {
    return _checkpointPersistor;
  }

  public MultiConsumerCallback getAsyncCallback()
  {
    return _asyncCallback;
  }

  public DatabusComponentStatus getStatus()
  {
    return getComponentStatus();
  }

  public long getCurrentWindowSizeInBytes() {
	  return _currentWindowSizeInBytes;
  }


  public void setCurrentWindowSizeInBytes(long currentWindowSizeInBytes) {
	  _currentWindowSizeInBytes = currentWindowSizeInBytes;
  }


  long getNumCheckPoints()
  {
	  return _numCheckPoints;
  }


  public void setNumCheckPoints(long numCheckPoints)
  {
	  _numCheckPoints = numCheckPoints;
  }


  protected abstract Checkpoint createCheckpoint(DispatcherState curState, DbusEvent event);

  public static Checkpoint createOnlineConsumptionCheckpoint(long lastCompleteWindowScn, long lastEowTsNsecs, DispatcherState curState, DbusEvent event)
  {
      //TODO: What does this mean? "For online consumption ; this means that a complete event window hasn't been read yet."
      //So until we have support from RelayPuller resuming from mid-window ; there is no point in trying to save  a parital window
      long windowScn = lastCompleteWindowScn;
      if (windowScn < 0)
      {
          if (event.isCheckpointMessage())
          {
              //control event; then safe to set to sequence; useful when relayPuller writes checkpoint to buffer to
              //be passed on to bootstrapPuller
              windowScn = event.sequence();
            // TODO: According to DbusEventFactory.createCheckpointEvent, event,sequence() is always 0!
            // Is this even executed? If we send a checkpoint event from the relay, we could be screwed!
          }
          else
          {
              //there's no sufficient data: not a single window has been processed.
              windowScn = event.sequence() > 0 ? event.sequence()-1 : 0;
              // TODO Can't do this math for timestamp. See DDSDBUS-3149
          }
      }
      return Checkpoint.createOnlineConsumptionCheckpoint(windowScn, lastEowTsNsecs);
  }


  protected void removeEvents(DispatcherState state)
  {
    boolean isDebugEnabled = _log.isDebugEnabled();


    if (isDebugEnabled)
    {
      _log.debug("removing events after checkpoint");
      _log.debug("buffer space available before remove: " +
                state.getEventsIterator().getEventBuffer().getBufferFreeSpace());
    }

    state.removeEvents();
    _currentWindowSizeInBytes=0;

    if (isDebugEnabled)
    {
      _log.debug("buffer space available after remove: " +
                state.getEventsIterator().getEventBuffer().getBufferFreeSpace());
    }
  }

  @Override
  public void shutdown()
  {
     if (_statusMbean != null)
	 _statusMbean.unregisterMbean(_mbeanServer);
     super.shutdown();
  }

  @Override
  protected void onShutdown()
  {
    if (DispatcherState.StateId.CLOSED != _internalState.getStateId())
      _internalState.switchToClosed();
  }


  @Override
  public void getQueueListString(StringBuilder sb)
  {
    super.getQueueListString(sb);
    sb.append('+');
    if (null != _internalState) sb.append(_internalState.toString());
  }

  public DispatcherState getDispatcherState()
  {
	  return _internalState;
  }


  /**
   * @see com.linkedin.databus.core.async.AbstractActorMessageQueue#enqueueMessage(java.lang.Object)
   */
  @Override
  public void enqueueMessage(Object message)
  {
    if (message == _internalState)
    {
      if (_inInternalLoop)
      {
        throw new RuntimeException("loop already scheduled; queued messages: " +
                                   getQueueListString() + "; new message: " + message);
      }
      else
      {
        _inInternalLoop = true;
      }
    }
    super.enqueueMessage(message);
  }
}
