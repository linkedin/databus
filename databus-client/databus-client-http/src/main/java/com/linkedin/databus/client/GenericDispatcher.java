/**
 *
 */
package com.linkedin.databus.client;

import com.linkedin.databus.core.DbusEventInternalReadable;
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
import com.linkedin.databus.core.DbusErrorEvent;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventV1;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DispatcherRetriesExhaustedException;
import com.linkedin.databus.core.async.AbstractActorMessageQueue;
import com.linkedin.databus.core.async.LifecycleMessage;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus2.core.mbean.DatabusReadOnlyStatus;
import com.linkedin.databus2.schemas.VersionedSchema;

/**
 * @author lgao
 *
 */
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
  private final MultiConsumerCallback<C> _asyncCallback;
  private final DispatcherState _internalState;
  private final DatabusReadOnlyStatus _statusMbean;
  private final MBeanServer _mbeanServer;
  private final DatabusSourcesConnection.StaticConfig _connConfig;
  private boolean _inInternalLoop;
  private DatabusHttpClientImpl _serverHandle = null;
  private Logger _log;
  private long _currentWindowSizeInBytes = 0;
  private long _numCheckPoints = 0;
  private RegistrationId _registrationId;

  public GenericDispatcher(String name,
                           DatabusSourcesConnection.StaticConfig connConfig,
                           List<DatabusSubscription> subsList,
                           CheckpointPersistenceProvider checkpointPersistor,
                           DbusEventBuffer dataEventsBuffer,
                           MultiConsumerCallback<C> asyncCallback
                           )
  {
    this(name,connConfig,subsList,checkpointPersistor,dataEventsBuffer,asyncCallback,null,null, null);
  }


  public GenericDispatcher(String name,
                           DatabusSourcesConnection.StaticConfig connConfig,
                           List<DatabusSubscription> subsList,
                           CheckpointPersistenceProvider checkpointPersistor,
                           DbusEventBuffer dataEventsBuffer,
                           MultiConsumerCallback<C> asyncCallback,
                           MBeanServer mbeanServer,
                           DatabusHttpClientImpl serverHandle,
                           RegistrationId registrationId)
  {
    super(name, connConfig.getDispatcherRetries());
    _log = Logger.getLogger(MODULE + "." + name);
    _subsList = subsList;
    _checkpointPersistor = checkpointPersistor;
    _dataEventsBuffer = dataEventsBuffer;
    _asyncCallback = asyncCallback;
    _internalState = DispatcherState.create();
    _inInternalLoop = false;
    _serverHandle = serverHandle;
    _mbeanServer = mbeanServer;
    _statusMbean = new DatabusReadOnlyStatus(getName(), getStatus(), -1);
    _statusMbean.registerAsMbean(_mbeanServer);
    _connConfig = connConfig;
    _currentWindowSizeInBytes = 0;
    _registrationId = registrationId;
  }

  public Logger getLog()
  {
    return _log;
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
          case START_DISPATCH_EVENTS:
              _log.info("starting dispatch");
              _internalState.switchToStartDispatchEventsInternal(newState, getName() + ".iter");
              doStartDispatchEvents(_internalState);
              break;
          default:
          {
            _log.error("Unknown dispatcher message: " + _internalState.getStateId());
            success = false;
            break;
          }
        }

        if (!_inInternalLoop)
        {
          _inInternalLoop = true;
          enqueueMessage(_internalState);
        }
      }
      else
      {
        switch (_internalState.getStateId())
        {
          case INITIAL: break;
          case CLOSED: doStopDispatch(_internalState); shutdown(); break;
          case STOP_DISPATCH_EVENTS: doStopDispatch(_internalState);  break;
          case START_DISPATCH_EVENTS: doStartDispatchEvents(_internalState); break;
          case EXPECT_EVENT_WINDOW:
          case REPLAY_DATA_EVENTS:
          case EXPECT_STREAM_DATA_EVENTS: doDispatchEvents(_internalState); break;
          default:
          {
            _log.error("Unkown internal: " + _internalState.getStateId());
            success = false;
            break;
          }
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

    enqueueMessage(curState);
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


  protected boolean doCheckStartSource(DispatcherState curState, Long eventSrcId)
  {
    boolean success = true;

    if (eventSrcId >= 0)
    {
      IdNamePair source = curState.getSources().get(eventSrcId);
      if (null == source)
      {
        _log.error("Unable to find source: id=" + eventSrcId);
        success = false;
      }
      else
      {
        VersionedSchema verSchema = curState.getSchemaSet().getLatestVersionByName(source.getName());
        if (null == verSchema)
        {
          _log.error("Unable to find schema: id=" + source.getId() + " name=" + source.getName());
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


    ConsumerCallbackResult callbackResult =
    		getAsyncCallback().onCheckpoint(winScn);
    success = ConsumerCallbackResult.isSuccess(callbackResult);
    if (success)
    {
      if (null != getCheckpointPersistor())
      {
    	getCheckpointPersistor().storeCheckpointV3(getSubsciptionsList(), cp, _registrationId);
        ++_numCheckPoints;
      }
      curState.switchToCheckpoint(cp, winScn);
      removeEvents(curState);
      if (debugEnabled) _log.debug("checkpoint saved: " + cp.toString());
    }
    else
    {
    	_log.warn("checkpoint not saved as callback returned false " + cp.toString());
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

  protected void doStartDispatchEvents(DispatcherState curState)
  {
    boolean debugEnabled = _log.isDebugEnabled();

    if (debugEnabled) _log.debug("Entered startDispatch");

    _asyncCallback.setSourceMap(curState.getSources());
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
      curState.switchToStopDispatch();
      doStopDispatch(curState);
    }
    else
    {
      doDispatchEvents(curState);
    }
  }

  protected void doDispatchEvents(DispatcherState curState)
  {
    boolean debugEnabled = _log.isDebugEnabled();
    boolean traceEnabled = _log.isTraceEnabled();

    //DbusEventIterator eventIter = curState.getEventsIterator();

    if (! _stopDispatch.get() && !curState.getEventsIterator().hasNext() && !checkForShutdownRequest())
    {
      if (debugEnabled) _log.debug("waiting for events");
      curState.getEventsIterator().await(50, TimeUnit.MILLISECONDS);
    }

    boolean success = true;
    boolean hasQueuedEvents = false;
    while (success && !_stopDispatch.get() && curState.getEventsIterator().hasNext() &&
        !checkForShutdownRequest())
    {
      //TODO MED: maybe add a counter limit the number of events processed in one pass
      //This will not delay the processing of other messages.
      DbusEventInternalReadable nextEvent = curState.getEventsIterator().next();
      //TODO: Add stats about last process time
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
          Checkpoint cp = createCheckpoint(curState, nextEvent);

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
              endWinScn = new SingleSourceSCN(nextEvent.physicalPartitionId(),
                                              nextEvent.sequence());
              curState.switchToEndStreamEventWindow(endWinScn);
              success = doEndStreamEventWindow(curState);
            }

            if (success)
            {
          	  try
              {
                doStoreCheckpoint(curState, nextEvent, cp, endWinScn);
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
            success = true;
            LOG.info("skipping empty window: " + nextEvent.sequence());
          }

          //for
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
          success = processErrorEvent(curState, nextEvent);
        }
        else
        {
          success = processSysEvent(curState, nextEvent);
        }
        if (success)
        {
        	//account for cases where control events are sent ; without windows being removed
            long maxWindowSizeInBytes = (long)(_dataEventsBuffer.getAllocatedSize() * (_connConfig.getCheckpointThresholdPct()) / 100.00);
            if (getCurrentWindowSizeInBytes() > maxWindowSizeInBytes)
            {
            	LOG.debug("Removing events from buffer; on receiving control events as it crosses threshold");
            	removeEvents(curState);
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
            success = doCheckStartSource(curState, eventSrcId);
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
            success = doCheckStartSource(curState, eventSrcId);
          }

        }

        if (success)
        {
          //finally: process data event
          success = processDataEvent(curState, nextEvent);
          if (success)
          {

        	hasQueuedEvents = true;
            long bufferFreeSpace = _dataEventsBuffer.getBufferFreeSpace();
            long maxWindowSizeInBytes = (long)(_dataEventsBuffer.getAllocatedSize() * (_connConfig.getCheckpointThresholdPct()) / 100.00);
            if (debugEnabled) _log.debug("Checking for max window size: currentSize=" + getCurrentWindowSizeInBytes() +
            		" maximum=" + maxWindowSizeInBytes + " bufferFreeSpace=" + bufferFreeSpace);

            if (getCurrentWindowSizeInBytes() > maxWindowSizeInBytes)
            {
              _log.info("Flushing events because of " + getCurrentWindowSizeInBytes() + " bytes reached without checkpoint ");

              success = processDataEventsBatch(curState);
              if (success)
              {
                hasQueuedEvents = false;
                 //need to checkpoint
                Checkpoint cp = createCheckpoint(curState, nextEvent);

                //Even if storeCheckpoint fails, we should continue (hoping for the best)
                boolean chkpt = false;
                try
                {
                  chkpt = doStoreCheckpoint(curState, nextEvent, cp, curState.getStartWinScn());
                }
                catch (SharedCheckpointException e)
                {
                  //shutdown
                  return;
                }

                if (!chkpt)
                {
                    //guarantee progress: risk being unable to rollback by removing events, but hope for the best
                    removeEvents(curState);
                }

                curState.switchToExpectStreamDataEvents();

                if (!getStatus().isRunningStatus()) getStatus().resume();
              }
            }
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
          if (success) hasQueuedEvents = false;
        }

        if (debugEnabled) _log.debug("doDispatchEvents to " + curState.toString() );
        enqueueMessage(curState); //loop around -- let any other messages be processed
      }
      else
      {
        curState.switchToRollback();
        doRollback(curState);
      }

    }

  }

  protected boolean doStoreCheckpoint(DispatcherState curState,
                                 DbusEvent nextEvent,
                                 Checkpoint cp,
                                 SCN endWinScn) throws SharedCheckpointException
  {
    boolean wroteCheckpoint = false;
    //store checkpoint
    try
    {
    	wroteCheckpoint = storeCheckpoint(curState, cp, endWinScn);
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
    //update timestamp DSC info here :
    if (wroteCheckpoint)
    {
    	updateDSCTimestamp(nextEvent.timestampInNanos()/(1000*1000));
    }

    return wroteCheckpoint;
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
      errorEvent = DbusEventV1.getErrorEventFromDbusEvent(nextEvent);

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

  public MultiConsumerCallback<C> getAsyncCallback()
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
  protected void doShutdown(LifecycleMessage lcMessage)
  {
    if (DispatcherState.StateId.CLOSED != _internalState.getStateId())
      _internalState.switchToClosed();
    super.doShutdown(lcMessage);
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

}
