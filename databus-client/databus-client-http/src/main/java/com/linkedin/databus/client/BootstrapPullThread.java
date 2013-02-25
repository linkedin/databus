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


import com.linkedin.databus.core.DbusEventInternalWritable;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import javax.management.MBeanServer;

import org.apache.log4j.Logger;

import com.linkedin.databus.client.ConnectionState.StateId;
import com.linkedin.databus.client.netty.RemoteExceptionHandler;
import com.linkedin.databus.client.pub.ServerInfo;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DatabusComponentStatus;
import com.linkedin.databus.core.DbusClientMode;
import com.linkedin.databus.core.DbusConstants;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventV1;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.InvalidEventException;
import com.linkedin.databus.core.async.LifecycleMessage;
import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus2.core.BackoffTimer;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.request.BootstrapDatabaseTooOldException;
import com.linkedin.databus2.core.filter.DbusKeyCompositeFilter;
import com.linkedin.databus2.core.filter.DbusKeyCompositeFilterConfig;
import com.linkedin.databus2.core.filter.DbusKeyFilter;
import com.linkedin.databus2.core.filter.KeyFilterConfigHolder;

public class BootstrapPullThread extends BasePullThread
{
  public static final String MODULE = BootstrapPullThread.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  public static final Short START_OF_SNAPSHOT_SRCID = (short)(DbusEvent.PRIVATE_RANGE_MAX_SRCID - 1);
  public static final Short START_OF_CATCHUP_SRCID = (short)(DbusEvent.PRIVATE_RANGE_MAX_SRCID - 2);
  public static final Short END_OF_BOOTSTRAP_SRCID = (short)(DbusEvent.PRIVATE_RANGE_MAX_SRCID - 3);


  private Checkpoint _resumeCkpt;

  //private long _errorSleepMs = 0;

  private final RemoteExceptionHandler _remoteExceptionHandler;
  private  DbusKeyCompositeFilter _bootstrapFilter;
  private final List<DbusKeyCompositeFilterConfig> _bootstrapFilterConfigs;

  // track number of events read during the current bootstrap phase
  private long numEventsInCurrentState = 0;
  private final double _pullerBufferUtilizationPct;

  // keep track of the last open bootstrap connection so we can close it on shutdown
  private DatabusBootstrapConnection _lastOpenConnection;

  private final BackoffTimer _retriesBeforeCkptCleanup;

  public BootstrapPullThread(String name,
                             DatabusSourcesConnection sourcesConn,
                             DbusEventBuffer bootstrapEventsBuffer,
                             Set<ServerInfo> bootstrapServers,
                             List<DbusKeyCompositeFilterConfig> bootstrapFilterConfigs,
                             double pullerBufferUtilPct,
                             MBeanServer mbeanServer)
  {
    super(name, sourcesConn, bootstrapEventsBuffer,bootstrapServers,mbeanServer);

    _retriesBeforeCkptCleanup = new BackoffTimer("BSPullerRetriesBeforeCkptCleanup",
										 sourcesConn.getConnectionConfig().getBsPullerRetriesBeforeCkptCleanup());
    _bootstrapFilterConfigs = bootstrapFilterConfigs;
    _remoteExceptionHandler = new RemoteExceptionHandler(sourcesConn, bootstrapEventsBuffer);
    _pullerBufferUtilizationPct = pullerBufferUtilPct;

    // TODO (DDSDBUS-84): if resumeCkpt is not empty, i.e. we are starting fresh, make sure the
    // sources passed in are exactly the same as what's stored in the checkpoint -
    // the order has to be the same as well. If not the same, we have to start fresh.
    // if (!matchSources(resumeCkpt.getAllBootstrapSources(), sources))
    //{
    //  _resumeCkpt = new Checkpoint();
    //  _snapshotSource = ...
    //  _catchupSource = ...
    //  ...
    // }
  }

  @Override
  public void shutdown()
  {
    _log.info("shutting down");
    if (null != _lastOpenConnection)
    {
      _log.info("closing open connection");
      _lastOpenConnection.close();
      _lastOpenConnection = null;
    }

    super.shutdown();
    _log.info("shut down comple");
  }

  @Override
  protected boolean shouldDelayTearConnection(StateId stateId)
  {
	  boolean delayTear = false;
	  switch(stateId)
	  {
	  	case START_SCN_REQUEST_SENT:
	  	case START_SCN_RESPONSE_SUCCESS:
	  	case START_SCN_REQUEST_ERROR:
	  	case START_SCN_RESPONSE_ERROR:
	  	case TARGET_SCN_REQUEST_SENT :
	  	case TARGET_SCN_RESPONSE_SUCCESS:
	  	case TARGET_SCN_REQUEST_ERROR:
	  	case TARGET_SCN_RESPONSE_ERROR:
	  	case STREAM_REQUEST_SENT:
	  	case STREAM_REQUEST_SUCCESS:
	  	case STREAM_REQUEST_ERROR:
	  	case STREAM_RESPONSE_ERROR:
	  	case BOOTSTRAP_DONE:
	  			delayTear = true;
	  			break;
	  }

	  return delayTear;
  }


  @Override
  protected boolean executeAndChangeState(Object message)
  {
    boolean success = true;

    if (message instanceof ConnectionStateMessage)
    {
      if (_componentStatus.getStatus() != DatabusComponentStatus.Status.RUNNING)
      {
        _log.warn("not running: " + message.toString());
      }
      else
      {
      	ConnectionStateMessage stateMsg = (ConnectionStateMessage)message;
        ConnectionState currentState = stateMsg.getConnState();

        switch (stateMsg.getStateId())
        {
          case INITIAL: break;
          case CLOSED: shutdown(); break;
          case BOOTSTRAP:
          case PICK_SERVER: doPickBootstrapServer(currentState); break;
          case REQUEST_START_SCN: doRequestStartScn(currentState); break;
          case START_SCN_RESPONSE_SUCCESS: doStartScnResponseSuccess(currentState); break;
          case REQUEST_TARGET_SCN: doRequestTargetScn(currentState); break;
          case TARGET_SCN_RESPONSE_SUCCESS: doTargetScnResponseSuccess(currentState); break;
          // no need to distinguish snapshot and catchup because ckpt has it already
          case REQUEST_STREAM: doRequestBootstrapStream(currentState); break;
          case STREAM_REQUEST_SUCCESS: doReadBootstrapEvents(currentState); break;
          case STREAM_RESPONSE_DONE: doStreamResponseDone(currentState); break;
          case STREAM_REQUEST_ERROR: processStreamRequestError(currentState); break;
          case STREAM_RESPONSE_ERROR: processStreamResponseError(currentState); break;
          case START_SCN_REQUEST_ERROR: processStartScnRequestError(currentState); break;
          case START_SCN_RESPONSE_ERROR: processStartScnResponseError(currentState); break;
          case TARGET_SCN_REQUEST_ERROR: processTargetScnRequestError(currentState); break;
          case TARGET_SCN_RESPONSE_ERROR: processTargetScnResponseError(currentState); break;
          default:
          {
            _log.error("Unkown state in BootstrapPullThread: " + currentState.getStateId());
            success = false;
            break;
          }
        }
      }
    }
    else if (message instanceof CheckpointMessage)
    {
      CheckpointMessage cpMessage = (CheckpointMessage)message;

      switch (cpMessage.getTypeId())
      {
        case SET_CHECKPOINT: doSetResumeCheckpoint(cpMessage); break;
        default:
        {
          _log.error("Unkown CheckpointMessage in BootstrapPullThread: " + cpMessage.getTypeId());
          success = false;
          break;
        }
      }
    }
    else if (message instanceof SourcesMessage)
    {
      SourcesMessage sourcesMessage = (SourcesMessage)message;

      switch (sourcesMessage.getTypeId())
      {
        case SET_SOURCES_IDS: doSetSourcesIds(sourcesMessage); break;
        case SET_SOURCES_SCHEMAS: doSetSourcesSchemas(sourcesMessage); break;
        default:
        {
          _log.error("Unkown CheckpointMessage in BootstrapPullThread: " + sourcesMessage.getTypeId());
          success = false;
          break;
        }
      }
    }
    else
    {
      success = super.executeAndChangeState(message);
    }

    return success;
  }

  private void doSetSourcesSchemas(SourcesMessage sourcesMessage)
  {
    _currentState.setSourcesSchemas(sourcesMessage.getSourcesSchemas());

    _sourcesConn.getBootstrapDispatcher().enqueueMessage(
                               DispatcherState.create().switchToStartDispatchEvents(
                                   _currentState.getSourceIdMap(),
                                   _currentState.getSourcesSchemas(),
                                   _currentState.getDataEventsBuffer()));
  }

  private void doSetSourcesIds(SourcesMessage sourcesMessage)
  {
    _currentState.setSourcesIds(sourcesMessage.getSources());
    _currentState.setSourcesIdListString(sourcesMessage.getSourcesIdListString());
  }

  private void doSetResumeCheckpoint(CheckpointMessage cpMessage)
  {
    _resumeCkpt = cpMessage.getCheckpoint();

    _log.info("resume checkpoint: " + _resumeCkpt.toString());
  }

  @Override
  protected void doStart(LifecycleMessage lcMessage)
  {
    /*if (_currentState.getStateId() != ConnectionState.StateId.INITIAL)
    {
      return;
    }*/

    super.doStart(lcMessage);

    _currentState.clearBootstrapState();

    _currentState.switchToPickServer();
    enqueueMessage(_currentState);
  }


  @Override
  protected void onResume()
  {
    _currentState.switchToPickServer();
    enqueueMessage(_currentState);
  }

  protected void doPickBootstrapServer(ConnectionState curState)
  {

    int serversNum = _servers.size();

    if (0 == serversNum)
    {
      //enqueueMessage(LifecycleMessage.createSuspendOnErroMessage(
      //    new DatabusException("No bootstrap services specified")));
      _sourcesConn.getConnectionStatus().suspendOnError(new DatabusException("No bootstrap services specified"));
      return;
    }

    if (null == _resumeCkpt)
    {
      _sourcesConn.getConnectionStatus().suspendOnError(new DatabusException("Bootstrapping checkpoint is not set!"));
      return;
    }

    boolean restartBootstrap = false;
    String bsServerInfo = _resumeCkpt.getBootstrapServerInfo();
    ServerInfo lastReadBS = null;
    if ( null != bsServerInfo)
    {
    	try
    	{
    		lastReadBS = ServerInfo.buildServerInfoFromHostPort(bsServerInfo, DbusConstants.HOSTPORT_DELIMITER);
    	} catch(Exception ex) {
    		_log.error("Unable to fetch bootstrap serverInfo from checkpoint, ServerInfo :" + bsServerInfo, ex);
    	}
    }

    if ( null == lastReadBS)
    	restartBootstrap = true;

    int retriesLeft = 0;
    DatabusBootstrapConnection bootstrapConn = null;
    ServerInfo serverInfo = lastReadBS;
    if ( !restartBootstrap )
    {
        while (null == bootstrapConn && (retriesLeft = _retriesBeforeCkptCleanup.getRemainingRetriesNum()) >= 0
               && !checkForShutdownRequest())
        {
        	if (lastReadBS.equals(_curServer) )
        		_retriesBeforeCkptCleanup.backoffAndSleep();

        	_log.info("Retry picking last used bootstrap server :" + serverInfo + "; retries left:" + retriesLeft);

        	try
            {
              bootstrapConn = _sourcesConn.getBootstrapConnFactory().createConnection(serverInfo, this, _remoteExceptionHandler);
              _log.info("picked last used bootstrap server:" + serverInfo);
            }
            catch (Exception e)
            {
              _log.error("Unable to get connection to bootstrap server:" + serverInfo, e);
            }
        }

        if ((null == bootstrapConn) && (_retriesBeforeCkptCleanup.getRemainingRetriesNum() < 0))
        {
        	_log.info("Exhausted retrying the same bootstrap server :" + lastReadBS);
        }
    }

    if(checkForShutdownRequest()) return;

    Random rng = new Random();

    if ( null == bootstrapConn)
    {
    	_log.info("Restarting bootstrap as client might be getting bootstrap data from different server instance !!");
    	_log.info("Old Checkpoint :" + _resumeCkpt);
    	_resumeCkpt.resetBSServerGeneratedState();
    	_log.info("New Checkpoint :" + _resumeCkpt);
    	_retriesBeforeCkptCleanup.reset();
    }  else {
        _curServer = serverInfo;
    }

    while ((null == bootstrapConn) && (retriesLeft = _status.getRetriesLeft()) >= 0 &&
           !checkForShutdownRequest())
    {
      _log.info("picking a bootstrap server; retries left:" + retriesLeft);

      backoffOnPullError();

      _curServerIdx =  (_curServerIdx < 0) ? rng.nextInt(serversNum)
                                           : (_curServerIdx + 1) % serversNum;

      Iterator<ServerInfo> setIter = _servers.iterator();
      for (int i = 0; i <= _curServerIdx; ++i) serverInfo = setIter.next();

      _curServer = serverInfo;

      try
      {
        bootstrapConn = _sourcesConn.getBootstrapConnFactory().createConnection(serverInfo, this, _remoteExceptionHandler);
        _log.info("picked a bootstrap server:" + serverInfo);
      }
      catch (Exception e)
      {
        _log.error("Unable to get connection to bootstrap server:" + serverInfo, e);
      }
    }

    /*
     * Close the old bootstrap COnnection
     */
     DatabusBootstrapConnection oldBootstrapConn = curState.getBootstrapConnection();

     if ( null != oldBootstrapConn)
         resetConnectionAndSetFlag();
     _lastOpenConnection = bootstrapConn;

    if (checkForShutdownRequest()) return;

    if (!_resumeCkpt.isBootstrapStartScnSet())
    {
      _resumeCkpt = initCheckpointForSnapshot(_resumeCkpt,
                                              _resumeCkpt.getBootstrapSinceScn());

      if (null == bootstrapConn)
      {
        _log.error("Unable to connect to a bootstrap server");
        enqueueMessage(LifecycleMessage.createSuspendOnErroMessage(new DatabusException("Bootstrap server retries exhausted")));
      }
      else
      {

        curState.switchToRequestStartScn(serverInfo.getAddress(),
                                         curState.getSourceIdMap(),
                                         curState.getSourcesSchemas(),
                                         _resumeCkpt,
                                         bootstrapConn,
                                         _curServer);
        enqueueMessage(curState);
      }
    }
    else
    {
      if (null == bootstrapConn)
      {
    	  _log.error("Unable to connect to a bootstrap server");
          enqueueMessage(LifecycleMessage.createSuspendOnErroMessage(new DatabusException("Bootstrap server retries exhausted")));
      } else {
    	  curState.switchToStartScnSuccess(_resumeCkpt, bootstrapConn, _curServer);
    	  enqueueMessage(curState);
          _lastOpenConnection = bootstrapConn;
      }
    }

  }

  private void doRequestTargetScn(ConnectionState curState)
  {
    _log.debug("Sending /targetScn request");
    curState.switchToTargetScnRequestSent();
    curState.getBootstrapConnection().requestTargetScn(curState.getCheckpoint(), curState);
  }

  private void doTargetScnResponseSuccess(ConnectionState curState)
  {
	boolean enqueueMessage = true;
    if (curState.getSourcesSchemas().size() != _sourcesConn.getSourcesNames().size())
    {
    	String msg = "Expected " + _sourcesConn.getSourcesNames().size() + " schemas, got: " +
                curState.getSourcesSchemas().size();
        _log.error(msg);
        curState.switchToTargetScnResponseError();
        enqueueMessage(LifecycleMessage.createSuspendOnErroMessage(new Exception(msg)));
        enqueueMessage = false;
    }
    else
    {
      if (toTearConnAfterHandlingResponse())
      {
    	enqueueMessage = false;
        tearConnectionAndEnqueuePickServer();
      }
      curState.switchToRequestStream(curState.getCheckpoint());
    }

    if ( enqueueMessage )
      enqueueMessage(curState);
  }

  private void doRequestStartScn(ConnectionState curState)
  {
    _log.debug("Sending /startScn request");
    String sourceNames = curState.getSourcesNameList();
    curState.switchToStartScnRequestSent();
    curState.getBootstrapConnection().requestStartScn(curState.getCheckpoint(), curState, sourceNames);
  }

  private void doStartScnResponseSuccess(ConnectionState curState)
  {
	boolean enqueueMessage = true;
    if (curState.getSourcesSchemas().size() != _sourcesConn.getSourcesNames().size())
    {
    	String msg = "Expected " + _sourcesConn.getSourcesNames().size() + " schemas, got: " +
                curState.getSourcesSchemas().size();
        _log.error(msg);
        curState.switchToStartScnResponseError();
        enqueueMessage(LifecycleMessage.createSuspendOnErroMessage(new Exception(msg)));
        enqueueMessage = false;
    }
    else
    {
      if (toTearConnAfterHandlingResponse())
      {
        tearConnectionAndEnqueuePickServer();
        enqueueMessage = false;
      } else {
    	  ServerInfo bsServerInfo = curState.getCurrentBSServerInfo();
    	  if ( null == bsServerInfo)
    	  {
    		  String msg = "Bootstrap Server did not provide its server info in StartSCN !! Switching to PICK_SERVER. CurrentServer :" + _curServer;
    		  _log.error(msg);
    	      curState.switchToStartScnResponseError();
    	  } else if (! bsServerInfo.equals(_curServer)){
    		  // Possible for VIP case
    		  _log.info("Bootstrap server responded and current server does not match. Switching to Pick Server !!  curServer: "
    		                  + _curServer + ", Responded Server :" + bsServerInfo);
    		  _log.info("Checkpoint before clearing :" + _resumeCkpt);
    		  String bsServerInfoStr = _resumeCkpt.getBootstrapServerInfo();
    		  _resumeCkpt.resetBSServerGeneratedState();
    		  _resumeCkpt.setBootstrapServerInfo(bsServerInfoStr);
    		  _log.info("Checkpoint after clearing :" + _resumeCkpt);
    		  curState.switchToPickServer();
    	  } else {
    		  curState.switchToRequestStream(curState.getCheckpoint());
    	  }
      }
    }

    if ( enqueueMessage)
    	enqueueMessage(curState);
  }

  protected void doRequestBootstrapStream(ConnectionState curState)
  {
    boolean debugEnabled = _log.isDebugEnabled();

    if (debugEnabled) _log.debug("Checking for free space");

    //curState.getDataEventsBuffer().waitForFreeSpace(FREE_BUFFER_THRESHOLD);
    int freeBufferThreshold=(int)(_sourcesConn.getConnectionConfig().getFreeBufferThreshold() *
        100.0 / _pullerBufferUtilizationPct);
    int freeSpace = curState.getDataEventsBuffer().getBufferFreeReadSpace();
    if (freeSpace >= freeBufferThreshold)
    {
      Checkpoint cp = curState.getCheckpoint();
      if (debugEnabled) _log.debug("Checkpoint at RequestBootstrapData: " + cp.toString());


      _log.debug("Sending /bootstrap request");

      Map<String, IdNamePair> srcNameMap = curState.getSourcesNameMap();
      String curSrcName = null;
      if (cp.getConsumptionMode() == DbusClientMode.BOOTSTRAP_SNAPSHOT)
      {
    	  curSrcName = cp.getSnapshotSource();
      } else {
    	  curSrcName = cp.getCatchupSource();
      }


      if ( null == _bootstrapFilter)
      {
    	_bootstrapFilter = new DbusKeyCompositeFilter();
      	Map<String, IdNamePair> srcNameIdMap = curState.getSourcesNameMap();

      	for (DbusKeyCompositeFilterConfig conf : _bootstrapFilterConfigs)
      	{
      		Map<String, KeyFilterConfigHolder> cMap = conf.getConfigMap();

      		Map<Long, KeyFilterConfigHolder> fConfMap = new HashMap<Long, KeyFilterConfigHolder>();
      		for ( Entry<String, KeyFilterConfigHolder> e : cMap.entrySet())
      		{
      			IdNamePair idName = srcNameIdMap.get(e.getKey());

      			if ( null != idName)
      			{
      				fConfMap.put(idName.getId(),e.getValue());
      			}
      		}

      		_bootstrapFilter.merge(new DbusKeyCompositeFilter(fConfMap));
      	}
      	_bootstrapFilter.dedupe();
      }

      DbusKeyFilter filter = null;
      IdNamePair srcEntry = srcNameMap.get(curSrcName);

      if ( null != srcEntry)
      {
    	  Map<Long, DbusKeyFilter> fMap = _bootstrapFilter.getFilterMap();

    	  if ( null != fMap)
    		  filter = fMap.get(srcEntry.getId());
      }

      int fetchSize = (int)((curState.getDataEventsBuffer().getBufferFreeReadSpace() / 100.0) *
                      _pullerBufferUtilizationPct);
      fetchSize = Math.max(freeBufferThreshold, fetchSize);
      curState.switchToStreamRequestSent();
      curState.getBootstrapConnection().requestStream(
          curState.getSourcesIdListString(),
          filter,
          fetchSize,
          cp, curState);
    }
    else
    {
      try
      {
        Thread.sleep(50);
      }
      catch (InterruptedException ie) {}
      enqueueMessage(curState);
    }
  }

  protected void doReadBootstrapEvents(ConnectionState curState)
  {
    boolean success = true;
    boolean debugEnabled = _log.isDebugEnabled();

    boolean enqueueMessage = true;

    try
    {
      Checkpoint cp = curState.getCheckpoint();
      DbusEventBuffer eventBuffer = curState.getDataEventsBuffer();

      if (debugEnabled) _log.debug("Sending bootstrap events to buffer");

      //eventBuffer.startEvents();
      DbusEventInternalWritable cpEvent =  DbusEventV1.createCheckpointEvent(cp);
      byte[] cpEventBytes = new byte[cpEvent.getRawBytes().limit()];

      if (debugEnabled)
      {
        _log.debug("checkpoint event size: " + cpEventBytes.length);
        _log.debug("checkpoint event:" + cpEvent.toString());
      }

      cpEvent.getRawBytes().get(cpEventBytes);
      ByteArrayInputStream cpIs = new ByteArrayInputStream(cpEventBytes);
      ReadableByteChannel cpRbc = Channels.newChannel(cpIs);

      int ecnt = eventBuffer.readEvents(cpRbc);

      success = (ecnt > 0);

      if (!success)
      {
        _log.error("Unable to write bootstrap phase marker");
      } else {
        ChunkedBodyReadableByteChannel readChannel = curState.getReadChannel();

        String remoteErrorName = RemoteExceptionHandler.getExceptionName(readChannel);
        Throwable remoteError = _remoteExceptionHandler.getException(readChannel);
        if (null != remoteError &&
            remoteError instanceof BootstrapDatabaseTooOldException)
        {
          _log.error("Bootstrap database is too old!");
          _remoteExceptionHandler.handleException(remoteError);
          curState.switchToStreamResponseError();
        }
        else if (null != remoteErrorName)
        {
          //remote processing error
          _log.error("read events error: " + RemoteExceptionHandler.getExceptionMessage(readChannel));
          curState.switchToStreamResponseError();
        }
        else
        {
          resetServerRetries();

          if (debugEnabled) _log.debug("Sending events to buffer");

          int eventsNum = eventBuffer.readEvents(readChannel, curState.getListeners(),
                                                 _sourcesConn.getBootstrapEventsStatsCollector());

          numEventsInCurrentState += eventsNum;

          _log.info("Bootstrap events read so far:" + numEventsInCurrentState);

          String status = readChannel.getMetadata("PhaseCompleted");

          if (status != null)
          { // set status in checkpoint to indicate that we are done with the current source
            if (cp.getConsumptionMode() == DbusClientMode.BOOTSTRAP_CATCHUP)
            {
              cp.endCatchupSource();
            }
            else if (cp.getConsumptionMode() == DbusClientMode.BOOTSTRAP_SNAPSHOT)
            {
              cp.endSnapShotSource();
            }
            else
            {
              throw new RuntimeException("Invalid bootstrap phase: " + cp.getConsumptionMode());
            }

            _log.info("Bootstrap events read :" + numEventsInCurrentState + " during phase:"
                    + cp.getConsumptionMode() + " [" + cp.getBootstrapSnapshotSourceIndex()
                    + "," + cp.getBootstrapCatchupSourceIndex() + "]");
            numEventsInCurrentState = 0;
          }
          else
          { // keep on reading more for the given snapshot
            // question: how is snapshotOffset maintained in ckpt
            if (eventsNum > 0)
            {
              cp.bootstrapCheckPoint();
            }
          }

          curState.switchToStreamResponseDone();
        }
      }
    }
    catch (InterruptedException ie)
    {
      _log.error("interupted", ie);
      success = false;
    }
    catch (InvalidEventException e)
    {
      _log.error("error reading events from server: " + e.getMessage(), e);
      success = false;
    }
    catch (RuntimeException e)
    {
      _log.error("runtime error reading events from server: " + e.getMessage(), e);
      success = false;
    }

    if (toTearConnAfterHandlingResponse())
    {
    	tearConnectionAndEnqueuePickServer();
    	enqueueMessage = false;
    } else if (!success) {
      curState.switchToPickServer();
    }

    if ( enqueueMessage )
      enqueueMessage(curState);
  }

  protected void doStreamResponseDone(ConnectionState curState)
  {
    boolean debugEnabled = _log.isDebugEnabled();
    boolean bootstrapDone = false;

    Checkpoint cp = curState.getCheckpoint();
    if (debugEnabled) _log.debug("Checkpoint at EventsDone: " + cp.toString());

    if (cp.getConsumptionMode() == DbusClientMode.BOOTSTRAP_SNAPSHOT &&
        cp.isSnapShotSourceCompleted())
    {
      logBootstrapPhase(DbusClientMode.BOOTSTRAP_SNAPSHOT, cp.getBootstrapSnapshotSourceIndex(), cp.getBootstrapCatchupSourceIndex());

      cp.incrementBootstrapSnapshotSourceIndex();

      cp = initCheckpointForCatchup(cp);
      // this allows us to get the target scn for catch up
      // we need this at the beginning of each catchup phase
      curState.switchToRequestTargetScn(curState.getServerInetAddress(),
                                        curState.getSourceIdMap(),
                                        curState.getSourcesSchemas(),
                                        cp,
                                        curState.getBootstrapConnection());
    }
    else if (cp.getConsumptionMode() == DbusClientMode.BOOTSTRAP_CATCHUP &&
             cp.isCatchupCompleted())
    {
      logBootstrapPhase(DbusClientMode.BOOTSTRAP_CATCHUP, cp.getBootstrapSnapshotSourceIndex(), cp.getBootstrapCatchupSourceIndex());

      cp.incrementBootstrapCatchupSourceIndex();
      if (cp.getBootstrapCatchupSourceIndex() < cp.getBootstrapSnapshotSourceIndex())
      {
        // move to next source for catchup source
        cp = initCheckpointForCatchup(cp);
        curState.switchToRequestStream(cp);
      }
      else if (cp.getBootstrapSnapshotSourceIndex() < curState.getSourcesNames().size())
      {
        // move to next snapshot
        cp = initCheckpointForSnapshot(cp, cp.getBootstrapSinceScn());
        curState.switchToRequestStream(cp);

        // need to reset _catchupSource to 0 because we need to do catchup
        // for all sources from the first source to the current snapshot source
        cp.setBootstrapCatchupSourceIndex(0);
      }
      else
      { // we are done
        logBootstrapPhase(DbusClientMode.BOOTSTRAP_CATCHUP, cp.getBootstrapSnapshotSourceIndex(), cp.getBootstrapCatchupSourceIndex());

        // write endOfPeriodMarker to trigger end sequence callback for bootstrap
        curState.getDataEventsBuffer().endEvents(false, cp.getBootstrapTargetScn(), false, false,null);

        // persist the checkpoint so BootstrapPullThread will get it and continue streaming
        try
        {
          processBootstrapComplete(cp, curState);
          bootstrapDone = true;
          _currentState.switchToBootstrapDone();
        }
        catch (IOException e)
        {
          throw new RuntimeException("Unable to persist checkpoint at the end of bootstrap", e);
        }
        //waitForEventsConsumption(curState.getDataEventsBuffer());
        //curState.switchToClosed();
      }
    }
    else
    {
      // nothing needs to be changed, keep on reading data from server
      curState.switchToRequestStream(cp);
    }

    if (!bootstrapDone) enqueueMessage(curState);
  }

  /**
   * Update and persist checkpoint at the end of bootstrap phase so that
   * the online phase can continue from it.
   * @param cp
   * @throws IOException
   */
  private void processBootstrapComplete(Checkpoint cp, ConnectionState curState)
    throws IOException
  {
	_log.info("Bootstrap got completed !! Checkpoint is :" + cp.toString());

    /*
     * DDS-989
     * WindowSCN need not match the bootstrapTargetSCN always when we are catching up multiple sources.
     * So set the windowSCN to be that of targetSCN as we are consistent as of targetSCN
     */
    cp.setWindowScn(cp.getBootstrapTargetScn());
    cp.setPrevScn(cp.getBootstrapTargetScn());

    cp.setConsumptionMode(DbusClientMode.ONLINE_CONSUMPTION);

    cp.resetBootstrap(); // clear Bootstrap scns for future bootstraps

    DbusEventBuffer eventBuffer = curState.getDataEventsBuffer();

    try
    {
      //eventBuffer.startEvents();
      DbusEventInternalWritable cpEvent =  DbusEventV1.createCheckpointEvent(cp);
      byte[] cpEventBytes = new byte[cpEvent.getRawBytes().limit()];
      cpEvent.getRawBytes().get(cpEventBytes);
      ByteArrayInputStream cpIs = new ByteArrayInputStream(cpEventBytes);
      ReadableByteChannel cpRbc = Channels.newChannel(cpIs);

      int ecnt = eventBuffer.readEvents(cpRbc);

      boolean success = (ecnt > 0);

      if (!success)
      {
        _log.error("Unable to write bootstrap phase marker");
      }
    }
    catch (InvalidEventException iee)
    {
      _log.error("Unable to write bootstrap phase marker", iee);
    }

    eventBuffer.endEvents(false, cp.getWindowScn(), false, false,null);

  }

  // TODO (DDSDBUS-85): For now, we use the same startScn, min(windowscn) from bootstrap_applier_state,
  // for snapshot all sources. It makes catchup inefficient. We need to optimize it later.
  private Checkpoint initCheckpointForSnapshot(Checkpoint ckpt,
                                               Long sinceScn)
  {

    String source = _currentState.getSourcesNames().get(ckpt.getBootstrapSnapshotSourceIndex());
    if (null == source)
    {
      throw new RuntimeException("no sources available for snapshot");
    }

    ckpt.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
    ckpt.setSnapshotSource(source);
    ckpt.startSnapShotSource();

    // need to reset scn because it means the rid in the snapshot table
    ckpt.setSnapshotOffset(0);

    // set since scn
    ckpt.setBootstrapSinceScn(sinceScn);

    return ckpt;
  }

  // TODO (DDSDBUS-86): For catchup of sources already made consistent prior to snapshotting the current
  // source, we could have used the scn on which they are consistent of. But for simplicity
  // for now, we are using the startScn. Optimization will be needed later for more efficient
  // catchup
  private Checkpoint initCheckpointForCatchup(Checkpoint ckpt)
  {
    String source = _currentState.getSourcesNames().get(ckpt.getBootstrapCatchupSourceIndex());
    if (null == source)
    {
      throw new RuntimeException("no sources available for catchup");
    }

    ckpt.setConsumptionMode(DbusClientMode.BOOTSTRAP_CATCHUP);
    ckpt.setCatchupSource(source);
    ckpt.startCatchupSource();
    ckpt.setWindowScn(ckpt.getBootstrapStartScn());
    return ckpt;
  }


  protected void sendErrorEventToDispatcher(ConnectionState curState)
  {
    // TODO: add implementation after CB's branch merges (DDSDBUS-87)
  }


  private void logBootstrapPhase(DbusClientMode mode, int snapshotSrcId, int catchupSrcId)
  {
    _log.info("Bootstrap phase completed - " + mode +
             " [" + snapshotSrcId +
             ", " + catchupSrcId +
             "]");
  }

  private void processStreamRequestError(ConnectionState state)
  {
    //TODO add statistics (DDSDBUS-88)
	if (toTearConnAfterHandlingResponse())
	{
		tearConnectionAndEnqueuePickServer();
	} else {
		state.switchToPickServer();
		enqueueMessage(state);
	}
  }

  private void processStreamResponseError(ConnectionState state)
  {
    //TODO add statistics (DDSDBUS-88)
	if (toTearConnAfterHandlingResponse())
	{
		tearConnectionAndEnqueuePickServer();
    } else {
    	state.switchToPickServer();
    	enqueueMessage(state);
    }
  }

  private void processTargetScnResponseError(ConnectionState currentState)
  {
    //TODO add statistics (DDSDBUS-88)
	if (toTearConnAfterHandlingResponse())
	{
		tearConnectionAndEnqueuePickServer();
	} else {
		currentState.switchToPickServer();
		enqueueMessage(currentState);
	}
  }

  private void processTargetScnRequestError(ConnectionState currentState)
  {
    //TODO add statistics (DDSDBUS-88)
	if (toTearConnAfterHandlingResponse())
	{
		tearConnectionAndEnqueuePickServer();
	} else {
		currentState.switchToPickServer();
		enqueueMessage(currentState);
	}
  }

  private void processStartScnResponseError(ConnectionState currentState)
  {
    //TODO add statistics((DDSDBUS-88)
	if (toTearConnAfterHandlingResponse())
	{
		tearConnectionAndEnqueuePickServer();
	} else {
		currentState.switchToPickServer();
		enqueueMessage(currentState);
	}
  }

  private void processStartScnRequestError(ConnectionState currentState)
  {
	  //TODO add statistics((DDSDBUS-88)
	  if (toTearConnAfterHandlingResponse())
	  {
		  tearConnectionAndEnqueuePickServer();
	  } else {
		  currentState.switchToPickServer();
		  enqueueMessage(currentState);
	  }
  }


  @Override
  protected void resetConnection()
  {
    DatabusServerConnection bootstrapConnection =  _currentState.getBootstrapConnection();
    if ( null != bootstrapConnection)
    {
      bootstrapConnection.close();
      _currentState.setBootstrapConnection(null);
    }
  }

  protected BackoffTimer getRetriesBeforeCkptCleanup()
  {
	  return _retriesBeforeCkptCleanup;
  }

}
