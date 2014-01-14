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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import javax.management.MBeanServer;

import org.apache.log4j.Logger;

import com.linkedin.databus.client.ConnectionState.StateId;
import com.linkedin.databus.client.netty.RemoteExceptionHandler;
import com.linkedin.databus.client.pub.ServerInfo;
import com.linkedin.databus.client.pub.mbean.UnifiedClientStats;
import com.linkedin.databus.core.BootstrapCheckpointHandler;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DatabusComponentStatus;
import com.linkedin.databus.core.DbusClientMode;
import com.linkedin.databus.core.DbusConstants;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventFactory;
import com.linkedin.databus.core.DbusEventInternalReadable;
import com.linkedin.databus.core.DbusEventInternalWritable;
import com.linkedin.databus.core.InvalidCheckpointException;
import com.linkedin.databus.core.InvalidEventException;
import com.linkedin.databus.core.PendingEventTooLargeException;
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

  public static final Short START_OF_SNAPSHOT_SRCID = (short)(DbusEventInternalWritable.PRIVATE_RANGE_MAX_SRCID - 1);
  public static final Short START_OF_CATCHUP_SRCID = (short)(DbusEventInternalWritable.PRIVATE_RANGE_MAX_SRCID - 2);
  public static final Short END_OF_BOOTSTRAP_SRCID = (short)(DbusEventInternalWritable.PRIVATE_RANGE_MAX_SRCID - 3);

  private static final EnumSet<ConnectionState.StateId> SHOULD_TEAR_CONNECTION =
      EnumSet.of(ConnectionState.StateId.START_SCN_REQUEST_SENT,
                 ConnectionState.StateId.START_SCN_RESPONSE_SUCCESS,
                 ConnectionState.StateId.START_SCN_REQUEST_ERROR,
                 ConnectionState.StateId.START_SCN_RESPONSE_ERROR,
                 ConnectionState.StateId.TARGET_SCN_REQUEST_SENT,
                 ConnectionState.StateId.TARGET_SCN_RESPONSE_SUCCESS,
                 ConnectionState.StateId.TARGET_SCN_REQUEST_ERROR,
                 ConnectionState.StateId.TARGET_SCN_RESPONSE_ERROR,
                 ConnectionState.StateId.STREAM_REQUEST_SENT,
                 ConnectionState.StateId.STREAM_REQUEST_SUCCESS,
                 ConnectionState.StateId.STREAM_REQUEST_ERROR,
                 ConnectionState.StateId.STREAM_RESPONSE_ERROR,
                 ConnectionState.StateId.BOOTSTRAP_DONE
                 );

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

  private ReentrantLock _v3BootstrapLock = null;

  public BootstrapPullThread(String name,
      DatabusSourcesConnection sourcesConn,
      DbusEventBuffer dbusEventBuffer,
      ConnectionStateFactory connStateFactory,
      Set<ServerInfo> bootstrapServers,
      List<DbusKeyCompositeFilterConfig> bootstrapFilterConfigs,
      double pullerBufferUtilPct,
      MBeanServer mbeanServer,
      DbusEventFactory eventFactory)
  {
    this(name, sourcesConn, dbusEventBuffer, connStateFactory, bootstrapServers, bootstrapFilterConfigs, pullerBufferUtilPct, mbeanServer, eventFactory, null);
  }

  public BootstrapPullThread(String name,
                             DatabusSourcesConnection sourcesConn,
                             DbusEventBuffer dbusEventBuffer,
                             ConnectionStateFactory connStateFactory,
                             Set<ServerInfo> bootstrapServers,
                             List<DbusKeyCompositeFilterConfig> bootstrapFilterConfigs,
                             double pullerBufferUtilPct,
                             MBeanServer mbeanServer,
                             DbusEventFactory eventFactory,
                             ReentrantLock v3BootstrapLock)
  {
    super(name, sourcesConn.getConnectionConfig().getBstPullerRetries(), sourcesConn, dbusEventBuffer,
          connStateFactory, bootstrapServers, mbeanServer, eventFactory);

    _retriesBeforeCkptCleanup = new BackoffTimer("BSPullerRetriesBeforeCkptCleanup",
                                                 sourcesConn.getConnectionConfig().getBsPullerRetriesBeforeCkptCleanup());
    _bootstrapFilterConfigs = bootstrapFilterConfigs;
    _remoteExceptionHandler = new RemoteExceptionHandler(sourcesConn, dbusEventBuffer, eventFactory);
    _pullerBufferUtilizationPct = pullerBufferUtilPct;
    _v3BootstrapLock = v3BootstrapLock;

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
  protected boolean shouldDelayTearConnection(StateId stateId)
  {
    boolean delayTear = SHOULD_TEAR_CONNECTION.contains(stateId);
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
          case BOOTSTRAP_DONE: break; //bootstrap is done -- wait for the next message
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
            _log.error("Unknown state in BootstrapPullThread: " + currentState.getStateId());
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
    if (null != _currentState.getSourcesSchemas())
    {
      final Set<Long> newIds = sourcesMessage.getSourcesSchemas().keySet();
      final Set<Long> curIds = _currentState.getSourcesSchemas().keySet();

      if (! newIds.containsAll(curIds))
      {
          String msg = "Expected schemas for sources " + curIds + "; got: " + newIds;
          _log.error(msg);
          _currentState.switchToClosed();
          enqueueMessage(LifecycleMessage.createSuspendOnErroMessage(new DatabusException(msg)));
          return;
      }
    }
    _currentState.setSourcesSchemas(sourcesMessage.getSourcesSchemas());
    _sourcesConn.getBootstrapDispatcher().enqueueMessage(sourcesMessage);
  }

  private void doSetSourcesIds(SourcesMessage sourcesMessage)
  {
    // sourcesMessage.getSources() has the sources that the relay returned. The sources call response handler
    // in the relay puller has already verified that all the subscriptions are covered by the list of sources
    // returned from the relay (see RelayPullThread.buildSubsList()
    _currentState.setSourcesIds(sourcesMessage.getSources());
    _currentState.setSourcesIdListString(sourcesMessage.getSourcesIdListString());
    _sourcesConn.getBootstrapDispatcher().enqueueMessage(sourcesMessage);
  }

  private void doSetResumeCheckpoint(CheckpointMessage cpMessage)
  {
    _resumeCkpt = cpMessage.getCheckpoint();
    if (null != _resumeCkpt)
    {
      DbusEventInternalReadable cpEvent = getEventFactory().createCheckpointEvent(_resumeCkpt);
      boolean success;
      try
      {
        success = _currentState.getDataEventsBuffer().injectEvent(cpEvent);
      }
      catch (InvalidEventException e)
      {
        _log.error("unable to create checkpoint event for checkpoint " + _resumeCkpt + "; error: "  + e, e);
        success = false;
      }
      if (!success)
      {
        _log.error("Unable to write bootstrap phase marker");
      }
    }

    _log.info("resume checkpoint: " + _resumeCkpt);
  }

  /**
   * Invoked when a LifeCycle message of type "START" is received by bootstrap puller thread
   * as defined in AbstractActorMessageQueue#executeAndChangeState(Object)
   *
   * 1. Acquire lock for Databus V3 bootstrap
   * 2. Invoke same method on super class
   * 3. Clear and switch state-machine to start choosing a server(relay) to connected to
   */
  @Override
  protected void doStart(LifecycleMessage lcMessage)
  {
    lockV3Bootstrap();
    super.doStart(lcMessage);

    _currentState.clearBootstrapState();

    _currentState.switchToPickServer();
    enqueueMessage(_currentState);
  }

  /**
   * Invoked when a LifeCycle message of type "RESUME" is received by bootstrap puller thread
   * as defined in AbstractActorMessageQueue#executeAndChangeState(Object)
   *
   * 1. Acquire lock for Databus V3 bootstrap
   * 2. Invoke same method in super class
   */
  @Override
  protected void doResume(LifecycleMessage lcMessage)
  {
    lockV3Bootstrap();
    super.doResume(lcMessage);
  }

  /**
   * Invoked when a LifeCycle message of type "SHUTDOWN" is received by bootstrap puller thread
   * as defined in AbstractActorMessageQueue#executeAndChangeState(Object)
   *
   * 1. Release lock for Databus V3 bootstrap
   * 2. Invoke same method in super class
   * 3. The currently open connection to server (relay) is tracked as we want "sticky" behavior, meaning
   *    the ability to be able to connect to the previously connected server. Close the connection if open.
   */
   // TODO:  seems misleading; stickiness not really achieved with _lastOpenConnection (which is good, since
   //        it's closed and forgotten here) but rather with lastReadBS in doPickBootstrapServer()
  @Override
  protected void onShutdown()
  {
    try
    {
      if (null != _lastOpenConnection)
      {
        _log.info("closing open connection");
        _lastOpenConnection.close();
        _lastOpenConnection = null;
      }
    }
    finally
    {
      unlockV3Bootstrap(true);
    }
    _log.info("shutdown complete.");
  }

  /**
   * Invoked when a LifeCycle message of type "PAUSE" is received by bootstrap puller thread
   * as defined in AbstractActorMessageQueue#executeAndChangeState(Object)
   *
   * 1. Release lock for Databus V3 bootstrap
   * 2. Invoke same method in super class
   */
  @Override
  protected void doPause(LifecycleMessage lcMessage)
  {
    try
    {
      super.doPause(lcMessage);
    }
    finally
    {
      unlockV3Bootstrap();
    }
  }

  /**
   * Invoked when a LifeCycle message of type "SUSPEND_ON_ERROR" is received by bootstrap puller thread
   * as defined in AbstractActorMessageQueue#executeAndChangeState(Object)
   *
   * 1. Invoke same method in super class
   * 2. Send an "I'm dead" heartbeat value as a failsafe
   * 3. Release lock for Databus V3 bootstrap
   */
  @Override
  protected void doSuspendOnError(LifecycleMessage lcMessage)
  {
    try
    {
      super.doSuspendOnError(lcMessage);
      sendHeartbeat(_sourcesConn.getUnifiedClientStats(), -1);
    }
    finally
    {
      unlockV3Bootstrap();
    }
  }

  /**
   * This method is not to be confused with the doResume method. The latter is invoked
   * on a LifeCycleMessage. This method is invoked when a RESUME message is received in
   * one of the inner workflows.
   */
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
        //attempt to reconnect to the last used bootstrap server
        while (null == bootstrapConn && (retriesLeft = _retriesBeforeCkptCleanup.getRemainingRetriesNum()) >= 0
               && !checkForShutdownRequest())
        {

            _log.info("Retry picking last used bootstrap server :" + serverInfo +
                      "; retries left:" + retriesLeft);

            if (lastReadBS.equals(_curServer) ) // if it is new server do not sleep?
              _retriesBeforeCkptCleanup.backoffAndSleep();

            try
            {
              bootstrapConn = _sourcesConn.getBootstrapConnFactory().createConnection(serverInfo, this,
                                                                                      _remoteExceptionHandler);
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

    if(checkForShutdownRequest()) {
      _log.info("Shutting down bootstrap");
      return;
    }

    Random rng = new Random();

    if ( null == bootstrapConn)
    {
      _log.info("Restarting bootstrap as client might be getting bootstrap data from different server instance !!");
      _log.info("Old Checkpoint :" + _resumeCkpt);
      curState.getBstCheckpointHandler().resetForServerChange(_resumeCkpt);
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
        _log.info("picked a bootstrap server:" + serverInfo.toSimpleString());
      }
      catch (Exception e)
      {
        _log.error("Unable to get connection to bootstrap server:" + serverInfo, e);
      }
    }

    /*
     * Close the old bootstrap Connection
     */
     DatabusBootstrapConnection oldBootstrapConn = curState.getBootstrapConnection();

     if ( null != oldBootstrapConn)
         resetConnectionAndSetFlag();
     _lastOpenConnection = bootstrapConn;

    if (checkForShutdownRequest()) return;

    if (null == bootstrapConn)
    {
      _log.error("bootstrap server retries exhausted");
      enqueueMessage(LifecycleMessage.createSuspendOnErroMessage(new DatabusException("bootstrap server retries exhausted")));
      return;
    }

    sendHeartbeat(_sourcesConn.getUnifiedClientStats());

    curState.bootstrapServerSelected(serverInfo.getAddress(), bootstrapConn, _curServer);

    //determine what to do next based on the current checkpoint
    _log.info("resuming bootstrap from checkpoint: " + _resumeCkpt);
    curState.setCheckpoint(_resumeCkpt);
    determineNextStateFromCheckpoint(curState);
    enqueueMessage(curState);
  }

  private void doRequestTargetScn(ConnectionState curState)
  {
    _log.debug("Sending /targetScn request");
    curState.switchToTargetScnRequestSent();
    sendHeartbeat(_sourcesConn.getUnifiedClientStats());
    curState.getBootstrapConnection().requestTargetScn(curState.getCheckpoint(), curState);
  }

  protected void doTargetScnResponseSuccess(ConnectionState curState)
  {
    if (toTearConnAfterHandlingResponse())
    {
      tearConnectionAndEnqueuePickServer();
    }
    else
    {
      final Checkpoint cp = curState.getCheckpoint();
      curState.getBstCheckpointHandler().advanceAfterSnapshotPhase(cp);
      curState.getBstCheckpointHandler().advanceAfterTargetScn(cp);
      curState.switchToRequestStream(curState.getCheckpoint());
      enqueueMessage(curState);
    }
  }

  private void doRequestStartScn(ConnectionState curState)
  {
    _log.debug("Sending /startScn request");
    String sourceNames = curState.getSourcesNameList();
    curState.switchToStartScnRequestSent();
    sendHeartbeat(_sourcesConn.getUnifiedClientStats());
    curState.getBootstrapConnection().requestStartScn(curState.getCheckpoint(), curState, sourceNames);
  }

  private void doStartScnResponseSuccess(ConnectionState curState)
  {
    if (toTearConnAfterHandlingResponse())
    {
      tearConnectionAndEnqueuePickServer();
    }
    else
    {
      ServerInfo bsServerInfo = curState.getCurrentBSServerInfo();
      if ( null == bsServerInfo)
      {
        String msg = "Bootstrap Server did not provide its server info in StartSCN !! Switching to PICK_SERVER. CurrentServer :" + _curServer;
        _log.error(msg);
          curState.switchToStartScnResponseError();
      }
      else if (! bsServerInfo.equals(_curServer)){
        // Possible for VIP case
        _log.info("Bootstrap server responded and current server does not match. Switching to Pick Server !!  curServer: "
                        + _curServer + ", Responded Server :" + bsServerInfo);
        _log.info("Checkpoint before clearing :" + _resumeCkpt);
        String bsServerInfoStr = _resumeCkpt.getBootstrapServerInfo();
        final Long startScn = _resumeCkpt.getBootstrapStartScn();
        curState.getBstCheckpointHandler().resetForServerChange(_resumeCkpt);
        curState.getBstCheckpointHandler().setStartScnAfterServerChange(_resumeCkpt, startScn);
        _resumeCkpt.setBootstrapServerInfo(bsServerInfoStr);
        _log.info("Checkpoint after clearing :" + _resumeCkpt);
        curState.switchToPickServer();
      } else {
        curState.switchToRequestStream(curState.getCheckpoint());
      }

      enqueueMessage(curState);
    }
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
      sendHeartbeat(_sourcesConn.getUnifiedClientStats());
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
      DbusEventInternalReadable cpEvent = getEventFactory().createCheckpointEvent(cp);
      byte[] cpEventBytes = new byte[cpEvent.size()];

      if (debugEnabled)
      {
        _log.debug("checkpoint event size: " + cpEventBytes.length);
        _log.debug("checkpoint event:" + cpEvent.toString());
      }

      cpEvent.getRawBytes().get(cpEventBytes);
      ByteArrayInputStream cpIs = new ByteArrayInputStream(cpEventBytes);
      ReadableByteChannel cpRbc = Channels.newChannel(cpIs);

      UnifiedClientStats unifiedClientStats = _sourcesConn.getUnifiedClientStats();
      sendHeartbeat(unifiedClientStats);
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
          sendHeartbeat(unifiedClientStats);
          int eventsNum = eventBuffer.readEvents(readChannel, curState.getListeners(),
                                                 _sourcesConn.getBootstrapEventsStatsCollector());

          if (eventsNum == 0 &&
              _remoteExceptionHandler.getPendingEventSize(readChannel) > eventBuffer.getMaxReadBufferCapacity())
          {
            String err = "ReadBuffer max capacity(" + eventBuffer.getMaxReadBufferCapacity() +
                         ") is less than event size(" +
                         _remoteExceptionHandler.getPendingEventSize(readChannel) +
                         "). Increase databus.client.connectionDefaults.bstEventBuffer.maxEventSize and restart.";
            _log.fatal(err);
            enqueueMessage(LifecycleMessage.createSuspendOnErroMessage(new PendingEventTooLargeException(err)));
            return;
          }
          else
          {
            resetServerRetries();

            if (debugEnabled) _log.debug("Sending events to buffer");

            numEventsInCurrentState += eventsNum;

            _log.info("Bootstrap events read so far: " + numEventsInCurrentState);

            String status = readChannel.getMetadata("PhaseCompleted");

            final BootstrapCheckpointHandler ckptHandler = curState.getBstCheckpointHandler();
            if (status != null)
            { // set status in checkpoint to indicate that we are done with the current source
              if (cp.getConsumptionMode() == DbusClientMode.BOOTSTRAP_CATCHUP)
              {
                ckptHandler.finalizeCatchupPhase(cp);
              }
              else if (cp.getConsumptionMode() == DbusClientMode.BOOTSTRAP_SNAPSHOT)
              {
                ckptHandler.finalizeSnapshotPhase(cp);
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

    Checkpoint cp = curState.getCheckpoint();
    if (debugEnabled) _log.debug("Checkpoint at EventsDone: " + cp);

    determineNextStateFromCheckpoint(curState);
    // if we successfully got some data - reset retries counter.
    _retriesBeforeCkptCleanup.reset();

    enqueueMessage(curState);
  }

  /**
   * Update and persist checkpoint at the end of bootstrap phase so that
   * the online phase can continue from it.
   * @param cp
   * @throws IOException
   */
  protected void processBootstrapComplete(Checkpoint cp, ConnectionState curState)
      throws IOException, DatabusException
  {
    logBootstrapPhase(DbusClientMode.BOOTSTRAP_CATCHUP, cp.getBootstrapSnapshotSourceIndex(), cp.getBootstrapCatchupSourceIndex());
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
      DbusEventInternalReadable cpEvent = getEventFactory().createCheckpointEvent(cp);
      boolean success = eventBuffer.injectEvent(cpEvent);
      if (!success)
      {
        _log.error("Unable to write bootstrap phase marker");
      }
      else
      {
        //TODO need real partition for partitioned bootstrap
        DbusEventInternalReadable eopEvent = curState.createEopEvent(cp, getEventFactory());
        success = eventBuffer.injectEvent(eopEvent);
        if (! success)
        {
          _log.error("Unable to write bootstrap EOP marker");
        }
      }
    }
    catch (InvalidEventException iee)
    {
      _log.error("Unable to write bootstrap phase marker", iee);
    }
    unlockV3Bootstrap();
  }

  //TODO REMOVE ME
  /*
  private Checkpoint initCheckpointForSnapshot(Checkpoint ckpt, Long sinceScn)
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
  */

  /*
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
  */


  protected void sendErrorEventToDispatcher(ConnectionState curState)
  {
    // TODO: add implementation after CB's branch merges (DDSDBUS-87)
  }


  private void logBootstrapPhase(DbusClientMode mode, int snapshotSrcId, int catchupSrcId)
  {
    _log.info("Bootstrap phase completed - " + mode + " [" + snapshotSrcId + ", " + catchupSrcId + "]");
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

  /**
   * Determines the next state based on the checkpoint. The idea is to determine where we are in the bootstrap flow and
   * move to the next state.
   *
   *  <pre>
   *    1. Request startSCN (State=REQUEST_START_SCN, SNAPSHOT, !cp.isBootstrapStartScnSet())
   *    2. For each snapshot source:
   *       2.1. Start snapshot (State=REQUEST_STREAM, SNAPSHOT, cp.isBootstrapStartScnSet() &&
   *                            0 == cp.getSnapshotOffset())
   *       2.2. While (! cp.isSnapShotSourceCompleted())
   *          2.2.1. Continue snapshot (State=REQUEST_STREAM, SNAPSHOT, cp.isBootstrapStartScnSet() &&
   *                                    0 < cp.getSnapshotOffset())
   *       2.3. Request targetSCN (State=REQUEST_TARGET_SCN, SNAPSHOT, cp.isBootstrapStartScnSet() &&
   *                               cp.isSnapShotSourceCompleted() && 0 == cp.getWindowOffset())
   *       2.4. For each catchup source <= the snapshot source:
   *          2.4.1. Start catchup (State=REQUEST_STREAM, CATCHUP, 0==cp.getWindowOffset() && handler.needsMoreCatchup())
   *          2.4.2. While (! cp.isCatchupSourceCompleted())
   *             2.4.2.1. Continue catchup (State=REQUEST_STREAM, CATCHUP, ! cp.isCatchupSourceCompleted())
   *  </pre>
   * @param curState        the bootstrap checkpoint
   */
  private void determineNextStateFromCheckpoint(ConnectionState curState)
  {
    try
    {
      final Checkpoint cp = curState.getCheckpoint();
      final BootstrapCheckpointHandler cpHandler = curState.getBstCheckpointHandler();
      cpHandler.assertBootstrapCheckpoint(cp);
      switch (cp.getConsumptionMode())
      {
      case BOOTSTRAP_SNAPSHOT:
        determineNextStateFromSnapshotCheckpoint(cp, cpHandler, curState);
        break;
      case BOOTSTRAP_CATCHUP:
        determineNextStateFromCatchupCheckpoint(cp, cpHandler, curState);
        break;
      default:
        _log.fatal("unexpected bootstrap checkpoint type: " + cp + "; shutting down");
        curState.switchToClosed();
      }
    }
    catch (InvalidCheckpointException e)
    {
      _log.fatal("invalid bootstrap checkpoint:", e);
      curState.switchToClosed();
    }
  }

  /**
   * Determines the next state based on snapshot the checkpoint. See comments for
   * {@link #determineNextStateFromCatchupCheckpoint(Checkpoint, BootstrapCheckpointHandler, ConnectionState)
   *
   * @param cp          the checkpoint
   * @param cpHandler   the handler to modify the checkpoint
   * @param curState    the state to modify
   */
  private void determineNextStateFromSnapshotCheckpoint(Checkpoint cp,
                                                        BootstrapCheckpointHandler cpHandler,
                                                        ConnectionState curState)
  {
    if (!cp.isBootstrapStartScnSet())
    {
      //(*, !cp.isBootstrapStartScnSet()) --> (REQUEST_START_SCN)
      curState.switchToRequestStartScn(cp);
    }
    else if (!cp.isSnapShotSourceCompleted())
    {
      //(*, cp.isBootstrapStartScnSet() && ! cp.isSnapShotSourceCompleted()) --> (REQUEST_STREAM)
      curState.switchToRequestStream(cp);
    }
    else
    {
      //Snapshot complete -- send /targetSCN
      logBootstrapPhase(DbusClientMode.BOOTSTRAP_SNAPSHOT, cp.getBootstrapSnapshotSourceIndex(),
                        cp.getBootstrapCatchupSourceIndex());

      //cpHandler.advanceAfterSnapshotPhase(cp);
      curState.switchToRequestTargetScn(cp);
    }
  }

  /**
   * Determines the next state based on catchup the checkpoint. See comments for
   * {@link #determineNextStateFromCatchupCheckpoint(Checkpoint, BootstrapCheckpointHandler, ConnectionState)
   * @param cp          the checkpoint
   * @param cpHandler   the handler to modify the checkpoint
   * @param curState    the state to modify
   */
  private void determineNextStateFromCatchupCheckpoint(Checkpoint cp,
                                                       BootstrapCheckpointHandler cpHandler,
                                                       ConnectionState curState)
  {
    if (!cp.isCatchupSourceCompleted())
    {
      //Finish current catchup source
      curState.switchToRequestStream(cp);
    }
    else
    {
      logBootstrapPhase(DbusClientMode.BOOTSTRAP_CATCHUP, cp.getBootstrapSnapshotSourceIndex(), cp.getBootstrapCatchupSourceIndex());
      cpHandler.advanceAfterCatchupPhase(cp);

      if (cpHandler.needsMoreCatchup(cp))
      {
        //Current catchup source is done but there are more
        curState.switchToRequestStream(cp);
      }
      else if (cpHandler.needsMoreSnapshot(cp))
      {
        //All catchup sources are done, try next snapshot source
        curState.switchToRequestStream(cp);
      }
      else
      {
        //no snapshot or catchup source left -- bootstrap complete
        // write endOfPeriodMarker to trigger end sequence callback for bootstrap
        DbusEventInternalReadable eopEvent =
            getEventFactory().createLongKeyEOPEvent(cp.getBootstrapTargetScn(), (short) 0);
        try
        {
          boolean success = curState.getDataEventsBuffer().injectEvent(eopEvent);
          if (success)
          {
            // persist the checkpoint so BootstrapPullThread will get it and continue streaming
            try
            {
              processBootstrapComplete(cp, curState);
              curState.switchToBootstrapDone();
            }
            catch (IOException e)
            {
              _log.error("Unable to persist checkpoint at the end of bootstrap", e);
              curState.switchToPickServer();
            }
            catch (DatabusException e)
            {
              _log.error("Unable to complete bootstrap", e);
              curState.switchToPickServer();
            }
          }
          else
          {
            _log.error("Unable to write bootstrap EOP marker");
            curState.switchToPickServer();
          }
        }
        catch (InvalidEventException e1)
        {
          _log.error("Unable to write bootstrap EOP marker", e1);
        }
      }
    }

  }

  /**
   * A method to safely acquire a lock on underlying re-entrant lock to serialize bootstrap
   * across partitions
   * The method is a no-op is the lock has *already* been acquired by current thread
   */
  private void lockV3Bootstrap()
  {
    if (null != _v3BootstrapLock)
    {
      if (_v3BootstrapLock.isHeldByCurrentThread())
      {
        _log.warn("lockV3Bootstrap is a no-op as the thread is already owner of bootstrap lock. Lock state = " +
                  _v3BootstrapLock.toString());
        return;
      }
      _log.info("Waiting for bootstrap lock " + toString());
      _v3BootstrapLock.lock();
      _log.info("Obtained the bootstrap lock " + toString());
    }
  }

  /**
   * A method to safely release a lock on underlying re-entrant lock to serialize bootstrap
   * across partitions.
   * The method is a no-op is the lock has *not* been acquired by current thread
   *
   * The shutdown flag is used to determine if not possessing the lock should be logged as a warning (rather than info)
   * This is because in normal processing:
   * Lock is acquired in doStart(), and relinquished in processBootstrapComplete(). By the time, shutdown is invoked,
   * the lock is not owned by current thread.
   *
   */
  private void unlockV3Bootstrap(boolean shutdownCase)
  {
    if (null != _v3BootstrapLock)
    {
      // If the lock is not held, invoking an unlock throws an IllegalStateMonitorException
      // Check for this case
      if (! _v3BootstrapLock.isHeldByCurrentThread())
      {
        String errMsg = "unlockV3Bootstrap is a no-op as current thread is NOT owner of bootstrap lock. Lock state = " +
                        _v3BootstrapLock.toString();
        if (shutdownCase)
        {
          _log.info(errMsg);
        }
        else
        {
          _log.warn(errMsg);
        }

        return;
      }
      _v3BootstrapLock.unlock();
      _log.info("Unlocked BootstrapPuller " + this.toString());
    }
  }

  /**
   * This unlock method is normally invoked, except in case of shutdown when we want some variation in how we log messages
   */
  private void unlockV3Bootstrap()
  {
    unlockV3Bootstrap(false);
  }

  protected ReentrantLock getV3BootstrapLock()
  {
    return _v3BootstrapLock;
  }

}
