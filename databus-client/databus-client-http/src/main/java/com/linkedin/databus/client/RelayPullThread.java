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
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
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
import com.linkedin.databus.client.pub.mbean.UnifiedClientStats;
import com.linkedin.databus.client.pub.ServerInfo;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.CheckpointMult;
import com.linkedin.databus.core.DatabusComponentStatus;
import com.linkedin.databus.core.DbusClientMode;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventFactory;
import com.linkedin.databus.core.InvalidEventException;
import com.linkedin.databus.core.PendingEventTooLargeException;
import com.linkedin.databus.core.PullerRetriesExhaustedException;
import com.linkedin.databus.core.SCNRegressMessage;
import com.linkedin.databus.core.ScnNotFoundException;
import com.linkedin.databus.core.async.LifecycleMessage;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.data_model.LogicalSource;
import com.linkedin.databus.core.data_model.LogicalSourceId;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus2.core.BackoffTimer;
import com.linkedin.databus2.core.BackoffTimerStaticConfig;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.monitoring.mbean.HttpStatisticsCollector;
import com.linkedin.databus2.core.container.request.BootstrapDatabaseTooOldException;
import com.linkedin.databus2.core.container.request.RegisterResponseEntry;
import com.linkedin.databus2.core.filter.DbusKeyCompositeFilter;
import com.linkedin.databus2.core.filter.DbusKeyCompositeFilterConfig;
import com.linkedin.databus2.core.filter.KeyFilterConfigHolder;

public class RelayPullThread extends BasePullThread
{

  public static final String MODULE = RelayPullThread.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private final static int RELAY_CALLS_MERGE_FREQ = 10;
  private static final ArrayList<RegisterResponseEntry> EMPTY_REGISTER_LIST =
      new ArrayList<RegisterResponseEntry>();
  private static final ArrayList<Integer> EMPTY_STREAM_LIST =
      new ArrayList<Integer>();

  private final boolean _isConsumeCurrent ;
  private final HttpStatisticsCollector _relayCallsStats;

  //Keep track of the last open relay connection so that we can close it on shutdown
  private DatabusRelayConnection _lastOpenConnection;

  private DbusKeyCompositeFilter _relayFilter;
  private final List<DbusKeyCompositeFilterConfig> _relayFilterConfigs;

  private int _unmergedHttpCallsStats = 0;
  private long _streamCallStartMs = 0;

  // if that much time passes with no events - reset the connection, and pick another server
  private long _noEventsConnectionResetTimeSec;
  private long _timeSinceEventsSec = System.currentTimeMillis();

  /**
   * When RelayPullThread  falls-off the relay, it goes to the Fell-Off mode and the below retryConfig (_retriesOnFallOff)
   * control the retries. The fell-off mode is also set when the client starts with a bootstrap-ckpt.
   *
   * <pre>
   * The fell-off mode will only be reset after
   *   (a) Bootstrap got completed
   *   (b) streamFromLastSCN gets triggered.
   *   (c) Server-Set Change which results in tearing the current connection
   *
   *
   *   Behavior:
   *
   *   (a) When bootstrap enabled,
   *          On first SCNNotFoundException,
   *            1 Fell-Off mode is set.
   *            2. In Pick_SERVER state, RelayPuller will re-establish the connection (hopefully to new relay in case of VIPS) and retry depending upon the retry-config.
   *            3. On subsequent retry, any failure could happen (SCNNotFound or ConnClose or any other). All these will still happen in fell-off mode and the _retriesOnFallOff config will control retries.
   *            4. If the _retriesOnFallOff gets exhausted, will go to bootstrap mode and reset the _retriesOnFallOff counter.
   *            5. When bootstrap gets completed, Fell_Off mode is reset
   *
   *   (b) When bootstrap disabled and streamFromLastSCNonFellOff set
   *            1 Fell-Off mode is set.
   *            2. In Pick_SERVER state, RelayPuller will re-establish the connection (hopefully to new relay in case of VIPS) and retry depending upon the retry-config.
   *            3. On subsequent retry, any failure could happen (SCNNotFound or ConnClose or any other). All these will still happen in fell-off mode and the _retriesOnFallOff config will control retries.
   *            4. If the _retriesOnFallOff gets exhausted, will set the flag to make current relay stream from the latest SCN and resets fell-Off mode
   *
   *   (c) When bootstrap disabled and  streamFromLastSCNonFellOff not set
   *            1. Fell-Off mode is set.
   *            2. In Pick_SERVER state, RelayPuller will re-establish the connection (hopefully to new relay in case of VIPS) and retry depending upon the retry-config.
   *            3. On subsequent retry, any failure could happen (SCNNotFound or ConnClose or any other). All these will still happen in fell-off mode and the _retriesOnFallOff config will control retries.
   *            4. If the _retriesOnFallOff gets exhausted, the relay-pull thread gets suspended.
   * </pre>
   */
  private final BackoffTimer _retriesOnFallOff;

  final private RemoteExceptionHandler _remoteExceptionHandler;
  private final boolean _isReadLatestScnOnErrorEnabled;
  private final double _pullerBufferUtilizationPct ;



  public RelayPullThread(String name,
                         DatabusSourcesConnection sourcesConn,
                         DbusEventBuffer dbusEventBuffer,
                         ConnectionStateFactory connStateFactory,
                         Set<ServerInfo> relays,
                         List<DbusKeyCompositeFilterConfig> relayFilterConfigs,
                         boolean isConsumeCurrent,
                         boolean isReadLatestScnOnErrorEnabled,
                         double pullerBufferUtilPct,
                         int noEventsConnectionResetTimeSec,
                         MBeanServer mbeanServer,
                         DbusEventFactory eventFactory)
  {
    super(name, sourcesConn.getConnectionConfig().getPullerRetries(), sourcesConn, dbusEventBuffer,
          connStateFactory, relays, mbeanServer, eventFactory);
    _relayFilterConfigs = relayFilterConfigs;
    _isConsumeCurrent = isConsumeCurrent;
    _remoteExceptionHandler = new RemoteExceptionHandler(sourcesConn, dbusEventBuffer, eventFactory);
    _relayCallsStats = _sourcesConn.getLocalRelayCallsStatsCollector();
    _isReadLatestScnOnErrorEnabled = isReadLatestScnOnErrorEnabled;
    _pullerBufferUtilizationPct = pullerBufferUtilPct;
    _retriesOnFallOff = new BackoffTimer("RetriesOnFallOff",
        new BackoffTimerStaticConfig(0, 0, 1, 0, sourcesConn.getConnectionConfig().getNumRetriesOnFallOff()));
    _noEventsConnectionResetTimeSec = noEventsConnectionResetTimeSec;
  }


  @Override
  protected void onResume()
  {
    _currentState.switchToPickServer();
    enqueueMessage(_currentState);
  }

  @Override
  protected void onShutdown()
  {
    if (null != _lastOpenConnection)
    {
      _log.info("closing open connection during onShutdown()");
      _lastOpenConnection.close();
      _lastOpenConnection = null;
      _log.info("Closed open connection during onShutdown()");
    }
  }

  /**
   * Invoked when a LifeCycle message of type "SUSPEND_ON_ERROR" is received by relay puller thread
   * as defined in AbstractActorMessageQueue#executeAndChangeState(Object)
   *
   * 1. Invoke same method in super class
   * 2. Send an "I'm dead" heartbeat value as a failsafe
   */
  @Override
  protected void doSuspendOnError(LifecycleMessage lcMessage)
  {
    super.doSuspendOnError(lcMessage);
    sendHeartbeat(_sourcesConn.getUnifiedClientStats(), -1);
  }

  @Override
  protected boolean shouldDelayTearConnection(StateId stateId)
  {
    boolean delayTear = false;
    switch(stateId)
    {
      case SOURCES_REQUEST_SENT:
      case SOURCES_RESPONSE_SUCCESS:
      case SOURCES_RESPONSE_ERROR:
      case SOURCES_REQUEST_ERROR:
      case REGISTER_REQUEST_SENT :
      case REGISTER_RESPONSE_SUCCESS:
      case REGISTER_RESPONSE_ERROR:
      case REGISTER_REQUEST_ERROR:
      case STREAM_REQUEST_SENT:
      case STREAM_REQUEST_SUCCESS:
      case STREAM_RESPONSE_ERROR:
      case STREAM_REQUEST_ERROR:
      case BOOTSTRAP_REQUESTED:
        delayTear = true;
        break;
      default:
        delayTear = false;
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
        _log.warn(" not running: " + message.toString());
      }
      else
      {
        ConnectionStateMessage stateMsg = (ConnectionStateMessage)message;
        ConnectionState currentState = stateMsg.getConnState();

        switch (stateMsg.getStateId())
        {
          case INITIAL: break;
          case CLOSED: shutdown(); break;
          case PICK_SERVER: doPickRelay(currentState); break;
          case REQUEST_SOURCES: doRequestSources(currentState); break;
          case SOURCES_RESPONSE_SUCCESS: doSourcesResponseSuccess(currentState); break;
          case REQUEST_REGISTER: doRequestRegister(currentState); break;
          case REGISTER_RESPONSE_SUCCESS: doRegisterResponseSuccess(currentState); break;
          case BOOTSTRAP: doBootstrap(currentState); break;
          case REQUEST_STREAM: doRequestStream(currentState); break;
          case STREAM_REQUEST_SUCCESS: doReadDataEvents(currentState); break;
          case STREAM_RESPONSE_DONE: doStreamResponseDone(currentState); break;
          case SOURCES_REQUEST_ERROR: processSourcesRequestError(currentState); break;
          case SOURCES_RESPONSE_ERROR: processSourcesResponseError(currentState); break;
          case REGISTER_REQUEST_ERROR: processRegisterRequestError(currentState); break;
          case REGISTER_RESPONSE_ERROR: processRegisterResponseError(currentState); break;
          case STREAM_REQUEST_ERROR: processStreamRequestError(currentState); break;
          case STREAM_RESPONSE_ERROR: processStreamResponseError(currentState); break;
          default:
          {
            _log.error("Unknown state in RelayPullThread: " + currentState.getStateId());
            success = false;
            break;
          }
        }
      }
    }
    else if (message instanceof BootstrapResultMessage)
    {
      BootstrapResultMessage bootstrapResultMessage = (BootstrapResultMessage)message;

      switch (bootstrapResultMessage.getTypeId())
      {
        case BOOTSTRAP_COMPLETE: doBootstrapComplete(bootstrapResultMessage); break;
        case BOOTSTRAP_FAILED: doBootstrapFailed(bootstrapResultMessage); break;
        default:
        {
          _log.error("Unknown BootstrapResultChangeMessage in RelayPullThread: " +
                    bootstrapResultMessage.getTypeId());
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

  private void doBootstrapFailed(BootstrapResultMessage bootstrapResultMessage)
  {
    _log.error("bootstrap failed", bootstrapResultMessage.getFailureReason());
    if (toTearConnAfterHandlingResponse())
    {
      tearConnectionAndEnqueuePickServer();
    } else {
      _currentState.switchToBootstrap(_currentState.getCheckpoint());
      enqueueMessage(_currentState);
    }
  }

  private void doBootstrapComplete(BootstrapResultMessage bootstrapResultMessage)
  {
    Checkpoint cp = bootstrapResultMessage.getBootstrapCheckpoint();

    // we must have persisted some checkpoint at the end of the bootstrap phase
    if (null == cp)
    {
      bootstrapResultMessage.switchToBootstrapFailed(
          new RuntimeException("No persistent checkpoint found at the end of bootstrap!"));
      doBootstrapFailed(bootstrapResultMessage);
    }
    else
    {
      _currentState.setRelayFellOff(false);

      DbusClientMode consumptionMode = cp.getConsumptionMode();

      _log.info("Bootstrap completed: " +
               "Consumption Mode=" + consumptionMode +
               " startScn=" + cp.getBootstrapStartScn() +
               " targetScn=" + cp.getBootstrapTargetScn() +
               " sinceScn=" + cp.getBootstrapSinceScn() +
               " windowScn=" + cp.getWindowScn());

      UnifiedClientStats unifiedClientStats = _sourcesConn.getUnifiedClientStats();
      if (unifiedClientStats != null)
      {
        // should always be false, but just in case of weird edge cases:
        boolean isBootstrapping = (consumptionMode == DbusClientMode.BOOTSTRAP_SNAPSHOT ||
                                   consumptionMode == DbusClientMode.BOOTSTRAP_CATCHUP);
        unifiedClientStats.setBootstrappingState(isBootstrapping);
      }

      cp.resetBootstrap(); // clear Bootstrap scns for future bootstraps

      if ( toTearConnAfterHandlingResponse())
      {
        tearConnectionAndEnqueuePickServer();
      } else {
        _currentState.switchToRequestStream(cp);
        enqueueMessage(_currentState);
      }
    }
  }

  @Override
  protected void doStart(LifecycleMessage lcMessage)
  {
    _log.info("Relay Puller doStart ");

    if (_currentState.getStateId() != ConnectionState.StateId.INITIAL)
    {
      return;
    }
    super.doStart(lcMessage);

    _currentState.switchToPickServer();
    enqueueMessage(_currentState);
  }

  protected void doPickRelay(ConnectionState curState)
  {
    int serversNum = _servers.size();
    if (0 == serversNum)
    {
      enqueueMessage(LifecycleMessage.createSuspendOnErroMessage(
          new DatabusException("No relays specified")));
      return;
    }

    Random rng = new Random();
    DatabusRelayConnection relayConn = null;
    ServerInfo serverInfo = null;

    int retriesLeft;
    BackoffTimer originalCounter =_status.getRetriesCounter();

    if( curState.isRelayFellOff())
      _status.setRetriesCounter(_retriesOnFallOff);

    while (null == relayConn && (retriesLeft = _status.getRetriesLeft()) >= 0 && !checkForShutdownRequest())
    {
      _log.info("picking a relay; retries left:" + retriesLeft +
                ", Backoff Timer :" + _status.getRetriesCounter() +
                ", Are we retrying because of SCNNotFoundException : " + curState.isRelayFellOff());

      backoffOnPullError();

      _curServerIdx =  (_curServerIdx < 0) ? rng.nextInt(serversNum)
                                           : (_curServerIdx + 1) % serversNum;

      Iterator<ServerInfo> setIter = _servers.iterator();
      for (int i = 0; i <= _curServerIdx; ++i) serverInfo = setIter.next();

      try
      {
        relayConn = _sourcesConn.getRelayConnFactory().createRelayConnection(
            serverInfo, this, _remoteExceptionHandler);
        _log.info("picked a relay:" + serverInfo.toSimpleString());
      }
      catch (Exception e)
      {
        _log.error("Unable to get connection to relay:" + serverInfo.toSimpleString(), e);
      }
    }

    _status.setRetriesCounter(originalCounter);

    if (!checkForShutdownRequest())
    {
      _curServer = serverInfo;

      if (null == relayConn)
      {
        if (_currentState.isRelayFellOff())
        {
          boolean enqueueMessage = false;

          try
          {
            enqueueMessage = onRelayFellOff(curState,
                              curState.getCheckpoint(),
                              new ScnNotFoundException("Retries on SCNNotFoundException exhausted !!"));
          } catch (InterruptedException ie) {
                _log.error("interrupted while processing onRelayFellOff", ie);
                curState.switchToPickServer();
                enqueueMessage(curState);
          } catch (InvalidEventException e) {
                _log.error("error trying to notify dispatcher of bootstrapping :" + e.getMessage(), e);
                curState.switchToPickServer();
                enqueueMessage(curState);
          }

          if ( enqueueMessage )
            enqueueMessage(curState);
        } else {
          // There are no retries left. Invoke an onError callback
          try
          {
            _log.info("Puller retries exhausted. Injecting an error event on dispatcher queue to invoke onError callback");
            _remoteExceptionHandler.handleException(new PullerRetriesExhaustedException());
          } catch (InterruptedException ie){
            _log.error("Interrupted while processing retries exhausted", ie);
          } catch (InvalidEventException e) {
            _log.error("Error trying to notify dispatcher of puller retries getting exhausted", e);
          }
          _log.error("Cannot find a relay");
        }
      }
      else
      {
        DatabusRelayConnection oldRelayConn = curState.getRelayConnection();

        if ( null != oldRelayConn)
        {
          resetConnectionAndSetFlag();
        }

        sendHeartbeat(_sourcesConn.getUnifiedClientStats());

        _log.info("Relay Puller switching to request sources");
        curState.switchToRequestSources(serverInfo, serverInfo.getAddress(), relayConn);
        _lastOpenConnection = relayConn;
        enqueueMessage(curState);
      }
    }
  }

  protected void doRequestSources(final ConnectionState curState)
  {
    if (null != _relayCallsStats) _relayCallsStats.registerSourcesCall();
    _log.debug("Sending /sources request");
    curState.switchToSourcesRequestSent();
    sendHeartbeat(_sourcesConn.getUnifiedClientStats());
    curState.getRelayConnection().requestSources(curState);
  }

  /**
   * Build the string of subs to be sent in the register call.
   * */
  private String buildSubsList(List<DatabusSubscription> subs, Map<String, IdNamePair> sourceNameMap) {
    StringBuilder sb = new StringBuilder(128);
    sb.append("[");
    boolean first = true;
    for(DatabusSubscription sub :subs) {
      if(!first)
        sb.append(',');
      DatabusSubscription realSub = sub;
      LogicalSource ls = sub.getLogicalSource();
      if (!ls.idKnown() && !ls.isWildcard())
      {
        IdNamePair sourceEntry = sourceNameMap.get(ls.getName());
        if (null == sourceEntry)
        {
          //this should never happen
          throw new RuntimeException("FATAL! unable to find logical source " + ls.getName() + " in "
                                     + sourceNameMap);
        }
        realSub = new DatabusSubscription(
            sub.getPhysicalSource(), sub.getPhysicalPartition(),
            new LogicalSourceId(new LogicalSource(sourceEntry.getId().intValue(), ls.getName()),
                                sub.getLogicalPartition().getId()));
      }
      sb.append(realSub.toJsonString());
      first = false;
    }
    sb.append("]");
    return sb.toString();
  }

  protected void doSourcesResponseSuccess(ConnectionState curState)
  {

    mergeRelayCallsStats();

    Map<String, IdNamePair> sourceNameMap = curState.getSourcesNameMap();
    StringBuilder sb = new StringBuilder();
    boolean firstSource = true;
    boolean error = false;
    List<IdNamePair> sourcesList = new ArrayList<IdNamePair>(_sourcesConn.getSourcesNames().size());
    if (curState.getRelayConnection().getProtocolVersion() < 3)
    {
      // Generate the list of sources for the sources= parameter to the /stream call.
      // This is needed only for version 2 of the protocol.  For version 3 and higher,
      // we pass a list of subscriptions.
      for (String sourceName: _sourcesConn.getSourcesNames())
      {
        IdNamePair source = sourceNameMap.get(sourceName);
        if (null == source)
        {
          _log.error("Source not found on server: " + sourceName);
          error = true;
          break;
        }
        else
        {
          if (! firstSource) sb.append(',');
          sb.append(source.getId().toString());
          firstSource = false;
          sourcesList.add(source);
        }
      }
    }
    String sourcesIdList = sb.toString();
    String subsString = error ? "ERROR" : buildSubsList(curState.getSubscriptions(), sourceNameMap);
    if (_log.isDebugEnabled())
    {
      _log.debug("Source ids: " + sourcesIdList);
      _log.debug("Subs : " + subsString);
    }

    _sourcesConn.getRelayDispatcher().enqueueMessage(
        SourcesMessage.createSetSourcesIdsMessage(curState.getSources(), sourcesIdList));

    if (toTearConnAfterHandlingResponse())
    {
      tearConnectionAndEnqueuePickServer();
    } else {
      if (error)
      {
        curState.switchToPickServer();
      }
      else
      {
        curState.switchToRequestSourcesSchemas(sourcesIdList, subsString);

        if (_sourcesConn.isBootstrapEnabled())
        {
          _sourcesConn.getBootstrapPuller().enqueueMessage(
              SourcesMessage.createSetSourcesIdsMessage(curState.getSources(),
                                                        curState.getSourcesIdListString()));
        }
        String hostHdr = curState.getHostName(), svcHdr = curState.getSvcName();
        _log.info("Connected to relay " + hostHdr + " with a service identifier " + svcHdr);
      }
      enqueueMessage(curState);
    }
  }

  protected void doRequestRegister(ConnectionState curState)
  {
    if (null != _relayCallsStats) _relayCallsStats.registerRegisterCall(EMPTY_REGISTER_LIST);
    _log.debug("Sending /sources request");
    curState.swichToRegisterRequestSent();
    sendHeartbeat(_sourcesConn.getUnifiedClientStats());
    curState.getRelayConnection().requestRegister(curState.getSourcesIdListString(), curState);
  }

  protected void doRegisterResponseSuccess(ConnectionState curState)
  {
    boolean enqueueMessage = true;
    mergeRelayCallsStats();

    if(curState.getSourcesSchemas().size() < _sourcesConn.getSourcesNames().size())
    {
      _log.error("Expected " + _sourcesConn.getSourcesNames().size() + " schemas, got: " +
                curState.getSourcesSchemas().size());
      curState.switchToPickServer();
    }
    else
    {
      _sourcesConn.getRelayDispatcher().enqueueMessage(
          SourcesMessage.createSetSourcesSchemasMessage(
              curState.getSourcesSchemas(),
              curState.getMetadataSchemas()
              )
          );

      // Determine the checkpoint for read events in the following order
      // 1. Existing checkpoint in the current state
      // 2. Checkpoint persisted on disk
      // 3. New checkpoint

      Checkpoint cp = _currentState.getCheckpoint();

      if (null == cp)
      {
        _log.info("no existing checkpoint found; attempting to load persistent checkpoint");
        cp = _sourcesConn.loadPersistentCheckpoint();
      }

      if (null == cp)
      {
        _log.info(getName() + ": no checkpoint found");
        cp = new Checkpoint();

        // set the mode to streaming first so relay will inspect the scn
        cp.setConsumptionMode(DbusClientMode.ONLINE_CONSUMPTION);

        // setting windowScn makes server to return scn not found error
        cp.setWindowScn(0L);
        cp.clearBootstrapStartScn();

        if (_isConsumeCurrent)
        {
          cp.setFlexible();
          LOG.info("Setting flexible checkpoint: consumeCurrent is true");
        }
      }
      else
      {
        _log.info("persisted checkpoint loaded: " + cp.toString());
      }

      if ( cp.getFlexible())
        curState.setFlexibleCheckpointRequest(true);

      if (toTearConnAfterHandlingResponse())
      {
        tearConnectionAndEnqueuePickServer();
        enqueueMessage = false;
      }
      else
      {
        if (_sourcesConn.isBootstrapEnabled())
        {
          _sourcesConn.getBootstrapPuller().enqueueMessage(
              SourcesMessage.createSetSourcesSchemasMessage(
                  curState.getSourcesSchemas(), curState.getMetadataSchemas()));
        }

        if (DbusClientMode.BOOTSTRAP_SNAPSHOT == cp.getConsumptionMode() ||
            DbusClientMode.BOOTSTRAP_CATCHUP == cp.getConsumptionMode())
        {
          curState.setRelayFellOff(true);

          if (_sourcesConn.isBootstrapEnabled())
          {
            curState.switchToBootstrap(cp);
          }
          else
          {
            String message = "bootstrap checkpoint found but bootstrapping is disabled:" + cp;
            _log.error(message);
            _status.suspendOnError(new DatabusException(message));
            enqueueMessage = false;
          }
        }
        else
        {
          if (cp.getWindowOffset() > 0)
          {
            // switched when in middle of Window
            LOG.info("RelayPuller reconnecting when in middle of event window. Will regress. Current Checkpoint :" + cp);
            if (cp.getPrevScn() > 0)
            {
              cp.setWindowScn(cp.getPrevScn());
              cp.setWindowOffset(-1);
              curState.setSCNRegress(true);
            } else if (curState.isFlexibleCheckpointRequest()) {
              LOG.info("Switched relays after reading partial window with flexible checkpoint !!");
              cp.setFlexible();
                curState.setSCNRegress(true);
              } else {
              LOG.fatal("Checkpoint does not have prevSCN !!. Suspending !! Checkpoint :" + cp);
              enqueueMessage(LifecycleMessage.createSuspendOnErroMessage(
                  new Exception("Checkpoint does not have prevSCN !!. Suspending !! Checkpoint :" + cp)));
              enqueueMessage = false;
            }
          }
        
          if ( enqueueMessage )
            curState.switchToRequestStream(cp);
        }
      }
    }

    if ( enqueueMessage)
      enqueueMessage(curState);
  }

  protected void doRequestStream(ConnectionState curState)
  {
    boolean debugEnabled = _log.isDebugEnabled();

    if (debugEnabled) _log.debug("Checking for free space in buffer");
    int freeBufferThreshold=(int)(_sourcesConn.getConnectionConfig().getFreeBufferThreshold() *
        100.0 / _pullerBufferUtilizationPct);
    try
    {
      curState.getDataEventsBuffer().waitForFreeSpace(freeBufferThreshold);
    }
    catch (InterruptedException ie)
    {
      //loop
      enqueueMessage(curState);
      return;
    }

    Checkpoint cp = curState.getCheckpoint();
    if (debugEnabled) _log.debug("Checkpoint at RequestDataEvents: " + cp.toString());

    if ( null == _relayFilter)
    {
      if (debugEnabled) _log.debug("Initializing relay filter config");
      _relayFilter = new DbusKeyCompositeFilter();
      Map<String, IdNamePair> srcNameIdMap = curState.getSourcesNameMap();

      for (DbusKeyCompositeFilterConfig conf : _relayFilterConfigs)
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
        if (debugEnabled) _log.debug("FilterConfMap is :" + fConfMap);
        _relayFilter.merge(new DbusKeyCompositeFilter(fConfMap));
      }

      if (debugEnabled)
        _log.debug("Merged Filter (before deduping) is :" + _relayFilter);
      _relayFilter.dedupe();
      if (debugEnabled)
        _log.debug("Merged Filter (after deduping) is :" + _relayFilter);
    }

    _streamCallStartMs = System.currentTimeMillis();
    if (null != _relayCallsStats)
      _relayCallsStats.registerStreamRequest(cp, EMPTY_STREAM_LIST);
    int fetchSize = (int)((curState.getDataEventsBuffer().getBufferFreeReadSpace() / 100.0) * _pullerBufferUtilizationPct);
    fetchSize = Math.max(freeBufferThreshold, fetchSize);
    CheckpointMult cpMult = new CheckpointMult();
    String args;
    if (curState.getRelayConnection().getProtocolVersion() >= 3) {
      // for version 3 and higher we pass subscriptions
      args = curState.getSubsListString();
      for (DatabusSubscription sub : curState.getSubscriptions()) {
        PhysicalPartition p = sub.getPhysicalPartition();
        cpMult.addCheckpoint(p, cp);
      }
    } else {
      args = curState.getSourcesIdListString();
      cpMult.addCheckpoint(PhysicalPartition.ANY_PHYSICAL_PARTITION, cp);
    }

    curState.switchToStreamRequestSent();
    sendHeartbeat(_sourcesConn.getUnifiedClientStats());
    curState.getRelayConnection().requestStream(
        args,
        _relayFilter,
        fetchSize,
        cpMult,
        _sourcesConn.getConnectionConfig().getKeyRange(),
        curState);
  }

  protected void doReadDataEvents(ConnectionState curState)
  {
    boolean debugEnabled = _log.isDebugEnabled();
    boolean enqueueMessage = true;
    try
    {
      ChunkedBodyReadableByteChannel readChannel = curState.getReadChannel();
      Checkpoint cp = curState.getCheckpoint();

      curState.setRelayFellOff(false);

      String remoteErrorName = RemoteExceptionHandler.getExceptionName(readChannel);
      Throwable knownRemoteError = _remoteExceptionHandler.getException(readChannel);

      if (null != knownRemoteError &&
          knownRemoteError instanceof ScnNotFoundException)
      {
        if (toTearConnAfterHandlingResponse())
        {
          tearConnectionAndEnqueuePickServer();
          enqueueMessage = false;
        } else {
          curState.setRelayFellOff(true);

          if ( _retriesOnFallOff.getRemainingRetriesNum() > 0 )
          {
            _log.error("Got SCNNotFoundException. Retry (" + _retriesOnFallOff.getRetriesNum() +
                       ") out of " + _retriesOnFallOff.getConfig().getMaxRetryNum());
            curState.switchToPickServer();
          } else {
            enqueueMessage = onRelayFellOff(curState, cp, knownRemoteError);
          }
        }
      }
      else if (null != remoteErrorName)
      {
        if (toTearConnAfterHandlingResponse())
        {
          tearConnectionAndEnqueuePickServer();
          enqueueMessage = false;
        } else {
        //remote processing error
        _log.error("read events error: " + RemoteExceptionHandler.getExceptionMessage(readChannel));
        curState.switchToStreamResponseError();
        }
      }
      else
      {
        /*DispatcherState dispatchState = curState.getDispatcherState();
          dispatchState.switchToDispatchEvents();
          _dispatcherThread.addNewStateBlocking(dispatchState);*/
        if (debugEnabled) _log.debug("Sending events to buffer");

        DbusEventsStatisticsCollector connCollector = _sourcesConn.getInboundEventsStatsCollector();

        if (curState.isSCNRegress())
        {
          LOG.info("SCN Regress requested !! Sending a SCN Regress Message to dispatcher. Curr Ckpt :" + curState.getCheckpoint());

          DbusEvent regressEvent = getEventFactory().createSCNRegressEvent(new SCNRegressMessage(curState.getCheckpoint()));

          writeEventToRelayDispatcher(curState, regressEvent, "SCN Regress Event from ckpt :" + curState.getCheckpoint());
          curState.setSCNRegress(false);
        }

        UnifiedClientStats unifiedClientStats = _sourcesConn.getUnifiedClientStats();
        if (unifiedClientStats != null)
        {
          unifiedClientStats.setBootstrappingState(false);  // failsafe:  we're definitely not bootstrapping here
          sendHeartbeat(unifiedClientStats);
        }
        int eventsNum = curState.getDataEventsBuffer().readEvents(readChannel,
                                                                  curState.getListeners(),
                                                                  connCollector);

        boolean resetConnection = false;
        if (eventsNum > 0) {
          _timeSinceEventsSec = System.currentTimeMillis();
          cp.checkPoint();
        } else {           // no need to checkpoint if nothing returned from relay
          // check how long it has been since we got some events
          if (_remoteExceptionHandler.getPendingEventSize(readChannel) >
               curState.getDataEventsBuffer().getMaxReadBufferCapacity())
          {
            // The relay had a pending event that we can never accommodate. This is fatal error.
            String err = "ReadBuffer max capacity(" + curState.getDataEventsBuffer().getMaxReadBufferCapacity() +
                      ") is less than event size(" +
                      _remoteExceptionHandler.getPendingEventSize(readChannel) +
                      "). Increase databus.client.connectionDefaults.eventBuffer.maxEventSize and restart.";
            _log.fatal(err);
            enqueueMessage(LifecycleMessage.createSuspendOnErroMessage(new PendingEventTooLargeException(err)));
            return;
          }
          else
          {
            if(_noEventsConnectionResetTimeSec > 0) { // unless the feature is disabled (<=0)
              resetConnection = (System.currentTimeMillis() - _timeSinceEventsSec)/1000 > _noEventsConnectionResetTimeSec;
              if(resetConnection) {
                _timeSinceEventsSec = System.currentTimeMillis();
                LOG.warn("about to reset connection to relay " + curState.getServerInetAddress() +
                       ", because there were no events for " + _noEventsConnectionResetTimeSec + "secs");
              }
            }
          }
        }

        if (debugEnabled) _log.debug("Events read: " + eventsNum);

        // if it has been too long since we got non-empty responses - the relay may be stuck, try to reconnect
        if (toTearConnAfterHandlingResponse() || resetConnection)
        {
          tearConnectionAndEnqueuePickServer();
          enqueueMessage = false;
        } else {
          curState.switchToStreamResponseDone();
          resetServerRetries();
        }
      }

      if ( enqueueMessage)
      enqueueMessage(curState);
    }
    catch (InterruptedException ie)
    {
      _log.warn("interrupted", ie);
      curState.switchToStreamResponseError();
      enqueueMessage(curState);
    }
    catch (InvalidEventException e)
    {
      _log.error("error reading events from server:" + e, e);
      curState.switchToStreamResponseError();
      enqueueMessage(curState);
    }
    catch (RuntimeException e)
    {
      _log.error("runtime error reading events from server: " + e, e);
      curState.switchToStreamResponseError();
      enqueueMessage(curState);
    }
  }

  protected void doStreamResponseDone(ConnectionState curState)
  {
    Checkpoint cp = curState.getCheckpoint();
    if (_log.isDebugEnabled())
    {
      _log.debug("Data events done");
      _log.debug("Checkpoint at EventsDone: " + cp.toString());
    }

    if (curState.isFlexibleCheckpointRequest()
        && (curState.getDataEventsBuffer().getSeenEndOfPeriodScn() > 0 ))
      curState.setFlexibleCheckpointRequest(false);

    if (null != _sourcesConn.getLocalRelayCallsStatsCollector())
    {
      long streamDuration = System.currentTimeMillis() - _streamCallStartMs;
      _sourcesConn.getLocalRelayCallsStatsCollector().registerStreamResponse(streamDuration);
      mergeRelayCallsStats();
    }

    _status.getRetriesCounter().sleep();

    curState.switchToRequestStream(cp);
    enqueueMessage(curState);
  }


  protected void doBootstrap(ConnectionState curState)
  {
    if (null != _lastOpenConnection)
    {
      _lastOpenConnection.close();
      _lastOpenConnection = null;
    }

    Checkpoint bootstrapCkpt = null;
    if (_sourcesConn.getBootstrapPuller() == null) {
      _log.warn("doBootstrap got called, but BootstrapPullThread is null. Is bootstrap disabled?");
      return;
    }
    try
    {
      bootstrapCkpt = curState.getCheckpoint().clone();
    }
    catch (Exception e)
    {
      String msg  = "Error copying checkpoint:" + curState.getCheckpoint();
      _log.error(msg, e);
      BootstrapResultMessage bootstrapResultMessage = BootstrapResultMessage.createBootstrapFailedMessage(e);
      doBootstrapFailed(bootstrapResultMessage);
      return;
    }

    if (!bootstrapCkpt.isBootstrapStartScnSet())
    {
      bootstrapCkpt = curState.getBstCheckpointHandler().createInitialBootstrapCheckpoint(bootstrapCkpt,
                                                                                          bootstrapCkpt.getWindowScn());
      //bootstrapCkpt.setBootstrapSinceScn(Long.valueOf(bootstrapCkpt.getWindowScn()));
    }

    _log.info("Bootstrap begin: sinceScn=" + bootstrapCkpt.getWindowScn());

    CheckpointMessage bootstrapCpMessage = CheckpointMessage.createSetCheckpointMessage(bootstrapCkpt);
    _sourcesConn.getBootstrapPuller().enqueueMessage(bootstrapCpMessage);

    try
    {
      Checkpoint cpForDispatcher = new Checkpoint(bootstrapCkpt.toString());
      cpForDispatcher.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
      DbusEvent cpEvent = getEventFactory().createCheckpointEvent(cpForDispatcher);
      writeEventToRelayDispatcher(curState, cpEvent, "Control Event to start bootstrap");
      curState.switchToBootstrapRequested();
    } catch (InterruptedException ie) {
      _log.error("Got interrupted while writing control message to bootstrap !!", ie);
      enqueueMessage(LifecycleMessage.createSuspendOnErroMessage(ie));
    } catch (Exception e) {
      enqueueMessage(LifecycleMessage.createSuspendOnErroMessage(e));
      _log.error("Exception occured while switching to bootstrap: ", e);
    }

    //wait for bootstrap to finish
    /*boolean bootstrapDone = false;
    while (! bootstrapDone)
    {
      try
      {
        bootstrapPullerThread.join();
        bootstrapDone = true;
      }
      catch (InterruptedException ie) {}
    }*/
  }

  private void mergeRelayCallsStats()
  {
    if (null != _sourcesConn.getLocalRelayCallsStatsCollector() &&
        null != _sourcesConn.getRelayCallsStatsCollector() &&
        _unmergedHttpCallsStats >= RELAY_CALLS_MERGE_FREQ)
    {
      _sourcesConn.getRelayCallsStatsCollector().merge(_sourcesConn.getLocalRelayCallsStatsCollector());
      _sourcesConn.getLocalRelayCallsStatsCollector().reset();
      _unmergedHttpCallsStats = 0;
    }
    else
    {
      ++_unmergedHttpCallsStats;
    }
  }

  private void processSourcesRequestError(ConnectionState state)
  {
  _log.info("Sources Request Error");

    if (null != _relayCallsStats) _relayCallsStats.registerInvalidSourceRequest();

    if (toTearConnAfterHandlingResponse())
    {
      tearConnectionAndEnqueuePickServer();
    } else {
      state.switchToPickServer();
      enqueueMessage(state);
    }
  }

  private void processSourcesResponseError(ConnectionState state)
  {
  _log.info("Sources Response Error");

    if (null != _relayCallsStats) _relayCallsStats.registerInvalidSourceRequest();
    if (toTearConnAfterHandlingResponse())
    {
      tearConnectionAndEnqueuePickServer();
    } else {
      state.switchToPickServer();
      enqueueMessage(state);
    }
  }

  private void processRegisterRequestError(ConnectionState state)
  {
  _log.info("Register Request Error");
    if (null != _relayCallsStats) _relayCallsStats.registerInvalidRegisterCall();
    if (toTearConnAfterHandlingResponse())
    {
      tearConnectionAndEnqueuePickServer();
    } else {
      state.switchToPickServer();
      enqueueMessage(state);
    }
  }

  private void processRegisterResponseError(ConnectionState state)
  {
  _log.info("Register Response Error");

    if (null != _relayCallsStats) _relayCallsStats.registerInvalidRegisterCall();
    if (toTearConnAfterHandlingResponse())
    {
      tearConnectionAndEnqueuePickServer();
    } else {
      state.switchToPickServer();
      enqueueMessage(state);
    }
  }

  private void processStreamRequestError(ConnectionState state)
  {
  _log.info("Stream Request Error");

    if (null != _relayCallsStats) _relayCallsStats.registerInvalidStreamRequest();
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
  _log.info("Stream Response Error");

    if (null != _relayCallsStats) _relayCallsStats.registerInvalidStreamRequest();
    if (toTearConnAfterHandlingResponse())
    {
      tearConnectionAndEnqueuePickServer();
    } else {
      state.switchToPickServer();
      enqueueMessage(state);
    }
  }

  @Override
  protected void resetConnection()
  {
    DatabusServerConnection conn =  _currentState.getRelayConnection();

    if ( null != conn)
    {
      conn.close();
      _currentState.setRelayConnection(null);
    }
  }

  private boolean onRelayFellOff(ConnectionState curState, Checkpoint cp, Throwable knownRemoteError)
      throws InvalidEventException, InterruptedException
  {
    boolean enqueueMessage = true;
    _log.error("Retries on SCNNotFoundException exhausted !!");
    _retriesOnFallOff.reset();

    //need to bootstrap, suspend or read From Latest SCN
    if (!_sourcesConn.isBootstrapEnabled())
    {
      _log.error("No scn found on relay while no bootstrap services provided:");
      _log.error(" bootstrapServices=" + _sourcesConn.getBootstrapServices() +
          "; bootstrapRegistrations=" + _sourcesConn.getBootstrapRegistrations());

      if (_isReadLatestScnOnErrorEnabled)
      {
        // Read From Latest SCN on Error
        _log.error("Read Latest SCN Window on SCNNotFoundException is enabled. Will start reading from the lastest SCN Window !!");
        curState.getRelayConnection().enableReadFromLatestScn(true);
          _currentState.setRelayFellOff(false);
        curState.switchToStreamResponseDone();
      } else {
        _log.fatal("Got SCNNotFoundException but Read Latest SCN Window and bootstrap are disabled !!");
        _remoteExceptionHandler.handleException(new BootstrapDatabaseTooOldException(knownRemoteError));
        enqueueMessage = false;
      }
    } else {
      _log.info("Requested scn " + cp.getWindowScn() +
          " not found on relay; switching to bootstrap service");
      curState.switchToBootstrap(cp);
      UnifiedClientStats unifiedClientStats = _sourcesConn.getUnifiedClientStats();
      if (unifiedClientStats != null)
      {
        unifiedClientStats.setBootstrappingState(true);
      }
    }
    return enqueueMessage;
  }

  @Override
  protected void tearConnection()
  {
    _currentState.setRelayFellOff(false);
    super.tearConnection();
  }

  /**
   * allows adjusting noEvents threshold dynamically
   * @param noEventsConnectionResetTimeSec - new threshold
   */
  public void setNoEventsConnectionResetTimeSec(long noEventsConnectionResetTimeSec)
  {
    _noEventsConnectionResetTimeSec = noEventsConnectionResetTimeSec;
  }

  protected BackoffTimer getRetryonFallOff()
  {
    return _retriesOnFallOff;
  }

  private void writeEventToRelayDispatcher(ConnectionState curState, DbusEvent event, String message)
          throws InterruptedException, InvalidEventException
  {
    boolean success = false;

    // Create a infinite backoff timer that waits for maximum of 1 sec
    // for writing the control message to evb
    BackoffTimerStaticConfig timerConfig = new BackoffTimerStaticConfig(
        1, 1000, 1, 1, -1);
    BackoffTimer timer = new BackoffTimer("EVB More Space Timer",
        timerConfig);
    timer.reset();

    byte[] eventBytes = new byte[event.size()];

    _log.info("Event size: " + eventBytes.length);
    _log.info("Event:" + event.toString());

    event.getRawBytes().get(eventBytes);

    UnifiedClientStats unifiedClientStats = _sourcesConn.getUnifiedClientStats();

    while ((!success) && (timer.getRemainingRetriesNum() > 0))
    {
      ByteArrayInputStream cpIs = new ByteArrayInputStream(
          eventBytes);
      ReadableByteChannel cpRbc = Channels.newChannel(cpIs);

      sendHeartbeat(unifiedClientStats);
      int ecnt = curState.getDataEventsBuffer().readEvents(cpRbc);
      if (ecnt <= 0) {
        _log.error("Not enough spece in the event buffer to add a control message :" + message);
        boolean interrupted = !timer.backoffAndSleep();

        if (interrupted)
          throw new InterruptedException(
              "Got interrupted while waiting to write control Message to EVB : " + message);
      } else {
        _log.info("Sent a control message :" + message);
        success = true;
      }
    }
   }

  public DatabusRelayConnection getLastOpenConnection()
  {
    return _lastOpenConnection;
  }
}
