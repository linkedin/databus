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


import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import javax.management.MBeanServer;

import org.apache.log4j.Logger;

import com.linkedin.databus.client.ConnectionState.StateId;
import com.linkedin.databus.client.pub.mbean.UnifiedClientStats;
import com.linkedin.databus.client.pub.ServerInfo;
import com.linkedin.databus.core.DatabusComponentStatus;
import com.linkedin.databus.core.DatabusComponentStatus.Status;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventFactory;
import com.linkedin.databus.core.async.AbstractActorMessageQueue;
import com.linkedin.databus.core.async.LifecycleMessage;
import com.linkedin.databus2.core.BackoffTimerStaticConfig;
import com.linkedin.databus2.core.mbean.DatabusReadOnlyStatus;

public abstract class BasePullThread extends AbstractActorMessageQueue
{
  protected Set<ServerInfo> _servers;
  protected int _curServerIdx = -1;
  protected ServerInfo _curServer = null;

  protected final ConnectionState _currentState;
  protected final DatabusComponentStatus _status;
  protected final DatabusReadOnlyStatus _statusMbean;
  protected final Logger _log;

  protected DatabusSourcesConnection _sourcesConn;

  private final DbusEventFactory _eventFactory;

  /* Flag to mark delaying tear connection after server-set change if client is waiting for response */
  private boolean tearConnAfterResponse = false;

  private final MBeanServer _mbeanServer;

  /*  Filter to clear message queue before adding PickServer */
  private final MessageQueueFilter pickServerFilter = new PickServerEnqueueFilter();

  public BasePullThread(String name,
                        BackoffTimerStaticConfig pullerRetries,
                        DatabusSourcesConnection sourcesConn,
                        DbusEventBuffer dbusEventBuffer,
                        ConnectionStateFactory connStateFactory,
                        Set<ServerInfo> servers,
                        MBeanServer mbeanServer,
                        DbusEventFactory eventFactory)
  {
    super(name,
          pullerRetries,
          sourcesConn.getConnectionConfig().isPullerMessageQueueLoggingEnabled());
    _log = Logger.getLogger(getClass().getName() + "." + name);
    _sourcesConn = sourcesConn;
    _currentState = connStateFactory.create(dbusEventBuffer);

    if ( null != servers)
      _servers = new TreeSet<ServerInfo>(servers);
    else
      _servers = new TreeSet<ServerInfo>();

    _eventFactory = eventFactory;
    _mbeanServer = mbeanServer;
    _status = new DatabusComponentStatus(name, pullerRetries);
    _statusMbean = new DatabusReadOnlyStatus(getName(), _status, -1);
    _statusMbean.registerAsMbean(_mbeanServer);
    resetServerRetries();
  }

  @Override
  protected boolean executeAndChangeState(Object message)
  {
    boolean success = true;
    if (message instanceof ServerSetChangeMessage)
    {
      ServerSetChangeMessage serverSetChangeMsg = (ServerSetChangeMessage)message;

      switch (serverSetChangeMsg.getTypeId())
      {
        case SET_SERVERS: doSetServers(serverSetChangeMsg); break;
        case ADD_SERVER: doAddServer(serverSetChangeMsg); break;
        case REMOVE_SERVER: doRemoveServer(serverSetChangeMsg); break;
        default:
        {
          _log.error("Unkown ServerSetChangeMessage in ServerPullThread: " + serverSetChangeMsg.getTypeId());
          success = false;
          break;
        }
      }

      if ( success && (_componentStatus.getStatus() == Status.SUSPENDED_ON_ERROR))
      {
          enqueueMessage(LifecycleMessage.createResumeMessage());
      }
    } else {
      return super.executeAndChangeState(message);
    }
    return success;
  }


  private void doRemoveServer(ServerSetChangeMessage serverSetChangeMsg)
  {
    ServerInfo newServer = serverSetChangeMsg.getServer();

    _log.info("About to remove Server (" + newServer + ") from Server set. Current Server set is :" + _servers);

    if (null == newServer)
    {
      _log.error("No Server to remove");
      return;
    }

    if (! _servers.contains(newServer))
    {
      _log.warn("Trying to remove a Server that does not exist:" + newServer.toString());
    }
    else
    {
      _log.info("Removing Server: " + newServer.toString());
      Iterator<ServerInfo> iter = _servers.iterator();
      int index = 0;
      for (; iter.hasNext(); ++index)
      {
        if(newServer.equals(iter.next()))
          break;
      }

      if ( index < _curServerIdx )
      {
        _curServerIdx--;
      } else if ( index == _curServerIdx) {
        _log.info("Trying to remove the active Server !!");
        handleServerSwitch();
        _curServerIdx = -1;
        _curServer = null;
      }
      _servers.remove(newServer);
    }
  }


  /*
   * Updates the server set.
   *
   * Logic: Compares the existing set and new set of servers.
   * If the currentServer ( connected Server) is available in the new set, then
   *     connection is retained and
   *     currentServerIdx is set such that it is pointing to the connected Server in the new set
   * else
   *     connection is closed and
   *     currentServerIdx is set to -1
   *
   */
  protected void doSetServers(ServerSetChangeMessage serverSetChangeMsg)
  {
    Set<ServerInfo> ServerSet =
        (null == serverSetChangeMsg.getServerSet()) ? null : new TreeSet<ServerInfo>(serverSetChangeMsg.getServerSet());

    if ( (ServerSet != null ) && (_servers != null) && _servers.equals(ServerSet))
    {
      _log.info("doSetServers : Both old set and new set is same. Skipping this message. ServerSet is :" + ServerSet);
      return;
    }

    boolean tearConnection = (_curServer != null)
        && ((null == ServerSet) || ( ! ServerSet.contains(_curServer)));

    _log.info("About to change Server set. Old Server set was :" + _servers + ", New Server Set is :" + ServerSet);

    _servers.clear();

    if ( tearConnection)
    {
      handleServerSwitch();

      if (null != ServerSet)
      {
        _servers.addAll(ServerSet);
      }
    } else {
      _servers.addAll(ServerSet);

      if ( _curServer != null)
      {
        // Set the index correctly
        Iterator<ServerInfo> iter = ServerSet.iterator();
        int index = 0;
        for (; iter.hasNext(); ++index)
        {
          if(_curServer.equals(iter.next()))
            break;
        }
        _curServerIdx = index;
      } else {
        _curServerIdx = -1;
        _curServer = null;
      }
    }
    resetServerRetries();
  }

  private void doAddServer(ServerSetChangeMessage serverSetChangeMsg)
  {
    ServerInfo newServer = serverSetChangeMsg.getServer();

    _log.info("About to add new Server (" + newServer + ") to Server set. Current Server set is :" + _servers);

    if (null == newServer)
    {
      _log.error("No new Server to add");
      return;
    }

    if (_servers.contains(newServer))
    {
      _log.warn("Server already exists:" + newServer.toString() + " Skipping this addition !!");
    }
    else
    {
      _log.info("Adding new Server: " + newServer.toString());
      _servers.add(newServer);
    }

    resetServerRetries();
  }

  protected void resetServerRetries()
  {
    _status.resume();
  }

  @Override
  public void shutdown()
  {
    if (_statusMbean != null)
    {
      _statusMbean.unregisterMbean(_mbeanServer);
      _log.info("mbean unregistered");
    }
    super.shutdown();
  }

  protected void backoffOnPullError()
  {
    if (_status.isRunningStatus()) _status.retryOnError("pull error");
    else _status.retryOnLastError();
  }

  @Override
  protected boolean shouldRetainMessageOnPause(Object msg)
  {
    if (msg instanceof ServerSetChangeMessage)
      return true;

    return super.shouldRetainMessageOnPause(msg);
  }

  @Override
  protected boolean shouldRetainMessageOnSuspend(Object msg)
  {
    if (msg instanceof ServerSetChangeMessage)
      return true;

    return super.shouldRetainMessageOnPause(msg);
  }

  /*
   * Allow subclasses to decide whether to tear active connection now or after getting
   * response to an outstanding request (or after bootstrap).
   */
  protected abstract boolean shouldDelayTearConnection(StateId stateId);

  /*
   * Allow subclass to close the server connection (relay/bootstrap-server)
   */
  protected abstract void resetConnection();

  /**
   *
   * Tear Connection now or set up for tearing later
   *
   * Contract:
   *   a) Subclass decides when to tear the active connection (tear now or later)
   *   b) Subclass closes the existing connection
   *   c)   If the Component status is not suspended or Paused, switch to Pick_server state.
   *
   *  If the Component status is suspended_on_error, then resume() message is added for all ServerSetChange
   *  Messages whether it affects the current connection or not.
   *
   */
  protected  void handleServerSwitch()
  {
    boolean delayTear = shouldDelayTearConnection(_currentState.getStateId());

    if ( ! delayTear )
    {
      tearConnection();
      Status currStatus = _status.getStatus();
      /*
       *  If it user - paused, then dont resume
       *  If not started, dont transition to pickServer (common to BootstrapPullThread)
       */
      if ((currStatus != Status.PAUSED)  &&
          (currStatus != Status.SUSPENDED_ON_ERROR) &&
          (_currentState.getStateId() != StateId.INITIAL))
      {
        enqueuePickServer(_currentState);
      }
    } else {
      tearConnAfterResponse = true;
    }
  }

  /*
   * Tear Connection and reset member variables associated with it
   */
  protected void resetConnectionAndSetFlag()
  {
    resetConnection();
    tearConnAfterResponse = false;
  }

  protected void killConnection()
  {
    _currentState.getRelayConnection().close();
  }

  /*
   * Tear Connection and reset member variables associated with it
   */
  protected void tearConnection()
  {
     resetConnectionAndSetFlag();
    _curServer = null;
    _curServerIdx = -1;
  }

  protected void tearConnectionAndEnqueuePickServer()
  {
    tearConnection();
    enqueuePickServer(_currentState);
  }

  protected boolean toTearConnAfterHandlingResponse()
  {
    return tearConnAfterResponse;
  }

  /**
   * Clears the message queue of any pending ConnectionState Messages and enqueues the PickServer message
   */
  protected void enqueuePickServer(ConnectionState connState)
  {
    connState.switchToPickServer();
    enqueueMessageAfterFilter(connState, pickServerFilter);
  }

  /**
   * Used when enqueing pickServer state as a result of Server-Set CHange.
   * This filter ensures that message queue does not contain any more ConnectionState message before the PickServer
   * message.
   */
  private class PickServerEnqueueFilter implements MessageQueueFilter
  {
    @Override
    public boolean shouldRetain(Object msg)
    {
      return shouldRetainMessageOnPause(msg);
    }

  }

  public Set<ServerInfo> getServers()
  {
    return _servers;
  }

  public int getCurrentServerIdx()
  {
    return _curServerIdx;
  }

  public ServerInfo getCurentServer()
  {
    return _curServer;
  }

  public ConnectionState getConnectionState()
  {
    return _currentState;
  }

  public DatabusSourcesConnection getSourcesConnection()
  {
    return _sourcesConn;
  }

  public void setSourcesConnection(DatabusSourcesConnection conn)
  {
    _sourcesConn = conn;
  }

  @Override
  protected Object preEnqueue(Object message)
  {
    Object ret = message;
    if ( message instanceof ConnectionState)
    {
      ConnectionState state = (ConnectionState)message;
      ConnectionStateMessage stateMsg = new ConnectionStateMessage(state.getStateId(), state);
      ret = stateMsg;
    }
    return ret;
  }

  /**
   * @return the log
   */
  protected Logger getLog()
  {
    return _log;
  }

  protected DbusEventFactory getEventFactory()
  {
    return _eventFactory;
  }

  protected static void sendHeartbeat(UnifiedClientStats unifiedClientStats)
  {
    sendHeartbeat(unifiedClientStats, System.currentTimeMillis());
  }

  protected static void sendHeartbeat(UnifiedClientStats unifiedClientStats, long timestampMs)
  {
    if (unifiedClientStats != null)
    {
      unifiedClientStats.setHeartbeatTimestamp(timestampMs);
    }
  }

}
