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


import java.util.Set;

import com.linkedin.databus.client.pub.ServerInfo;

/**
 * A message that denotes the change of membership in a set of servers. Available messages are
 *
 * <ul>
 *   <li>SET_SERVERS - sets the entire set</li>
 *   <li>ADD_SERVER  - adds a server to the set</li>
 *   <li>REMOVE_SERVER  - removes a server from the set</li>
 * </ul>
 *
 * @author cbotev
 *
 */
public class ServerSetChangeMessage
{

  public enum TypeId
  {
    SET_SERVERS,
    ADD_SERVER,
    REMOVE_SERVER,
  }

  private TypeId _typeId;
  private ServerInfo _server;
  private Set<ServerInfo> _serverSet;

  private ServerSetChangeMessage(TypeId typeId, ServerInfo server, Set<ServerInfo> serverSet)
  {
    _typeId = typeId;
    _server = server;
    _serverSet = serverSet;
  }

  /** Creates new SET_SERVERS message */
  public static ServerSetChangeMessage createSetServersMessage(Set<ServerInfo> serverSet)
  {
    return new ServerSetChangeMessage(TypeId.SET_SERVERS, null, serverSet);
  }

  /** Creates new ADD_SERVER message */
  public static ServerSetChangeMessage createAddServerMessage(ServerInfo server)
  {
    return new ServerSetChangeMessage(TypeId.ADD_SERVER, server, null);
  }

  /** Creates new REMOVE_SERVER message */
  public static ServerSetChangeMessage createRemoveServerMessage(ServerInfo server)
  {
    return new ServerSetChangeMessage(TypeId.REMOVE_SERVER, server, null);
  }

  /**
   * Switches the message to a SET_SERVERS message
   * @param  serverSet          the new server set
   * @return this message object
   */
  public ServerSetChangeMessage switchToSetServers(Set<ServerInfo> serverSet)
  {
    _typeId = TypeId.SET_SERVERS;
    _server = null;
    _serverSet = serverSet;

    return this;
  }

  /**
   * Switches the message to a ADD_SERVER message
   * @param  server           the server to add
   * @return this message object
   */
  public ServerSetChangeMessage switchToAddServer(ServerInfo server)
  {
    _typeId = TypeId.ADD_SERVER;
    _server = server;
    _serverSet = null;

    return this;
  }

  /**
   * Switches the message to a REMOVE_SERVER message
   * @param  server           the server to remove
   * @return this message object
   */
  public ServerSetChangeMessage switchToRemoveServer(ServerInfo server)
  {
    _typeId = TypeId.REMOVE_SERVER;
    _server = server;
    _serverSet = null;

    return this;
  }

  /** Returns the type of the message */
  public TypeId getTypeId()
  {
    return _typeId;
  }

  /** Returns the server parameter of the message; meaningful only for ADD_SERVER and REMOVE_SERVER*/
  public ServerInfo getServer()
  {
    return _server;
  }

  /** Returns the server set parameter of the message; meaningful only for SET_SERVERS*/
  public Set<ServerInfo> getServerSet()
  {
    return _serverSet;
  }


  @Override  
  public String toString() {
	return "" + _typeId;
  }

  
}
