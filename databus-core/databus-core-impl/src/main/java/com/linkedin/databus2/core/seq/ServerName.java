package com.linkedin.databus2.core.seq;
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


import com.linkedin.databus.core.NamedObject;

/** Pusher name for bookeeping purposes*/
public class ServerName implements NamedObject
{
  private int _serverId;
  private String _serverName;

  public ServerName(int id)
  {
    setId(id);
  }

  public void setId(int id)
  {
    _serverId = id;
    _serverName = "server_" + id;
  }

  public int getId()
  {
    return _serverId;
  }

  @Override
  public String getName()
  {
    return _serverName;
  }

  @Override
  public String toString()
  {
    return getName();
  }

  @Override
  public boolean equals(Object o)
  {
    if (null == o || !(o instanceof ServerName)) return false;
    return equalsServerName((ServerName)o);
  }

  @Override
  public int hashCode()
  {
    return _serverId;
  }

  public boolean equalsServerName(ServerName sn)
  {
    return _serverId == sn._serverId;
  }

}
