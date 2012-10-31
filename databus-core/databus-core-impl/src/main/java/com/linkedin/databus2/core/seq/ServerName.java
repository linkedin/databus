package com.linkedin.databus2.core.seq;

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