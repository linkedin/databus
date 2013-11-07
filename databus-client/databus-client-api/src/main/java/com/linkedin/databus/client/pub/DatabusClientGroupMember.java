package com.linkedin.databus.client.pub;
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


import java.util.List;

import org.apache.log4j.Logger;

import com.linkedin.databus.groupleader.impl.zkclient.GroupLeadershipConnectionZkClientImpl;
import com.linkedin.databus.groupleader.pub.AcceptLeadershipCallback;
import com.linkedin.databus.groupleader.pub.GroupLeadershipConnection;
import com.linkedin.databus.groupleader.pub.GroupLeadershipInfo;
import com.linkedin.databus.groupleader.pub.GroupLeadershipSession;

/**
 *
 * @author snagaraj
 * Client group member that specifies a domain, group and name. After joining a group, the member can be ready to accept and relinquish leadership
 * responsibly using a previously existing connection to a shared state .
 * The client can either wait to acquire leadership ; or be notified of leadership asynchronously
 * The client has the responsibility to leave the group; if it couldn't complete leadership responsibilities
 * The client also has the facility to read and write shared data; the assumption here is that only the leader can write/read/remove data ;
 */
public class DatabusClientGroupMember
{
  private final GroupLeadershipConnection _groupConnection;
  private final AcceptLeadershipCallback _callback;
  private GroupLeadershipSession _groupSession;

  private final String _groupName ;
  private final String _domainName;
  private final String _nodeName ;

  private final boolean _requiresMastership ;

  private final Logger _log = Logger.getLogger(getClass());
  private String _sharedDataPath="shareddata";


  /**
   *
   * @author snagaraj
   * Inner class that provides the default call back if none provided; notifies outer object;
   */
  protected class DefaultLeaderCallBack  implements AcceptLeadershipCallback
  {
    @Override
    public  void doAcceptLeadership(GroupLeadershipSession groupLeadershipSession)
    {
      synchronized (DatabusClientGroupMember.this) {
          _log.info("Node acquired leadership: " + DatabusClientGroupMember.this.toString());
          DatabusClientGroupMember.this.notify();
      }
    }

  }

  public DatabusClientGroupMember(String domainName,String groupName, String nodeName, String sharedDataPath,
                                  GroupLeadershipConnection connection,AcceptLeadershipCallback callback,
                                  boolean requiresMastership)
  {
    _domainName = domainName;
    _groupName = groupName;
    _nodeName = nodeName;
    _callback= callback;
    _groupConnection = connection;
    _groupSession = null;
    _sharedDataPath = sharedDataPath;
    _requiresMastership = requiresMastership;
  }

   public synchronized boolean joinWithoutLeadershipDuties()
  {
	  if (null != _groupConnection)
	  {
		 _groupSession =   _groupConnection.joinGroupWithoutLeadershipDuties(getDomainName(),getGroupName(),getName());
		 if ((_sharedDataPath!=null) && !_sharedDataPath.isEmpty())
	     {
	        _groupSession.setSharedDataPath(_sharedDataPath);
	     }
	  }
	  return _groupSession != null;
  }
  
  public DatabusClientGroupMember(String domainName,String groupName, String nodeName, String sharedDataPath, GroupLeadershipConnection connection,AcceptLeadershipCallback callback)
  {
    this(domainName,groupName,nodeName,sharedDataPath, connection,callback,true);

  }

  public DatabusClientGroupMember(String domainName,String groupName, String nodeName, GroupLeadershipConnection connection) {
    this(domainName,groupName,nodeName,null, connection,null,true);
  }

  public DatabusClientGroupMember(String domainName,String groupName, String nodeName, GroupLeadershipConnection connection,AcceptLeadershipCallback callback) {
    this(domainName,groupName,nodeName,null, connection,callback,true);
  }

  public DatabusClientGroupMember(String domainName,String groupName, String nodeName,String sharedDataPath, GroupLeadershipConnection connection) {
    this(domainName,groupName,nodeName,sharedDataPath, connection,null,true);
  }

  public synchronized boolean join()
  {
    if (null != _groupConnection)
    {
      AcceptLeadershipCallback leaderCallback  = (_callback != null) ? _callback : this.new DefaultLeaderCallBack();
      _groupSession = _groupConnection.joinGroup(getDomainName(),getGroupName(), getName(),leaderCallback);
      if ((_sharedDataPath!=null) && !_sharedDataPath.isEmpty())
      {
        _groupSession.setSharedDataPath(_sharedDataPath);
      }
    }
    return _groupSession != null;
  }

  public synchronized boolean leave()
  {
    if (_groupSession != null)
    {
        _groupSession.leaveGroup();
        _groupSession = null;
        this.notifyAll();
        return true;
    }
    return false;
  }

  public boolean waitForLeaderShip() {
    return waitForLeaderShip(0);
  }

  public  boolean waitForLeaderShip(int timeOutInMs)
  {
    try {
      if (_callback != null) {
        //async api - doesn't block;
        return true;
      }
      synchronized (this) 
	  {
      	//if already leader; then return
       	if ((_groupSession != null) && _groupSession.isLeader() )
      	{
      	  return true;
      	}

      	if (timeOutInMs < 0) timeOutInMs=0;
      	long remainingTime = timeOutInMs;
        //guard against spurious wakeups;
        long start = System.currentTimeMillis();
        while ((_groupSession!=null) && !_groupSession.isLeader() && (timeOutInMs==0 || remainingTime>0)) {
            this.wait(remainingTime);
            _log.info("Waiting for leadership on node " + toString() + " awoken");
            if (timeOutInMs > 0)
            {
              remainingTime -= (System.currentTimeMillis() - start);
            }
        }
        return (_groupSession==null) ? false :_groupSession.isLeader();
      }
    } catch (InterruptedException e) {
      _log.error("Waiting for leadership on node" + toString() + " interrupted ");
    }
    return false;
  }

  public synchronized boolean removeSharedData(String key)
  {
    if (_groupSession != null)
    {

      if (!_requiresMastership || _groupSession.isLeader())
      {
        return _groupSession.removeGroupData(key);
      }
      else
      {
        _log.error("Remove failed! Attempted when " + toString() + " was not leader!");
      }

    }
    return false ;

  }

  public  boolean removeSharedData()
  {
    return removeSharedData(null);
  }



  public Object readSharedData()
  {
    return readSharedData(null);
  }

  public synchronized  Object readSharedData(String key)
  {
    if (_groupSession != null)
    {
      return _groupSession.readGroupData(key);
    }
    return null ;
  }

  public synchronized  boolean writeSharedData(String key, Object obj)
  {
    if (_groupSession != null)
    {
      if (!_requiresMastership || _groupSession.isLeader())
      {
        return _groupSession.writeGroupData(key,obj);
      }
      else
      {
        _log.error("Write failed! Attempted when " + toString() + " was not leader!");
      }
    }
    return false;
  }

  public boolean writeSharedData(Object obj)
  {
    return writeSharedData(null,obj);
  }


  public String getName()
  {
    return _nodeName;
  }

  public String getGroupName()
  {
    return _groupName;
  }

  public String getDomainName()
  {
    return _domainName;
  }

  @Override
  public String toString()
  {
    return getDomainName() + "/" + getGroupName() + "/" + getName();
  }

  public synchronized boolean isLeader()
  {
    if (_groupSession != null)
    {
      return _groupSession.isLeader();
    }
    return false;
  }

  public synchronized String getLeader()
  {
     if (_groupSession != null) {
       return _groupSession.getLeaderName();
     }
     return null;
  }
  
  public synchronized  List<String> getMembers()
  {
	  if (_groupSession != null) {
		  GroupLeadershipInfo info =  _groupSession.getGroupLeadershipInfo();
		  return info.getMemberNames();
	  }
	  return null;
  }

  public synchronized List<String> getSharedKeys() 
  {
	  if (_groupSession != null) 
	  {
		  return _groupSession.getKeysOfGroupData();
	  }
	  return null;
  }

  public synchronized boolean createPaths() 
  {
	  if (_groupConnection != null)
	  {
		  return ( (GroupLeadershipConnectionZkClientImpl) _groupConnection).createBasePath(getDomainName(),getGroupName());
	  }
	  return false;
  }




}
