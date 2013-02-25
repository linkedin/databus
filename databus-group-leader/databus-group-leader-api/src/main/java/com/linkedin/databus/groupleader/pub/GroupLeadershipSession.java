/**
 * $Id: GroupLeadershipSession.java 269419 2011-05-12 01:53:15Z snagaraj $ */
package com.linkedin.databus.groupleader.pub;
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

/**
 * Represents the membership of a member within a group.
 *
 * @author Mitch Stuart
 * @version $Revision: 269419 $
 */
public interface GroupLeadershipSession
{
  /**
   * Return the {@link GroupLeadershipConnection} which created this session.
   *
   * @return the {@code GroupLeadershipConnection}
   */
  GroupLeadershipConnection getGroupLeadershipConnection();

  /**
   * Return the group name for which this leadership session applies.
   *
   * @return the group name
   */
  String getGroupName();

  /**
   * Return the member name for which this leadership session applies.
   *
   * @return the member name
   */
  String getMemberName();


  /**
   * Return the name of the current leader of this group. This may be {@code null}
   * if there is no current leader.
   *
   * @return the name of the current group leader, or {@code null} if there is no leader
   */
  String getLeaderName();

  /**
   * Return the namespace of the current group session
   * @return the name of the current group namespace
   */
  String getBasePathName();

  /**
   * Determine whether this member is currently the leader of this group.
   *
   * @return true if this member is currently the leader of this group, otherwise false
   */
  boolean isLeader();

  /**
   * Leave this group.
   * <p>
   * If this member is currently the leader of this group:
   * <ul>
   * <li>A new leader will be elected, if there is at least one other member of this group.
   * <li>This method does <b>not</b> block until a new leader is elected.
   * </ul>
   */
  void leaveGroup();

  /**
   * Shorthand for {@code getGroupLeadershipConnection().getGroupLeadershipInfo(getGroupName())}
   *
   * @return the {@code GroupLeadershipInfo} for the group that this session is associated with
   */
  GroupLeadershipInfo getGroupLeadershipInfo();

  /**
   * Return the directory under which shared data is stored .
   *
   * @return the member name
   */
   String getSharedDataPath();


   /**
    * Set the directory under which shared data is stored .
    */
    void setSharedDataPath(String sharedDataPath);

  /**
   * read keys of groupData
   */
    List<String> getKeysOfGroupData();

  /**
   * read data keyed by 'key' from shared node of group;
   * @return data that is read or null if group or key doesn't exist
   */
  Object readGroupData(String key);

  /**
   * read data from shared node of group
   * @return data that is read or null if group doesn't exist
   */
  Object readGroupData();

  /**
   * write data to shared node of group;
   * @return true if successful, false otherwise
   */
  boolean writeGroupData(Object obj);

  /**
   * remove data   node of group;
   * @return true if successful , false otherwise
   */
  boolean removeGroupData();

  /**
   * remove data   node of a particular key in a  group;
   * @return true if successful , false otherwise
   */
  boolean removeGroupData(String key );

  /**
   * Writes a key , val into group data
   * @param key
   * @param obj
   * @return true if successful , false otherwise
   */
  boolean writeGroupData(String key, Object obj);



}
