/**
 * $Id: GroupLeadershipInfo.java 154385 2010-12-08 22:05:55Z mstuart $ */
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
 * A simple immutable class to hold info about all the members of one group.
 *
 * @author Mitch Stuart
 * @version $Revision: 154385 $
 */
public class GroupLeadershipInfo
{
  public final String _groupName;
  public final String _leaderName;
  public final List<String> _memberNames;
  public final List<GroupMemberInfo> _groupMemberInfos;

  /**
   * Constructor
   */
  public GroupLeadershipInfo(String groupName,
                             String leaderName,
                             List<String> memberNames,
                             List<GroupMemberInfo> groupMemberInfos)
  {
    _groupName = groupName;
    _leaderName = leaderName;
    _memberNames = memberNames;
    _groupMemberInfos = groupMemberInfos;
  }

  public String getGroupName()
  {
    return _groupName;
  }

  public String getLeaderName()
  {
    return _leaderName;
  }

  public List<String> getMemberNames()
  {
    return _memberNames;
  }

  public List<GroupMemberInfo> getMemberInfos()
  {
    return _groupMemberInfos;
  }

  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append(_groupName);
    sb.append(": ");
    sb.append("{ ");

    boolean isFirstMember = true;

    for (GroupMemberInfo groupMemberInfo : _groupMemberInfos)
    {
      if (!isFirstMember)
      {
        sb.append(", ");
      }

      sb.append(groupMemberInfo.getMemberName());

      if (groupMemberInfo.isLeader())
      {
        sb.append(" [LEADER]");
      }

      isFirstMember = false;
    }

    sb.append(" }");

    return sb.toString();
  }

}
