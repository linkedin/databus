/**
 * $Id: GroupMemberInfo.java 154385 2010-12-08 22:05:55Z mstuart $ */
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


/**
 * A simple immutable class to hold info about the membership of one member in one group.
 *
 * @author Mitch Stuart
 * @version $Revision: 154385 $
 */
public class GroupMemberInfo
{
  private final String _groupName;
  private final String _memberName;
  private final boolean _isLeader;

  /**
   * Constructor
   */
  public GroupMemberInfo(String groupName, String memberName, boolean isLeader)
  {
    _groupName = groupName;
    _memberName = memberName;
    _isLeader = isLeader;
  }

  public String getGroupName()
  {
    return _groupName;
  }

  public String getMemberName()
  {
    return _memberName;
  }

  /**
   *
   * @return true if this member is currently the leader of this group, otherwise false
   */
  public boolean isLeader()
  {
    return _isLeader;
  }

  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append(_groupName);
    sb.append(" : ");
    sb.append(_memberName);

    if (_isLeader)
    {
      sb.append(" [LEADER]");
    }

    return sb.toString();
  }

}
