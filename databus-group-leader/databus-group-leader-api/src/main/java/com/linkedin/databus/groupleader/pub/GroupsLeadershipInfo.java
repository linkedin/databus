/**
 * $Id: GroupsLeadershipInfo.java 154385 2010-12-08 22:05:55Z mstuart $ */
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
 * A simple immutable class to hold info about a set of groups.
 *
 * @author Mitch Stuart
 * @version $Revision: 154385 $
 */
public class GroupsLeadershipInfo
{
  private final List<String> _groupNames;
  private final List<GroupLeadershipInfo> _groupLeadershipInfos;

  /**
   * Constructor
   */
  public GroupsLeadershipInfo(List<String> groupNames,
                              List<GroupLeadershipInfo> groupLeadershipInfos)
  {
    _groupNames = groupNames;
    _groupLeadershipInfos = groupLeadershipInfos;
  }

  public List<String> getGroupNames()
  {
    return _groupNames;
  }

  public List<GroupLeadershipInfo> getGroupLeadershipInfos()
  {
    return _groupLeadershipInfos;
  }

  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append("{ ");

    boolean isFirstGroup = false;


    for (GroupLeadershipInfo groupLeadershipInfo : _groupLeadershipInfos)
    {
      if (!isFirstGroup)
      {
        sb.append("; ");
      }

      sb.append(groupLeadershipInfo.toString());
      isFirstGroup = false;
    }

    sb.append(" }");

    return sb.toString();
  }
}
