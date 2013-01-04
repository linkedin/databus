/**
 * $Id: GroupsLeadershipInfo.java 154385 2010-12-08 22:05:55Z mstuart $ */
package com.linkedin.databus.groupleader.pub;

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
