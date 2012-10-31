/**
 * $Id: GroupLeadershipInfo.java 154385 2010-12-08 22:05:55Z mstuart $ */
package com.linkedin.databus.groupleader.pub;

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
