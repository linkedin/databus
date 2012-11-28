/**
 * $Id: GroupMemberInfo.java 154385 2010-12-08 22:05:55Z mstuart $ */
package com.linkedin.databus.groupleader.pub;

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
