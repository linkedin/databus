/**
 * $Id: GroupLeadershipConnection.java 269419 2011-05-12 01:53:15Z snagaraj $ */
package com.linkedin.databus.groupleader.pub;

import java.util.List;

/**
 * A {@code group} is made up of {@code member}s. The group has a unique name, and each member
 * of a group has a name that is unique within the group.
 * <p>
 * A {@link GroupLeadershipConnection} is used to interact with the group management service.
 * A {@link GroupLeadershipSession} represents the membership of a member within a group.
 *
 * @author Mitch Stuart
 * @version $Revision: 269419 $
 */
public interface GroupLeadershipConnection
{
  /**
   * Join the group {@code groupName}. Either at the time of joining the group, or at any
   * future time, the {@code GroupLeadershipConnection} will call {@code acceptLeadershipCallback}
   * when the member must accept the leadership role. The member name
   * can be specified by the caller, or can be {@code null}, in which case the
   * {@code GroupLeadershipConnection} will generate and assign a unique name.
   * The member name must be unique within the group. If a non-unique name is passed in,
   * an {@code IllegalArgumentException} will be thrown.
   *
   * @param groupName the name of the group to join
   * @param memberName the name of the member joining the group, or {@code null} if the
   * name should be generated and assigned
   * @param acceptLeadershipCallback the callback that will be called when this member
   * must accept leadership
   * @return a session representing the membership of the given {@code memberName} in the given
   * {@code groupName}. The {@code memberName} in the session will be either what was passed in to
   * this method (if the input name was not null), or the assigned name (if the input name
   * was null).
   * @throws IllegalArgumentException if {@code memberName} is not unique within the group
   */
  GroupLeadershipSession joinGroup(String baseZkPath, String groupName, String memberName,
                                   AcceptLeadershipCallback acceptLeadershipCallback);

  /**
   * Return the member name of the current leader of the specified {@code groupName}.
   * <p>
   * The returned name may be null, due to any of the following conditions:
   * <ul>
   * <li>The group does not exist</li>
   * <li>The group has no members</li>
   * <li>The group leadership is currently undergoing transition - for example, if the previous
   * leader has died and a new leader has not taken over yet</li>
   * </ul>
   *
   * @param groupName the name of the group
   * @return the name of the current leader
   */
  String getLeaderName(String baseZkPath, String groupName);

  /**
   * Return whether the specified {@code memberName} is the current leader of the specified
   * {@code groupName}.
   * <p>
   * Calling {@code groupLeadershipConnection.isLeader(memberName)}
   * is simply a convenience wrapper for:
   * {@code memberName.equals(groupLeadershipConnection.getLeaderName(groupName))}.
   *
   * @param groupName
   * @param memberName
   * @return true if {@code memberName} is the current leader of {@code groupName}, otherwise false
   */
  boolean isLeader(String baseZkPath, String groupName, String memberName);

  List<String> getGroupNames(String baseZkPath);

  GroupsLeadershipInfo getGroupsLeadershipInfo(String baseZkPath);

  GroupLeadershipInfo getGroupLeadershipInfo(String baseZkPath,String groupName);

  void close();

  /**
   * Join without ever becoming a leader 
   *
  */
  public GroupLeadershipSession joinGroupWithoutLeadershipDuties(String domainName, String groupName,
		  String name);
  
}

