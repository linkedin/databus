/**
 * $Id: GroupLeadershipConnectionZkClientImpl.java 269419 2011-05-12 01:53:15Z snagaraj $ */
package com.linkedin.databus.groupleader.impl.zkclient;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;

import com.linkedin.databus.groupleader.pub.AcceptLeadershipCallback;
import com.linkedin.databus.groupleader.pub.GroupLeadershipConnection;
import com.linkedin.databus.groupleader.pub.GroupLeadershipInfo;
import com.linkedin.databus.groupleader.pub.GroupLeadershipSession;
import com.linkedin.databus.groupleader.pub.GroupMemberInfo;
import com.linkedin.databus.groupleader.pub.GroupsLeadershipInfo;

/**
 * Assume that we have a group called member2RepDS and members called rep1 and rep2, where
 * rep1 is the current leader. The ZooKeeper nodes would appear as follows. Nodes marked with
 * [p] are persistent, with [e] are ephemeral:
 * <code><pre>
 *        /
 * [p]      GroupLeadershipConn/
 * [p]        groups/
 * [p]          member2RepDS/
 * [e]            leader : rep1
 * [p]            members/
 * [e]              rep1
 * [e]              rep2
 * </pre></code>
 *
 *
 * @author Mitch Stuart
 * @version $Revision: 269419 $
 */
public class GroupLeadershipConnectionZkClientImpl implements GroupLeadershipConnection
{
  private static final Logger LOG = Logger.getLogger(GroupLeadershipConnectionZkClientImpl.class);

  private enum LeadershipAction
  {
    NONE,
    TOOK_LEADERSHIP,
    RETAINED_LEADERSHIP,
    DECLINED_LEADERSHIP
  }

  protected static final String GROUPS_ZK_PATH_PART = "groups";
  protected static final String MEMBERS_ZK_PATH_PART =  "members";
  protected static final String LEADER_ZK_PATH_PART = "leader";
  protected static final String SHARED_DATA_ZK_PATH_PART = "shareddata";

  protected static String makeUniqueMemberName()
  {
    return UUID.randomUUID().toString();
  }

  protected static String makeGroupsNodePath(String baseZkPath)
  {
    return baseZkPath + "/" +
      GROUPS_ZK_PATH_PART;
  }

  /**
   * Package-private - use externally for <b>TESTING ONLY</b>
   *
   */
  static String makeGroupNodePath(String baseZkPath,String groupName)
  {
    return makeGroupsNodePath(baseZkPath) + "/" +
           groupName;
  }

  /**
   * Package-private - use externally for <b>TESTING ONLY</b>
   *
   */
  static String makeGroupSharedDataNodePath(String baseZkPath,String groupName,String sharedDataPath)
  {
    return makeGroupsNodePath(baseZkPath) + "/" +
           groupName  + "/" + sharedDataPath;
  }


  /**
   * Package-private - use externally for <b>TESTING ONLY</b>
   *
   */
  static String makeMembersNodePath(String baseZkPath,String groupName)
  {
    return makeGroupNodePath(baseZkPath,groupName) + "/" +
           MEMBERS_ZK_PATH_PART;
  }

  /**
   * Package-private - use externally for <b>TESTING ONLY</b>
   *
   */
  static String makeMemberNodePath(String baseZkPath,String groupName, String memberName)
  {
    return makeGroupNodePath(baseZkPath,groupName) + "/" +
           MEMBERS_ZK_PATH_PART + "/" +
           memberName;
  }

  /**
   * Package-private - use externally for <b>TESTING ONLY</b>
   *
   */
  static String makeLeaderNodePath(String baseZkPath,String groupName)
  {
    return makeGroupNodePath(baseZkPath,groupName) + "/" +
           LEADER_ZK_PATH_PART;
  }


  private final ZkClient _zkClient;


  /**
   * Constructor
   */
  public GroupLeadershipConnectionZkClientImpl(ZkClient zkClient)
  {
    _zkClient = zkClient;
  }

  protected String ensurePersistentPathExists(String path)
  {
    if (!_zkClient.exists(path))
    {
      _zkClient.createPersistent(path, true);
    }

    return path;
  }

  protected String ensurePersistentGroupsZkPathExists(String baseZkPath)
  {
    String groupsZkPath = makeGroupsNodePath(baseZkPath);
    return ensurePersistentPathExists(groupsZkPath);
  }

  protected String ensurePersistentMembersZkPathExists(String baseZkPath,String groupName)
  {
    String membersZkPath = makeMembersNodePath(baseZkPath,groupName);
    return ensurePersistentPathExists(membersZkPath);
  }


  @Override
  public GroupLeadershipSession joinGroup(final String baseZkPath,
                                          final String groupName,
                                          String memberName,
                                          final AcceptLeadershipCallback acceptLeadershipCallback)
  {
    final String resolvedMemberName = memberName == null ? makeUniqueMemberName() : memberName;

    LOG.info("joinGroup: groupName=" + groupName +
      "; memberName=" + memberName +
      "; resolvedMemberName=" + resolvedMemberName);

    ensurePersistentMembersZkPathExists(baseZkPath,groupName);

    final GroupLeadershipSessionZkClientImpl groupLeadershipSession =
      new GroupLeadershipSessionZkClientImpl(baseZkPath,groupName, resolvedMemberName, acceptLeadershipCallback);

    //Create and set call backs for data and child changes;
    IZkChildListener childListener = new IZkChildListener()
    {
      @Override
      public void handleChildChange(String parentPath, List<String> currentChildren)
          throws Exception
      {
        takeLeadershipIfNeeded(groupLeadershipSession);
      }
    };

    //Data change call back ; try and get notified on creation of the ephemeral member node
    IZkDataListener dataListener = new IZkDataListener()
    {

      @Override
      public void handleDataChange(String dataPath, Object data) throws Exception
      {
        LOG.info("LeaderElection: Data change in: " + dataPath);
        takeLeadershipIfNeeded(groupLeadershipSession);
      }

      @Override
      public void handleDataDeleted(String dataPath) throws Exception
      {
        relinquishLeadership(groupLeadershipSession);
      }


    };

    groupLeadershipSession.setListener(childListener);
    groupLeadershipSession.setDataListener(dataListener);

    //create a ephemeral node for the member
    String memberPath = makeMemberNodePath(baseZkPath,groupName, memberName);

    String path = _zkClient.createEphemeralSequential(memberPath, groupLeadershipSession.getUniqueID());
    groupLeadershipSession.setMemberZkName(memberName + path.substring(path.length()-10));
    String dataWatchPath = makeMemberNodePath(baseZkPath,groupName,groupLeadershipSession.getMemberZkName());

    LOG.info("Created path:" + path  + " zkName = " + groupLeadershipSession.getMemberZkName() + " data watch path: " + dataWatchPath);

    //subscribe for data changes in this node; in case the node is not created at time takeLeadership is invoked
    //could be a back door to trigger takeLeadershipIfNeeded without a restart
    _zkClient.subscribeDataChanges(dataWatchPath,dataListener);

    takeLeadershipIfNeeded(groupLeadershipSession);

    return groupLeadershipSession;
  }

  public boolean createBasePath(String baseZkPath,String groupName) {
	  //create top level path for leadership etc
	  String path = makeGroupNodePath(baseZkPath, groupName);
	  String paths = ensurePersistentPathExists(path);
	  return  paths != null;
  }
  
  @Override
  public GroupLeadershipSession joinGroupWithoutLeadershipDuties(
		  String domainName, String groupName, String name)
  {
	  GroupLeadershipSessionZkClientImpl groupLeadershipSession =
		  new GroupLeadershipSessionZkClientImpl(domainName,groupName, name, null);

	  return groupLeadershipSession;
  }

  protected void leaveGroup(GroupLeadershipSession groupLeadershipSession)
  {
    LOG.info("leaveGroup: groupName=" + groupLeadershipSession.getGroupName() +
      "; memberName=" + groupLeadershipSession.getMemberName());

    if (! (groupLeadershipSession instanceof GroupLeadershipSessionZkClientImpl) )
    {
      throw new IllegalArgumentException("groupLeadershipSession must be an instance of: " +
        GroupLeadershipSessionZkClientImpl.class.getName());
    }

    GroupLeadershipSessionZkClientImpl zkLeadershipSession =
      (GroupLeadershipSessionZkClientImpl) groupLeadershipSession;


    zkLeadershipSession.unsubscribeChanges();
    zkLeadershipSession.unsubscribeDataChanges();

    ensurePersistentMembersZkPathExists(zkLeadershipSession.getBasePathName(),zkLeadershipSession.getGroupName());
    // ZkClient simply returns false (does not throw an exception) if the node does not exist
    _zkClient.delete(makeMemberNodePath(zkLeadershipSession.getBasePathName(),zkLeadershipSession.getGroupName(),zkLeadershipSession.getMemberZkName()));

    if (zkLeadershipSession.isLeader())
    {
      String leaderZkPath = makeLeaderNodePath(zkLeadershipSession.getBasePathName(),zkLeadershipSession.getGroupName());
      // ZkClient simply returns false (does not throw an exception) if the node does not exist
      _zkClient.delete(leaderZkPath);
    }
  }

  @Override
  public String getLeaderName(String baseZkPath,String groupName)
  {
    String leader = getLeaderZkName(baseZkPath,groupName);
    if (leader != null  && leader.length() > 10)
    {
      leader = leader.substring(0,leader.length()-10);
    }
    return leader;
  }

  public String getLeaderZkName(String baseZkPath,String groupName)
  {
    String leaderZkPath = makeLeaderNodePath(baseZkPath,groupName);
    String currentLeader = _zkClient.readData(leaderZkPath, true);
    return currentLeader;
  }

  @Override
  public boolean isLeader(String baseZkPath, String groupName, String memberName)
  {
	String leaderZkName = getLeaderZkName(baseZkPath,groupName);
	if (leaderZkName != null)
	{
		return memberName.equals(leaderZkName);
	}
	return false;
  }

  @Override
  public List<String> getGroupNames(String baseZkPath)
  {
    String groupsZkPath = ensurePersistentGroupsZkPathExists(baseZkPath);
    List<String> groupNames = _zkClient.getChildren(groupsZkPath);
    return groupNames;
  }

  @Override
  public GroupsLeadershipInfo getGroupsLeadershipInfo(String baseZkPath)
  {
    List<String> sortedGroupNames = getGroupNames(baseZkPath);
    Collections.sort(sortedGroupNames);

    // This list will be sorted by group name, since it is added to in order
    List<GroupLeadershipInfo> sortedGroupLeadershipInfos = new ArrayList<GroupLeadershipInfo>();

    for (String groupName : sortedGroupNames)
    {
      GroupLeadershipInfo groupLeadershipInfo = getGroupLeadershipInfo(baseZkPath,groupName);
      sortedGroupLeadershipInfos.add(groupLeadershipInfo);
    }

    GroupsLeadershipInfo groupsLeadershipInfo = new GroupsLeadershipInfo(
      Collections.unmodifiableList(sortedGroupNames),
      Collections.unmodifiableList(sortedGroupLeadershipInfos));

    return groupsLeadershipInfo;
  }

  @Override
  public GroupLeadershipInfo getGroupLeadershipInfo(String baseZkPath, String groupName)
  {
    String membersZkPath = ensurePersistentMembersZkPathExists(baseZkPath,groupName);
    List<String> sortedMemberNames = _zkClient.getChildren(membersZkPath);
    for (int i=0; i < sortedMemberNames.size(); ++i)
    {
      sortedMemberNames.set(i,sortedMemberNames.get(i).substring(0,sortedMemberNames.get(i).length()-10));
    }

    Collections.sort(sortedMemberNames);
    String leaderName = getLeaderName(baseZkPath,groupName);

    // This list will be sorted by member name, since it is added to in order
    List<GroupMemberInfo> sortedMemberInfos = new ArrayList<GroupMemberInfo>();

    for (String memberName : sortedMemberNames)
    {
      GroupMemberInfo groupMemberInfo = new GroupMemberInfo(groupName,
        memberName,
        memberName.equals(leaderName));
      sortedMemberInfos.add(groupMemberInfo);
    }

    GroupLeadershipInfo groupLeadershipInfo = new GroupLeadershipInfo(groupName,
      leaderName,
      Collections.unmodifiableList(sortedMemberNames),
      Collections.unmodifiableList(sortedMemberInfos));

    return groupLeadershipInfo;
  }

  private void takeLeadershipIfNeeded(GroupLeadershipSessionZkClientImpl groupLeadershipSession)
  {
    String baseZkPath = groupLeadershipSession.getBasePathName();
    String groupName = groupLeadershipSession.getGroupName();
    String memberName = groupLeadershipSession.getMemberName();
    //debug aid
    String leaderZkPath = makeLeaderNodePath(baseZkPath,groupName);
    String currentLeader = _zkClient.readData(leaderZkPath, true);
    String membersZkPath = ensurePersistentMembersZkPathExists(baseZkPath,groupName);
    List<String> currentMembers = _zkClient.getChildren(membersZkPath);

    LOG.info("takeLeadershipIfNeeded: groupName=" + groupName +
             "; memberName=" + memberName +
             "; currentLeader=" + currentLeader +
             "; members=" + currentMembers);

    LeadershipAction leadershipAction = LeadershipAction.DECLINED_LEADERSHIP;
    //current members have sequential numbers appended to path
    String newLeaderZkName = groupLeadershipSession.subscribeChangesOnHigherPriorityLeader();

    if (newLeaderZkName != null)
    {
      if (newLeaderZkName.equals(groupLeadershipSession.getMemberZkName()))
      {
        if (currentLeader == null || !currentLeader.equals(newLeaderZkName))
        {
          takeLeadership(groupLeadershipSession);
          leadershipAction = LeadershipAction.TOOK_LEADERSHIP;
          groupLeadershipSession.doAcceptLeadership();
        }
        else
        {
          LOG.info("takeLeadershipIfNeeded: Current Leader is same as this node; the new leader! Do nothing! Leader= " + currentLeader + " node=" + newLeaderZkName);
        }

      }
      else
      {
        LOG.info("takeLeaderShipIfNeeded: " + memberName + " declined leadership in favour of leader " + newLeaderZkName);
      }
    }
    else
    {
      LOG.info("takeLeadershipIfNeeded: No nodes seen yet! Declined leadership at " + groupLeadershipSession.getMemberName());
    }

    //debug aid
    List<String> uponExitMembers = _zkClient.getChildren(membersZkPath);
    String uponExitLeader = _zkClient.readData(leaderZkPath, true);
    String msg =
      "takeLeadershipIfNeeded: " + leadershipAction +
      "; groupName=" + groupName +
      "; memberName=" + memberName +
      "; uponExitLeader=" + uponExitLeader +
      "; uponExitMembers=" + uponExitMembers;

    if (leadershipAction == LeadershipAction.NONE)
    {
      LOG.warn("Unexpected leadershipAction: " + msg);
    }
    else
    {
      LOG.info(msg);
    }
  }



  private void relinquishLeadership(GroupLeadershipSessionZkClientImpl groupLeadershipSession)
  {
    LOG.info("Relinquishing leadership: " + groupLeadershipSession.getMemberZkName());
    groupLeadershipSession.unsubscribeChanges();
    groupLeadershipSession.unsubscribeDataChanges();
  }

  private boolean takeLeadership(GroupLeadershipSessionZkClientImpl groupLeadershipSession)
  {
    String leaderZkPath = makeLeaderNodePath(groupLeadershipSession.getBasePathName(),groupLeadershipSession.getGroupName());
    //if leader node doesn't exist , leader is ready to take control
    if (_zkClient.exists(leaderZkPath))
    {
      LOG.info(leaderZkPath + " exists! Issuing delete at " + groupLeadershipSession.getMemberZkName());
      //issue delete ; could be redundant; but still;
      _zkClient.delete(leaderZkPath);

    }
    _zkClient.createEphemeral(leaderZkPath, groupLeadershipSession.getMemberZkName());
    //leader doesn't follow anyone else
    groupLeadershipSession.unsubscribeChanges();
    return true;
  }

  /**
   * Package-private - use externally for <b>TESTING ONLY</b>
   *
   * @return the zkClient instance
   */
  ZkClient getZkClient()
  {
    return _zkClient;
  }

  @Override
  public void close()
  {
    _zkClient.close();
  }




  protected class GroupLeadershipSessionZkClientImpl implements GroupLeadershipSession
  {
    private final String _groupName;
    private final String _memberName;
    private final String _basePathName;
    private final AcceptLeadershipCallback _acceptLeadershipCallback;
    private final String _uniqueID;

    private IZkChildListener _childListener;
    private boolean _hasLeftGroup;

    private final Logger LOG = Logger.getLogger(GroupLeadershipSessionZkClientImpl.class);
    private IZkDataListener _dataListener;
    private final ZkSeqComparator  _comparator;

    //zk derived state
    private String _memberZkName;
    private String _listeningMemberZkName;
    private String _sharedDataPath = SHARED_DATA_ZK_PATH_PART;


    /**
     * Constructor
     */
    public GroupLeadershipSessionZkClientImpl(String basePathName,
                                              String groupName,
                                              String memberName,
                                              AcceptLeadershipCallback acceptLeadershipCallback)
    {
      _groupName = groupName;
      _memberName = memberName;
      _basePathName = basePathName;
      _acceptLeadershipCallback = acceptLeadershipCallback;
      _uniqueID = UUID.randomUUID().toString();
      _comparator = new ZkSeqComparator();
      _memberZkName = null;
      _listeningMemberZkName = null;
    }

    public String getListeningMemberZkName()
    {
      return _listeningMemberZkName;
    }

    public void setListeningMemberZkName(String listenPath)
    {
      _listeningMemberZkName = listenPath;
    }

    public void setMemberZkName(String memberZkPath)
    {
      _memberZkName = memberZkPath;
    }

    public String getMemberZkName()
    {
      return _memberZkName;
    }




    protected void setDataListener(IZkDataListener dataListener)
    {
      _dataListener = dataListener;
    }

    public String getUniqueID()
    {
      return _uniqueID;
    }

    @Override
    public GroupLeadershipConnection getGroupLeadershipConnection()
    {
      return GroupLeadershipConnectionZkClientImpl.this;
    }

    protected GroupLeadershipConnectionZkClientImpl getGroupLeadershipConnectionInternal()
    {
      return GroupLeadershipConnectionZkClientImpl.this;
    }

    @Override
    public String getGroupName()
    {
      return _groupName;
    }

    @Override
    public String getMemberName()
    {
      return _memberName;
    }

    public AcceptLeadershipCallback getAcceptLeadershipCallback()
    {
      return _acceptLeadershipCallback;
    }

    protected void setListener(IZkChildListener childListener)
    {
      _childListener = childListener;
    }

    protected void subscribeChanges(String zkMemberName)
    {
      unsubscribeChanges();
      String memberPath = makeMemberNodePath(_basePathName,_groupName,zkMemberName);
      LOG.info("Adding listener on: " + memberPath + " by: " +  this.getMemberZkName());

      // No exception is thrown if the path does not exist; the listener will be called if/when
      // the path comes into existence
      _zkClient.subscribeChildChanges(memberPath, _childListener);
      _listeningMemberZkName = zkMemberName;
    }

    /** Return zk name (with seq number) of first node seen in order  and set watches for failover **/
    protected String subscribeChangesOnHigherPriorityLeader()
    {
     boolean done = false;
     List<String> memberZkNames = null;
     while (!done)
     {
      String membersZkPath = ensurePersistentMembersZkPathExists(_basePathName,_groupName);
      memberZkNames = _zkClient.getChildren(membersZkPath);
      sortByLeaderPriority(memberZkNames);

      int idx = memberZkNames.indexOf(getMemberZkName());
      if (idx > 0)
      {
        do
        {
          //search for a node higher in the order that still is 'seen' by this node
          --idx;
          String higherPriorityLeaderZkName = memberZkNames.get(idx);
          //set anticipatory subscription on the adjacent node; then check the possibility that the node on which the watch was set is no longer present
          if (!higherPriorityLeaderZkName.equals(getListeningMemberZkName()))
          {
            subscribeChanges(higherPriorityLeaderZkName);
          }
          String higherPriorityLeaderZkPath = makeMemberNodePath(_basePathName,_groupName,higherPriorityLeaderZkName);
          done = _zkClient.exists(higherPriorityLeaderZkPath);
          //debug aid
          if (!done)
          {
            LOG.info("subscribeChangesOnHigherPriorityLeader: " + getMemberZkName() + "Potential race avoided! " + higherPriorityLeaderZkPath + " not present! ");
          }
        }
        while (!done && (idx > 0));
      }
      else
      {
        //this node is the leader; or this node has not been created yet
        done=true;
      }
     }
      return  ( (memberZkNames!=null) && (memberZkNames.size() > 0))? memberZkNames.get(0):null;
    }

    protected void unsubscribeChanges()
    {
      if (_listeningMemberZkName != null)
      {
        ensurePersistentMembersZkPathExists(_basePathName,_groupName);
        LOG.info("Removing listener on: " + getListeningMemberZkName() + " by: " + getMemberZkName());


        // No exception is thrown if node or listener does not exist
        _zkClient.unsubscribeChildChanges(makeCurrentListeningOnMemberPath(), _childListener);
      }
    }

    protected String makeCurrentListeningOnMemberPath()
    {
      String currentListeningMemberPath = makeMemberNodePath(_basePathName,_groupName, _listeningMemberZkName);
      return currentListeningMemberPath;
    }



    protected void unsubscribeDataChanges()
    {
      if (_dataListener != null)
      {
         LOG.info("Removing data listener on: " + getMemberZkName());
        _zkClient.unsubscribeDataChanges(getMemberZkName(),_dataListener);
      }
    }



    protected void sortByLeaderPriority(List<String> memberNames)
    {
     //use the fact that sequential nodes were created
      Collections.sort(memberNames,_comparator);
    }

    @Override
    public boolean isLeader()
    {
      return getGroupLeadershipConnection().isLeader(getBasePathName(),getGroupName(), getMemberZkName());
    }

    @Override
    public String getLeaderName()
    {
      return getGroupLeadershipConnection().getLeaderName(getBasePathName(),getGroupName());
    }

    public String getLeaderZkName()
    {
      GroupLeadershipConnection conn = getGroupLeadershipConnection();
      if (conn instanceof GroupLeadershipConnectionZkClientImpl)
      {
        return ((GroupLeadershipConnectionZkClientImpl) conn).getLeaderZkName(getBasePathName(),getGroupName());
      }
      return null;
    }

    @Override
    public void leaveGroup()
    {
      if (_hasLeftGroup)
      {
        throw new IllegalStateException("This session has already left the group: " + this);
      }

      getGroupLeadershipConnectionInternal().leaveGroup(this);
      _hasLeftGroup = true;
    }

    protected void doAcceptLeadership()
    {
      _acceptLeadershipCallback.doAcceptLeadership(this);
    }

    @Override
    public GroupLeadershipInfo getGroupLeadershipInfo()
    {
      return getGroupLeadershipConnection().getGroupLeadershipInfo(getBasePathName(),getGroupName());
    }

    @Override
    public String toString()
    {
      return "basePathName=" + _basePathName + " ; groupName=" + _groupName +
        "; memberName=" + _memberName +
        "; uniqueID=" + _uniqueID +
        "; listeningOnMemberName=" + _listeningMemberZkName +
        "; hasLeftGroup=" + _hasLeftGroup +
        "; leadershipInfo=" + getGroupLeadershipInfo();
    }

    @Override
    public String getBasePathName()
    {
      return _basePathName;
    }

    @Override
    public Object readGroupData()
    {
      return readGroupData(null);
    }

    @Override
    public Object readGroupData(String key)
    {
      String groupNodePath = makeGroupSharedDataNodePath(this.getBasePathName(),this.getGroupName(),this.getSharedDataPath());
      if (key != null) groupNodePath = groupNodePath + "/"  + key;
      //don't throw exception; set that flag to 'true'
      if (LOG.isDebugEnabled()) 
     	 LOG.debug("reading data from: "+groupNodePath);
     //don't throw exception; set that flag to 'true'
      return _zkClient.readData(groupNodePath, true);
    }

    @Override
    public boolean writeGroupData(Object obj)
    {
      return writeGroupData(null,obj);
    }

    protected void createPersistentSharedDataNode()
    {
      String groupNodePath = makeGroupSharedDataNodePath(this.getBasePathName(),this.getGroupName(),this.getSharedDataPath());
      if (!_zkClient.exists(groupNodePath))
      {
        LOG.info("creating data node: "+groupNodePath);
        _zkClient.createPersistent(groupNodePath,null);
      }
    }

    @Override
    public boolean writeGroupData(String key, Object obj)
    {
      String groupNodePath = makeGroupSharedDataNodePath(this.getBasePathName(),this.getGroupName(),this.getSharedDataPath());
      createPersistentSharedDataNode();
      if (null != key) {
        groupNodePath = groupNodePath + "/" + key;
        if (!_zkClient.exists(groupNodePath))
        {
          LOG.info("creating data node: "+groupNodePath);
          _zkClient.createPersistent(groupNodePath,obj);
          return true;
        }
      }
      if (LOG.isDebugEnabled()) 
    	 LOG.debug("writing data to: "+groupNodePath);
      _zkClient.writeData(groupNodePath, obj);
      return true;
    }

    @Override
    public boolean removeGroupData()
    {
      return removeGroupData(null);
    }

    @Override
    public boolean removeGroupData(String key)
    {
      String groupNodePath = makeGroupSharedDataNodePath(this.getBasePathName(),this.getGroupName(),this.getSharedDataPath());
      if (key == null) {
        LOG.info("removing data node: "+groupNodePath);
        return _zkClient.deleteRecursive(groupNodePath);
      } else {
        LOG.info("removing data node: "+groupNodePath+"/"+key);
        return _zkClient.delete(groupNodePath+"/"+key);
      }
    }

    @Override
    public String getSharedDataPath()
    {
      return _sharedDataPath;
    }

    @Override
    public void setSharedDataPath(String sharedDataPath)
    {
      _sharedDataPath = sharedDataPath;
    }

	@Override
	public List<String> getKeysOfGroupData()
	{
		String groupNodePath = makeGroupSharedDataNodePath(this.getBasePathName(),this.getGroupName(),this.getSharedDataPath());
		if (_zkClient.exists(groupNodePath)) {
			return _zkClient.getChildren(groupNodePath);
		}
		return null;
	}


  }
 

  class ZkSeqComparator implements Comparator<String>
  {

    @Override
    public int compare(String s1,String s2)
    {
      String s = s1.substring(s1.length()-10);
      return s.compareTo(s2.substring(s2.length()-10));
    }

  }

  



}
