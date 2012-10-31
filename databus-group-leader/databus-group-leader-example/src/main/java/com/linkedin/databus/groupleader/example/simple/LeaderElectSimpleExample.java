/**
 * $Id: LeaderElectSimpleExample.java 261005 2011-04-15 00:36:51Z snagaraj $ */
package com.linkedin.databus.groupleader.example.simple;

import org.apache.log4j.Logger;

import com.linkedin.databus.groupleader.impl.zkclient.GroupLeadershipConnectionFactoryZkClientImpl;
import com.linkedin.databus.groupleader.pub.AcceptLeadershipCallback;
import com.linkedin.databus.groupleader.pub.GroupLeadershipConnection;
import com.linkedin.databus.groupleader.pub.GroupLeadershipConnectionFactory;
import com.linkedin.databus.groupleader.pub.GroupLeadershipSession;

/**
 *
 * @author Mitch Stuart
 * @version $Revision: 261005 $
 */
public class LeaderElectSimpleExample
{
  private static final Logger LOG = Logger.getLogger(LeaderElectSimpleExample.class);

  /**
   * main
   */
  public static void main(String[] args)
  {
    LeaderElectSimpleExample leaderElectProto = new LeaderElectSimpleExample();
    leaderElectProto.run();
  }

  private final String _groupName;
  private final String _memberName;
  private final GroupLeadershipConnectionFactory _groupLeadershipConnFactory;
  private final GroupLeadershipConnection _groupLeadershipConn;

  /**
   * Constructor
   */
  public LeaderElectSimpleExample()
  {
    _groupName = ExampleUtils.getRequiredStringProperty("groupName", LOG);

    String memberName = System.getProperty("memberName");
    if (memberName != null && memberName.trim().isEmpty())
    {
      memberName = null;
    }

    _memberName = memberName;
    _groupLeadershipConnFactory = new GroupLeadershipConnectionFactoryZkClientImpl(
      ExampleUtils.getRequiredStringProperty("zkServerList", LOG),
      ExampleUtils.getRequiredIntProperty("sessionTimeoutMillis", LOG),
      ExampleUtils.getRequiredIntProperty("connectTimeoutMillis", LOG));
    _groupLeadershipConn = _groupLeadershipConnFactory.getConnection();
  }

  protected void run()
  {
    String baseName = "/databus2.testGroupLeaderSimple";
    LOG.info("Before joining group: groupName=" + _groupName + "; memberName=" + _memberName +
      "; leadershipInfo=" + _groupLeadershipConn.getGroupLeadershipInfo(baseName,_groupName));

    GroupLeadershipSession groupLeadershipSession = _groupLeadershipConn.joinGroup(baseName,
      _groupName, _memberName, new AcceptLeadershipCallback()
      {
        @Override
        public void doAcceptLeadership(GroupLeadershipSession groupLeadershipSession)
        {
          LOG.info("Accepting leadership: " + groupLeadershipSession);
        }
      });

    LOG.info("After joining group: " + groupLeadershipSession);

    while (true)
    {
      try
      {
        Thread.sleep(3000); // sleep is OK, this is a demo/example program
        LOG.debug(groupLeadershipSession);
      }
      catch (InterruptedException e)
      {
        Thread.currentThread().interrupt();
      }
    }
  }

}
