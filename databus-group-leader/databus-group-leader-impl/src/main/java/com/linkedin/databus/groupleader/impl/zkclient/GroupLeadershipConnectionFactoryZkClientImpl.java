/**
 * $Id: GroupLeadershipConnectionFactoryZkClientImpl.java 154385 2010-12-08 22:05:55Z mstuart $ */
package com.linkedin.databus.groupleader.impl.zkclient;

import org.I0Itec.zkclient.ZkClient;

import com.linkedin.databus.groupleader.pub.GroupLeadershipConnection;
import com.linkedin.databus.groupleader.pub.GroupLeadershipConnectionFactory;

/**
 *
 * @author Mitch Stuart
 * @version $Revision: 154385 $
 */
public class GroupLeadershipConnectionFactoryZkClientImpl implements
    GroupLeadershipConnectionFactory
{
  private final String _zkServerList;
  private final int _sessionTimeoutMillis;
  private final int _connectTimeoutMillis;

  /**
   * Constructor
   */
  public GroupLeadershipConnectionFactoryZkClientImpl(String zkServerList,
                                                      int sessionTimeoutMillis,
                                                      int connectTimeoutMillis)
  {
    // TODO MED MAS (DDSDBUS-64) switch millis to Timespan values after integrating LI util

    _zkServerList = zkServerList;
    _sessionTimeoutMillis = sessionTimeoutMillis;
    _connectTimeoutMillis = connectTimeoutMillis;
  }

  /**
   * @see com.linkedin.incubator.mstuart.leaderelect.api.GroupLeadershipConnectionFactory#getConnection()
   */
  @Override
  public GroupLeadershipConnection getConnection()
  {
    ZkClient zkClient = new ZkClient(_zkServerList, _sessionTimeoutMillis, _connectTimeoutMillis);
    GroupLeadershipConnection groupLeadershipConnection = new GroupLeadershipConnectionZkClientImpl(zkClient);
    return groupLeadershipConnection;
  }

}
