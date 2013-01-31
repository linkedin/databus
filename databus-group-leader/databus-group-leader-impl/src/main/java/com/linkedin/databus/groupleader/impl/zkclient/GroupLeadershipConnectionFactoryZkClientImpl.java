/**
 * $Id: GroupLeadershipConnectionFactoryZkClientImpl.java 154385 2010-12-08 22:05:55Z mstuart $ */
package com.linkedin.databus.groupleader.impl.zkclient;
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
