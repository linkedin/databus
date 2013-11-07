/**
 * $Id: TestLeaderElect.java 267263 2011-05-05 19:57:19Z snagaraj $ */
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


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

public class LeaderElectUtils 
{
  private static final Logger LOG = Logger.getLogger(LeaderElectUtils.class);

  public static List<Integer> parseLocalPorts(String hostPortList)
  {
    List<Integer> localPortsList = new ArrayList<Integer>();
    String[] hostPortArray = hostPortList.split(",");

    for (String hostPortString : hostPortArray)
    {
      String[] hostAndPort = hostPortString.split(":");

      if (hostAndPort[0].equals("localhost") || hostAndPort[0].equals("127.0.0.1"))
      {
        int port = Integer.parseInt(hostAndPort[1]);
        if (!localPortsList.contains(port))
        {
          localPortsList.add(port);
        }
      }
    }

    return localPortsList;
  }

  public static List<ZkServer> startLocalZookeeper(List<Integer> localPortsList,
                                                   String zkTestDataRootDir,
                                                   int tickTime)
    throws IOException
  {
    List<ZkServer> localZkServers = new ArrayList<ZkServer>();

    int count = 0;
    for (int port : localPortsList)
    {
      ZkServer zkServer = startZkServer(zkTestDataRootDir, count++, port, tickTime);
      localZkServers.add(zkServer);
    }

    return localZkServers;
  }

  public static ZkServer startZkServer(String zkTestDataRootDir,
                                       int machineId,
                                       int port,
                                       int tickTime) throws IOException
  {
    File zkTestDataRootDirFile = new File(zkTestDataRootDir);
    zkTestDataRootDirFile.mkdirs();

    String dataPath = zkTestDataRootDir + "/" + machineId + "/" + port + "/data";
    String logPath = zkTestDataRootDir + "/" + machineId + "/" +  port + "/log";

    FileUtils.deleteDirectory(new File(dataPath));
    FileUtils.deleteDirectory(new File(logPath));

    IDefaultNameSpace mockDefaultNameSpace = new IDefaultNameSpace()
      {
        @Override
        public void createDefaultNameSpace(ZkClient zkClient)
        {
        }
      };

    LOG.info("Starting local zookeeper on port=" + port + "; dataPath=" + dataPath);
    ZkServer zkServer = new ZkServer(dataPath, logPath, mockDefaultNameSpace, port, tickTime);

    zkServer.start();
    return zkServer;
  }

  static public void stopLocalZookeeper(List<ZkServer> localZkServers)
  {
    for (ZkServer zkServer : localZkServers)
    {
      zkServer.shutdown();
    }
  }
}
