/**
 * $Id: RemovePaths.java 155034 2010-12-12 06:43:52Z mstuart $ */
package com.linkedin.databus.groupleader.example.simple;
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


import java.util.Arrays;

import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;

/**
 *
 * @author Mitch Stuart
 * @version $Revision: 155034 $
 */
public class RemovePaths
{
  private static final Logger LOG = Logger.getLogger(RemovePaths.class);

  /**
   * main
   */
  public static void main(String[] args)
  {
    String pathsToDeleteStr = ExampleUtils.getRequiredStringProperty("paths", LOG);

    String[] pathsToDelete = pathsToDeleteStr.split(",");

    LOG.info("Deleting paths: " + Arrays.toString(pathsToDelete));

    ZkClient zkClient = new ZkClient(
      ExampleUtils.getRequiredStringProperty("zkServerList", LOG),
      ExampleUtils.getRequiredIntProperty("sessionTimeoutMillis", LOG),
      ExampleUtils.getRequiredIntProperty("connectTimeoutMillis", LOG));

    for (String pathToDelete : pathsToDelete)
    {
      zkClient.deleteRecursive(pathToDelete);
    }

    zkClient.close();
  }

}
