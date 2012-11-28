/**
 * $Id: RemovePaths.java 155034 2010-12-12 06:43:52Z mstuart $ */
package com.linkedin.databus.groupleader.example.simple;

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
