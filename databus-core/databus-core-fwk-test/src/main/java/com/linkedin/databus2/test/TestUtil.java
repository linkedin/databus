package com.linkedin.databus2.test;
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
import java.net.ConnectException;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.testng.Assert;



public class TestUtil
{
  public static final SimpleDateFormat SIMPLE_TIMESTAMP_FORMAT =
      new SimpleDateFormat("yyyyMMdd-HHmmss");

  public static void setupLoggingWithTimestampedFile(boolean logToConsole,
                                                     String fileNamePrefix,
                                                     String fileNameSuffix,
                                                     Level logLevel)
  {
    final String fileName = fileNamePrefix + SIMPLE_TIMESTAMP_FORMAT.format(new Date()) +
        fileNameSuffix;
    setupLogging(logToConsole, fileName, logLevel);
  }
  public static void setupLogging(Level logLevel) {
    setupLogging(true, "", logLevel);
  }

  public static void setupLogging(boolean logToConsole, String fileLogPath, Level logLevel)
  {
    PatternLayout defaultLayout = new PatternLayout("%d{ISO8601} [%t] (%p) {%c{1}} %m%n");
    Logger.getRootLogger().removeAllAppenders();

    if (logToConsole)
    {
      ConsoleAppender defaultAppender = new ConsoleAppender(defaultLayout);
      Logger.getRootLogger().addAppender(defaultAppender);
    }
    if (null != fileLogPath && 0 < fileLogPath.length())
    {
      File logFile = new File(fileLogPath);
      if (null != logFile.getParentFile() && ! logFile.getParentFile().exists())
      {
        if (! logFile.getParentFile().mkdirs())
        {
          Logger.getRootLogger().error("unable to create parent directory for log file: " + logFile);
        }
      }
      FileAppender fileAppender = null;
      try
      {
        fileAppender = new FileAppender(defaultLayout, fileLogPath);
      }
      catch (IOException io)
      {
        Logger.getRootLogger().error(io);
      }
      if (null != fileAppender) Logger.getRootLogger().addAppender(fileAppender);
    }

    Logger.getRootLogger().setLevel(logLevel);
  }

  /**
   * Sleep with catch of InterrupedException. If the exception is thrown, the sleep will be
   * interrupted without reaching the full timeout.*/
  public static void sleep(long millis)
  {
    try
    {
      Thread.sleep(millis);
    }
    catch (InterruptedException ie){}
  }

  /**
   * in case we are running in Eclipse - adjust base dir
   * @return true if running in eclipse
   */
  public static final String ECLIPSE_TEST_BASE_DIR = "ECLIPSE_TEST_BASE_DIR";
  public static boolean setupEclipsePath() {
    String prop = System.getProperty("java.class.path");
    if(prop!=null && prop.contains("eclipse")) {
      System.setProperty(ECLIPSE_TEST_BASE_DIR, "../..");
      return true;
    } else {
      return false;
    }
  }

  /**
   * Perform an assert on a condition with exponentially increasing timeouts between checks. Useful
   * for checking asynchronous conditions. The timeouts start from 1 ms and double on every failure
   * of the condition check until a maximum threshold is reached.
   *
   * @param check           implements the condition check
   * @param message         message for logging purposes
   * @param maxTimeoutMs    the maximum timeout in milliseconds.
   * @param log             logger for diagnostic messages (can be null)
   */
  public static void assertWithBackoff(ConditionCheck check, String message, long maxTimeoutMs,
                                       Logger log)
  {
    boolean done = check.check();
    long sleepDuration = 1;
    while (!done && sleepDuration < maxTimeoutMs)
    {
      if (null != log) log.info("sleeping for " + sleepDuration + " ms while waiting for condition: "
                                + message);
      sleep(sleepDuration);
      done = check.check();
      sleepDuration = (long)(1.3 * sleepDuration + 1);
    }

    Assert.assertTrue(check.check(), message);
  }


  /**
   * Checks if a server is running on a given host and port
   * @param host        the server host
   * @param port        the server port
   * @param log         logger for diagnostic messages (can be null)
   * @return true if successful
   */
  public static boolean checkServerRunning(String host, int port, Logger log)
  {
    return checkServerRunning(host, port, log, true);
  }

  /**
   * Checks if a server is running on a given host and port
   * @param host        the server host
   * @param port        the server port
   * @param log         logger for diagnostic messages (can be null)
   * @return true if successful
   * @throws IOException
   */
  public static boolean checkServerRunning(String host, int port, Logger log, boolean logError)
  {
    boolean success = false;

    try
    {
      Socket socket = new Socket(host, port);
      log.info( "host=" + host + " port=" + port);
      log.info("Socket Info:" + socket.toString());
      log.info("IsConnected=" + socket.isConnected() + " isClosed=" + socket.isClosed() + " isBound=" + socket.isBound());
      success = socket.isConnected();
      socket.close();
    }
    catch (ConnectException ce)
    {
      if (null != log) log.error("Fail to connect to port:" + port);
      if (logError && null != log) log.error("Connect error", ce);
      success = false;
    }
    catch (IOException e)
    {
      if (logError && null != log) log.error("connect error", e);
    }
    catch (RuntimeException e)
    {
      if (logError && null != log) log.error("runtime error", e);
    }

    return success;
  }

	/**
	 *
	 * @author snagaraj (originally by mitch stuart)
	 *
	 */


	public static List<ZkServer> startLocalZookeeper(
			List<Integer> localPortsList, String zkTestDataRootDir, int tickTime)
			throws IOException
	{
		List<ZkServer> localZkServers = new ArrayList<ZkServer>();

		int count = 0;
		for (int port : localPortsList)
		{
			ZkServer zkServer = startZkServer(zkTestDataRootDir, count++, port,
					tickTime);
			localZkServers.add(zkServer);
		}

		return localZkServers;
	}

	public static ZkServer startZkServer(String zkTestDataRootDir,
			int machineId, int port, int tickTime) throws IOException
	{
		File zkTestDataRootDirFile = new File(zkTestDataRootDir);
		zkTestDataRootDirFile.mkdirs();

		String dataPath = zkTestDataRootDir + "/" + machineId + "/" + port
				+ "/data";
		String logPath = zkTestDataRootDir + "/" + machineId + "/" + port
				+ "/log";

		FileUtils.deleteDirectory(new File(dataPath));
		FileUtils.deleteDirectory(new File(logPath));

		IDefaultNameSpace mockDefaultNameSpace = new IDefaultNameSpace()
		{
			@Override
			public void createDefaultNameSpace(ZkClient zkClient)
			{
			}
		};

		ZkServer zkServer = new ZkServer(dataPath, logPath,
				mockDefaultNameSpace, port, tickTime);

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

	static public String join(String[] name, String delim)
	{
	  StringBuilder joined = new StringBuilder();
	  if (name.length > 0)
	  {
	    joined.append(name[0]);
	    for (int i = 1; i < name.length; ++i)
	    {
	      joined.append(delim);
	      joined.append(name[i]);
	    }
	  }
	  return joined.toString();
	}

}
