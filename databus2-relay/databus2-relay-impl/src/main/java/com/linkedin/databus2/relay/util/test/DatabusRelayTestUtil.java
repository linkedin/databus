package com.linkedin.databus2.relay.util.test;

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

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.BackoffTimerStaticConfigBuilder;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.netty.ServerContainer;
import com.linkedin.databus2.relay.DatabusRelayMain;
import com.linkedin.databus2.relay.config.LogicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;

public class DatabusRelayTestUtil
{
  public final static String MODULE = DatabusRelayTestUtil.class.getName();
  public final static Logger LOG = Logger.getLogger(MODULE);

  /**
   * ======================================= Config helper methods ============================
   */

  /**
   * helper function used by createDatabusRelay to create buffer config.
   * Useful for creating eventBuffers as well
   *
   * @param eventBufferMaxSize
   * @param readBufferSize
   * @param scnIndexSize
   * @return
   */
  public static DbusEventBuffer.Config createBufferConfig(
      long eventBufferMaxSize, int readBufferSize, int scnIndexSize)
  {
    DbusEventBuffer.Config bconf = new DbusEventBuffer.Config();
    bconf.setMaxSize(eventBufferMaxSize);
    bconf.setAverageEventSize(readBufferSize);
    bconf.setScnIndexSize(scnIndexSize);
    bconf.setAllocationPolicy("DIRECT_MEMORY");
    return bconf;
  }

  /**
   * helper function used by createDatabusRelay to create a container config
   * (jetty)
   *
   * @param id
   * @param httpPort
   * @return
   */
  public static ServerContainer.Config createContainerConfig(int id,
                                                             int httpPort)
  {
    ServerContainer.Config sconf = new ServerContainer.Config();
    sconf.setHealthcheckPath("/admin");
    sconf.setHttpPort(httpPort);
    sconf.setId(id);
    sconf.getJmx().setRmiEnabled(false);
    final int threadNum = Math.max(Runtime.getRuntime().availableProcessors() / 2, 3);
    sconf.getRuntime().getDefaultExecutor().setCoreThreadsNum(threadNum);
    sconf.getRuntime().getDefaultExecutor().setMaxThreadsNum(threadNum);
    return sconf;
  }

  /**
   * Return the physical db name given a com.linkedin.events.<dbname>.
   * <table>
   * string
   *
   * @param s
   * @return
   */

  public static String getPhysicalSrcName(String s)
  {
    String[] cmpt = s.split("\\.");
    String name = (cmpt.length >= 4) ? cmpt[3] : s;
    return name;
  }

  /**
   * Convenience class to create config builders for relays
   *
   * @param id
   * @param name
   * @param uri
   * @param pollIntervalMs
   * @param eventRatePerSec
   * @param logicalSources
   * @return
   */
  public static PhysicalSourceConfig createPhysicalConfigBuilder(short id,
                                                                 String name, String uri, long pollIntervalMs, int eventRatePerSec,
                                                                 String[] logicalSources)
  {
    return createPhysicalConfigBuilder(id, name, uri, pollIntervalMs,
                                       eventRatePerSec, 0, 1 * 1024 * 1024, 5 * 1024 * 1024,
                                       logicalSources);
  }

  /**
   * Convenience class to create configs for relays
   *
   * @param id
   * @param name
   * @param uri
   * @param pollIntervalMs
   * @param eventRatePerSec
   * @param restartScnOffset
   * @param largestEventSize
   * @param largestWindowSize
   * @param logicalSources
   * @return
   */
  public static PhysicalSourceConfig createPhysicalConfigBuilder(short id,
                                                                 String name, String uri, long pollIntervalMs, int eventRatePerSec,
                                                                 long restartScnOffset, int largestEventSize,
                                                                 long largestWindowSize, String[] logicalSources)
  {
    LogicalSourceConfig[] lSourceConfigs = new LogicalSourceConfig[logicalSources.length];
    short lid = (short) (id + 1);
    int i=0;
    for (String schemaName : logicalSources)
    {
      LogicalSourceConfig lConf = new LogicalSourceConfig();
      lConf.setId(lid++);
      lConf.setName(schemaName);
      // this is table name in the oracle source world
      lConf.setUri(schemaName);
      lConf.setPartitionFunction("constant:1");
      lSourceConfigs[i]=lConf;
      i++;
    }

    return createPhysicalConfigBuilder(id,
                                       name,
                                       uri,
                                       pollIntervalMs,
                                       eventRatePerSec,
                                       restartScnOffset,
                                       largestEventSize,
                                       largestWindowSize,
                                       lSourceConfigs);
  }

  public static PhysicalSourceConfig createPhysicalConfigBuilder(short id,
                                                                  String name,
                                                                  String uri,
                                                                  long pollIntervalMs,
                                                                  int eventRatePerSec,
                                                                  long restartScnOffset,
                                                                  int largestEventSize,
                                                                  long largestWindowSize,
                                                                  LogicalSourceConfig[] lSourceConfigs)
  {
    PhysicalSourceConfig pConfig = new PhysicalSourceConfig();
    pConfig.setId(id);
    pConfig.setName(name);
    pConfig.setUri(uri);
    pConfig.setEventRatePerSec(eventRatePerSec);
    pConfig.setRestartScnOffset(restartScnOffset);
    pConfig.setLargestEventSizeInBytes(largestEventSize);
    pConfig.setLargestWindowSizeInBytes(largestWindowSize);
    BackoffTimerStaticConfigBuilder retriesConf = new BackoffTimerStaticConfigBuilder();
    retriesConf.setInitSleep(pollIntervalMs);
    pConfig.setRetries(retriesConf);

    for(LogicalSourceConfig lConf : lSourceConfigs)
    {
      pConfig.addSource(lConf);
    }

    return pConfig;
  }

  public static LogicalSourceConfig createLogicalSourceConfig(short logicalSourceId, String logicalSourceName, String logicalSourceUri, String logicalPartitionFunction)
  {
    LogicalSourceConfig lconf = new LogicalSourceConfig();
    lconf.setId(logicalSourceId);
    lconf.setName(logicalSourceName);
    lconf.setUri(logicalSourceUri);
    lconf.setPartitionFunction(logicalPartitionFunction);
    return lconf;
  }

  public static HttpRelay.Config createHttpRelayConfig(int relayId, int httpPort, long maxBufferSize)
      throws IOException
  {
    HttpRelay.Config httpRelayConfig = new HttpRelay.Config();
    ServerContainer.Config containerConfig = createContainerConfig(
        relayId, httpPort);
    DbusEventBuffer.Config bufferConfig = createBufferConfig(
        maxBufferSize, safeLongToInt(maxBufferSize / 10), safeLongToInt(Math.max(maxBufferSize / 256, 256)));
    httpRelayConfig.setContainer(containerConfig);
    httpRelayConfig.setEventBuffer(bufferConfig);
    httpRelayConfig.setStartDbPuller("true");
    return httpRelayConfig;
  }

  public static int getRandomRelayId()
  {
    return -1;
  }

  private static int safeLongToInt(long l) {
    if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) {
      throw new IllegalArgumentException
          (l + " cannot be cast to int without changing its value.");
    }
    return (int) l;
  }

  public static void setSchemaRegistryLocation(HttpRelay.Config relayHttpConfig, String schemaRegistryLocation)
  {
    relayHttpConfig.getSchemaRegistry().getFileSystem().setSchemaDir(schemaRegistryLocation);
  }

  /**
   * ============================================== Databus relay creation helper methods ==============================================
   */


  /**
   * Convenience class to create DatabusRelayMain
   *
   * @param relayId
   * @param httpPort
   * @param maxBufferSize
   * @param sourceConfigs
   * @return
   */
  public static DatabusRelayMain createDatabusRelay(int relayId,
                                                    int httpPort, long maxBufferSize,
                                                    PhysicalSourceConfig[] sourceConfigs)
  {
    try
    {
      HttpRelay.Config httpRelayConfig = createHttpRelayConfig(relayId, httpPort, maxBufferSize);
      return createDatabusRelay(sourceConfigs, httpRelayConfig);
    }
    catch (IOException e)
    {
      LOG.error("IO Exception " + e);
    }
    catch (InvalidConfigException e)
    {
      LOG.error("Invalid config " + e);
    }
    catch (DatabusException e)
    {
      LOG.error("Databus Exception " + e);
    }
    return null;
  }

  public static DatabusRelayMain createDatabusRelayWithSchemaReg(
         int relayId, int httpPort, long maxBufferSize, PhysicalSourceConfig[] sourceConfigs, String schemRegPath)
  {
    try
    {
      HttpRelay.Config httpRelayConfig = createHttpRelayConfig(relayId, httpPort, maxBufferSize);
      httpRelayConfig.getSchemaRegistry().getFileSystem().setSchemaDir(schemRegPath);
      return createDatabusRelay(sourceConfigs, httpRelayConfig);
    }
    catch (IOException e)
    {
      LOG.error("IO Exception " + e);
    }
    catch (InvalidConfigException e)
    {
      LOG.error("Invalid config " + e);
    }
    catch (DatabusException e)
    {
      LOG.error("Databus Exception " + e);
    }
    return null;
  }

  public static DatabusRelayMain createDatabusRelay(PhysicalSourceConfig[] sourceConfigs,
                                                     HttpRelay.Config httpRelayConfig)
      throws IOException, DatabusException
  {
    PhysicalSourceStaticConfig[] pStaticConfigs = new PhysicalSourceStaticConfig[sourceConfigs.length];
    int i = 0;
    for (PhysicalSourceConfig pConf : sourceConfigs)
    {
      // register logical sources! I guess the physical db name is
      // prefixed to the id or what?
      for (LogicalSourceConfig lsc : pConf.getSources())
      {
        httpRelayConfig.setSourceName("" + lsc.getId(),
                                      lsc.getName());
      }
      pStaticConfigs[i++] = pConf.build();
    }
    DatabusRelayMain relayMain = new DatabusRelayMain(
        httpRelayConfig.build(), pStaticConfigs);
    return relayMain;
  }

  /**
   * Convenience class to operate DatabusRelayMain in a thread
   *
   * @author snagaraj
   *
   */
  static public class RelayRunner extends Thread
  {
    private final DatabusRelayMain _relay;

    public RelayRunner(DatabusRelayMain relay)
    {
      _relay = relay;
    }

    @Override
    public void run()
    {
      try
      {
        _relay.initProducers();
      }
      catch (Exception e)
      {
        LOG.error("Exception ", e);
        return;
      }
      _relay.startAsynchronously();
    }

    public void pause()
    {
      _relay.pause();
    }

    public void unpause()
    {
      _relay.resume();
    }

    public boolean shutdown(int timeoutMs)
    {
      _relay.shutdown();
      try
      {
        if (timeoutMs > 0)
        {
          _relay.awaitShutdown(timeoutMs);
        }
        else
        {
          _relay.awaitShutdown();
        }
      }
      catch (TimeoutException e)
      {
        LOG.error("Not shutdown in " + timeoutMs + " ms");
        return false;
      }
      catch (InterruptedException e)
      {
        LOG.info("Interrupted before " + timeoutMs + " ms");
        return true;
      }
      return true;
    }

    public DatabusRelayMain getRelay()
    {
      return _relay;
    }

  }
}
