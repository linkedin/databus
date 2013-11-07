package com.linkedin.databus.bootstrap.server;

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
import java.nio.ByteOrder;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.linkedin.databus.bootstrap.common.BootstrapHttpStatsCollector;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.monitoring.mbean.DatabusComponentAdmin;
import com.linkedin.databus2.core.container.netty.ServerContainer;
import com.linkedin.databus2.core.container.request.ConfigRequestProcessor;
import com.linkedin.databus2.core.container.request.ContainerOperationProcessor;
import com.linkedin.databus2.core.container.request.EchoRequestProcessor;
import com.linkedin.databus2.core.container.request.RequestProcessorRegistry;

public class BootstrapHttpServer extends ServerContainer
{
  public static final String MODULE = BootstrapHttpServer.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private final BootstrapServerStaticConfig _bootstrapServerConfig;
  private final BootstrapHttpStatsCollector _bootstrapHttpStatsCollector;

  public BootstrapHttpServer(BootstrapServerConfig config)
         throws IOException, InvalidConfigException, DatabusException
  {
    this(config.build());
  }


  //V2 bootstrap server should use BIG_ENDIAN and v3 (which extends this class) will use LITTLE_ENDIAN for byte order.
  public BootstrapHttpServer(BootstrapServerStaticConfig bootstrapServerConfig, ByteOrder byteOrder)
         throws IOException, InvalidConfigException, DatabusException
  {
    super(bootstrapServerConfig.getDb().getContainer(), byteOrder);
    _bootstrapServerConfig = bootstrapServerConfig;

    BootstrapHttpStatsCollector httpStatsColl = null;
    if (null == httpStatsColl)
    {

      httpStatsColl = new BootstrapHttpStatsCollector(getContainerStaticConfig().getId(),
                                                  "bootstrapHttpOutbound",
                                                  true,
                                                  //_bootstrapStaticConfig.getRuntime().getHttpStatsCollector().isEnabled(),
                                                  true,
                                                  getMbeanServer());

    }
    _bootstrapHttpStatsCollector = httpStatsColl;

    initializeBootstrapServerCommandProcessors();
  }

  /**
   * The default constructor. If the byte order is not explicitly set, we use BIG_ENDIAN (preserving existing v2 bootstrap behaviour).
   * @param bootstrapServerConfig
   * @throws DatabusException
   * @throws IOException
   */
  public BootstrapHttpServer(BootstrapServerStaticConfig bootstrapServerConfig)
      throws DatabusException, IOException
  {
    this(bootstrapServerConfig, ByteOrder.BIG_ENDIAN);
  }

  public BootstrapHttpStatsCollector getBootstrapStatsCollector() {
    return _bootstrapHttpStatsCollector;
  }

  @Override
  protected DatabusComponentAdmin createComponentAdmin()
  {
    return new DatabusComponentAdmin(this,
                                     getMbeanServer(),
                                     BootstrapHttpServer.class.getSimpleName());
  }

  public static void main(String[] args) throws Exception
  {
    // use server container to pass the command line
    Properties startupProps = ServerContainer.processCommandLineArgs(args);


    BootstrapServerConfig config = new BootstrapServerConfig();

    ConfigLoader<BootstrapServerStaticConfig> configLoader =
      new ConfigLoader<BootstrapServerStaticConfig>("databus.bootstrap.", config);

    BootstrapServerStaticConfig staticConfig = configLoader.loadConfig(startupProps);

    BootstrapHttpServer bootstrapServer = new BootstrapHttpServer(staticConfig);

    // Bind and start to accept incoming connections.
    try
    {
      bootstrapServer.registerShutdownHook();
      bootstrapServer.startAndBlock();
    }
    catch (Exception e)
    {
      LOG.error("Error starting the bootstrap server", e);
    }
    LOG.info("Exiting bootstrap server");
  }

  @Override
  public void pause()
  {
    getComponentStatus().pause();
  }

  @Override
  public void resume()
  {
    // tell all processes to resume processing of client request
    getComponentStatus().resume();
  }

  @Override
  public void suspendOnError(Throwable cause)
  {
    getComponentStatus().suspendOnError(cause);
  }

  protected void initializeBootstrapServerCommandProcessors() throws DatabusException
  {
    LOG.info("Initializing Bootstrap HTTP Server");
    LOG.info("Config=" + _bootstrapServerConfig);
    try{
      RequestProcessorRegistry processorRegistry = getProcessorRegistry();
      processorRegistry.register(EchoRequestProcessor.COMMAND_NAME, new EchoRequestProcessor(null));
      processorRegistry.register(ConfigRequestProcessor.COMMAND_NAME,
                                 new ConfigRequestProcessor(null, this));
      processorRegistry.register(BootstrapRequestProcessor.COMMAND_NAME,
                                 new BootstrapRequestProcessor(null, _bootstrapServerConfig, this));
      processorRegistry.register(StartSCNRequestProcessor.COMMAND_NAME,
                                 new StartSCNRequestProcessor(null, _bootstrapServerConfig, this));
      processorRegistry.register(TargetSCNRequestProcessor.COMMAND_NAME,
                                 new TargetSCNRequestProcessor(null, _bootstrapServerConfig, this));
      processorRegistry.register(ContainerOperationProcessor.COMMAND_NAME,
                                 new ContainerOperationProcessor(null, this));
    }
    catch (SQLException sqle)
    {
      throw new DatabusException("command registration failed", sqle);
    }
    catch (InstantiationException e)
    {
      throw new DatabusException("command registration failed", e);
    }
    catch (IllegalAccessException e)
    {
      throw new DatabusException("command registration failed", e);
    }
    catch (ClassNotFoundException e)
    {
      throw new DatabusException("command registration failed", e);
    }

    LOG.info("Done Initializing Bootstrap HTTP Server");

  }
}
