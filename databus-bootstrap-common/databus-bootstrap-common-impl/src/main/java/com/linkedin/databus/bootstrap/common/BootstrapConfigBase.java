package com.linkedin.databus.bootstrap.common;
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


import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import com.linkedin.databus.client.DatabusHttpClientImpl;
import com.linkedin.databus.client.DatabusHttpClientImpl.CheckpointPersistenceStaticConfig.ProviderType;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.BackoffTimerStaticConfigBuilder;
import com.linkedin.databus2.core.container.netty.ServerContainer;

public class BootstrapConfigBase
{
  public static final String MODULE = BootstrapConfig.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  public static final String BOOTSTRAP_DB_PROPS_OPT_LONG_NAME = "bootstrap_db_props";
  public static final char BOOTSTRAP_DB_PROP_OPT_CHAR = 'p';

  public static final int DEFAULT_BOOTSTRAP_HTTP_PORT = 6060;
  public static final String DEFAULT_BOOTSTRAP_DB_USERNAME = "bootstrap";
  public static final String DEFAULT_BOOTSTRAP_DB_PASSWORD = "bootstrap";
  public static final String DEFAULT_BOOTSTRAP_DB_HOSTNAME = "localhost";
  public static final String DEFAULT_BOOTSTRAP_DB_NAME = "bootstrap";

  public static final long DEFAULT_BOOTSTRAP_FETCH_SIZE = 1000;
  public static final int DEFAULT_BOOTSTRAP_LOG_SIZE = 10000;
  public static final boolean DEFAULT_BOOTSTRAP_DB_STATE_CHECK = false;

  protected String _bootstrapDBUsername = DEFAULT_BOOTSTRAP_DB_USERNAME;
  protected String _bootstrapDBPassword = DEFAULT_BOOTSTRAP_DB_PASSWORD;
  protected String _bootstrapDBHostname = DEFAULT_BOOTSTRAP_DB_HOSTNAME;
  protected String _bootstrapDBName = DEFAULT_BOOTSTRAP_DB_NAME;
  protected long _bootstrapBatchSize = DEFAULT_BOOTSTRAP_FETCH_SIZE;
  protected int _bootstrapLogSize = DEFAULT_BOOTSTRAP_LOG_SIZE;
  protected boolean _bootstrapDBStateCheck = DEFAULT_BOOTSTRAP_DB_STATE_CHECK;

  protected DatabusHttpClientImpl.Config _client;
  protected ServerContainer.Config _container;
  protected BackoffTimerStaticConfigBuilder _retryTimer;


  @SuppressWarnings("static-access")
  public static Properties loadConfigProperties(String[] args) throws IOException
  {
      CommandLineParser cliParser = new GnuParser();

      Option dbOption = OptionBuilder.withLongOpt(BOOTSTRAP_DB_PROPS_OPT_LONG_NAME)
      .withDescription("Bootstrap producer properties to use")
      .hasArg()
      .withArgName("property_file")
      .create(BOOTSTRAP_DB_PROP_OPT_CHAR);
      Options options = new Options();
      options.addOption(dbOption);

      CommandLine cmd = null;
      try
      {
          cmd = cliParser.parse(options, args);
      }
      catch (ParseException pe)
      {
          throw new RuntimeException("BootstrapConfig: failed to parse command-line options.", pe);
      }

      Properties props = null;
      if (cmd.hasOption(BOOTSTRAP_DB_PROP_OPT_CHAR))
      {
          String propFile = cmd.getOptionValue(BOOTSTRAP_DB_PROP_OPT_CHAR);
          LOG.info("Loading bootstrap DB config from properties file " + propFile);
          props = new Properties();
          FileInputStream f = new FileInputStream(propFile);
          try
          {
        	  props.load(f);
          } finally {
        	  if ( null != f)
        		  f.close();
          }
      }
      else
      {
          LOG.info("Using system properties for bootstrap DB config");
      }

      return props;
  }

  public BootstrapConfigBase() throws IOException
  {
      super();
      _client = new DatabusHttpClientImpl.Config();
      _container = new ServerContainer.Config();
      setBootstrapHttpPort(DEFAULT_BOOTSTRAP_HTTP_PORT);
      _client.getCheckpointPersistence().setType(ProviderType.FILE_SYSTEM.toString());
      _client.getCheckpointPersistence().getFileSystem().setRootDirectory("./bootstrap-checkpoints");

      _retryTimer = new BackoffTimerStaticConfigBuilder();
  }

  public String getBootstrapDBUsername()
  {
      return _bootstrapDBUsername;
  }

  public void setBootstrapDBUsername(String dbUsername)
  {
      _bootstrapDBUsername = dbUsername;
  }

  public String getBootstrapDBName()
  {
      return _bootstrapDBName;
  }

  public void setBootstrapDBName(String dbName)
  {
      _bootstrapDBName = dbName;
  }
  
  
  public String getBootstrapDBPassword()
  {
      return _bootstrapDBPassword;
  }

  public String getBootstrapDBHostname()
  {
      return _bootstrapDBHostname;
  }

  /**
   * The HTTP port of the bootstrap producer or server.
   *
   * This property is deprecated. Please use {@link #getContainer()}.getHttpPort().
   */
  @Deprecated
  public int getBootstrapHttpPort()
  {
      return _container.getHttpPort();
  }

  public long getBootstrapBatchSize()
  {
      return _bootstrapBatchSize;
  }

  public int getBootstrapLogSize()
  {
      return _bootstrapLogSize;
  }

  /**
   * Changes the HTTP port of the bootstrap producer or server.
   *
   * This property is deprecated. Please use {@link #getContainer()}.setHttpPort(int).
   */
  @Deprecated
  public void setBootstrapHttpPort(int bootstrapHttpPort)
  {
      getContainer().setHttpPort(bootstrapHttpPort);
  }

  public void setBootstrapDBPassword(String bootstrapDBPassword)
  {
      _bootstrapDBPassword = bootstrapDBPassword;
  }

  public void setBootstrapDBHostname(String bootstrapDBHostname)
  {
      _bootstrapDBHostname = bootstrapDBHostname;
  }

  public void setBootstrapBatchSize(long bootstrapBatchSize)
  {
      _bootstrapBatchSize = bootstrapBatchSize;
  }

  public void setBootstrapLogSize(int bootstrapLogSize)
  {
      _bootstrapLogSize = bootstrapLogSize;
  }

  public void setBootstrapDBStateCheck(boolean bootstrapDBStateCheck)
  {
      this._bootstrapDBStateCheck = bootstrapDBStateCheck;
  }

  public boolean getBootstrapDBStateCheck()
  {
      return _bootstrapDBStateCheck;
  }



  public DatabusHttpClientImpl.Config getClient()
  {
      return _client;
  }

  public void setClient(DatabusHttpClientImpl.Config client)
  {
      _client = client;
  }

  public ServerContainer.Config getContainer()
  {
      return _container;
  }

  public void setContainer(ServerContainer.Config container)
  {
      _container = container;
  }

  public BackoffTimerStaticConfigBuilder getRetryTimer() {
      return _retryTimer;
  }

  public void setRetryTimer(BackoffTimerStaticConfigBuilder retryTimer) {
      this._retryTimer = retryTimer;
  }
}
