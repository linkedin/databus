package com.linkedin.databus.bootstrap.common;

import com.linkedin.databus.client.DatabusHttpClientImpl;
import com.linkedin.databus2.core.BackoffTimerStaticConfig;
import com.linkedin.databus2.core.container.netty.ServerContainer;

/**
 * Configuration for bootstrap-related components: DatabusBootstrapProducer, BootstrapHttpServer,
 * BootstrapSeederMain.
 */
public class BootstrapReadOnlyConfig
{

  private final String _bootstrapDBUsername;
  private final String _bootstrapDBPassword;
  private final String _bootstrapDBHostname;
  private final String _bootstrapDBName;
  private final long _bootstrapBatchSize;
  private final int _bootstrapLogSize;
  private final boolean _bootstrapDBStateCheck;
  private final DatabusHttpClientImpl.StaticConfig _client;
  private final ServerContainer.StaticConfig _container;
  private final BackoffTimerStaticConfig _retryConfig;
  
  public BootstrapReadOnlyConfig(String bootstrapDBUsername,
                                 String bootstrapDBPassword,
                                 String bootstrapDBHostname,
                                 String dbName,
                                 long bootstrapBatchSize,
                                 int bootstrapLogSize,
                                 boolean bootstrapDBStateCheck,
                                 DatabusHttpClientImpl.StaticConfig client,
                                 ServerContainer.StaticConfig container,
                                 BackoffTimerStaticConfig retryConfig)
  {
    super();
    _bootstrapDBUsername = bootstrapDBUsername;
    _bootstrapDBPassword = bootstrapDBPassword;
    _bootstrapDBHostname = bootstrapDBHostname;
    _bootstrapDBName = dbName;
    _bootstrapBatchSize = bootstrapBatchSize;
    _bootstrapLogSize = bootstrapLogSize;
    _bootstrapDBStateCheck = bootstrapDBStateCheck;
    _client = client;
    _container = container;
    _retryConfig = retryConfig;
  }

  /**
   * The bootstrap service http port.
   *
   * This is deprecated. Please use {@link #getContainer()}.getHttpPort()
   */
  @Deprecated
  public int getBootstrapHttpPort()
  {
    return _container.getHttpPort();
  }

  /**
   * The user name for access to the Bootstrap DB
   */
  public String getBootstrapDBUsername()
  {
    return _bootstrapDBUsername;
  }

  /**
   * The password for access to the Bootstrap DB
   */
  public String getBootstrapDBPassword()
  {
    return _bootstrapDBPassword;
  }

  /**
   * The hostname of the Bootstrap DB server
   */
  public String getBootstrapDBHostname()
  {
    return _bootstrapDBHostname;
  }

  /**
   * The Databus client configuration for the bootstrap producer
   */
  public DatabusHttpClientImpl.StaticConfig getClient()
  {
    return _client;
  }

  /**
   * The Netty container configuration
   */
  public ServerContainer.StaticConfig getContainer()
  {
    return _container;
  }

  /**
   * The maximum number of rows per bootstrap snapshot or catchup call.
   */
  public long getBootstrapBatchSize()
  {
    return _bootstrapBatchSize;
  }

  /**
   * The maximum number of rows in a bootstrap DB log_* table.
   */
  public int getBootstrapLogSize()
  {
    return _bootstrapLogSize;
  }


  public boolean isBootstrapDBStateCheck() {
	return _bootstrapDBStateCheck;
  }

  public BackoffTimerStaticConfig getRetryConfig()
  {
	  return _retryConfig;
  }

  public String getBootstrapDBName()
  {
	  return _bootstrapDBName;
  }
  
  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
	  return "BootstrapReadOnlyConfig [_bootstrapDBUsername="
	  + _bootstrapDBUsername + ", _bootstrapDBPassword=xxxxxx"
	  + ", _bootstrapDBHostname="
	  + _bootstrapDBHostname + ", _bootstrapBatchSize="
	  + _bootstrapBatchSize + ", _bootstrapLogSize=" + _bootstrapLogSize
	  + ", _bootstrapDBStateCheck=" + _bootstrapDBStateCheck
	  + ", _client=" + _client + ", _container=" + _container + ", BootstrapDBRetryTimer=" + _retryConfig + "]";
  }


}
