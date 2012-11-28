package com.linkedin.databus.bootstrap.producer;

import java.io.IOException;

import com.linkedin.databus.bootstrap.common.BootstrapCleanerConfig;
import com.linkedin.databus.bootstrap.common.BootstrapConfig;
import com.linkedin.databus.bootstrap.common.BootstrapConfigBase;
import com.linkedin.databus.bootstrap.common.BootstrapReadOnlyConfig;
import com.linkedin.databus.client.DatabusHttpClientImpl.StaticConfig;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.BackoffTimerStaticConfig;

public class BootstrapProducerConfig extends BootstrapConfigBase 
	implements ConfigBuilder<BootstrapProducerStaticConfig> 
{
  public static final boolean DEFAULT_RUN_APPLIERTHREAD_ONSTART = true;
  
  private boolean runApplierThreadOnStart = DEFAULT_RUN_APPLIERTHREAD_ONSTART;
  private BootstrapCleanerConfig cleaner;
  
  public BootstrapProducerConfig() throws IOException
  {
    super();
    cleaner = new BootstrapCleanerConfig();
  }

  @Override
  public BootstrapProducerStaticConfig build() throws InvalidConfigException
  {
    return new BootstrapProducerStaticConfig(runApplierThreadOnStart,
                                             _bootstrapDBUsername,
                                             _bootstrapDBPassword, 
                                             _bootstrapDBHostname,
                                             _bootstrapDBName,
                                             _bootstrapBatchSize, 
                                             _bootstrapLogSize,
                                             _bootstrapDBStateCheck,
                                             _client.build(), 
                                             _container.build(), 
                                             _retryTimer.build(),
                                             cleaner.build());
  }

  public boolean getRunApplierThreadOnStart()
  {
    return runApplierThreadOnStart;
  }

  public void setRunApplierThreadOnStart(boolean runApplierThreadOnStart)
  {
    this.runApplierThreadOnStart = runApplierThreadOnStart;
  }

  public BootstrapCleanerConfig getCleaner()
  {
	  return cleaner;
  }
  
  public void setCleaner(BootstrapCleanerConfig cleaner)
  {
	  this.cleaner = cleaner;
  }
}
