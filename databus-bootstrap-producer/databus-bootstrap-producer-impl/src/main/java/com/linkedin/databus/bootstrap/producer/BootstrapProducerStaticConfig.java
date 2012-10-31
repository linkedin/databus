package com.linkedin.databus.bootstrap.producer;

import com.linkedin.databus.bootstrap.common.BootstrapCleanerStaticConfig;
import com.linkedin.databus.bootstrap.common.BootstrapReadOnlyConfig;
import com.linkedin.databus.client.DatabusHttpClientImpl.StaticConfig;
import com.linkedin.databus2.core.BackoffTimerStaticConfig;

public class BootstrapProducerStaticConfig extends BootstrapReadOnlyConfig {

  private final boolean runApplierThreadOnStart;
  private final BootstrapCleanerStaticConfig cleaner;
  

  public BootstrapProducerStaticConfig(boolean runApplierThreadOnStart,
                                       String _bootstrapDBUsername,
                                       String _bootstrapDBPassword,
                                       String _bootstrapDBHostname,
                                       String _bootstrapName,
                                       long _bootstrapBatchSize,
                                       int _bootstrapLogSize,
                                       boolean _bootstrapDBStateCheck,
                                       StaticConfig _client,
                                       com.linkedin.databus2.core.container.netty.ServerContainer.StaticConfig _container,
                                       BackoffTimerStaticConfig _retryConfig,
                                       BootstrapCleanerStaticConfig cleaner)
  {

    super(_bootstrapDBUsername,
           _bootstrapDBPassword,
           _bootstrapDBHostname,
           _bootstrapName,
           _bootstrapBatchSize,
           _bootstrapLogSize,
           _bootstrapDBStateCheck,
           _client,
           _container,
           _retryConfig);

    this.runApplierThreadOnStart = runApplierThreadOnStart;
    this.cleaner = cleaner;
  }

  public boolean getRunApplierThreadOnStart() {
    return runApplierThreadOnStart;
  }

  public BootstrapCleanerStaticConfig getCleaner()
  {
	  return cleaner;
  }
}
