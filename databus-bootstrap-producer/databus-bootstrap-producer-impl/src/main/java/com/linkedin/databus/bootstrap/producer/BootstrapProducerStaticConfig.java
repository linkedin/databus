package com.linkedin.databus.bootstrap.producer;

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

import com.linkedin.databus.bootstrap.common.BootstrapCleanerStaticConfig;
import com.linkedin.databus.bootstrap.common.BootstrapReadOnlyConfig;
import com.linkedin.databus.client.DatabusHttpClientImpl.StaticConfig;
import com.linkedin.databus2.core.BackoffTimerStaticConfig;

public class BootstrapProducerStaticConfig extends BootstrapReadOnlyConfig
{

  private final boolean runApplierThreadOnStart;
  private final BootstrapCleanerStaticConfig cleaner;

  public BootstrapProducerStaticConfig(
      boolean runApplierThreadOnStart,
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

    super(_bootstrapDBUsername, _bootstrapDBPassword, _bootstrapDBHostname,
        _bootstrapName, _bootstrapBatchSize, _bootstrapBatchSize,
        _bootstrapBatchSize, _bootstrapLogSize, _bootstrapDBStateCheck,
        _client, _container, _retryConfig);

    this.runApplierThreadOnStart = runApplierThreadOnStart;
    this.cleaner = cleaner;
  }

  public boolean getRunApplierThreadOnStart()
  {
    return runApplierThreadOnStart;
  }

  public BootstrapCleanerStaticConfig getCleaner()
  {
    return cleaner;
  }
}
