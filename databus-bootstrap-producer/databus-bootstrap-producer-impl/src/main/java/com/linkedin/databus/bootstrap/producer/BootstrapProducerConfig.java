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

import java.io.IOException;

import com.linkedin.databus.bootstrap.common.BootstrapCleanerConfig;
import com.linkedin.databus.bootstrap.common.BootstrapConfigBase;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;

public class BootstrapProducerConfig extends BootstrapConfigBase implements
ConfigBuilder<BootstrapProducerStaticConfig>
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
        _bootstrapDBUsername, _bootstrapDBPassword, _bootstrapDBHostname,
        _bootstrapDBName, _bootstrapBatchSize, _bootstrapLogSize,
        _bootstrapDBStateCheck, _client.build(), _container.build(),
        _retryTimer.build(), cleaner.build());
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
