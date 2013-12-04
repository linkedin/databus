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


import java.io.IOException;

import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;

public class BootstrapConfig extends BootstrapConfigBase implements ConfigBuilder<BootstrapReadOnlyConfig>
{

  public BootstrapConfig() throws IOException
  {
    super();
  }

  @Override
  public BootstrapReadOnlyConfig build() throws InvalidConfigException
  {
    LOG.info("Bootstrap service using http port:" + _container.getHttpPort());
    LOG.info("Bootstrap service DB hostname:" + _bootstrapDBHostname);
    LOG.info("Bootstrap service batch size:" + _bootstrapBatchSize);
    LOG.info("Bootstrap service snapshot batch size:" + getBootstrapSnapshotBatchSize());
    LOG.info("Bootstrap service catchup batch size:" + getBootstrapCatchupBatchSize());
    LOG.info("Bootstrap producer log size:" + _bootstrapLogSize);
    LOG.info("Backoff Timer Config :" + _retryTimer);
    return new BootstrapReadOnlyConfig(_bootstrapDBUsername,
                                       _bootstrapDBPassword, _bootstrapDBHostname, _bootstrapDBName,
                                       _bootstrapBatchSize, getBootstrapSnapshotBatchSize(), getBootstrapCatchupBatchSize(), _bootstrapLogSize,
                                       _bootstrapDBStateCheck,
                                       _client.build(), _container.build(), _retryTimer.build());
  }

}
