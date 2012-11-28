package com.linkedin.databus.bootstrap.common;

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
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.BackoffTimerStaticConfig;
import com.linkedin.databus2.core.BackoffTimerStaticConfigBuilder;
import com.linkedin.databus2.core.container.netty.ServerContainer;

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
    LOG.info("Bootstrap service DB username:" + _bootstrapDBUsername);
    LOG.info("Bootstrap service DB password: ***");
    LOG.info("Bootstrap service DB hostname:" + _bootstrapDBHostname);
    LOG.info("Bootstrap service batch size:" + _bootstrapBatchSize);
    LOG.info("Bootstrap produer log size:" + _bootstrapLogSize);
    LOG.info("Backoff Timer COnfig :" + _retryTimer);
    return new BootstrapReadOnlyConfig(_bootstrapDBUsername,
                                       _bootstrapDBPassword, _bootstrapDBHostname, _bootstrapDBName,
                                       _bootstrapBatchSize, _bootstrapLogSize,
                                       _bootstrapDBStateCheck,
                                       _client.build(), _container.build(), _retryTimer.build());
  }

}
