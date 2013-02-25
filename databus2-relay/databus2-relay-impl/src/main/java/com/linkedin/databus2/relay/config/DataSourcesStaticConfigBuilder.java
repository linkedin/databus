package com.linkedin.databus2.relay.config;

import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.seq.MaxSCNReaderWriterConfig;

public class DataSourcesStaticConfigBuilder
       implements ConfigBuilder<DataSourcesStaticConfig>
{
  private MaxSCNReaderWriterConfig _sequenceNumbersHandler = new MaxSCNReaderWriterConfig();

  @Override
  public DataSourcesStaticConfig build() throws InvalidConfigException
  {
    return new DataSourcesStaticConfig(_sequenceNumbersHandler.build());
  }

  public MaxSCNReaderWriterConfig getSequenceNumbersHandler()
  {
    return _sequenceNumbersHandler;
  }

}
