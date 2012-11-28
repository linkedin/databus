package com.linkedin.databus2.schemas;

import com.linkedin.databus.core.util.InvalidConfigException;


public class StandardSchemaRegistryFactory
{
  private final SchemaRegistryStaticConfig _config;
  private final SchemaRegistryConfigBuilder _configBuilder;

  public StandardSchemaRegistryFactory(SchemaRegistryStaticConfig config)
  {
    _config = config;
    _configBuilder = null;
  }

  public StandardSchemaRegistryFactory(SchemaRegistryConfigBuilder configBuilder)
  {
    _config = null;
    _configBuilder = configBuilder;
  }

  public SchemaRegistryService createSchemaRegistry() throws InvalidConfigException
  {
    SchemaRegistryStaticConfig conf = (null != _configBuilder) ? _configBuilder.build() : _config;

    SchemaRegistryService result = null;
    switch (conf.getType())
    {
      case EXISTING: result = conf.getExistingService(); break;
      case FILE_SYSTEM: result = FileSystemSchemaRegistryService.build(conf.getFileSystem()); break;
      default: throw new IllegalStateException("schema registry type not supported: " + conf.getType());
    }

    return result;
  }

}
