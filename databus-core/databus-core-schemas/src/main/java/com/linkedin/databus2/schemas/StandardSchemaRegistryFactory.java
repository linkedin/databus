package com.linkedin.databus2.schemas;
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
