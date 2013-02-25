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


import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.schemas.SchemaRegistryStaticConfig.RegistryType;

/**
 * Config builder for Schema Registry configs
 * @author cbotev
 *
 */
public class SchemaRegistryConfigBuilder implements ConfigBuilder<SchemaRegistryStaticConfig>
{

  private String _type;
  private FileSystemSchemaRegistryService.Config _fileSystem;
  private SchemaRegistryService _existingService;

  public SchemaRegistryConfigBuilder()
  {
    _type = RegistryType.FILE_SYSTEM.toString();
    _fileSystem = new FileSystemSchemaRegistryService.Config();
    _existingService = null;
  }

  @Override
  public SchemaRegistryStaticConfig build() throws InvalidConfigException
  {
    RegistryType registryType = null;

    try
    {
      registryType = RegistryType.valueOf(_type);
    }
    catch (Exception e)
    {
      throw new InvalidConfigException("invalid schema registry type: " + _type);
    }

    _fileSystem.setEnabled(SchemaRegistryStaticConfig.RegistryType.FILE_SYSTEM == registryType);

    // TODO Add config verification
    return new SchemaRegistryStaticConfig(registryType, getFileSystem().build(), getExistingService());
  }

  public String getType()
  {
    return _type;
  }

  public void setType(String type)
  {
    _type = type;
  }

  public FileSystemSchemaRegistryService.Config getFileSystem()
  {
    return _fileSystem;
  }

  public void setFileSystem(FileSystemSchemaRegistryService.Config fileSystem)
  {
    _fileSystem = fileSystem;
  }

  public SchemaRegistryService getExistingService()
  {
    return _existingService;
  }

  public void useExistingService(SchemaRegistryService schemaRegistryService)
  {
    _existingService = schemaRegistryService;
  }

}
