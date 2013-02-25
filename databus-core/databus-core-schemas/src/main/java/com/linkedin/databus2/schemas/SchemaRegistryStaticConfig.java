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



/**
 * Readonly configuration for schema registry
 * @author cbotev
 *
 */
public class SchemaRegistryStaticConfig
{
  public enum RegistryType
  {
    FILE_SYSTEM,
    REMOTE,
    EXISTING
  }

  private final RegistryType _type;
  private final FileSystemSchemaRegistryService.StaticConfig _fileSystemConfig;
  private final SchemaRegistryService _existingService;

  public SchemaRegistryStaticConfig(RegistryType type,
                                    FileSystemSchemaRegistryService.StaticConfig fileSystemConfig,
                                    SchemaRegistryService existingService)
  {
    super();
    _type = type;
    _fileSystemConfig = fileSystemConfig;
    _existingService = existingService;
  }

  public RegistryType getType()
  {
    return _type;
  }

  public FileSystemSchemaRegistryService.StaticConfig getFileSystem()
  {
    return _fileSystemConfig;
  }

  public SchemaRegistryService getExistingService()
  {
    return _existingService;
  }

}
