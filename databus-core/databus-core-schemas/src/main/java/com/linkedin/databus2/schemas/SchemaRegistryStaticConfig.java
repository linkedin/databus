package com.linkedin.databus2.schemas;


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
