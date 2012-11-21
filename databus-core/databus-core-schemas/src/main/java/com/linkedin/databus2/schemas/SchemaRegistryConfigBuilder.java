package com.linkedin.databus2.schemas;

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
