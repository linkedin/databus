package com.linkedin.databus2.core.seq;

import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;

public class MaxSCNReaderWriterConfig implements ConfigBuilder<MaxSCNReaderWriterStaticConfig>
{

  private String _type;
  private FileMaxSCNHandler.Config _file;
  private MaxSCNReaderWriter _existing;

  public MaxSCNReaderWriterConfig()
  {
    _type = MaxSCNReaderWriterStaticConfig.Type.FILE.toString();
    _existing = null;
    _file = new FileMaxSCNHandler.Config();
  }

  public String getType()
  {
    return _type;
  }

  public void setType(String type)
  {
    _type = type;
  }

  public FileMaxSCNHandler.Config getFile()
  {
    return _file;
  }

  public void setFile(FileMaxSCNHandler.Config file)
  {
    _file = file;
  }

  public MaxSCNReaderWriter fixmeGetExisting()
  {
    return _existing;
  }

  public void fixmeSetExisting(MaxSCNReaderWriter existing)
  {
    _existing = existing;
  }

  @Override
  public MaxSCNReaderWriterStaticConfig build() throws InvalidConfigException
  {
    MaxSCNReaderWriterStaticConfig.Type handlerType = null;
    try
    {
      handlerType = MaxSCNReaderWriterStaticConfig.Type.valueOf(_type);
    }
    catch (Exception e)
    {
      throw new InvalidConfigException("invalid max scn reader/writer type:" + _type );
    }

    if (MaxSCNReaderWriterStaticConfig.Type.EXISTING == handlerType && null == _existing)
    {
      throw new InvalidConfigException("No existing max scn reader/writer specified ");
    }

    return new MaxSCNReaderWriterStaticConfig(handlerType, _file.build(), _existing);
  }

}
