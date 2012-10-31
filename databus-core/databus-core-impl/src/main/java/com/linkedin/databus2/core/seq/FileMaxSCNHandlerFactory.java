package com.linkedin.databus2.core.seq;

import java.io.IOException;

import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;

public class FileMaxSCNHandlerFactory implements SequenceNumberHandlerFactory
{
  private final FileMaxSCNHandler.Config _configBuilder;

  public FileMaxSCNHandlerFactory(FileMaxSCNHandler.Config configBuilder)
  {
    _configBuilder = configBuilder;
  }

  @Override
  public MaxSCNReaderWriter createHandler(String id) throws DatabusException
  {
    FileMaxSCNHandler result;

    synchronized (_configBuilder)
    {
      String saveKey = _configBuilder.getKey();
      _configBuilder.setKey(saveKey + "_" + id);
      FileMaxSCNHandler.StaticConfig config;
      try
      {
        config = _configBuilder.build();
      }
      catch (InvalidConfigException ice)
      {
        throw new DatabusException("unable to create sequence number handler: " + ice.getMessage(),
                                   ice);
      }
      try
      {
        result = FileMaxSCNHandler.create(config);
      }
      catch (IOException ioe)
      {
        throw new DatabusException("unable to create sequence number handler: " + ioe.getMessage(),
                                   ioe);
      }
      _configBuilder.setKey(saveKey);
    }

    return result;
  }

}
