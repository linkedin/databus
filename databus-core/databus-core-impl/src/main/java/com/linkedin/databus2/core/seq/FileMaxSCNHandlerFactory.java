package com.linkedin.databus2.core.seq;
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
