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


import java.util.Hashtable;
import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.databus.core.NamedObject;
import com.linkedin.databus2.core.DatabusException;

/** A class that handles the sequence number handler servers */
public class MultiServerSequenceNumberHandler
{
  public static final String MODULE = MultiServerSequenceNumberHandler.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private final SequenceNumberHandlerFactory _handlerFactory;
  private final Map<NamedObject, MaxSCNReaderWriter> _handlers;

  public MultiServerSequenceNumberHandler(SequenceNumberHandlerFactory handlerFactory)
  {
    _handlerFactory = handlerFactory;
    _handlers = new Hashtable<NamedObject, MaxSCNReaderWriter>(4);
  }

  /** Return the sequence number handler for a physical partition/physical source or null if none exists */
  public MaxSCNReaderWriter getHandler(NamedObject serverName)
  {
    return _handlers.get(serverName);
  }

  /** Return the sequence number handler for sever or creates a new one if none exists*/
  public MaxSCNReaderWriter getOrCreateHandler(NamedObject serverName) throws DatabusException
  {
    MaxSCNReaderWriter handler = getHandler(serverName);
    if (null == handler)
    {
      LOG.info("creating sequence number handler for server name:" + serverName);
      handler = _handlerFactory.createHandler(serverName.getName());
      _handlers.put(serverName, handler);
    }

    return handler;
  }

  /**
   * Obtains the last seen sequence number from a Server
   * @return the sequence number or -1 if none has been seen or there was an error
   */
  public long readLastSequenceNumber(NamedObject serverName)
  {
    try
    {
      MaxSCNReaderWriter handler = getOrCreateHandler(serverName);
      return handler.getMaxScn();
    }
    catch (DatabusException de)
    {
      LOG.error("error obtaining sequence number for serverName " + serverName + ": " +
                de.getMessage(), de);
      return -1;
    }
  }

  /**
   * Changes the last sequence number for a server. If there is no existing handler for that server,
   * one will be created.
   */
  public void writeLastSequenceNumber(NamedObject serverName, long seq)
         throws DatabusException
  {
    MaxSCNReaderWriter handler = getOrCreateHandler(serverName);
    handler.saveMaxScn(seq);
  }

}
