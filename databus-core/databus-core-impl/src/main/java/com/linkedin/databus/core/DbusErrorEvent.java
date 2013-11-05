package com.linkedin.databus.core;
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


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class DbusErrorEvent
{
  public static final String MODULE = DbusErrorEvent.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private Throwable _error;
  private short _errorId;
  private static ObjectMapper _mapper = new ObjectMapper();

  public DbusErrorEvent(){}

  public DbusErrorEvent(Throwable error, short errorId)
  {
    _error = error;
    _errorId = errorId;
  }

  public static DbusErrorEvent createDbusErrorEvent(String serilizedErrorEvent)
    throws JsonParseException, JsonMappingException, IOException
  {
    return _mapper.readValue(new ByteArrayInputStream(serilizedErrorEvent.getBytes()), DbusErrorEvent.class);
  }

  @Override
  public String toString()
  {
    try
    {
      OutputStream byteArrayStream = new ByteArrayOutputStream();
      _mapper.writeValue(byteArrayStream, this);
      return byteArrayStream.toString();
    }
    catch (Exception e)
    {
      LOG.error("JSON error: " + e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

  public void setError(Throwable error)
  {
    _error = error;
  }

  public Throwable getError()
  {
    return _error;
  }

  public void setErrorId(short errorId)
  {
    _errorId = errorId;
  }

  public short getErrorId()
  {
    return _errorId;
  }

  public Throwable returnActualException()
  {
    Throwable error = null;

    switch (_errorId)
    {
      case DbusEventInternalWritable.BOOTSTRAPTOOOLD_ERROR_SRCID:
        error = new ScnNotFoundException(_error);
        break;
      case DbusEventInternalWritable.PULLER_RETRIES_EXPIRED:
    	error = new PullerRetriesExhaustedException(_error);
    	break;
      default:
        LOG.error("Invalid error id: " + _errorId);
    }

    return error;
  }
}
