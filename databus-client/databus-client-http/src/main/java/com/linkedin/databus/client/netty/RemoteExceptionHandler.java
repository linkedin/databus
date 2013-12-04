package com.linkedin.databus.client.netty;
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
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.linkedin.databus.client.ChunkedBodyReadableByteChannel;
import com.linkedin.databus.client.DatabusSourcesConnection;
import com.linkedin.databus.core.DbusErrorEvent;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventFactory;
import com.linkedin.databus.core.DbusEventInternalReadable;
import com.linkedin.databus.core.DbusEventInternalWritable;
import com.linkedin.databus.core.InvalidEventException;
import com.linkedin.databus.core.PullerRetriesExhaustedException;
import com.linkedin.databus.core.ScnNotFoundException;
import com.linkedin.databus2.core.container.DatabusHttpHeaders;
import com.linkedin.databus2.core.container.request.BootstrapDBException;
import com.linkedin.databus2.core.container.request.BootstrapDatabaseTooOldException;
import com.linkedin.databus2.core.container.request.BootstrapDatabaseTooYoungException;


public class RemoteExceptionHandler
{
  public static final String MODULE = RemoteExceptionHandler.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private final DatabusSourcesConnection _sourcesConn;
  private final DbusEventBuffer _dbusEventBuffer;
  private final DbusEventFactory _eventFactory;

  public RemoteExceptionHandler(DatabusSourcesConnection sourcesConn,
                                DbusEventBuffer dataEventsBuffer,
                                DbusEventFactory eventFactory)
  {
    _sourcesConn = sourcesConn;
    _dbusEventBuffer = dataEventsBuffer;
    _eventFactory = eventFactory;
  }

  public int getPendingEventSize(ChunkedBodyReadableByteChannel readChannel)
  {
    String result = readChannel.getMetadata(DatabusHttpHeaders.DATABUS_PENDING_EVENT_SIZE);
    if (result == null)
    {
      // There was no such header.
      return 0;
    }
    try
    {
      return Integer.parseInt(result);
    }
    catch(NumberFormatException e)
    {
      LOG.error("Could not parse pending event size:" + result);
      return 0;
    }
  }

  public static String getExceptionName(ChunkedBodyReadableByteChannel readChannel)
  {
    String result = readChannel.getMetadata(DatabusHttpHeaders.DATABUS_ERROR_CAUSE_CLASS_HEADER);
    if (result==null)
    {
      result = readChannel.getMetadata(DatabusHttpHeaders.DATABUS_ERROR_CLASS_HEADER);
    }
    return result;
  }

  public static String getExceptionMessage(ChunkedBodyReadableByteChannel readChannel)
  {
    String exceptionName = getExceptionName(readChannel);
    if (null == exceptionName) return null;

    StringBuilder result = new StringBuilder(exceptionName.length() + 100);
    result.append("Remote exception");
    String reqId = readChannel.getMetadata(DatabusHttpHeaders.DATABUS_REQUEST_ID_HEADER);
    if (null != reqId) result.append(" for request id");
    result.append(":");
    result.append(exceptionName);

    return result.toString();
  }

  public Throwable getException(ChunkedBodyReadableByteChannel readChannel)
  {
    Throwable remoteException = null;
    String err = getExceptionName(readChannel);
    if (null != err)
    { // in theory, we shall be reading the actual exception from the read channel.
      if (err.equalsIgnoreCase(ScnNotFoundException.class.getName()))
      {
        remoteException = new ScnNotFoundException();
      }
      else if (err.equalsIgnoreCase(BootstrapDatabaseTooOldException.class.getName()))
      {
        remoteException = new BootstrapDatabaseTooOldException();
      }
      else if (err.equalsIgnoreCase( PullerRetriesExhaustedException.class.getName()))
      {
        remoteException = new PullerRetriesExhaustedException();
      }
      else if (err.equalsIgnoreCase(BootstrapDatabaseTooYoungException.class.getName()))
      {
        remoteException = new BootstrapDatabaseTooYoungException();
      }
      else if (err.equalsIgnoreCase(BootstrapDBException.class.getName()))
      {
        remoteException = new BootstrapDBException();
      }
      else if (err.equalsIgnoreCase(SQLException.class.getName()))
      {
        remoteException = new SQLException();
      }
      else
      {
        LOG.error("Unexpected remote error received: " + err);
      }
      LOG.info("Remote exception received: " + remoteException);
    }

    return remoteException;
  }

  public void handleException(Throwable remoteException)
    throws InvalidEventException, InterruptedException
  {
    if ((remoteException instanceof BootstrapDatabaseTooOldException) || (remoteException instanceof PullerRetriesExhaustedException))
    {
      suspendConnectionOnError(remoteException);
    }
    else
    {
      LOG.error("Unexpected exception received: " + remoteException);
    }
  }

  private void suspendConnectionOnError(Throwable exception)
    throws InvalidEventException, InterruptedException
  {
    // suspend pull threads
    _sourcesConn.getConnectionStatus().suspendOnError(exception);

    // send an error event to dispatcher through dbusEventBuffer

    DbusEventInternalReadable errorEvent = null;
    if (exception instanceof BootstrapDatabaseTooOldException)
    {
      errorEvent = _eventFactory.createErrorEvent(
          new DbusErrorEvent(exception, DbusEventInternalWritable.BOOTSTRAPTOOOLD_ERROR_SRCID));
    }
    else if (exception instanceof PullerRetriesExhaustedException)
    {
      errorEvent = _eventFactory.createErrorEvent(
          new DbusErrorEvent(exception, DbusEventInternalWritable.PULLER_RETRIES_EXPIRED));
    }
    else
    {
    	throw new InvalidEventException("Got an unrecognizable exception ");
    }
    byte[] errorEventBytes = new byte[errorEvent.getRawBytes().limit()];

    if (LOG.isDebugEnabled())
    {
      LOG.debug("error event size: " + errorEventBytes.length);
      LOG.debug("error event:" + errorEvent.toString());
    }

    errorEvent.getRawBytes().get(errorEventBytes);
    ByteArrayInputStream errIs = new ByteArrayInputStream(errorEventBytes);
    ReadableByteChannel errRbc = Channels.newChannel(errIs);

    boolean success = false;
    int retryCounter = 0;
    while (!success && retryCounter < 10)
    {
      LOG.info("Sending an error event to dispatcher: " + exception.getMessage() +
               "; retry " + retryCounter, exception);
      success = _dbusEventBuffer.readEvents(errRbc) > 0 ? true : false;
      if (!success)
      {
        LOG.warn("Unable to send an error event to dispatcher. Will retry again later! " + retryCounter);
        retryCounter ++;
        Thread.sleep(1000);
      }
    }
  }
}
