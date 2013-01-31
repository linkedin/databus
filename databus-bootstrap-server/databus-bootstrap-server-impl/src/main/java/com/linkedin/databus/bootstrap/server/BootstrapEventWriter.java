package com.linkedin.databus.bootstrap.server;
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



import com.linkedin.databus.bootstrap.api.BootstrapEventCallback;
import com.linkedin.databus.bootstrap.api.BootstrapEventProcessResult;
import com.linkedin.databus.bootstrap.api.BootstrapProcessingException;
import com.linkedin.databus.bootstrap.common.BootstrapEventProcessResultImpl;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DbusEventInternalReadable;
import com.linkedin.databus.core.DbusEventV1;
import com.linkedin.databus.core.Encoding;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus2.core.filter.DbusFilter;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.log4j.Logger;

/**
 * @author lgao
 *
 */
public class BootstrapEventWriter implements BootstrapEventCallback
{
  public static final String MODULE = BootstrapEventWriter.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);
  public boolean _debug;

  private WritableByteChannel _writeChannel;
  private long _clientFreeBufferSize;
  private long _bytesSent;
  private long _rowCount;
  private DbusEventInternalReadable _event;
  private DbusFilter _filter;
  private Encoding _encoding;

  public BootstrapEventWriter(WritableByteChannel writeChannel,
		  					  long clientFreeBufferSize,
		  					  DbusFilter filter,
		  					  Encoding enc)
  {
    _event = null;
    _writeChannel = writeChannel;
    _encoding = enc;
    _clientFreeBufferSize = clientFreeBufferSize;
    _bytesSent = 0;
    _rowCount = 0;
    _filter = filter;
    _debug = LOG.isDebugEnabled();
  }

  @Override
  public BootstrapEventProcessResult onEvent(ResultSet rs, DbusEventsStatisticsCollector statsCollector) throws BootstrapProcessingException
  {
    long rid = -1;
    boolean exceededBufferLimit = false;
    boolean dropped = true;
    try
    {
      if (null == _event)
      {
        ByteBuffer tmpBuffer = ByteBuffer.wrap(rs.getBytes(4));
        if ( _debug) LOG.debug("BUFFER SIZE:" + tmpBuffer.limit());
        _event = new DbusEventV1(tmpBuffer, tmpBuffer.position());
      }
      else
      {
        ByteBuffer tmpBuffer = ByteBuffer.wrap(rs.getBytes(4));
        if ( _debug)  LOG.debug("Resized BUFFER SIZE:" + tmpBuffer.limit());
        _event.reset(tmpBuffer, 0);
      }

      if ( _debug)  LOG.debug("Event fetched: " + _event.size() + " for source:" + _event.srcId());
      if (!_event.isValid())
      {
        LOG.error("got an error event :" + _event.toString());
      }

      rid = rs.getLong(1);


      if ( _debug)
      {
    	  LOG.debug("sending: " + _event.key() + " " + _event.sequence());
    	  LOG.debug("event size:" + _event.size());
      }

      if (_bytesSent + _event.size() < _clientFreeBufferSize)
      {
    	// client has enough space for this event
    	if ( (null == _filter) || (_filter.allow(_event)))
    	{
    		if ( _debug )
    		{
    			if ( null != _filter )
    				LOG.debug("Event :" + _event.key() + " passed filter check !!");
    		}

    		_event.writeTo(_writeChannel, _encoding);
    		if (null != statsCollector)
    		{
    			statsCollector.registerDataEventFiltered(_event);
    			if ( _debug)  LOG.debug("Stats NumFilteredEvents :" + statsCollector.getTotalStats().getNumDataEventsFiltered());
    		}
    		_bytesSent += _event.size();
    		dropped = false;
    		if ( _debug)  LOG.debug("SENT " + _bytesSent);
    	} else {
    		if ( _debug )
    		{
    			LOG.debug("Event :" + _event.key() + " failed filter check !!");
    		}
    	}
        _rowCount ++; //tracks processed Rows only
		if (null != statsCollector)
		{
			statsCollector.registerDataEvent(_event);
			if ( _debug)  LOG.debug("Stats NumEvents :" + statsCollector.getTotalStats().getNumDataEvents());
		}
      }
      else
      {
        exceededBufferLimit = true;
        LOG.info("Terminating batch with max. size of " + _clientFreeBufferSize +
                 "; Bytes sent in the current batch is " + _bytesSent +
                 "; Rows processed in the batch is " + _rowCount);
        //_bytesSent = 0;
        //_rowCount = 0;
      }
    }
    catch (SQLException e)
    {
      LOG.error("SQLException encountered while sending to client row " + rid);
      throw new BootstrapProcessingException(e);
    }

    return new BootstrapEventProcessResultImpl(_rowCount, exceededBufferLimit, dropped);
  }

  @Override
  public void onCheckpointEvent(Checkpoint currentCheckpoint,
                                DbusEventsStatisticsCollector curStatsCollector)
  {
	//refresh LOG level
	_debug = LOG.isDebugEnabled();

    // store values in the internal structure
    currentCheckpoint.bootstrapCheckPoint();

    // write ckpt back to client
    DbusEventInternalReadable checkpointEvent = DbusEventV1.createCheckpointEvent(currentCheckpoint);
    checkpointEvent.writeTo(_writeChannel, _encoding);
    //LOG.info("Sending snapshot checkpoint to client: " + currentCheckpoint.toString());
  }

  public long getRowCount() {
	return _rowCount;
  }
}
