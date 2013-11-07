package com.linkedin.databus.bootstrap.utils;
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


import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import com.linkedin.databus.core.DbusEventBufferAppendable;
import com.linkedin.databus.core.DbusEventBufferStreamAppendable;
import com.linkedin.databus.core.DbusEventInfo;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.InternalDatabusEventsListener;
import com.linkedin.databus.core.InvalidEventException;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.util.RateMonitor;

/*
 * Supports one writer and reader
 */
public class BootstrapEventBuffer implements DbusEventBufferAppendable, DbusEventBufferStreamAppendable
{
  public static final String MODULE = BootstrapEventBuffer.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  public static final int END_OF_FILE = -1;
  public static final int END_OF_SOURCE = -2;
  public static final int ERROR_CODE = -3;


  public static enum EventType
  {
    EVENT_VALID,
    EVENT_EOF,
    EVENT_EOP,
    EVENT_EOS,
    EVENT_ERROR,
  };

  public static interface EventProcessor
  {
    /*
     * Handler for processing next event from the buffer
     * @param entry EventBufferEntry to be processed
     * @return true if processing was successful
     *         false if processing was unsuccessful
     */
    boolean process(EventBufferEntry entry, long scn);
  };

  public static class EventBufferEntry
  {
    private EventType   type;
    private DbusEventKey key;
    private DbusEventKey seederChunkKey;
    private short pPartitionId;
    private short lPartitionId;
    private long timeStamp;
    private short srcId;
    private byte[] schemaId;
    private byte[] value;
    private boolean enableTracing;
    private  DbusEventsStatisticsCollector statsCollector;

    /** Flag to determine if the event is replicated into the source DB **/
    private boolean isReplicated;
    
    public void setTimeStamp(long timeStamp)
    {
      this.timeStamp = timeStamp;
    }

    public EventType getType()
    {
      return type;
    }

    public void setType(EventType type)
    {
      this.type = type;
    }

    public DbusEventKey getKey()
    {
      return key;
    }

    public short getLogicalPartitionId()
    {
      return lPartitionId;
    }

    public short getPhysicalPartitionId()
    {
      return pPartitionId;
    }

    public long getTimeStamp()
    {
      return timeStamp;
    }

    public short getSrcId()
    {
      return srcId;
    }

    public byte[] getSchemaId()
    {
      return schemaId;
    }

    public byte[] getValue()
    {
      return value;
    }

    public boolean isEnableTracing()
    {
      return enableTracing;
    }

    public boolean isReplicated()
    {
      return isReplicated;
    }
    
    public DbusEventsStatisticsCollector getStatsCollector()
    {
      return statsCollector;
    }


    public DbusEventKey getSeederChunkKey() {
      return seederChunkKey;
    }

    public void setSeederChunkKey(DbusEventKey seederChunkKey) {
      this.seederChunkKey = seederChunkKey;
    }

    public void reset(DbusEventKey key,
                      DbusEventKey seederChunkKey,
                      short pPartitionId,
                      short lPartitionId,
                      long timeStamp,
                      short srcId,
                      byte[] schemaId,
                      byte[] value,
                      boolean enableTracing,
                      boolean isReplicated,
                      DbusEventsStatisticsCollector statsCollector)
    {
      this.type = EventType.EVENT_VALID;
      this.key = key;
      this.seederChunkKey = seederChunkKey;
      this.lPartitionId = lPartitionId;
      this.pPartitionId = pPartitionId;
      this.timeStamp = timeStamp;
      this.srcId = srcId;
      this.schemaId = schemaId;
      this.value = value;
      this.enableTracing = enableTracing;
      this.isReplicated = isReplicated;
      this.statsCollector = statsCollector;
    }

    @Override
    public String toString()
    {
      return "EventBufferEntry [type=" + type + ", key=" + key + ", seederChunkKey="
          + seederChunkKey + ", pPartitionId=" + pPartitionId + ", lPartitionId="
          + lPartitionId + ", timeStamp=" + timeStamp + ", srcId=" + srcId
          + ", schemaId=" + Arrays.toString(schemaId) + ", value="
          + Arrays.toString(value) + ", enableTracing=" + enableTracing
          + ", statsCollector=" + statsCollector + ", isReplicated=" + isReplicated + "]";
    }
  }

  private BlockingQueue<EventBufferEntry> _buffer = null;
  private BlockingQueue<EventBufferEntry> _freePool = null;
  private volatile RateMonitor            _freePoolTakeLatency = new RateMonitor("FreePoolTake");
  private volatile RateMonitor            _freePoolPutLatency = new RateMonitor("FreePoolPut");
  private volatile RateMonitor            _bufferTakeLatency = new RateMonitor("BufferTake");
  private volatile RateMonitor            _bufferPutLatency = new RateMonitor("BufferPut");
  private volatile RateMonitor            _handlerLatency   = new RateMonitor("Handler");

  private volatile long                   _scn;
  private volatile EventBufferEntry       _eofEntry;

  public BootstrapEventBuffer(int capacity)
  {
    _buffer = new ArrayBlockingQueue<EventBufferEntry>(capacity);
    _freePool =  new ArrayBlockingQueue<EventBufferEntry>(capacity);
    _eofEntry = new EventBufferEntry();
    _eofEntry.setType(EventType.EVENT_EOF);

    for (int i = 0; i < capacity; i++)
    {
      _freePool.add(new EventBufferEntry());
    }
  }

  @Override
  public void start(long startSCN)
  {
    _scn = startSCN;

    _freePoolTakeLatency.start();
    _freePoolTakeLatency.suspend();

    _freePoolPutLatency.start();
    _freePoolPutLatency.suspend();

    _bufferTakeLatency.start();
    _bufferTakeLatency.suspend();

    _bufferPutLatency.start();
    _bufferPutLatency.suspend();

    _handlerLatency.start();
    _handlerLatency.suspend();
  }

  @Override
  public void startEvents()
  {
  }

  public void logLatency()
  {
    LOG.info("_freePoolTakeLatency - Latency :" + _freePoolTakeLatency.getDuration()/1000000);
    LOG.info("_freePoolPutLatency Latency :" + _freePoolPutLatency.getDuration()/1000000);
    LOG.info("_bufferTakeLatency - Latency :" + _bufferTakeLatency.getDuration()/1000000);
    LOG.info("_bufferPutLatency - Latency :" + _bufferPutLatency.getDuration()/1000000);
    LOG.info("_handlerLatency - Latency :" + _handlerLatency.getDuration()/1000000);
    LOG.info("Buffer SIZE:" + _buffer.size());
  }

  @Override
  public boolean appendEvent(DbusEventKey key,
                             long sequenceId,
                             short pPartitionId,
                             short lPartitionId,
                             long timeStamp,
                             short srcId,
                             byte[] schemaId,
                             byte[] value,
                             boolean enableTracing,
                             DbusEventsStatisticsCollector statsCollector)
  {
    throw new RuntimeException("Method not supported ||");
  }

  @Override
  public boolean appendEvent(DbusEventKey key,
                             short pPartitionId,
                             short lPartitionId,
                             long timeStamp,
                             short srcId,
                             byte[] schemaId,
                             byte[] value,
                             boolean enableTracing)
  {
    throw new RuntimeException("Method not supported ||");
  }

  private void addSpecialEvent(EventType type, long rowId)
      throws InterruptedException
      {
    EventBufferEntry entry = null;

    try
    {
      _freePoolTakeLatency.resume();
      entry = _freePool.take();
    } finally {
      _freePoolTakeLatency.suspend();
    }

    entry.setType(type);
    entry.setTimeStamp(rowId); //Hack: Using TimeStamp field for rowId

    try
    {
      _bufferPutLatency.resume();
      _buffer.put(entry);
    } finally {
      _bufferPutLatency.suspend();
    }
      }

  public boolean readNextEvent(EventProcessor processor)
  {
    EventBufferEntry entry = null;
    try
    {
      _bufferTakeLatency.resume();
      entry = _buffer.take();
    } catch (InterruptedException ie) {
      LOG.error("Got interrupted while waiting for next event !!", ie);
      processor.process(_eofEntry,_scn);
      return false;
    } finally {
      _bufferTakeLatency.suspend();
    }


    boolean success = false;
    try
    {
      _handlerLatency.resume();
      success = processor.process(entry,_scn);
    } finally {
      _handlerLatency.suspend();
    }

    try
    {
      _freePoolPutLatency.resume();
      _freePool.put(entry);
    } catch (InterruptedException ie) {
      LOG.error("Got interrupted while waiting for returning to free pool !!", ie);
      processor.process(_eofEntry,_scn);
    } finally {
      _freePoolPutLatency.suspend();
    }

    return success;
  }

  public boolean appendEvent(DbusEventKey key,
                             DbusEventKey seederChunkKey,
                             short pPartitionId,
                             short lPartitionId,
                             long timeStamp,
                             short srcId,
                             byte[] schemaId,
                             byte[] value,
                             boolean enableTracing,
                             boolean isReplicated,
                             DbusEventsStatisticsCollector statsCollector)
  {
    DbusEventInfo eventInfo = new DbusEventInfo(null, 0L, pPartitionId, lPartitionId,
                                                timeStamp, srcId, schemaId, value, enableTracing,
                                                false);
    eventInfo.setReplicated(isReplicated);
    return appendEvent(key, seederChunkKey, eventInfo, statsCollector);
  }

  public boolean appendEvent(DbusEventKey key,
                             DbusEventKey seederChunkKey,
                             DbusEventInfo eventInfo,
                             DbusEventsStatisticsCollector statsCollector)
  {
    EventBufferEntry entry = null;
    try
    {
      _freePoolTakeLatency.resume();
      entry = _freePool.take();
    } catch (InterruptedException ie) {
      LOG.error("Got interrupted while waiting for free pool !!", ie);
      try { addSpecialEvent(EventType.EVENT_EOF, 0); } catch (InterruptedException ie2) {};
      _freePool.add(entry); //Adding back to pool
      return false; //TODO: Work on graceful shutdown
    } finally {
      _freePoolTakeLatency.suspend();
    }

    entry.reset(key,
                seederChunkKey,
                eventInfo.getpPartitionId(), eventInfo.getlPartitionId(),
                eventInfo.getTimeStampInNanos(), eventInfo.getSrcId(), eventInfo.getSchemaId(),
                eventInfo.getValueBytes(), eventInfo.isEnableTracing(), eventInfo.isReplicated(), statsCollector);


    try
    {
      _bufferPutLatency.resume();
      _buffer.put(entry);
    } catch (InterruptedException ie) {
      LOG.error("Got interrupted while putting to busy pool !!", ie);
      try { addSpecialEvent(EventType.EVENT_EOF, 0); } catch (InterruptedException ie2) {};
      _freePool.add(entry); //Adding back to pool
      return false; //TODO: Work on graceful shutdown
    } finally {
      _bufferPutLatency.suspend();
    }
    return true;
  }


  @Override
  public void rollbackEvents()
  {
  }

  @Override
  public void endEvents(boolean updateWindowScn,
                        long sequence,
                        boolean updateIndex,
                        boolean callListener,
                        DbusEventsStatisticsCollector statsCollector)
  {

  }


  public void endEvents(long rowId, long scn, DbusEventsStatisticsCollector statsCollector)
  {
    try
    {
      if ( rowId >= 0)
      {
        addSpecialEvent(EventType.EVENT_EOP, rowId);
        LOG.info("Adding EOP Event : " + rowId);
      } else if ( END_OF_FILE == rowId)
      {
        addSpecialEvent(EventType.EVENT_EOF, scn);
      } else if ( END_OF_SOURCE == rowId ) {
        // The last event has current maxSCN in the passed SCN
        addSpecialEvent(EventType.EVENT_EOS, scn);
      } else if (ERROR_CODE == rowId) {
        addSpecialEvent(EventType.EVENT_ERROR,scn);
      } else {
        throw new RuntimeException("Unknown rowId :" + rowId);
      }
    } catch (InterruptedException ie) {
      try {addSpecialEvent(EventType.EVENT_EOF,rowId);} catch (InterruptedException ie2) {}
    }
  }

  @Override
  public boolean empty()
  {
    return _buffer.size() == 0;
  }

  @Override
  public int readEvents(ReadableByteChannel readChannel,
                        Iterable<InternalDatabusEventsListener> eventListeners,
                        DbusEventsStatisticsCollector statsCollector) throws InvalidEventException
                        {
    return 0;
                        }

  @Override
  public long getMinScn()
  {
    return 0;
  }

  @Override
  public long lastWrittenScn()
  {
    return 0;
  }

  @Override
  public void setStartSCN(long sinceSCN)
  {

  }

  @Override
  public long getPrevScn()
  {
    return 0;
  }

  @Override
  public void endEvents(long sequence,
                        DbusEventsStatisticsCollector statsCollector) {
  }

  @Override
  public boolean appendEvent(DbusEventKey key, short pPartitionId,
                             short lPartitionId, long timeStamp, short srcId, byte[] schemaId,
                             byte[] value, boolean enableTracing,
                             DbusEventsStatisticsCollector statsCollector) {
    return false;
  }

  @Override
  public boolean appendEvent(DbusEventKey key, DbusEventInfo eventInfo,
                             DbusEventsStatisticsCollector statsCollector) {
    return false;
  }

  @Override
  public boolean appendEvent(DbusEventKey key, short pPartitionId,
                             short lPartitionId, long timeStamp, short srcId, byte[] schemaId,
                             byte[] value, boolean enableTracing, boolean isReplicated,
                             DbusEventsStatisticsCollector statsCollector) {
    return false;
  }
}
