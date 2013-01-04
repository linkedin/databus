package com.linkedin.databus.client.consumer;

import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.log4j.Logger;

import com.linkedin.databus.client.SingleSourceSCN;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DatabusCombinedConsumer;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.client.pub.SCN;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventBufferAppendable;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.seq.MaxSCNWriter;

/**
 * 
 * @author snagaraj Databus2 Consumer that writes event windows to a single
 *         event buffer The event buffer and the stats collector are
 *         instantiated elsewhere
 */
public class DatabusConsumerEventBuffer implements DatabusCombinedConsumer
{
	private final DbusEventBufferAppendable _eventBuffer;
	private final DbusEventsStatisticsCollector _stats;
	private final MaxSCNWriter _scnWriter;
	
	public static final String MODULE = DatabusConsumerEventBuffer.class
			.getName();
	public static final Logger LOG = Logger.getLogger(MODULE);

	/**
	 * 
	 * @param buffer : this could be an active buffer[leader->follower] or a brand new one.  
	 * @param stats : stats for event buffer
	 * @param scnWriter : scn retrieval from std relay locations
	 */
	
	public DatabusConsumerEventBuffer(DbusEventBufferAppendable buffer,
			DbusEventsStatisticsCollector stats,
			MaxSCNWriter scnWriter
			)
	{
		_eventBuffer = buffer;
		_stats = stats;
		_scnWriter=scnWriter;
	}
	
	public void setStartSCN(long scn)
	{
		_eventBuffer.start(scn);
	}
	
	public long getStartSCN()
	{
		return _eventBuffer.getPrevScn();
	}
	
	@Override
	public ConsumerCallbackResult onStartConsumption()
	{
		LOG.info("Started consumption");
		return ConsumerCallbackResult.SUCCESS;
	}

	@Override
	public ConsumerCallbackResult onStopConsumption()
	{
		return ConsumerCallbackResult.SUCCESS;
	}

	@Override
	public ConsumerCallbackResult onStartDataEventSequence(SCN startScn)
	{
		long scn = ((SingleSourceSCN) startScn).getSequence();
		if (getStartSCN() <= 0)
		{
			//uninitialized buffer; set start to 1 scn before the first window; conservative but correct
			setStartSCN(scn-1);
			LOG.warn("onStartDataSequence: Eventbuffer start scn = " + getStartSCN());
		} 
		_eventBuffer.startEvents();
		return ConsumerCallbackResult.SUCCESS;
	}

	@Override
	public ConsumerCallbackResult onEndDataEventSequence(SCN endScn)
	{
		long endSCN = ((SingleSourceSCN) endScn).getSequence();
		_eventBuffer.endEvents(endSCN, _stats);
		if (_scnWriter != null)
		{
			try 
			{
				_scnWriter.saveMaxScn(endSCN);
			} 
			catch (DatabusException e) 
			{
				LOG.warn("Unable to save scn=" + endSCN + ": " + e);
				return ConsumerCallbackResult.SUCCESS;
			}
		}
		return ConsumerCallbackResult.SUCCESS;
	}

	@Override
	public ConsumerCallbackResult onRollback(SCN rollbackScn)
	{
		long endSCN = ((SingleSourceSCN) rollbackScn).getSequence();
		LOG.warn("Rollback called with SCN=" + endSCN + " lastWrittenScn of buffer= " +  _eventBuffer.lastWrittenScn());
		_eventBuffer.rollbackEvents();
		return ConsumerCallbackResult.SUCCESS;
	}

	@Override
	public ConsumerCallbackResult onStartSource(String source,
			Schema sourceSchema)
	{
		return ConsumerCallbackResult.SUCCESS;
	}

	@Override
	public ConsumerCallbackResult onEndSource(String source, Schema sourceSchema)
	{
		return ConsumerCallbackResult.SUCCESS;
	}

	@Override
	public ConsumerCallbackResult onDataEvent(DbusEvent e,
			DbusEventDecoder eventDecoder)
	{
		DbusEventKey k;
		if (e.isKeyNumber())
		{
			k = new DbusEventKey(e.key());
		}
		else
		{
			k = new DbusEventKey(e.keyBytes());
		}
		ByteBuffer payload = e.value();
		byte[] value = new byte[payload.limit()];
		payload.get(value,0,value.length);
		payload.position(0);
		_eventBuffer.appendEvent(k, e.physicalPartitionId(),
				e.logicalPartitionId(), e.timestampInNanos(), e.srcId(),
				e.schemaId(),value, false, _stats);
		return ConsumerCallbackResult.SUCCESS;
	}

	@Override
	public ConsumerCallbackResult onCheckpoint(SCN checkpointScn)
	{
		return ConsumerCallbackResult.SUCCESS;
	}

	@Override
	public ConsumerCallbackResult onError(Throwable err)
	{
		return ConsumerCallbackResult.SUCCESS;
	}

	@Override
	public ConsumerCallbackResult onStartBootstrap()
	{
		return onStartConsumption();
	}

	@Override
	public ConsumerCallbackResult onStopBootstrap()
	{
		return onStopConsumption();
	}

	@Override
	public ConsumerCallbackResult onStartBootstrapSequence(SCN startScn)
	{
		return onStartDataEventSequence(startScn);
	}

	@Override
	public ConsumerCallbackResult onEndBootstrapSequence(SCN endScn)
	{
		return onEndDataEventSequence(endScn);
	}

	@Override
	public ConsumerCallbackResult onStartBootstrapSource(String sourceName,
			Schema sourceSchema)
	{
		return onStartSource(sourceName, sourceSchema);
	}

	@Override
	public ConsumerCallbackResult onEndBootstrapSource(String name,
			Schema sourceSchema)
	{
		return onEndSource(name, sourceSchema);
	}

	@Override
	public ConsumerCallbackResult onBootstrapEvent(DbusEvent e,
			DbusEventDecoder eventDecoder)
	{
		return onDataEvent(e, eventDecoder);
	}

	@Override
	public ConsumerCallbackResult onBootstrapRollback(SCN batchCheckpointScn)
	{
		return onRollback(batchCheckpointScn);
	}

	@Override
	public ConsumerCallbackResult onBootstrapCheckpoint(SCN checkpointScn)
	{
		return onCheckpoint(checkpointScn);
	}

	@Override
	public ConsumerCallbackResult onBootstrapError(Throwable err)
	{
		return onError(err);
	}
	
	@Override
	public boolean canBootstrap() 
	{
	  return true;
	}

}
