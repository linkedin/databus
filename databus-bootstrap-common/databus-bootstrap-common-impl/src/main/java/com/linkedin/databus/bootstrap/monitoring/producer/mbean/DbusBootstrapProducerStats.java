package com.linkedin.databus.bootstrap.monitoring.producer.mbean;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Hashtable;
import java.util.concurrent.locks.Lock;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;

import com.linkedin.databus.core.monitoring.mbean.AbstractMonitoringMBean;
import com.linkedin.databus.bootstrap.monitoring.producer.events.DbusBootstrapProducerStatsEvent;

public class DbusBootstrapProducerStats extends
		AbstractMonitoringMBean<DbusBootstrapProducerStatsEvent> implements
		DbusBootstrapProducerStatsMBean 
{

	
	public DbusBootstrapProducerStats(int id,
              String dimension,
              boolean enabled,
              boolean threadSafe,
              DbusBootstrapProducerStatsEvent initData)
	{
		super(enabled, threadSafe, initData);
		_event.ownerId = id;
		_event.dimension = dimension;
		reset();
	}
	  
	@Override
	protected void resetData() {
		// TODO Auto-generated method stub
		_event.currentLogId = 0;
		_event.currentRowId = 0;
		_event.currentSCN = 0;
		_event.numDataEventsPerWindow = 0;
		_event.numErrFellOffRelay = 0;
		_event.numErrSqlException = 0;
	    _event.timestampLastResetMs = System.currentTimeMillis();
	    _event.timeSinceLastResetMs = 0;
	    _event.latencyPerWindow = 0;
	    _event.numWindows = 0;
	}

	@Override
	public long getNumErrFellOffRelay() {
	    Lock readLock = acquireReadLock();
	    long result = 0;
	    try
	    {
	      result = _event.numErrFellOffRelay;
	    }
	    finally
	    {
	      releaseLock(readLock);
	    }

	    return result;
	}

	@Override
	public long getNumErrSqlException() {
	    Lock readLock = acquireReadLock();
	    long result = 0;
	    try
	    {
	      result = _event.numErrSqlException;
	    }
	    finally
	    {
	      releaseLock(readLock);
	    }

	    return result;
	}

	@Override
	public long getLatencyPerWindow() {
	    Lock readLock = acquireReadLock();
	    long result = 0;
	    try
	    {
	      result = _event.latencyPerWindow;
	    }
	    finally
	    {
	      releaseLock(readLock);
	    }

	    return result;
	}

	@Override
	public long getNumDataEventsPerWindow() {
	    Lock readLock = acquireReadLock();
	    long result = 0;
	    try
	    {
	      result = _event.numDataEventsPerWindow;
	    }
	    finally
	    {
	      releaseLock(readLock);
	    }

	    return result;
	}

	@Override
	public long getNumWindows() {
	    Lock readLock = acquireReadLock();
	    long result = 0;
	    try
	    {
	      result = _event.numWindows;
	    }
	    finally
	    {
	      releaseLock(readLock);
	    }

	    return result;
	}
	
	@Override
	public long getCurrentSCN() {
	    Lock readLock = acquireReadLock();
	    long result = 0;
	    try
	    {
	      result = _event.currentSCN;
	    }
	    finally
	    {
	      releaseLock(readLock);
	    }

	    return result;
	}

	@Override
	public long getCurrentLogId() {
	    Lock readLock = acquireReadLock();
	    long result = 0;
	    try
	    {
	      result = _event.currentLogId;
	    }
	    finally
	    {
	      releaseLock(readLock);
	    }

	    return result;
	}

	@Override
	public long getCurrentRowId() {
	    Lock readLock = acquireReadLock();
	    long result = 0;
	    try
	    {
	      result = _event.currentRowId;
	    }
	    finally
	    {
	      releaseLock(readLock);
	    }

	    return result;
	}

	@Override
	public long getTimestampLastResetMs() {
	    Lock readLock = acquireReadLock();
	    long result = 0;
	    try
	    {
	      result = _event.timestampLastResetMs;
	    }
	    finally
	    {
	      releaseLock(readLock);
	    }

	    return result;
	}

	@Override
	public long getTimeSinceLastResetMs() {
	    Lock readLock = acquireReadLock();
	    long result = 0;
	    try
	    {
	      result = _event.timeSinceLastResetMs;
	    }
	    finally
	    {
	      releaseLock(readLock);
	    }

	    return result;
	}

	@Override
	public JsonEncoder createJsonEncoder(OutputStream out) throws IOException {
	    return new JsonEncoder(_event.getSchema(), out);
	}

	@Override
	public ObjectName generateObjectName() throws MalformedObjectNameException {
	    Hashtable<String, String> mbeanProps = generateBaseMBeanProps();
	    mbeanProps.put("ownerId", Integer.toString(_event.ownerId));
	    mbeanProps.put("dimension", _event.dimension.toString());
	    return new ObjectName(AbstractMonitoringMBean.JMX_DOMAIN, mbeanProps);
	}

	@Override
	protected void cloneData(DbusBootstrapProducerStatsEvent eventObj) {
		eventObj.currentLogId = _event.currentLogId ;
		eventObj.currentRowId = _event.currentRowId;
		eventObj.currentSCN =_event.currentSCN ;
		eventObj.numDataEventsPerWindow =_event.numDataEventsPerWindow ;
		eventObj.numErrFellOffRelay =_event.numErrFellOffRelay ;
		eventObj.numErrSqlException =_event.numErrSqlException ;
		eventObj.timestampLastResetMs =_event.timestampLastResetMs;
		eventObj.timeSinceLastResetMs =_event.timeSinceLastResetMs;
		eventObj.latencyPerWindow =_event.latencyPerWindow;	
		eventObj.numWindows = _event.numWindows;
	}

	@Override
	protected DbusBootstrapProducerStatsEvent newDataEvent() {
	    return new DbusBootstrapProducerStatsEvent();

	}

	@Override
	protected SpecificDatumWriter<DbusBootstrapProducerStatsEvent> getAvroWriter() {
	    return new SpecificDatumWriter<DbusBootstrapProducerStatsEvent>(DbusBootstrapProducerStatsEvent.class);

	}

	@Override
	protected void doMergeStats(Object eventData) {
	    if (! (eventData instanceof DbusBootstrapProducerStatsEvent))
	    {
	      LOG.warn("Attempt to merge unknown event class" + eventData.getClass().getName());
	      return;
	    }
	    DbusBootstrapProducerStatsEvent e = (DbusBootstrapProducerStatsEvent) eventData;

	    /** Allow use negative relay IDs for aggregation across multiple relays */
	    if (_event.ownerId > 0 && e.ownerId != _event.ownerId)
	    {
	      LOG.warn("Attempt to data for a different relay " + e.ownerId);
	      return;
	    }
	    
	    if ((e.currentLogId > _event.currentLogId) || 
	    		( (e.currentLogId == _event.currentLogId) && ( e.currentRowId > _event.currentRowId)))
	    {
	    	_event.currentLogId = e.currentLogId;
	    	_event.currentRowId = e.currentRowId;
	    } 
	    

	    _event.currentSCN =Math.max(_event.currentSCN,e.currentSCN) ;
	    _event.numDataEventsPerWindow += e.numDataEventsPerWindow ;
	    _event.numErrFellOffRelay += e.numErrFellOffRelay ;
	    _event.numErrSqlException += e.numErrSqlException ;
	    _event.latencyPerWindow += e.latencyPerWindow;
	    _event.numWindows += e.numWindows;
	}
	
	@Override
	public void registerFellOffRelay()
	{
	    if (! _enabled.get()) return;
	    Lock writeLock = acquireWriteLock();

	    try
	    {
			_event.numErrFellOffRelay++;
	    }
	    finally
	    {
	      releaseLock(writeLock);
	    }		
	}
	  
	@Override
	public void registerSQLException()
	{
	    if (! _enabled.get()) return;
	    Lock writeLock = acquireWriteLock();

	    try
	    {
			_event.numErrSqlException++;
	    }
	    finally
	    {
	      releaseLock(writeLock);
	    }		
	}

	
	@Override
	public void registerBatch(long latency, long numEvents, long currentSCN, long currentLogId, long currentRowId)
	{
	    if (! _enabled.get()) return;
	    Lock writeLock = acquireWriteLock();

	    try
	    {
			_event.numWindows++;
			_event.latencyPerWindow += latency;
			_event.currentLogId = currentLogId;
			_event.currentRowId = currentRowId;
			_event.currentSCN = currentSCN;
			_event.numDataEventsPerWindow += numEvents;
	    }
	    finally
	    {
	      releaseLock(writeLock);
	    }		
	}
}
