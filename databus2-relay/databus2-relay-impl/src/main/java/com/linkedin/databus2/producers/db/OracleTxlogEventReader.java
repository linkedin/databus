/*
 * $Id: OracleTxlogEventReader.java 266719 2011-05-04 20:14:36Z cbotev $
 */
package com.linkedin.databus2.producers.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

import com.linkedin.databus.core.DbusEventBufferAppendable;
import com.linkedin.databus.core.UnsupportedKeyException;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.seq.MaxSCNWriter;
import com.linkedin.databus2.producers.EventCreationException;
import com.linkedin.databus2.relay.config.LogicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig.ChunkingType;
import com.linkedin.databus2.util.DBHelper;

/**
 * Class which can read events from the Oracle databus transaction log (sy$txlog) starting from
 * a given SCN. Each call to the readEventsFromAllSources will perform one event cycle against all
 * sources. That is, it will fetch any existing events starting at the given SCN and then return.
 *
 * @author Jemiah Westerman<jwesterman@linkedin.com>
 * @version $Revision: 266719 $
 */
public class OracleTxlogEventReader
	implements SourceDBEventReader
{
  public static final String MODULE = OracleTxlogEventReader.class.getName();

  private final String _name;
  private final List<MonitoredSourceInfo> _sources;
  private final String _selectSchema;

  private final DataSource _dataSource;

  private static final int DEFAULT_STMT_FETCH_SIZE = 100;
  private static final int MAX_STMT_FETCH_SIZE = 1000;

  private final DbusEventBufferAppendable _eventBuffer;
  private final boolean _enableTracing;

  private final Map<Short, String> _eventQueriesBySource;
  private final Map<Short, String> _eventChunkedScnQueriesBySource;
  private final Map<Short, String> _eventChunkedTxnQueriesBySource;

  private final Map<String, Integer> _sourceBitmasks = new HashMap<String, Integer>();
  private int _allSourcesMask = 0;

  /** Logger for error and debug messages. */
  private final Logger _log;
  private final Logger _eventsLog;

  private Connection _eventSelectConnection;
  private Connection _filterSourcesConnection;
  private final DbusEventsStatisticsCollector _relayInboundStatsCollector;
  private final MaxSCNWriter _maxScnWriter;
  private long _lastquerytime;
  private long _lastMaxScnTime;
  private final long _slowQuerySourceThreshold  ;
  
  private final ChunkingType _chunkingType;
  private final long _txnsPerChunk;  
  private final long _scnChunkSize;
  private final long _chunkedScnThreshold;
  private final long _maxScnDelayMs;
 
  private long _lastSeenEOP = EventReaderSummary.NO_EVENTS_SCN;
  private String _currEventQuery = null;
  
  private volatile boolean _inChunkingMode = false;
  private volatile long _catchupTargetMaxScn = -1L;
  

  public OracleTxlogEventReader(String name,
                              List<MonitoredSourceInfo> sources,
                             DataSource dataSource,
                             DbusEventBufferAppendable eventBuffer,
                             boolean enableTracing,
                             DbusEventsStatisticsCollector dbusEventsStatisticsCollector,
                             MaxSCNWriter maxScnWriter,
                             long slowQuerySourceThreshold,
                             ChunkingType chunkingType,
                             long txnsPerChunk,
                             long scnChunkSize,
                             long chunkedScnThreshold,
                             long maxScnDelayMs)
  {
    List<MonitoredSourceInfo> sourcesTemp = new ArrayList<MonitoredSourceInfo>();
    sourcesTemp.addAll(sources);
    _name = name;
    _sources = Collections.unmodifiableList(sourcesTemp);
    _dataSource = dataSource;
    _eventBuffer = eventBuffer;
    _enableTracing = enableTracing;
    _relayInboundStatsCollector = dbusEventsStatisticsCollector;
    _maxScnWriter = maxScnWriter;
    _slowQuerySourceThreshold = slowQuerySourceThreshold;
    _log = Logger.getLogger(getClass() + "." + _name);
    _eventsLog = Logger.getLogger("com.linkedin.databus2.producers.db.events." + _name);
    _chunkingType = chunkingType;
    _txnsPerChunk = txnsPerChunk;
    _scnChunkSize = scnChunkSize;
    _chunkedScnThreshold = chunkedScnThreshold;
    _maxScnDelayMs = maxScnDelayMs;
    
    _lastquerytime = System.currentTimeMillis() ;
    // Make sure all logical sources come from the same database schema
    for(MonitoredSourceInfo source : sourcesTemp)
    {
      if(!source.getEventSchema().equals(sourcesTemp.get(0).getEventSchema()))
      {
        throw new IllegalArgumentException("All logical sources must have the same schema.");
      }
    }
    _selectSchema = sourcesTemp.get(0).getEventSchema() == null ? "" : sourcesTemp.get(0).getEventSchema() + ".";

    // Generate the event queries for each source
    _eventQueriesBySource = new  HashMap<Short, String>();
    for(MonitoredSourceInfo sourceInfo : sources)
    {
      String eventQuery = generateEventQuery(sourceInfo);
      _log.info("Generated events query. source: " + sourceInfo + " ; eventQuery: " + eventQuery);
      _eventQueriesBySource.put(sourceInfo.getSourceId(), eventQuery);
    }
    
    _eventChunkedTxnQueriesBySource = new HashMap<Short, String>();
    for(MonitoredSourceInfo sourceInfo : sources)
    {
      String eventQuery = generateTxnChunkedQuery(sourceInfo);
      _log.info("Generated Chunked Txn events query. source: " + sourceInfo + " ; chunkTxnEventQuery: " + eventQuery);
      _eventChunkedTxnQueriesBySource.put(sourceInfo.getSourceId(), eventQuery);
    }
    
    _eventChunkedScnQueriesBySource = new HashMap<Short, String>();
    for(MonitoredSourceInfo sourceInfo : sources)
    {
      String eventQuery = generateScnChunkedQuery(sourceInfo);
      _log.info("Generated Chunked Scn events query. source: " + sourceInfo + " ; chunkScnEventQuery: " + eventQuery);
      _eventChunkedScnQueriesBySource.put(sourceInfo.getSourceId(), eventQuery);
    }
  }

  @Override
  public ReadEventCycleSummary readEventsFromAllSources(long sinceSCN)
	throws DatabusException, EventCreationException, UnsupportedKeyException
  {
    boolean eventBufferNeedsRollback = true;
    boolean debugEnabled = _log.isDebugEnabled();
    List<EventReaderSummary> summaries = new ArrayList<EventReaderSummary>();
    try
    {

      long cycleStartTS = System.currentTimeMillis();
       _eventBuffer.startEvents();

      // Open the database connection if it is closed (at start or after an SQLException)
      if(_eventSelectConnection == null || _eventSelectConnection.isClosed())
      {
    	  resetConnections();
      }

      /**
       * Chunking in Relay:
       * =================
       * 
       *  Variables used:
       *  ===============
       *  
       *  1. _inChunking : Flag to indicate if the relay is in chunking mode
       *  2. _chunkingType : Type of chunking supported
       *  3. _chunkedScnThreshold : 
       *               The threshold Scn diff which triggers chunking. If the relay's maxScn is older 
       *               than DB's maxScn by this threshold, then chunking will be enabled.
       *  4. _txnsPerChunk : Chunk size of txns for txn based chunking.
       *  5. _scnChunkSize : Chunk Size for scn based chunking.
       *  6. _catchupTargetMaxScn : Cached copy of DB's maxScn used as chunking's target SCN.
       *   
       *  =========================================
       *  Behavior of Chunking for Slow Sources:
       *  =========================================
       *  
       *  The slow sources case that is illustrated here is when all the sources in the sourcesList (fetched by relay) is slow. 
       *  In this case, the endOfPeriodSCN will not increase on its own whereas in all other cases, it will.
       *       
       *  At startup, if the _catchupTargetMaxScn - currScn > _chunkedScnThreshold, then chunking is enabled.  
       *  1. Txn_based_chunking
       *       
       *    a) If chunking is on at startup, then txn-based chunking query is used. Otherwise, regular query is used.
       *    b) For a period till SLOW_SOURCE_QUERY_THRESHOLD msec, the endOfPeriodSCN/SinceSCN will not increase.
       *    c) After SLOW_SOURCE_QUERY_THRESHOLD msec, the sinceScn/endOfPeriodSCN will be increased to current MaxScn. If chunking was previously enabled 
       *        at this time, it will be disabled upto MAX_SCN_DELAY_MS msec after which _catchupTargetMaxScn will be refreshed.
       *    d) if the new _catchupTargetMaxScn - currScn > _chunkedScnThreshold, then chunking is again enabled.
       *    e) go to (b)
       *        
       *  2. SCN based Chunking
       *    a) If chunking is on at startup, then scn-based chunking query is used. Otherwise, regular query is used.
       *    b) For a period till SLOW_SOURCE_QUERY_THRESHOLD msec, the endOfPeriodSCN/SinceSCN keep increasing by _scnChunkSize with no rows fetched.
       *    c) When _catchupTargetMaxScn - endOfPeriodSCN <  _chunkedScnThreshold, then chunking is disabled and regular query kicks in and in this
       *       phase sinceSCN/endOfPeriodSCN will not increase. After MAX_SCN_DELAY_MS interval, _catchupTargetSCN will be refreshed. 
       *    d) If the new _catchupTargetMaxScn - currScn > _chunkedScnThreshold, then SCN chunking is again enabled.
       *    e) go to (b)       *    
       *  
       */   
      if(sinceSCN <= 0)
      {
        _catchupTargetMaxScn = sinceSCN = getMaxTxlogSCN(_eventSelectConnection);
        _log.debug("sinceSCN was <= 0. Overriding with the current max SCN=" + sinceSCN);
        _eventBuffer.setStartSCN(sinceSCN);

        _eventSelectConnection.commit();
      } else if ((_chunkingType.isChunkingEnabled()) && (_catchupTargetMaxScn <= 0)) {
    	_catchupTargetMaxScn = getMaxTxlogSCN(_eventSelectConnection);
        _log.debug("catchupTargetMaxScn was <= 0. Overriding with the current max SCN=" + _catchupTargetMaxScn);
      }
      
      if (_catchupTargetMaxScn <= 0) 
    	  _inChunkingMode = false;

      // Get events for each source
      List<MonitoredSourceInfo> filteredSources = filterSources(sinceSCN);

      long endOfPeriodScn = EventReaderSummary.NO_EVENTS_SCN;
      for(MonitoredSourceInfo source : _sources)
      {
        if(filteredSources.contains(source))
        {
          long startTS = System.currentTimeMillis();
          EventReaderSummary summary = readEventsFromOneSource(_eventSelectConnection, source, sinceSCN);
          summaries.add(summary);
          endOfPeriodScn = Math.max(endOfPeriodScn, summary.getEndOfPeriodSCN());
          long endTS = System.currentTimeMillis();
          source.getStatisticsBean().addTimeOfLastDBAccess(endTS);

          if (_eventsLog.isDebugEnabled() || (_eventsLog.isInfoEnabled() && summary.getNumberOfEvents() >0))
          {
            _eventsLog.info(summary.toString());
          }

          // Update statistics for the source
          if(summary.getNumberOfEvents() > 0)
          {
            source.getStatisticsBean().addEventCycle(summary.getNumberOfEvents(), endTS - startTS,
                                                     summary.getSizeOfSerializedEvents(),
                                                     summary.getEndOfPeriodSCN());
          }
          else
          {
            source.getStatisticsBean().addEmptyEventCycle();
          }
        }
        else
        {
          source.getStatisticsBean().addEmptyEventCycle();
        }
      }
      
      _lastSeenEOP = Math.max(_lastSeenEOP, Math.max(endOfPeriodScn, sinceSCN));

      // If we did not read any events in this cycle then get the max SCN from the txlog. This
      // is for slow sources so that the endOfPeriodScn never lags too far behind the max scn
      // in the txlog table.
      long curtime = System.currentTimeMillis();
      if(endOfPeriodScn == EventReaderSummary.NO_EVENTS_SCN)
      {
          // If in SCN Chunking mode, its possible to get empty batches for a SCN range, 
          if ((sinceSCN + _scnChunkSize <= _catchupTargetMaxScn) &&
               (ChunkingType.SCN_CHUNKING == _chunkingType))
          {
            endOfPeriodScn = sinceSCN + _scnChunkSize;
            _lastquerytime = curtime;
          } else if ((curtime - _lastquerytime) > _slowQuerySourceThreshold)
    	  {
    	      _lastquerytime = curtime;
    	      //get new start scn for subsequent calls;
    		  endOfPeriodScn = getMaxTxlogSCN(_eventSelectConnection);
    		  if (debugEnabled)
    		  {
    		    _log.debug("No events processed. Read max SCN from txlog table for endOfPeriodScn. endOfPeriodScn=" + endOfPeriodScn);
    		  }
    	  }
      }
      else
      {
        //we have appended some events; and a new end of period has been found
        _lastquerytime = curtime;
        _eventBuffer.endEvents(endOfPeriodScn,_relayInboundStatsCollector);
        if (debugEnabled)
        {
          _log.debug("End of events: " + endOfPeriodScn + " windown range= " 
                       + _eventBuffer.getMinScn() + "," + _eventBuffer.lastWrittenScn());
        }
        //no need to roll back
        eventBufferNeedsRollback = false;
      }

      //save endOfPeriodScn if new one has been discovered
      if (endOfPeriodScn != EventReaderSummary.NO_EVENTS_SCN)
      {
        if (null != _maxScnWriter && (endOfPeriodScn != sinceSCN))
        {
          _maxScnWriter.saveMaxScn(endOfPeriodScn);
        }
        for(MonitoredSourceInfo source : _sources) {
          //update maxDBScn here
          source.getStatisticsBean().addMaxDBScn(endOfPeriodScn);
          source.getStatisticsBean().addTimeOfLastDBAccess(System.currentTimeMillis());
        }
      }
      long cycleEndTS = System.currentTimeMillis();

      //check if we should refresh _catchupTargetMaxScn
      if ( _chunkingType.isChunkingEnabled() &&
          (_lastSeenEOP >= _catchupTargetMaxScn) &&
          (curtime - _lastMaxScnTime >= _maxScnDelayMs))
      {
        //reset it to -1 so it gets refreshed next time around
        _catchupTargetMaxScn = -1;
      }
      
      boolean chunkMode = _chunkingType.isChunkingEnabled() &&
    		                  (_catchupTargetMaxScn > 0) &&
              	              (_lastSeenEOP < _catchupTargetMaxScn);
      
      if (!chunkMode && _inChunkingMode)
    	  _log.info("Disabling chunking for sources !!");
      
      _inChunkingMode = chunkMode;
      
      if ( _inChunkingMode  && debugEnabled)
    	 _log.debug("_inChunkingMode = true, _catchupTargetMaxScn=" + _catchupTargetMaxScn 
    			        + ", endOfPeriodScn=" + endOfPeriodScn + ", _lastSeenEOP=" + _lastSeenEOP);
      
      ReadEventCycleSummary summary = new ReadEventCycleSummary(_name, summaries,
                                                                Math.max(endOfPeriodScn, sinceSCN),
                                                                (cycleEndTS - cycleStartTS));

      // Return the event summaries
      return summary;
    }
    catch(SQLException ex)
    {
      DBHelper.close(_eventSelectConnection);
      DBHelper.close(_filterSourcesConnection);

      _eventSelectConnection = null;
      _filterSourcesConnection = null;

      _log.error("readEventsFromAllSources SQL exception. Current Event Query:" 
    		  			+ _currEventQuery + ", Message :" + ex.getMessage(), ex);
      for(MonitoredSourceInfo source : _sources) {
        //update maxDBScn here
        source.getStatisticsBean().addError();
      }
      throw new DatabusException(ex);
    }
    catch(Exception e)
    {
      DBHelper.close(_eventSelectConnection);
      DBHelper.close(_filterSourcesConnection);

      _eventSelectConnection = null;
      _filterSourcesConnection = null;

      _log.error("readEventsFromAllSources exception:" + e.getMessage(), e);
      for(MonitoredSourceInfo source : _sources) {
        //update maxDBScn here
        source.getStatisticsBean().addError();
      }
      throw new DatabusException(e);
    }
    finally
    {
      try
      {
        // Have to commit the transaction since we are in serializable isolation level
        if(_eventSelectConnection != null && !_eventSelectConnection.isClosed())
        {
          _eventSelectConnection.commit();
        }
      }
      catch(SQLException ex)
      {
        // Ignore this. The commit is just to end the read TX.
      }
      // If events were not processed successfully then eventBufferNeedsRollback will be true.
      // If that happens, rollback the event buffer.
      if(eventBufferNeedsRollback)
      {
        if (_log.isDebugEnabled())
        {
         _log.debug("Rolling back the event buffer because eventBufferNeedsRollback is true.");
        }
        _eventBuffer.rollbackEvents();
      }
    }
  }

  private PreparedStatement createQueryStatement(Connection conn,
		                            MonitoredSourceInfo source, 
		  							long sinceScn,
		  							int currentFetchSize,
		  							boolean useChunking) 
		throws SQLException		
  {
	  boolean debugEnabled = _log.isDebugEnabled();
	  String eventQuery = null;
	  ChunkingType type = _chunkingType;
	  
	  if ( ! useChunking || (! type.isChunkingEnabled()))
	  {
		  eventQuery = _eventQueriesBySource.get(source.getSourceId());
	  } else {	      
		  if ( type == ChunkingType.SCN_CHUNKING)
		      eventQuery = _eventChunkedScnQueriesBySource.get(source.getSourceId());
		  else
			  eventQuery = _eventChunkedTxnQueriesBySource.get(source.getSourceId());
	  }
	  
	  _currEventQuery = eventQuery;
	  
	  if (debugEnabled) _log.debug("source[" + source.getEventView() + "]: " + eventQuery +  
					"; skipInfinityScn=" + source.isSkipInfinityScn() + " ; sinceScn=" + sinceScn);
	  
	  PreparedStatement pStmt = conn.prepareStatement(eventQuery);
	  if ( ! useChunking || (!type.isChunkingEnabled()))
	  {
	    pStmt.setFetchSize(currentFetchSize);
		pStmt.setLong(1, sinceScn);    
		if (! source.isSkipInfinityScn()) pStmt.setLong(2, sinceScn);	  
	  } else {
		int i = 1;
	    pStmt.setLong(i++, sinceScn);
		pStmt.setLong(i++, sinceScn);

		if ( ChunkingType.TXN_CHUNKING == type)
		{
	      pStmt.setLong(i++, _txnsPerChunk);
		} else {
		  long untilScn = sinceScn + _scnChunkSize;
	      _log.info("SCN chunking mode, next target SCN is: " + untilScn);
	      pStmt.setLong(i++, untilScn);
		}
	  }
	  return pStmt;
  }
  
  private EventReaderSummary readEventsFromOneSource(Connection con, MonitoredSourceInfo source, long sinceScn)
  throws SQLException, UnsupportedKeyException, EventCreationException
  {
	boolean useChunking = false; // do not use chunking by default
	    
	if (_chunkingType.isChunkingEnabled())	    
	{
	  // use the upper bound for chunking if not caught up yet
      useChunking = (sinceScn + _chunkedScnThreshold  <= _catchupTargetMaxScn);	    
      if ( useChunking && !_inChunkingMode)
    	  _log.info("Enabling chunking for sources !!");
      
      _log.debug("SinceScn :" + sinceScn +", _ChunkedScnThreshold :" 
    	            +  _chunkedScnThreshold + ", _catchupTargetMaxScn:" + _catchupTargetMaxScn
    	            +", useChunking :" + useChunking);
	}

	_inChunkingMode = _inChunkingMode || useChunking;
	
    PreparedStatement pstmt = null;
    ResultSet rs = null;
    long endOfPeriodSCN = EventReaderSummary.NO_EVENTS_SCN;

    int currentFetchSize = DEFAULT_STMT_FETCH_SIZE;
    int numRowsFetched = 0;
    try
    {
      long startTS = System.currentTimeMillis();
      long totalEventSerializeTime = 0;
      pstmt = createQueryStatement(con, source, sinceScn, currentFetchSize, useChunking);

      long t = System.currentTimeMillis();
      rs = pstmt.executeQuery();
      long queryExecTime = System.currentTimeMillis() - t;
      long totalEventSize = 0;
      long tsWindowStart = Long.MAX_VALUE ; long tsWindowEnd=Long.MIN_VALUE;
      while(rs.next())
      {
        long scn = rs.getLong(1);
        long timestamp = rs.getTimestamp(2).getTime();
        tsWindowEnd = Math.max(timestamp,tsWindowEnd);
        tsWindowStart = Math.min(timestamp, tsWindowStart);

        // Delegate to the source's EventFactory to create the event and append it to the buffer
        // and then update endOfPeriod to the new max SCN
        long tsStart = System.currentTimeMillis();
        long eventSize = source.getFactory().createAndAppendEvent(scn,
                                                                  timestamp,
                                                                  rs,
                                                                  _eventBuffer,
                                                                  _enableTracing,
                                                                  _relayInboundStatsCollector);
        totalEventSerializeTime += System.currentTimeMillis()-tsStart;
        totalEventSize += eventSize;
        endOfPeriodSCN = Math.max(endOfPeriodSCN, scn);

        // Count the row
        numRowsFetched ++;

        // If we are fetching a large number of rows, increase the fetch size until
        // we reach MAX_STMT_FETCH_SIZE
        if(numRowsFetched > currentFetchSize && currentFetchSize != MAX_STMT_FETCH_SIZE)
        {
          currentFetchSize = Math.min(2 * currentFetchSize, MAX_STMT_FETCH_SIZE);
          pstmt.setFetchSize(currentFetchSize);
        }
      }
      long endTS = System.currentTimeMillis();
      
      if (_inChunkingMode && (ChunkingType.TXN_CHUNKING == _chunkingType))
      {
          _log.info("txn chunking mode: since=" + sinceScn + " eop=" + endOfPeriodSCN);
      }

      // Build the event summary and return
      EventReaderSummary summary = new EventReaderSummary(source.getSourceId(), source.getSourceName(),
                                                          endOfPeriodSCN, numRowsFetched,
                                                          totalEventSize, (endTS - startTS),totalEventSerializeTime,tsWindowStart,tsWindowEnd,queryExecTime);
      return summary;
    }
    finally
    {
      DBHelper.close(rs, pstmt, null);
    }
  }
  void loadSourceBitmasks()
  throws DatabusException
  {
    Connection con = null;
    PreparedStatement pstmt = null;
    ResultSet rs = null;
    try
    {
      // Empty the member variables, even though they should be empty already
      _allSourcesMask = 0;
      _sourceBitmasks.clear();

      // Read the bitmask data for all sources in sy$sources
      con = _dataSource.getConnection();
      pstmt = con.prepareStatement("select name, bitnum from " + _selectSchema + "sy$sources");
      rs = pstmt.executeQuery();
      while(rs.next())
      {
        int sourceMask = 1 << rs.getInt(2);
        _allSourcesMask |= sourceMask;
        _sourceBitmasks.put(rs.getString(1).toLowerCase(), sourceMask);
      }

      // Make sure we have bitmasks for all sources
      for(MonitoredSourceInfo source : _sources)
      {
        String eventView = source.getEventView().toLowerCase();
        if(!_sourceBitmasks.containsKey(eventView))
        {
          // handle the case the event_view is versioned. e.g., liar_job_relay_1
          String origSourceName = null;
          Pattern pattern = Pattern.compile("_[0-9]+$");
          boolean sourceExist = false;
          Matcher matcher = pattern.matcher(eventView);
          if(matcher.find())
          {
            origSourceName = matcher.replaceAll("");
            sourceExist = _sourceBitmasks.containsKey(origSourceName);
          }
          if(sourceExist)
          {
            int sourceMask = _sourceBitmasks.remove(origSourceName);
            _sourceBitmasks.put(eventView, sourceMask);
            _log.info("Use versioned view name " + eventView + " to replace " + origSourceName + " in sourceBitmask");
          }
          else
          {
            throw new DatabusException("Databus source (" + source + ") does not exist. ");
          }
        }
      }
    }
    catch(SQLException ex)
    {
      throw new DatabusException("Unable to load source bitmasks.", ex);
    }
    finally
    {
      DBHelper.close(rs, pstmt, con);
    }
  }

  /**
   * 
   * Filtering is disabled !!
   * @param startSCN
   * @return
   * @throws DatabusException
   */
  List<MonitoredSourceInfo> filterSources(long startSCN)
  throws DatabusException
  {
    return _sources;
  }

  public void resetConnections()
  	throws SQLException
  {
      _eventSelectConnection = _dataSource.getConnection();
      _filterSourcesConnection = _dataSource.getConnection();
      _log.info("JDBC Version is: " + _eventSelectConnection.getMetaData().getDriverVersion());
      _eventSelectConnection.setAutoCommit(false);
      _eventSelectConnection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
  }


  String generateEventQuery(MonitoredSourceInfo sourceInfo)
  {
    String sql = generateEventQuery(sourceInfo, _selectSchema);

    _log.info("EventQuery=" + sql);
    return sql;
  }

  static String generateSkipInfScnQuery(MonitoredSourceInfo sourceInfo, String selectSchema)
  {
    StringBuilder sql = new StringBuilder();

    sql.append("select ");
    if(sourceInfo.hasEventQueryHints())
    {
      sql.append(sourceInfo.getEventQueryHints());
      sql.append(" ");
    }
    else
    {
      sql.append(LogicalSourceConfig.DEFAULT_EVENT_QUERY_HINTS);
      sql.append(" ");
    }

    sql.append(" tx.scn scn, tx.ts event_timestamp, src.* ");
    sql.append("from ");
    sql.append(selectSchema + "sy$").append(sourceInfo.getEventView()).append(" src, " + selectSchema + "sy$txlog tx ");
    sql.append("where ");
    sql.append("src.txn=tx.txn and ");
    sql.append("tx.scn > ? and tx.scn < 9999999999999999999999999999");

    return sql.toString();
  }

  static String generateNoSkipInfScnQuery(MonitoredSourceInfo sourceInfo, String selectSchema)
  {
    StringBuilder sql = new StringBuilder();

    sql.append("select ");
    if(sourceInfo.hasEventQueryHints())
    {
      sql.append(sourceInfo.getEventQueryHints());
      sql.append(" ");
    }
    else
    {
      sql.append(LogicalSourceConfig.DEFAULT_EVENT_QUERY_HINTS);
      sql.append(" ");
    }

    sql.append(selectSchema + "sync_core.getScn(tx.scn, tx.ora_rowscn) scn, tx.ts event_timestamp, src.* ");
    sql.append("from ");
    sql.append(selectSchema + "sy$").append(sourceInfo.getEventView()).append(" src, " + selectSchema + "sy$txlog tx ");
    sql.append("where ");
    sql.append("src.txn=tx.txn and ");
    sql.append("tx.scn > ? and ");
    sql.append("tx.ora_rowscn > ?");

    return sql.toString();
  }

  static String generateEventQuery(MonitoredSourceInfo sourceInfo, String selectSchema)
  {
    if (sourceInfo.isSkipInfinityScn()) return generateSkipInfScnQuery(sourceInfo, selectSchema);
    else return generateNoSkipInfScnQuery(sourceInfo, selectSchema);
  }
  
  static String generateTxnChunkedQuery(MonitoredSourceInfo sourceInfo)
  {
    StringBuilder sql = new StringBuilder();

    sql.append("SELECT scn, event_timestamp, src.* ");
    sql.append("FROM sy$").append(sourceInfo.getEventView()).append(" src, ");
    sql.append("( SELECT ");
    String hints = sourceInfo.getEventTxnChunkedQueryHints();
    sql.append(hints).append(" "); // hint for oracle

    sql.append(" sync_core.getScn(tx.scn, tx.ora_rowscn) scn, tx.ts event_timestamp, ");
    sql.append("tx.txn, row_number() OVER (ORDER BY TX.SCN) r ");
    sql.append("FROM sy$txlog tx ");
    sql.append("WHERE tx.scn > ? AND tx.ora_rowscn > ? AND tx.scn < 9999999999999999999999999999) t ");
    sql.append("WHERE src.txn = t.txn AND r<= ? ");
    sql.append("ORDER BY r ");

    return sql.toString();
  }

  
  static String generateScnChunkedQuery(MonitoredSourceInfo sourceInfo)
  {
    StringBuilder sql = new StringBuilder();
    sql.append("SELECT ");
    String hints = sourceInfo.getEventScnChunkedQueryHints();
    sql.append(hints).append(" "); // hint for oracle
    sql.append("sync_core.getScn(tx.scn, tx.ora_rowscn) scn, tx.ts event_timestamp, src.* ");
    sql.append("FROM sy$").append(sourceInfo.getEventView()).append(" src, sy$txlog tx ");
    sql.append("WHERE src.txn=tx.txn AND tx.scn > ? AND tx.ora_rowscn > ? AND ");
    sql.append(" tx.ora_rowscn <= ?");

    return sql.toString();
  }
  
  /**
   * Returns the max SCN from the sy$txlog table
   * @param db
   * @return the max scn
   * @throws SQLException
   */
  private long getMaxTxlogSCN(Connection db) throws SQLException
  {
	_lastMaxScnTime = System.currentTimeMillis();
    long maxScn = EventReaderSummary.NO_EVENTS_SCN;

    String sql = "select " +
                 "max(" + _selectSchema + "sync_core.getScn(scn,ora_rowscn)) " +
                 "from " + _selectSchema + "sy$txlog where " +
                 "scn >= (select max(scn) from " + _selectSchema + "sy$txlog)";

    if (_log.isDebugEnabled()) _log.debug(sql);

    PreparedStatement pstmt = null;
    ResultSet rs = null;

    try
    {
      pstmt = db.prepareStatement(sql);
      rs = pstmt.executeQuery();

      if(rs.next())
      {
        maxScn = rs.getLong(1);
      }
    }
    finally
    {
      DBHelper.close(rs, pstmt, null);
    }

    if (_log.isDebugEnabled()) _log.debug("MaxSCN Query :" + sql + ", MaxSCN :" + maxScn);

    return maxScn;
  }

  @Override
  public List<MonitoredSourceInfo> getSources() {
	  return _sources;
  }

  public void close()
  {
    if (null != _filterSourcesConnection) DBHelper.close(_filterSourcesConnection);
    if (null != _eventSelectConnection) DBHelper.close(_eventSelectConnection);
  }
  
  public void setCatchupTargetMaxScn(long catchupTargetMaxScn)
  {
	  _catchupTargetMaxScn = catchupTargetMaxScn;
  }
}
