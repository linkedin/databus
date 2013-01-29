package com.linkedin.databus.bootstrap.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import java.lang.reflect.Method;

import java.net.URL;
import java.net.URLClassLoader;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.DbusEventKey.KeyType;
import com.linkedin.databus.core.UnsupportedKeyException;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus.core.util.RateMonitor;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.producers.EventCreationException;
import com.linkedin.databus2.producers.db.EventReaderSummary;
import com.linkedin.databus2.producers.db.MonitoredSourceInfo;
import com.linkedin.databus2.producers.db.ReadEventCycleSummary;
import com.linkedin.databus2.producers.db.SourceDBEventReader;
import com.linkedin.databus2.util.DBHelper;

public class BootstrapSrcDBEventReader
      extends DbusSeederBaseThread
      implements SourceDBEventReader
{
	private static final Logger LOG = Logger.getLogger(BootstrapSrcDBEventReader.class);
	private static final long MILLISEC_TO_MIN = (1000 * 60);

	private final List<MonitoredSourceInfo> _sources;
	private final DataSource _dataSource;
	private final BootstrapEventBuffer _bootstrapSeedWriter;
	private final int _numRetries;
	private final int _numRowsPrefetch;
	private final int _LOBPrefetchSize;
	private final int _commitInterval;
	private final long _sinceSCN;
	private final int _numRowsPerQuery;
	private final boolean _enableNumRowsQuery;
	private final Map<String, Long> _lastRows;
	private final Map<String, String> _lastKeys;
	private final Map<String, File> _keyTxnFilesMap;
	private final Map<String, Integer> _keyTxnBufferSizeMap;
	private final Map<String, String> _pKeyNameMap;
	private final Map<String,DbusEventKey.KeyType> _pKeyTypeMap;
	private final Map<String, String> _pKeyIndexMap;
	private final Map<String, String> _queryHintMap;
	private final Map<String,String> _checkpointPersistanceScript;
	private final Map<String,String> _eventQueryMap;
	private final Map<String,String> _beginSrcKeyMap;
	private final Map<String,String> _endSrcKeyMap;
    private final Method _setLobPrefetchSizeMethod;
    private final Class _oraclePreparedStatementClass;
    
	public Map<String, File> getKeyTxnFilesMap() {
		return _keyTxnFilesMap;
	}

	public Map<String, Integer> getKeyTxnBufferSizeMap() {
		return _keyTxnBufferSizeMap;
	}

	public Map<String, String> getpKeyNameMap() {
		return _pKeyNameMap;
	}

	public Map<String, DbusEventKey.KeyType> getpKeyTypeMap() {
		return _pKeyTypeMap;
	}

	public BootstrapSrcDBEventReader(DataSource dataSource,
									 BootstrapEventBuffer eventBuffer,
			                         StaticConfig   config,
			                         List<MonitoredSourceInfo> sources,
			                         Map<String, Long> lastRows,
			                         Map<String, String> lastKeys,
			                         long           sinceSCN,
			                         Map<String, String> checkpointPersistanceScript
			                         )
		throws Exception
	{
	    super("BootstrapSrcDBEventReader");
	    List<MonitoredSourceInfo> sourcesTemp = new ArrayList<MonitoredSourceInfo>();
	    sourcesTemp.addAll(sources);
	    _sources = Collections.unmodifiableList(sourcesTemp);
		_dataSource = dataSource;
		_bootstrapSeedWriter = eventBuffer;
		_keyTxnFilesMap = new HashMap<String, File>();
		_numRowsPerQuery = config.getNumRowsPerQuery();
		_keyTxnBufferSizeMap = config.getKeyTxnBufferSizeMap();

		Map<String,String> keyTxnFiles = config.getKeyTxnFilesMap();

		Iterator<Entry<String, String>> itr = keyTxnFiles.entrySet().iterator();

		while (itr.hasNext())
		{
			Entry<String,String> entry = itr.next();
			LOG.info("Adding KeyTxnMapFile Entry :" + entry);
			_keyTxnFilesMap.put(entry.getKey(),new File(entry.getValue()));
		}

		_enableNumRowsQuery = config.isEnableNumRowsQuery();
		_commitInterval = config.getCommitInterval();
		_numRowsPrefetch = config.getNumRowsPrefetch();
		_LOBPrefetchSize = config.getLOBPrefetchSize();
		_numRetries      = config.getNumRetries();
		_sinceSCN = sinceSCN;
		_lastRows = new HashMap<String,Long>(lastRows);
		_lastKeys = new HashMap<String,String>(lastKeys);
		_pKeyNameMap = config.getPKeyNameMap();
		_pKeyTypeMap = config.getPKeyTypeMap();
		_pKeyIndexMap = config.getPKeyIndexMap();
		_queryHintMap = config.getQueryHintMap();
		_checkpointPersistanceScript = checkpointPersistanceScript;
		_eventQueryMap = config.getEventQueryMap();
		_beginSrcKeyMap = config.getBeginSrcKeyMap();
		_endSrcKeyMap = config.getEndSrcKeyMap();
		
        File file = new File("ojdbc6-11.2.0.2.0.jar");
		URL ojdbcJarFile = file.toURL();
		URLClassLoader cl = URLClassLoader.newInstance(new URL[]{ojdbcJarFile});
		_oraclePreparedStatementClass = cl.loadClass("oracle.jdbc.OraclePreparedStatement");
		_setLobPrefetchSizeMethod = _oraclePreparedStatementClass.getMethod("setLobPrefetchSize", int.class);

		validate();
	}


	public void validate()
		throws Exception
	{
		if ( null == _checkpointPersistanceScript)
			throw new Exception("_checkpointPersistanceScript cannot be null !!");

		if ( null == _pKeyTypeMap)
			throw new Exception("_pKeyTypeMap cannot be null !!");

		if ( null == _pKeyNameMap)
			throw new Exception("_pKeyNameMap cannot be null !!");

		boolean isNullIndex = (null == _pKeyIndexMap);
		boolean isNullQueryHint = ( null == _queryHintMap);

		if ( isNullIndex && isNullQueryHint)
			throw new Exception("Index and Query Hint both cannot be null !!");

		for (MonitoredSourceInfo s : _sources)
		{
			if ( null == _checkpointPersistanceScript.get(s.getEventView()))
				throw new Exception("CheckpointPersistance Script for Source (" + s.getEventView() + ") not provided !!");

			try
			{
				File f = new File(_checkpointPersistanceScript.get(s.getEventView()));
				boolean perm = f.isFile() && f.canExecute();
				if (!perm)
					throw new Exception("Checkpoint Persistance File (" + f + ") for source (" + s.getEventView() + ") is either not present or cannot be executed !!");
			}	catch (Exception ex) {
				throw new Exception("Exception while checking checkpoint persistance script validity for source (" + s.getEventView() + ")");
			}

			if ( null == _pKeyTypeMap.get(s.getEventView()))
					throw new Exception("pKey Type for Source (" + s.getEventView() + ") not provided !!");

			if ( null == _pKeyNameMap.get(s.getEventView()))
				throw new Exception("pKey Name for Source (" + s.getEventView() + ") not provided !!");

			if ( (isNullIndex || (null == _pKeyIndexMap.get(s.getEventView())))
					&& (isNullQueryHint || ( null == _queryHintMap.get(s.getEventView()))))
			{
				throw new Exception("Both pkey Index and Query Hint for source (" + s.getEventView() + ") not provided !!");
			}
		}
	}

	@Override
	public void run()
	{
	  try
	  {
	    readEventsFromAllSources(_sinceSCN);
	  } catch (Exception ex) {
		 LOG.error("Got Error when executing readEventsFromAllSources !!",ex);
	  }
	  LOG.info(Thread.currentThread().getName() + " done seeding ||");
	}

	@Override
	public ReadEventCycleSummary readEventsFromAllSources(long sinceSCN)
			throws DatabusException, EventCreationException,
			UnsupportedKeyException
	{
	    List<EventReaderSummary> summaries = new ArrayList<EventReaderSummary>();
	    long maxScn = EventReaderSummary.NO_EVENTS_SCN;
	    long endScn = maxScn;
	    boolean error = false;

	    long startTS = System.currentTimeMillis();
	    try
	    {
	        _rate.start();
	        _rate.suspend();


	        Connection conn = null;
	        try
	        {
	        	conn = _dataSource.getConnection();
	        	LOG.info("Oracle JDBC Version :" + conn.getMetaData().getDriverVersion());
	        } finally {
	        	DBHelper.close(conn);
	        }

	    	if ( ! _sources.isEmpty())
	    	{
	    		// Script assumes seeding is done for one schema at a time
	    		// just use one source to get the schema name for sy$txlog
	    		maxScn = getMaxScn(_sources.get(0));
	    	}

	    	for ( MonitoredSourceInfo sourceInfo : _sources)
	    	{
	    		LOG.info("Bootstrapping " + sourceInfo.getEventView());
	    		_bootstrapSeedWriter.start(maxScn);
	    		EventReaderSummary summary = readEventsForSource(sourceInfo, maxScn);
	    		// Script assumes seeding is done for one schema at a time
	    		// just use one source to get the schema name for sy$txlog
	    		endScn = getMaxScn(_sources.get(0));
	    		_bootstrapSeedWriter.endEvents(BootstrapEventBuffer.END_OF_SOURCE, endScn, null);
	    		summaries.add(summary);
	    	}
	    } catch (Exception ex) {
	    	error = true;
	    	throw new DatabusException(ex);
	    } finally {
	    	// Notify writer that I am done
	    	if ( error )
	    	{
	    		_bootstrapSeedWriter.endEvents(BootstrapEventBuffer.ERROR_CODE, endScn,null);
	    		LOG.error("Seeder stopping unexpectedly !!");
	    	} else {
	    		_bootstrapSeedWriter.endEvents(BootstrapEventBuffer.END_OF_FILE, endScn,null);
	    		LOG.info("Completed Seeding !!");
	    	}
	    	LOG.info("Start SCN :" + maxScn);
	    	LOG.info("End SCN :" + endScn);
	    }
        long endTS = System.currentTimeMillis();

	    ReadEventCycleSummary cycleSummary = new ReadEventCycleSummary("seeder",
	                                                                   summaries, maxScn,
	                                                                   (endTS - startTS));
		return cycleSummary;
	}

	public long getNumRows(Connection conn, String table)
		throws SQLException
	{
		String sql = generateCountQuery(table);
	    PreparedStatement pstmt = null;
	    ResultSet rs = null;
	    long numRows = 0;
		try
		{
			conn  = _dataSource.getConnection();
			LOG.info("NumRows Query :" + sql);
			pstmt = conn.prepareStatement(sql);
			rs = pstmt.executeQuery();
			rs.next();
			numRows = rs.getLong(1);
		} finally {
			DBHelper.close(rs,pstmt,conn);
		}
		return numRows;
	}

	private EventReaderSummary readEventsForSource(MonitoredSourceInfo sourceInfo, long maxScn)
	throws DatabusException, EventCreationException,
			UnsupportedKeyException,SQLException, IOException
	{
		int retryMax = _numRetries;
		int numRetry = 0;
	    Connection conn = null;
	    PreparedStatement pstmt = null;
	    ResultSet rs = null;
		KeyType keyType = _pKeyTypeMap.get(sourceInfo.getEventView());
		String keyName = _pKeyNameMap.get(sourceInfo.getEventView());
		String sql = _eventQueryMap.get(sourceInfo.getEventView());
		String beginSrcKey = _beginSrcKeyMap.get(sourceInfo.getEventView());
		String endSrcKey = _endSrcKeyMap.get(sourceInfo.getEventView());
		
		if (sql == null)
		{
			sql = generateEventQuery2(sourceInfo,keyName, keyType, getPKIndex(sourceInfo), getQueryHint(sourceInfo));
		}
		LOG.info("Chunked  Query for Source (" + sourceInfo + ") is :" + sql);
		LOG.info("EndSrcKey for source (" + sourceInfo +") is :" + endSrcKey);

		PrimaryKeyTxn endKeyTxn = null;
		if ((null != endSrcKey) && (! endSrcKey.trim().isEmpty()) )
		{
			if ( KeyType.LONG == keyType )
				endKeyTxn = new PrimaryKeyTxn(new Long(endSrcKey));
			else
				endKeyTxn = new PrimaryKeyTxn(endSrcKey);
		}
		
		
		
		long timestamp = System.currentTimeMillis();
		int numRowsFetched = 0;
		long totalEventSize = 0;
		long timeStart = System.currentTimeMillis();
		long checkpointInterval = _commitInterval;
		boolean done = false;
		long lastTime = timeStart;
		long numRows = 0;
		PrimaryKeyTxn pKey = null;

		String minKeySQL = generateMinKeyQuery(sourceInfo, keyName);

		String srcName = sourceInfo.getEventView();
		LOG.info("Bootstrapping for Source :" + srcName);
		String lastKey = _lastKeys.get(sourceInfo.getEventView());
		File f = _keyTxnFilesMap.get(srcName);
		FileWriter oStream = new FileWriter(f,f.exists());
		BufferedWriter keyTxnWriter = new BufferedWriter(oStream,_keyTxnBufferSizeMap.get(srcName));

		_bootstrapSeedWriter.startEvents();
		RateMonitor seedingRate = new RateMonitor("Seeding Rate");
		RateMonitor queryRate = new RateMonitor("Query Rate");
		seedingRate.start();
		seedingRate.suspend();
		queryRate.start();
		queryRate.suspend();
		boolean isException = false;
		long totProcessTime =0;
		try
		{
			conn  = _dataSource.getConnection();
			pstmt = conn.prepareStatement(sql);

			if ( _enableNumRowsQuery )
				numRows = getNumRows(conn, getTableName(sourceInfo));
			else
				numRows = -1;

			long currRowId = _lastRows.get(sourceInfo.getEventView());

			/**
			 * First Key to be seeded will be decided in the following order:
			 * 1. Use bootstrap_seeder_state's last srcKey as the key for the first chunk.
			 * 2. If (1) is empty, use passed-in begin srcKey.
			 * 3. If (2) is also empty, use Oracle's minKey as the first Chunk Key.
			 */
			if ( null == lastKey )
			{
				lastKey = _beginSrcKeyMap.get(sourceInfo.getEventView());	
				LOG.info("No last Src Key available in bootstrap_seeder_state for source ("  + sourceInfo + ". Trying beginSrc Key from config :" + lastKey);
			}
						
			if ( (null == lastKey) || (lastKey.trim().isEmpty()) )
			{				
				if ( KeyType.LONG == keyType )
					pKey = new PrimaryKeyTxn(executeAndGetLong(minKeySQL));
				else 
					pKey = new PrimaryKeyTxn(executeAndGetString(minKeySQL));				
			} else {
				if ( KeyType.LONG == keyType )
					pKey = new PrimaryKeyTxn(Long.parseLong(lastKey));
				else
					pKey = new PrimaryKeyTxn(lastKey);
			}

			PrimaryKeyTxn lastRoundKeyTxn = new PrimaryKeyTxn(pKey);
			PrimaryKeyTxn lastKeyTxn = new PrimaryKeyTxn(pKey);
			long numUniqueKeys = 0;
			
			boolean first  = true;
			_rate.resume();
			while ( ! done )
			{
				LOG.info("MinKey being used for this round:" + pKey );

				try
				{
					lastRoundKeyTxn.copyFrom(pKey);
					
					if ( KeyType.LONG == keyType )
					{
						pstmt.setLong(1,pKey.getKey());
					} else {
						String key = pKey.getKeyStr();
						String[] subKeys = key.split("_");
						pstmt.setString(1, subKeys[0]);
					}

					pstmt.setLong(2, _numRowsPerQuery);
					pstmt.setFetchSize(_numRowsPrefetch);
					
					if ( _oraclePreparedStatementClass.isInstance(pstmt))
					{
						try
						{
							_setLobPrefetchSizeMethod.invoke(pstmt, _LOBPrefetchSize);							
						} catch (Exception e)
						{
							throw new EventCreationException("Unable to set Lob Prefetch size" + e.getMessage());
						}
					}

					LOG.info("Executing Oracle Query :" + sql + ". Key: " + pKey + ",NumRows: " +  _numRowsPerQuery);
					queryRate.resume();
					rs = pstmt.executeQuery();
					queryRate.suspend();

					LOG.info("Total Query Latency :" + queryRate.getDuration()/1000000000L);
					long totLatency = 0;
					long txnId = 0;
					int numRowsThisRound = 0;
					seedingRate.resume();
					while (rs.next())
					{
						_rate.tick();
						seedingRate.tick();
						currRowId++;
						txnId = rs.getLong(2);
						if ( KeyType.LONG == keyType )
						{
							pKey.setKeyTxn(rs.getLong(1),txnId);
						} else {
							String key = rs.getString(1);
							String[] subKeys = key.split("_");
							pKey.setKeyStrTxn(subKeys[0],txnId);
						}
						
						//Write TXN to file
						pKey.writeTo(keyTxnWriter);

						//LOG.info("TXNId is :" + txnId + ",RowId is :" + currRowId);
						long start = System.nanoTime();
						long eventSize = sourceInfo.getFactory().createAndAppendEvent(maxScn, timestamp, rs,
								_bootstrapSeedWriter, false, null);
						long latency = System.nanoTime() - start;
						totLatency += latency;
						totalEventSize += eventSize;
						totProcessTime += (totLatency/1000*1000);
						numRowsFetched++;
						numRowsThisRound++;
						
						if ( lastKeyTxn.compareKey(pKey) != 0)
						{
							numUniqueKeys++;
							lastKeyTxn.copyFrom(pKey);
						}
						
						if ( numRowsFetched%checkpointInterval == 0 )
						{
							// Commit this batch and reinit
							_bootstrapSeedWriter.endEvents(currRowId,timestamp,null);
							keyTxnWriter.flush();
							_bootstrapSeedWriter.startEvents();
							long procTime = totLatency/1000000000;
							long currTime = System.currentTimeMillis();
							long diff = (currTime - lastTime)/1000;
							long timeSinceStart = (currTime - timeStart)/1000;
							double currRate = _rate.getRate();
							currRate = (currRate <= 0) ? 1 : currRate;

							if ( _enableNumRowsQuery)
							{
								double remTime = (numRows - currRowId)/(currRate);

								LOG.info("Processed " + checkpointInterval + " rows in " + diff
										+ " seconds, Processing Time (seconds) so far :" + (procTime)
										+ ",Seconds elapsed since start :" + (timeSinceStart)
										+ ",Approx Seconds remaining :" + remTime
										+ ",Overall Row Rate:" + _rate.getRate() + "(" + seedingRate.getRate() + ")" +
										",NumRows Fetched so far:" + numRowsFetched +
										". TotalEventSize :" + totalEventSize);
							} else {
								LOG.info("Processed " + checkpointInterval + " rows in " + diff
										+ " seconds, Processing Time (seconds) so far :" + (procTime)
										+ ",Seconds elapsed since start :" + (timeSinceStart)
										+ ",Overall Row Rate:" + _rate.getRate() + "(" + seedingRate.getRate() + ")" +
										",NumRows Fetched so far:" + numRowsFetched +
										". TotalEventSize :" + totalEventSize);
							}
							lastTime = currTime;
						}
						
						if ( (null != endKeyTxn) &&  (endKeyTxn.compareKey(lastKeyTxn) < 0) ) 
						{
							LOG.info("Seeding to be stopped for current source as it has completed seeding upto endSrckey :" + endKeyTxn
									+ ", Current SrcKey :" + lastKeyTxn);
							break;
						}
						
					}
					seedingRate.suspend();
					
					if ( (numRowsThisRound <= 1) || 							
							((numRowsThisRound < _numRowsPerQuery) && (numUniqueKeys == 1)))
					{
						LOG.info("Seeding Done for source :" + sourceInfo.getEventView() + ", numRowsThisRound :" 
					               + numRowsThisRound + ", _numRowsPerQuery :" + _numRowsPerQuery 
					               + ", numUniqueKeys :" + numUniqueKeys);
						done = true;
					} else if ((numRowsThisRound == _numRowsPerQuery) && (numUniqueKeys == 1)) {
						String msg = "Seeding stuck at infinte loop for source : " + sourceInfo.getEventView() + ", numRowsThisRound :" 
					               + numRowsThisRound + ", _numRowsPerQuery :" + _numRowsPerQuery 
					               + ", numUniqueKeys :" + numUniqueKeys + ", lastChunkKey :" + lastRoundKeyTxn;
						LOG.error(msg);
						throw new DatabusException(msg);						
					} else if ( null != endKeyTxn) {
						if ( endKeyTxn.compareKey(lastKeyTxn) < 0 ) {
							LOG.info("Seeding stopped for source :" + sourceInfo.getEventView() 
									  + ", as it has completed seeding upto the endSrckey :" + endKeyTxn + ", numRowsThisRound :" 
						               + numRowsThisRound + ", _numRowsPerQuery :" + _numRowsPerQuery 
						               + ", numUniqueKeys :" + numUniqueKeys + " , Current SrcKey :" + lastKeyTxn);
							done = true;
						}
					}

					if ( !first || done)
					{
						currRowId--; //Since next time, we will read the last seen record again
					}
					first = false;
					_bootstrapSeedWriter.endEvents(currRowId,timestamp,null);

				} finally {
					DBHelper.close(rs);
					rs = null;
				}
			}
		} catch (SQLException ex) {
			LOG.error("Got SQLException for source (" + sourceInfo + ")", ex);

			_bootstrapSeedWriter.rollbackEvents();

			numRetry++;
			isException = true;

			if (numRetry >= retryMax)
			{
				throw new DatabusException(
						"Error: Reached max retries for reading/processing bootstrap", ex);
			}
		} catch (DatabusException ex) {
			
			isException = true;
			throw ex;
			
		}  finally {
		
			DBHelper.close(rs,pstmt,conn);
			keyTxnWriter.close();
			rs = null;
			_rate.suspend();

			if ( ! isException)
			{
				dedupeKeyTxnFile(_keyTxnFilesMap.get(srcName), keyType);
			}
		}
		long timeEnd = System.currentTimeMillis();
		long elapsedMin = (timeEnd - timeStart)/(MILLISEC_TO_MIN);
		LOG.info("Processed " + numRowsFetched + " rows of Source: " +  sourceInfo.getSourceName() + " in " + elapsedMin + " minutes" );
		return new EventReaderSummary(sourceInfo.getSourceId(), sourceInfo.getSourceName(), -1,
		                              numRowsFetched, totalEventSize, (timeEnd - timeStart),totProcessTime,0,0,0);
	}

	private long getMaxScn(MonitoredSourceInfo sourceInfo)
	  throws SQLException
	{
		String schema = ( sourceInfo.getEventSchema() == null) ? "" :
			                             sourceInfo.getEventSchema() + ".";
		String table = schema + "sy$txlog";
		/*
	    StringBuilder sql = new StringBuilder();
	    sql.append("select max(scn) from ");
	    sql.append(table);
	    String query = sql.toString();
	    */
	    String query = "select " +
        	"max(" + schema + "sync_core.getScn(scn,ora_rowscn)) " +
        	"from " + table + " where " +
        	"scn >= (select max(scn) from " + table + ")";
	    long maxScn = executeAndGetLong(query);
	    return maxScn;
	}

	private long executeAndGetLong(String query)
	   throws SQLException
	{
		long val = -1;
	    Connection conn = null;
	    PreparedStatement pstmt = null;
	    ResultSet rs = null;
	    try
	    {
	        conn = _dataSource.getConnection();
	        pstmt = conn.prepareStatement(query);
	        rs = pstmt.executeQuery();
	        boolean ret = rs.next();
	        assert(ret);
	        val = rs.getLong(1);

	        LOG.info("Query:" + query + ",Result is :" + val);

	    } catch ( SQLException sqlEx) {
	    	LOG.error("Got error while executing query:" + query, sqlEx);
	    	throw sqlEx;
	    } finally {
	        DBHelper.close(rs, pstmt, conn);
	    }
		return val;
	}

	private String executeAndGetString(String query)
	   throws SQLException
	{
		String val = null;
	    Connection conn = null;
	    PreparedStatement pstmt = null;
	    ResultSet rs = null;
	    try
	    {
	        conn = _dataSource.getConnection();
	        pstmt = conn.prepareStatement(query);
	        rs = pstmt.executeQuery();
	        boolean ret = rs.next();
	        assert(ret);
	        val = rs.getString(1);

	        LOG.info("Query:" + query + ",Result is :" + val);

	    } catch ( SQLException sqlEx) {
	    	LOG.error("Got error while executing query:" + query, sqlEx);
	    	throw sqlEx;
	    } finally {
	        DBHelper.close(rs, pstmt, conn);
	    }
		return val;
	}

	public static String getTableName(MonitoredSourceInfo sourceInfo)
	{
		String schema = ( sourceInfo.getEventSchema() == null) ? "" :
										sourceInfo.getEventSchema() + ".";
		String table = schema + "sy$" + sourceInfo.getEventView();
		return table;
	}

	public String getPKIndex(MonitoredSourceInfo sourceInfo)
	{
		String index = _pKeyIndexMap.get(sourceInfo.getEventView());

		if ( null == index)
		{
			index = sourceInfo.getEventView() +  "_pk";
		}
		return index;
	}

	public String getQueryHint(MonitoredSourceInfo sourceInfo)
	{
	   String index = _queryHintMap.get(sourceInfo.getEventView());

	   if ( null == index)
	   {
	     return null;
	   }

	   return index;
	}
	/*
	private String generateEventQuery(MonitoredSourceInfo sourceInfo)
	{
		String table = getTableName(sourceInfo);
	    StringBuilder sql = new StringBuilder();
	    sql.append("select txn, src.* from ");
	    sql.append(table);
	    sql.append(" src");
	    sql.append(" where txn > ? and txn <= ?");
		return sql.toString();
	}
	*/

	public static String generateEventQuery2(MonitoredSourceInfo sourceInfo, String keyName, KeyType keyType, String pkIndex, String queryHint)
	{
		return generateEventQuery2(getTableName(sourceInfo),keyName, keyType, pkIndex, queryHint);
	}

	public static String generateEventQuery2(String table, String keyName, KeyType keyType, String pkIndex, String queryHint)
	{
	    StringBuilder sql = new StringBuilder();

	    sql.append("select * from (");
	    if ( (null == queryHint) || ( queryHint.isEmpty()))
	      sql.append("select /*+ INDEX(src ").append(pkIndex).append(") */ ");
	    else
	       sql.append("select /*+ " + queryHint + " */ ");

	    sql.append(keyName).append( " keyn,");
	    sql.append(" txn txnid, src.*, ROW_NUMBER() OVER(order by src.").append(keyName).append(" asc) as row_counter from ");
	    sql.append(table);
	    sql.append(" src");
		sql.append(" where src." + keyName + " >= ?");
	    sql.append(" ) where row_counter <= ?");
		return sql.toString();
	}

	public static String generateMinKeyQuery(MonitoredSourceInfo sourceInfo, String keyName)
	{
		return generateMinKeyQuery(getTableName(sourceInfo), keyName );
	}

	public static String generateMinKeyQuery(String table, String keyName)
	{
	    StringBuilder sql = new StringBuilder();
	    sql.append("select min(" + keyName + ") ");
	    sql.append("from " + table);
		return sql.toString();
	}

	private static String generateCountQuery(String table)
	{
	    StringBuilder sql = new StringBuilder();
	    sql.append("select count(*) from ");
	    sql.append(table);
	    return sql.toString();
	}

	private void dedupe(String file, String tmpFile, KeyType type)
		throws Exception
	{
		BufferedReader reader = null;
		BufferedWriter writer = null;
		PrimaryKeyTxn keyTxn = null;
		PrimaryKeyTxn oldKeyTxn = null;
		try
		{
			reader = new BufferedReader(new FileReader(file));
			writer = new BufferedWriter(new FileWriter(tmpFile));

			keyTxn = new PrimaryKeyTxn(Long.MIN_VALUE);
			oldKeyTxn = new PrimaryKeyTxn(Long.MIN_VALUE);
			keyTxn.setType(type);
			oldKeyTxn.setType(type);
			boolean first = true;
			while (true)
			{
				String line = reader.readLine();

				if (null == line)
					break;

				keyTxn.readFrom(line);

				long cmp = keyTxn.compareKey(oldKeyTxn);

				if ( cmp != 0)
				{
					if ( ! first )
					{
						oldKeyTxn.writeTo(writer);
					}
					oldKeyTxn.copyFrom(keyTxn);
				}
				first = false;
			}
			oldKeyTxn.writeTo(writer);
		} catch ( Exception ex ) {
			LOG.error("Got exception while deduping key_txns", ex);
		} finally {
			if ( null != reader)
				reader.close();

			if ( null != writer)
				writer.close();
		}
	}

	private void dedupeKeyTxnFile(File keyTxnFile, KeyType keyType)
	{
		boolean numericKey = (keyType == KeyType.LONG);
		StringBuilder cmd = new StringBuilder();
		String tmpFile = keyTxnFile.getAbsolutePath() + ".tmp";

		String backupFile = keyTxnFile.getAbsolutePath() + ".pre";

		LOG.info("Post Processing the KeyTXN File :" + keyTxnFile.toString());
		cmd.append("sort -t ");
		cmd.append(PrimaryKeyTxn.DELIMITER);
		cmd.append(" -k 1");
		if ( numericKey ) cmd.append("n");
		cmd.append(" -k 2nr ");
		cmd.append(keyTxnFile.getAbsolutePath());
		cmd.append(" -o ").append(tmpFile);

		String cmd1 = "cp " + keyTxnFile.getAbsolutePath() + " " + backupFile;
		String cmd2 = cmd.toString();
		String cmd3 = "rm " + tmpFile + " " ;
		try
		{
			Runtime rt = Runtime.getRuntime();

			LOG.info("Executing command :" + cmd1);
			Process pr1 = rt.exec(cmd1);

			int res = pr1.waitFor();

			if ( res != 0)
			{
              LOG.error("**********************");
              LOG.error("Error Executing CMD (" + cmd1 + "), Error: (STDOUT=" + getStream(pr1.getInputStream()) + ") (STDERR=" +  getStream(pr1.getErrorStream()) + "). Result was :" + res);
              LOG.error("**********************");
			}

			LOG.info("Executing command :" + cmd2);
			Process pr2 = rt.exec(cmd2);
			res = pr2.waitFor();
			if ( res != 0)
			{
			    LOG.error("**********************");
				LOG.error("Error Executing CMD (" + cmd2 + "), Error: (STDOUT=" + getStream(pr2.getInputStream()) + ") (STDERR=" +  getStream(pr2.getErrorStream()) + "). Result was :" + res);
                LOG.error("**********************");
			}

			LOG.info("Removing duplicate Entries from the sorted list");
			dedupe(tmpFile,keyTxnFile.getAbsolutePath(), keyType);

			LOG.info("Executing command :" + cmd3);
			Process pr3 = rt.exec(cmd3);
			res = pr3.waitFor();
			if ( res != 0)
			{
              LOG.error("**********************");
              LOG.error("Error Executing CMD (" + cmd3 + "), Error: (STDOUT=" + getStream(pr3.getInputStream()) + ") (STDERR=" +  getStream(pr3.getErrorStream()) + "). Result was :" + res);
              LOG.error("**********************");
			}

			LOG.info("Post Processing the KeyTXN File done successfully :" + keyTxnFile.toString());

		} catch ( Exception io) {
			LOG.error("Postprocessing the KeyTXNFile :" + keyTxnFile + " failed !!", io);
			LOG.info("CMD1 :" + cmd1);
			LOG.info("CMD2 :" + cmd2);
			LOG.info("CMD3 :" + cmd3);
		}
	}

	private String getStream(InputStream stream)
		throws IOException
	{
		BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
		StringBuilder str = new StringBuilder();

		while (true)
		{
			String s = reader.readLine();

			if ( null == s)
				break;

			str.append(s);
		}
		return str.toString();
	}

	public static class StaticConfig
	{

		public int getNumRowsPrefetch() {
			return _numRowsPrefetch;
		}

		public Map<String, String> getEventQueryMap() {
			// TODO Auto-generated method stub
			return _eventQueryMap;
		}

		public int getLOBPrefetchSize() {
			return _LOBPrefetchSize;
		}

		public int getCommitInterval() {
			return _commitInterval;
		}

		public int getNumRetries() {
			return _numRetries;
		}

		public Map<String, String> getKeyTxnFilesMap()
		{
			return _keyTxnFilesMap;
		}

		public Map<String, Integer> getKeyTxnBufferSizeMap()
		{
			return _keyTxnBufferSizeMap;
		}

		public Map<String, String> getPKeyNameMap()
		{
			return _pKeyNameMap;
		}

        public Map<String, String> getQueryHintMap()
        {
            return _queryHintMap;
        }
        

		public Map<String, DbusEventKey.KeyType> getPKeyTypeMap()
		{
			return _pKeyTypeMap;
		}

		public Map<String, String> getPKeyIndexMap()
		{
			return _pKeyIndexMap;
		}

		public int getNumRowsPerQuery()
		{
			return _numRowsPerQuery;
		}

		public boolean isEnableNumRowsQuery() {
			return _enableNumRowsQuery;
		}
		
        public Map<String, String> getBeginSrcKeyMap()
        {
            return _beginSrcKeyMap;
        }
        
        public Map<String, String> getEndSrcKeyMap()
        {
            return _endSrcKeyMap;
        }

		public StaticConfig(boolean enableNumRowsQuery,
				int numRowsPrefetch, int LOBPrefetchSize,
				int commitInterval, int numRetries,
				int numRowsPerQuery,
				Map<String, String> keyTxnFilesMap,
				Map<String, Integer> keyTxnBufferSizeMap,
				Map<String, String> pKeyNameMap,
				Map<String, DbusEventKey.KeyType> pKeyTypeMap,
				Map<String, String> pKeyIndexMap,
				Map<String, String> queryHintMap,
				Map<String, String> eventQueryMap,
				Map<String, String> beginSrcKeyMap,
				Map<String, String> endSrcKeyMap)
		{
			super();
			this._enableNumRowsQuery = enableNumRowsQuery;
			this._numRowsPrefetch = numRowsPrefetch;
			this._LOBPrefetchSize = LOBPrefetchSize;
			this._commitInterval = commitInterval;
			this._numRetries = numRetries;
			this._numRowsPerQuery = numRowsPerQuery;
			this._keyTxnFilesMap = keyTxnFilesMap;
			this._keyTxnBufferSizeMap = keyTxnBufferSizeMap;
			this._pKeyNameMap = pKeyNameMap;
			this._pKeyTypeMap = pKeyTypeMap;
			this._pKeyIndexMap = pKeyIndexMap;
			this._queryHintMap = queryHintMap;
			this._eventQueryMap = eventQueryMap;
			this._beginSrcKeyMap = beginSrcKeyMap;
			this._endSrcKeyMap = endSrcKeyMap;

		}

		private final boolean _enableNumRowsQuery;
		private final int _numRowsPrefetch;
		private final int _LOBPrefetchSize;
		private final int _commitInterval;
		private final int _numRetries;
		private final int _numRowsPerQuery;
		private final Map<String, String> _keyTxnFilesMap;
		private final Map<String, Integer> _keyTxnBufferSizeMap;
		private final Map<String, String> _pKeyNameMap;
		private final Map<String, DbusEventKey.KeyType> _pKeyTypeMap;
		private final Map<String, String> _pKeyIndexMap;
		private final Map<String, String> _queryHintMap;
		private final Map<String, String> _eventQueryMap;
		private final Map<String, String> _beginSrcKeyMap;
		private final Map<String, String> _endSrcKeyMap;
	}

	public static class Config implements ConfigBuilder<StaticConfig>
	{
		private static final boolean DEFAULT_ENABLE_NUM_ROWS_QUERY = true;
		private static final int DEFAULT_NUM_ROWS_PREFETCH = 10;
		private static final int DEFAULT_LOB_PREFETCH_SIZE = 4000;
		private static final int DEFAULT_COMMIT_INTERVAL = 10000;
		private static final int DEFAULT_NUM_ROWS_PER_QUERY = 100000;
		private static final int DEFAULT_NUM_RETRIES = 2;
		private static final String DEFAULT_KEYTXN_MAP_FILES = "keyTxnMapFile.txt" ;
		private static final Integer DEFAULT_BUFFER_SIZE  = 0;
		private static final String DEFAULT_PKEY_NAME = "key";
		private static final String DEFAULT_PKEY_TYPE = "LONG";
	    private static final String DEFAULT_QUERY_HINT = "";
	    private static final String DEFAULT_BEGINSRC_KEY = "";
	    private static final String DEFAULT_ENDSRC_KEY = "";

	  
		public Config()
		{
			_enableNumRowsQuery = DEFAULT_ENABLE_NUM_ROWS_QUERY;
			_numRowsPrefetch = DEFAULT_NUM_ROWS_PREFETCH;
			_LOBPrefetchSize = DEFAULT_LOB_PREFETCH_SIZE;
			_commitInterval = DEFAULT_COMMIT_INTERVAL ;
			_numRetries = DEFAULT_NUM_RETRIES;
			_numRowsPerQuery = DEFAULT_NUM_ROWS_PER_QUERY;
			_keyTxnFilesMap = new HashMap<String, String>();
			_keyTxnBufferSizeMap = new HashMap<String, Integer>();
			_pKeyNameMap = new HashMap<String, String>();
			_pKeyTypeMap = new HashMap<String, String>();
			_pKeyIndexMap = new HashMap<String, String>();
			_queryHintMap = new HashMap<String, String>();
			_eventQueryMap = new HashMap<String, String>();
			_beginSrcKeyMap = new HashMap<String, String>();
			_endSrcKeyMap = new HashMap<String, String>();
		}

		@Override
		public StaticConfig build() throws InvalidConfigException
		{
			LOG.info("enableNumRowsQuery:" + _enableNumRowsQuery);
			LOG.info("NumRowsPrefetch:" + _numRowsPrefetch);
			LOG.info("_LOBPrefetchSize:" + _LOBPrefetchSize);
			LOG.info("Commit Interval:" + _commitInterval);
			LOG.info("Num Retries:" + _numRetries);
			LOG.info("_numRowsPerQuery:" + _numRowsPerQuery);
			LOG.info("_keyTxnFilesMap:" + _keyTxnFilesMap);
			LOG.info("_keyTxnBufferSizeMap:" + _keyTxnBufferSizeMap);
			LOG.info("_pKeyNameMap:" + _pKeyNameMap);
			LOG.info("_pKeyTypeMap:" + _pKeyTypeMap);
			LOG.info("_pKeyIndexMap:" + _pKeyIndexMap);
			LOG.info("_queryHintMap:" + _queryHintMap);
			LOG.info("_beginSrcKeyMap:" + _beginSrcKeyMap);
			LOG.info("_endSrcKeyMap:" + _endSrcKeyMap);

			
			HashMap<String, DbusEventKey.KeyType> pKeyTypeMap = new HashMap<String, DbusEventKey.KeyType>();
			Iterator<Entry<String, String>> itr = _pKeyTypeMap.entrySet().iterator();
			while ( itr.hasNext())
			{
				Entry<String, String> entry = itr.next();
				DbusEventKey.KeyType kType = DbusEventKey.KeyType.valueOf(entry.getValue());
				pKeyTypeMap.put(entry.getKey(), kType);
			}

			return new StaticConfig(_enableNumRowsQuery, _numRowsPrefetch,_LOBPrefetchSize,
									_commitInterval,_numRetries, _numRowsPerQuery,
									_keyTxnFilesMap, _keyTxnBufferSizeMap,
									_pKeyNameMap, pKeyTypeMap, _pKeyIndexMap, _queryHintMap,_eventQueryMap, _beginSrcKeyMap, _endSrcKeyMap);
		}

		public int getNumRowsPrefetch() {
			return _numRowsPrefetch;
		}

		public void setNumRowsPrefetch(int numRowsPrefetch) {
			this._numRowsPrefetch = numRowsPrefetch;
		}

		public int getLOBPrefetchSize() {
			return _LOBPrefetchSize;
		}

		public void setLOBPrefetchSize(int lOBPrefetchSize) {
			_LOBPrefetchSize = lOBPrefetchSize;
		}

		public int getCommitInterval() {
			return _commitInterval;
		}

		public void setCommitInterval(int commitInterval) {
			this._commitInterval = commitInterval;
		}

		public int getNumRetries() {
			return _numRetries;
		}

		public void setNumRetries(int numRetries) {
			this._numRetries = numRetries;
		}

		public String getKeyTxnMapFile(String sourceName)
		{
			String file = _keyTxnFilesMap.get(sourceName);

			if ( null == file)
			{
				_keyTxnFilesMap.put(sourceName, DEFAULT_KEYTXN_MAP_FILES);
				return DEFAULT_KEYTXN_MAP_FILES;
			}
			return file;
		}

		public void setKeyTxnMapFile(String sourceName, String file)
		{
			_keyTxnFilesMap.put(sourceName, file);
		}

		public  String getPKeyName(String srcName)
		{
			String key = _pKeyNameMap.get(srcName);
			if ( null == key)
			{
				_pKeyNameMap.put(srcName, DEFAULT_PKEY_NAME);
				return DEFAULT_PKEY_NAME;
			}
			return key;
		}

		public void setBeginSrcKey(String srcName, String key)
		{
			_beginSrcKeyMap.put(srcName, key);
		}
		
		public String getBeginSrcKey(String srcName)
		{
		      String key = _beginSrcKeyMap.get(srcName);

		      if ( null == key)
		      {
		    	key = DEFAULT_BEGINSRC_KEY;
		    	_beginSrcKeyMap.put(srcName, key);
		      }

		      return key;
		}
		
		public void setEndSrcKey(String srcName, String key)
		{
			_endSrcKeyMap.put(srcName, key);
		}
		
		public String getEndSrcKey(String srcName)
		{
		      String key = _endSrcKeyMap.get(srcName);

		      if ( null == key)
		      {
		    	key = DEFAULT_ENDSRC_KEY;
		    	_endSrcKeyMap.put(srcName, key);
		      }

		      return key;
		}
		
		public void setQueryHint(String srcName, String key)
		{
			_queryHintMap.put(srcName, key);
		}

	    public  String getQueryHint(String srcName)
	    {
	      String key = _queryHintMap.get(srcName);

	      if ( null == key)
	      {

	        _queryHintMap.put(srcName, DEFAULT_QUERY_HINT);

	        return DEFAULT_QUERY_HINT;
	      }

	      return key;
	    }

	    public void setPKeyName(String srcName, String key)
	    {
	       _pKeyNameMap.put(srcName, key);
	    }

		public String getPKeyType(String srcName)
		{
			String type = _pKeyTypeMap.get(srcName);
			if ( null == type)
			{
				_pKeyTypeMap.put(srcName, DEFAULT_PKEY_NAME);
				return DEFAULT_PKEY_TYPE;
			}
			return type;
		}

		public void setPKeyType(String srcName, String type)
		{
			_pKeyTypeMap.put(srcName, type);
		}

		public String getPKeyIndex(String srcName)
		{
			String index = _pKeyIndexMap.get(srcName);
			if ( null == index)
			{
				String index2 =  srcName + "_pk";
				_pKeyIndexMap.put(srcName, index2);
				return index2;
			}
			return index;
		}

		public void setPKeyIndex(String srcName, String index)
		{
			_pKeyIndexMap.put(srcName, index);
		}

		public Integer getKeyTxnFileBufferSize(String sourceName)
		{
			Integer size = _keyTxnBufferSizeMap.get(sourceName);

			if ( null == size)
			{
				_keyTxnBufferSizeMap.put(sourceName, DEFAULT_BUFFER_SIZE);
				return DEFAULT_BUFFER_SIZE;
			}
			return size;
		}

		public void setKeyTxnFileBufferSize(String sourceName, Integer size)
		{
			_keyTxnBufferSizeMap.put(sourceName, size);
		}

		public int getNumRowsPerQuery() {
			return _numRowsPerQuery;
		}

		public void setNumRowsPerQuery(int numRowsPerQuery) {
			this._numRowsPerQuery = numRowsPerQuery;
		}


		public boolean isEnableNumRowsQuery() {
			return _enableNumRowsQuery;
		}

		public void setEnableNumRowsQuery(boolean enableNumRowsQuery) {
			this._enableNumRowsQuery = enableNumRowsQuery;
		}
		
		public void setEventQuery(String name,String value)
		{
			_eventQueryMap.put(name, value);
		}
		
		public String getEventQuery(String src)
		{
			return _eventQueryMap.get(src);
		}


		private  boolean _enableNumRowsQuery;
		private  int _numRowsPrefetch;
		private  int _LOBPrefetchSize;
		private  int _commitInterval;
		private  int _numRetries;
		private  int _numRowsPerQuery;
		private Map<String, String> _keyTxnFilesMap;
		private Map<String, Integer> _keyTxnBufferSizeMap;
		private Map<String, String> _pKeyNameMap;
		private Map<String, String> _pKeyTypeMap;
		private Map<String, String> _pKeyIndexMap;
        private Map<String, String> _queryHintMap;
        private Map<String, String> _eventQueryMap;
        private Map<String, String> _beginSrcKeyMap;
        private Map<String, String> _endSrcKeyMap;

	}

	public static class PrimaryKeyTxn
	{
		private long key;
		private String keyStr;
		private KeyType keyType;
		private long txn;

		public static final String DELIMITER = "@";


		@Override
		public String toString() {
			return "PrimaryKeyTxn [key=" + key + ", keyStr=" + keyStr
					+ ", keyType=" + keyType + ", txn=" + txn + "]";
		}

		public long getKey() {
			return key;
		}

		public long getTxn() {
			return txn;
		}

		public void setType(KeyType t)
		{
			keyType = t;
		}

		public void setKeyTxn(long key, long txn) {
			this.keyType = KeyType.LONG;
			this.txn = txn;
			this.key = key;
		}

		public String getKeyStr() {
			return keyStr;
		}

		public void setKeyStrTxn(String keyStr, long txn) {
			this.keyType = KeyType.STRING;
			this.txn = txn;
			this.keyStr = keyStr;
		}

		public KeyType getKeyType() {
			return keyType;
		}
		
		public PrimaryKeyTxn(PrimaryKeyTxn keyTxn) 
		{
			super();			
			copyFrom(keyTxn);
		}
				
		public PrimaryKeyTxn(long key) {
			super();
			this.key = key;
			this.keyStr = null;
			this.keyType = KeyType.LONG;
			this.txn = -1;
		}

		public PrimaryKeyTxn(String key)
		{
			super();
			this.keyStr = key;
			this.keyType = KeyType.STRING;
			this.key = -1;
			this.txn = -1;
		}

		public void writeTo(BufferedWriter writer)
			throws IOException
		{
			StringBuffer buffer = new StringBuffer();
			if ( KeyType.LONG == keyType)
			{
				buffer.append(key).append(DELIMITER);
			} else {
				buffer.append(keyStr).append(DELIMITER);
			}
			buffer.append(txn).append("\n");
			writer.write(buffer.toString());
		}

		public void readFrom(String line)
			throws IOException
		{
			String[] toks = line.split(DELIMITER);

			if ( KeyType.LONG == keyType)
			{
				key = Long.parseLong(toks[0]);
			} else {
				keyStr = toks[0];
			}
			txn = Long.parseLong(toks[1]);
		}

		public void readFrom(ResultSet rs, String keyName, KeyType keyType)
			throws SQLException
		{
			try
			{
				this.keyType = keyType;
				if (KeyType.LONG == keyType)
				{
					this.key = rs.getLong(keyName);
				} else {
					this.keyStr = rs.getString(keyName);
				}
			} finally {
			}
		}

		public long compareKey(PrimaryKeyTxn key2)
		{
			if ( this.keyType == KeyType.LONG)
			{
				return this.key - key2.getKey();
			} else {
				return this.keyStr.compareTo(key2.getKeyStr());
			}
		}

		public void copyFrom(PrimaryKeyTxn entry)
		{
			keyType = entry.getKeyType();
			key = entry.getKey();
			keyStr = entry.getKeyStr();
			txn = entry.getTxn();
		}

		public long compareTxn(PrimaryKeyTxn txn)
		{
			return this.txn - txn.getTxn();
		}

	}

	@Override
	public List<MonitoredSourceInfo> getSources() {
		return _sources;
	}

}
