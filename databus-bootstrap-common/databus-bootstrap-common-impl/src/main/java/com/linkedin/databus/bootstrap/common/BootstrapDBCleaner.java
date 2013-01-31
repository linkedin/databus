package com.linkedin.databus.bootstrap.common;
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


import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.databus.bootstrap.common.BootstrapCleanerStaticConfig.BootstrapDBType;
import com.linkedin.databus.bootstrap.common.BootstrapCleanerStaticConfig.RetentionStaticConfig;
import com.linkedin.databus.bootstrap.common.BootstrapDBMetaDataDAO.SourceStatusInfo;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventV1;
import com.linkedin.databus2.core.container.request.BootstrapDatabaseTooOldException;
import com.linkedin.databus2.util.DBHelper;

public class BootstrapDBCleaner 
{
	public static final long MILLISEC_IN_SECONDS = 1000;
	public static final long NANOSEC_IN_MILLISECONDS = 1000000L;
	public static final long MILLISEC_IN_MINUTES = 60*MILLISEC_IN_SECONDS;
	public static final long MILLISEC_IN_HOUR = 60*MILLISEC_IN_MINUTES;
	public static final long MILLISEC_IN_DAY = 24*MILLISEC_IN_HOUR;
	
	public static final String MODULE = BootstrapDBCleaner.class.getName();
	public static final Logger LOG = Logger.getLogger(MODULE);

	public BootstrapCleanerStaticConfig _cleanerConfig;
	public BootstrapReadOnlyConfig _bootstrapConfig;
	public List<SourceStatusInfo> _sources;
	private final BootstrapProducerThreadBase _applier;
	private BootstrapDBMetaDataDAO         _bootstrapDao       = null;
	private Map<Short, LogInfo>   _lastValidLogMap = new HashMap<Short, LogInfo>();
	private volatile boolean isCleaning = false;
	

	public boolean isCleanerRunning()
	{
		return isCleaning;
	}
		
	public List<SourceStatusInfo> getSources() {
		return _sources;
	}

	public void setSources(List<SourceStatusInfo> sources) {
		this._sources = sources;
	}

	public BootstrapDBMetaDataDAO getBootstrapDao() {
		return _bootstrapDao;
	}

	public BootstrapDBCleaner(String name, 
			BootstrapCleanerStaticConfig config,
			BootstrapReadOnlyConfig bootstrapConfig,
			BootstrapProducerThreadBase applier,
			List<String> sources)
					throws SQLException					
	{
		_cleanerConfig = config;
		_bootstrapConfig = bootstrapConfig;
		_applier = applier;
		getOrCreateConnection();
		if ( null != sources)
		{
			try
			{
				_sources = _bootstrapDao.getSourceIdAndStatusFromName(sources,false);
			} catch (BootstrapDatabaseTooOldException bto) {
				LOG.error("Not expected to receive this exception as activeCheck is turned-off", bto);
				throw new RuntimeException(bto);
			}
		}
		LOG.info("Cleaner Config is :" + _cleanerConfig);
	}

	/*
	 * @return a bootstrapDB connection object.
	 * Note: The connection object is still owned by BootstrapConn. SO dont close it
	 */
	public Connection getOrCreateConnection() 
			throws SQLException
	{
		Connection conn = null;

		if (_bootstrapDao == null)
		{
			LOG.info("<<<< Creating Bootstrap Connection!! >>>>");
			BootstrapConn dbConn = new BootstrapConn();
			_bootstrapDao = new BootstrapDBMetaDataDAO(dbConn,
					_bootstrapConfig.getBootstrapDBHostname(),
					_bootstrapConfig.getBootstrapDBUsername(),
					_bootstrapConfig.getBootstrapDBPassword(),
					_bootstrapConfig.getBootstrapDBName(),
					false);
			try
			{
				dbConn.initBootstrapConn(false,
						_bootstrapConfig.getBootstrapDBUsername(),
						_bootstrapConfig.getBootstrapDBPassword(),
						_bootstrapConfig.getBootstrapDBHostname(),
						_bootstrapConfig.getBootstrapDBName());
			} catch (Exception e) {
				LOG.fatal("Unable to open BootstrapDB Connection !!", e);
				throw new RuntimeException("Got exception when getting bootstrap DB Connection.",e);
			}
		}

		try
		{
			conn = _bootstrapDao.getBootstrapConn().getDBConn();
		} catch ( SQLException e) {
			LOG.fatal("Not able to open BootstrapDB Connection !!", e);
			throw new RuntimeException("Got exception when getting bootstrap DB Connection.",e);
		}
		return conn;
	}

	/**
	 * Return the milli-second threshold for delete criteria.
	 * 
	 * @param config RetentionConfig
	 * @return milliSecThreshold
	 */
	private long getMilliSecTime(RetentionStaticConfig config)
	{
		long qty = config.getRetentionQuantity();
		long milliSecQty = -1;
		
		switch ( config.getRetentiontype())
		{
			case RETENTION_SECONDS:
					milliSecQty = qty * MILLISEC_IN_SECONDS;
					break;					
					
			default:
					throw new RuntimeException("Retention Config (" + config + ") expected to be time based but is not !!");
					
		}
		return milliSecQty;
	}
	
	public long filterCandidateLogInfo(short srcId, 
												List<LogInfo> candidateLogsInfo, 
												RetentionStaticConfig config) throws SQLException 
	{
		switch (config.getRetentiontype())
		{
			case NO_CLEANUP:
				return -1;
			case RETENTION_LOGS:
			{
				Iterator<LogInfo> itr = candidateLogsInfo.iterator();
				LogInfo lastValidLog = null;
				int i = 0 ;
				while ( i < config.getRetentionQuantity() && itr.hasNext())
				{
					LogInfo log = itr.next();
					LOG.info("Removing the log table :" + getLogTable(log) + " from the delete List as it is too recent. Retaining :" + config.getRetentionQuantity() + " logs");
					itr.remove();
					lastValidLog = log;
					i++;
				}
				_lastValidLogMap.put(srcId, lastValidLog);
				break;
			}
			
			case RETENTION_SECONDS:	
			{
				long quantity = config.getRetentionQuantity();
				LOG.info("Retaining tables which could contain events which is less than " 
				          + quantity 
				          + " seconds old !!"); 
				long currTs = System.currentTimeMillis() * NANOSEC_IN_MILLISECONDS;
				long nanoSecQty = getMilliSecTime(config) * NANOSEC_IN_MILLISECONDS;
				long threshold = (currTs - nanoSecQty);
				
				LOG.info("Removing tables from the delete-list whose last row has timestamp newer than :" 
							+ threshold + " nanosecs");
				
				Iterator<LogInfo> itr = candidateLogsInfo.iterator();
				LogInfo lastValidLog = null;
				LOG.info("Timestamp Threshold for src id :" + srcId 
						  + " is :" + threshold + ", Retention Config " 
						  + config + "(" + nanoSecQty + " nanosecs)");
				
				while (itr.hasNext())
				{
					LogInfo log = itr.next();

					long timestamp = getNanoTimestampOfLastEventinLog(log);
					
					if ( timestamp < threshold)
					{
						LOG.info("Reached the log table whose timestamp (" + timestamp 
								     + ") is less than the threshold (" + threshold + ").");
						break;
					} else {
						LOG.info("Removing the log table :" + getLogTable(log) 
								+ " from the delete List as it is too recent. Last Event Timestamp :" 
					              + timestamp + ", threshold :" + threshold);
						lastValidLog = log;
						itr.remove();
					}
				}
				_lastValidLogMap.put(srcId, lastValidLog);
			}
			break;
		}
		
		long scn = -1;
		
		if ( ! candidateLogsInfo.isEmpty() )
			scn = getSCNOfLastEventinLog(candidateLogsInfo.get(0));
		
		return scn;
	}
	
	
	public synchronized void doClean()
	{    	
		try
		{	
			isCleaning = true;
			
			for (SourceStatusInfo s : _sources)
			{
				BootstrapDBType type = _cleanerConfig.getBootstrapType(s.getSrcName());
				
				LOG.info("Cleaner running for source :" + s.getSrcName() + "(" + s.getSrcId() +") with bootstrapDB type :" + type);
			
				LogInfo logInfo = getThresholdWindowSCN(type);

				if ( null == logInfo)
				{
					LOG.info("No WindowSCN. Nothing to cleanup for source : " + s.getSrcName());
					continue;
				}
				
				LOG.info("LOG info with lowest windowSCN :" + logInfo);
				
				LOG.info("Begin phase 1 : Gather candidate loginfo :");
				List<LogInfo> candidateLogsInfo = getCandidateLogsInfo(logInfo.getMinWindowSCN(), (short)(s.getSrcId()));
				if((null == candidateLogsInfo) || (candidateLogsInfo.isEmpty()))
				{
					LOG.info("No logs to cleanup for source :" + s.getSrcName() + "(" + s.getSrcId() +")");
					continue;
				}
				LOG.info("End phase 1 : Gather candidate loginfo :");

				LOG.info("Initial Candidate Set for Source :" + s.getSrcName() + " is :" + candidateLogsInfo);
				RetentionStaticConfig rConf = _cleanerConfig.getRetentionConfig(s.getSrcName());
				LOG.info("Retention Config for source :" + s.getSrcName() + " is :" + rConf);
				
				LOG.info("Begin phase 2 : Filter based on retention config :");
				long scn = filterCandidateLogInfo((short)s.getSrcId(),
																candidateLogsInfo,
																_cleanerConfig.getRetentionConfig(s.getSrcName()));
				
				LOG.info("Log tables to be deleted for source :" + s.getSrcName() 
							+ "(" + s.getSrcId() + ") are :" + candidateLogsInfo + ", Max SCN of deleted logs:" + scn);
				LOG.info("End phase 2 : Filter based on retention config :");
				
				if ( (scn <= 0) || ( candidateLogsInfo.isEmpty()))
				{
					LOG.info("Source :" + s.getSrcName() + "(" + s.getSrcId() 
							  + ") No log tables to be deleted !! MaxSCN : " + scn 
							  + ", candidateLogs :" + candidateLogsInfo);
					continue;
				}
				
				LOG.info("Begin phase 3 : Updating Meta Info :");
				LogInfo firstValidLog = getFirstLogTableWithGreaterSCN((short)s.getSrcId(), scn);
				updateSource(firstValidLog);
				LOG.info("End phase 3 : Updating Meta Info :");

				LOG.info("Begin phase 4 : Deleting Log tables :");
				List<LogInfo> deletedLogsInfo = dropTables(candidateLogsInfo);
		        markDeleted(deletedLogsInfo);
				LOG.info("End phase 4 : Deleting Log tables :");	

				if ( (_cleanerConfig.getBootstrapType(s.getSrcName()) == BootstrapDBType.BOOTSTRAP_CATCHUP_APPLIER_RUNNING) &&
						((_applier != null) || _cleanerConfig.forceTabTableCleanup(s.getSrcName())))
				{
					LOG.info("Source :" + s.getSrcName() + "(" + s.getSrcId() +") is running in catchup_applier_running mode. "
							    + "Will delete all rows whose scn is less than or equal to " + scn);
					if ( (null != _applier) && (_applier.isAlive()))
					{
						LOG.info("Begin phase 5 : Pausing Applier and deleting Rows from tab table :");
				
						LOG.info("Requesting applier to pause !!");
						_applier.pause();
						LOG.info("Applier paused !!");
					}
					
					try
					{
						int numRowsDeleted = deleteTable(getSrcTable(s.getSrcId()), scn);
					
						LOG.info("Number of Rows deleted for source  :" + s.getSrcName() + "(" + s.getSrcId() +") :" + numRowsDeleted);
						if ( numRowsDeleted > 0 && _cleanerConfig.isOptimizeTableEnabled(s.getSrcName()))
						{
							LOG.info("Optimizing table to reclaim space for source :" + s.getSrcName() + "(" + s.getSrcId() +")");
							optimizeTable(getSrcTable(s.getSrcId()));
						} 
					} finally {
						if ( (null != _applier) && (_applier.isAlive()))
						{
							LOG.info("Requesting applier to resume !!");
							_applier.unpause();
							LOG.info("Applier resumed !!");
						}
					}
					
					LOG.info("End phase 5 : Deleting Rows from tab table :");
				}
				
				LOG.info("Cleaner done for source :" + s.getSrcName() + "(" + s.getSrcId() +")");
			}
		} catch (SQLException ex) {
			LOG.error("Got SQL exception while cleaning bootstrapDB !!", ex);
		} catch (InterruptedException ie) {
			LOG.error("Got interrupted exception while cleaning bootstrapDB !!", ie);
		} finally {
			isCleaning = false;
		}
	}

	/*
	 * @return the LogInfo(srcId,logId,windowSCN) for
	 *         the entry in bootstrap_applier_state with minimum SCN
	 */
	private LogInfo getThresholdWindowSCN(BootstrapDBType type)
			throws SQLException
	{
		String sql = null;
		
		switch (type)
		{
			case BOOTSTRAP_CATCHUP_APPLIER_NOT_RUNNING: 
				sql = getMinProducerSCNLogStmt();
				break;
			case BOOTSTRAP_CATCHUP_APPLIER_RUNNING:
			case BOOTSTRAP_FULL:
				sql = getMinApplierSCNLogStmt();
				break;
		}
		
		LOG.info("Threshold WindowScn fetch Query :" + sql);
		LogInfo logInfo = new LogInfo();
		ResultSet rs = null;
		PreparedStatement stmt = null;
		try
		{
			Connection conn = _bootstrapDao.getBootstrapConn().getDBConn();
			stmt = conn.prepareStatement(sql);
			rs = stmt.executeQuery();
			if ( ! rs.next() )
			{
				LOG.error("SQL :(" + sql + ") returned empty records");
				return null;
			}

			logInfo.setLogId(rs.getInt(1));
			logInfo.setSrcId(rs.getShort(2));
			logInfo.setMinWindowSCN(rs.getLong(3));
			logInfo.setMaxWindowSCN(rs.getLong(3));

		} finally {
			DBHelper.close(rs,stmt,null);
		}

		return logInfo;
	}

	/*
	 * @param srcId : SourceId
	 * @param windowSCN : windowSCN to be checked
	 * @return the LogInfo(srcId,logId,minWindowSCN) for
	 *         the entry in bootstrap_loginfo with minimum SCN > passed SCN
	 */
	private LogInfo getFirstLogTableWithGreaterSCN(short srcId, long windowSCN)
			throws SQLException
	{
		String sql = getFirstLogTableWithGreaterSCNStmt();
		ResultSet rs = null;
		PreparedStatement stmt = null;
		LogInfo logInfo = new LogInfo();

		try
		{
			Connection conn = _bootstrapDao.getBootstrapConn().getDBConn();
			stmt = conn.prepareStatement(sql);
			stmt.setShort(1, srcId);
			stmt.setLong(2,windowSCN);
			rs = stmt.executeQuery();

			if (rs.next())
			{
				logInfo.setSrcId(srcId);
				logInfo.setLogId(rs.getInt(1));
				logInfo.setMinWindowSCN(rs.getLong(2));
				logInfo.setMaxWindowSCN(rs.getLong(3));
			} else {
				return null;
			}
		} finally {
			DBHelper.close(rs,stmt,null);
		}
		return logInfo;
	}
	

	/*
	 * Find the list of logTables for a source that are candidates for cleanup
	 *
	 * @param windowSCN : MinimumSCN in bootstrap_applier_state table
	 * @param srcid : SourceId
	 * @return list of LogInfo for log tables for the source that can be removed
	 *         in descending order of logIds
	 */
	private List<LogInfo> getCandidateLogsInfo(long windowSCN, short srcId)
			throws SQLException
	{
		List<LogInfo> logsInfo = new ArrayList<LogInfo>();
		String sql = getCandidateLogIdsForSrcStmt();
		LOG.info("SQL statement for fetching candidate logIds :" + sql);

		ResultSet rs = null;
		PreparedStatement stmt = null;
		try
		{
			Connection conn = _bootstrapDao.getBootstrapConn().getDBConn();
			stmt = conn.prepareStatement(sql);
			stmt.setShort(1, srcId);
			stmt.setLong(2,windowSCN);
			stmt.setShort(3,srcId);
			rs = stmt.executeQuery();

			while (rs.next())
			{
				LogInfo logInfo = new LogInfo();
				logInfo.setSrcId(srcId);
				logInfo.setLogId(rs.getInt(1));
				logInfo.setMinWindowSCN(rs.getLong(2));
				logInfo.setMaxWindowSCN(rs.getLong(3));
				logsInfo.add(logInfo);
			}
		} finally {
			DBHelper.close(rs,stmt,null);
		}
		return logsInfo;
	}

	private void updateSource(LogInfo logInfo)
			throws SQLException
	{
		LOG.info("Updating logStartSCN to " + logInfo.getMinWindowSCN()
				+ " for srcid :" + logInfo.getSrcId());
		String sql = getUpdateLogStartSCNStmt();
		PreparedStatement stmt = null;
		try
		{
			Connection conn = getOrCreateConnection();
			stmt = conn.prepareStatement(sql);
			stmt.setLong(1,logInfo.getMinWindowSCN());
			stmt.setShort(2, logInfo.getSrcId());
			stmt.executeUpdate();
			conn.commit();
		} finally {
			DBHelper.close(stmt);
		}
	}

	/*
	 * Drop Tables corresponding to the list of LogInfo
	 * @param logsInfo : LogInfo of the tables needed to be deleted
	 * @return the list of LogInfo corresponding to tables that have been successfully dropped.
	 */
	private List<LogInfo> dropTables(List<LogInfo> logsInfo)
	{
		List<LogInfo> deletedLogsInfo = new ArrayList<LogInfo>();

		for (LogInfo logInfo : logsInfo)
		{
			try
			{
				dropTable(logInfo);
				deletedLogsInfo.add(logInfo);
			} catch ( SQLException ex) {
				LOG.error("Unable to delete log table :" + getLogTable(logInfo));
			}
		}
		return deletedLogsInfo;
	}

	/*
	 * Mark Tables deleted corresponding to the list of LogInfo
	 * @param logsInfo : LogInfo of the tables needed to be marked
	 * @return the list of LogInfo corresponding to tables that have been successfully marked.
	 */
	private List<LogInfo> markDeleted(List<LogInfo> logsInfo)
			throws SQLException
	{
		Connection conn = null;
		PreparedStatement stmt = null;
		List<LogInfo> deletedLogsInfo = new ArrayList<LogInfo>();
		String sql = getMarkDeletedStmt();
		try
		{
			conn = _bootstrapDao.getBootstrapConn().getDBConn();
			stmt = conn.prepareStatement(sql);
			for (LogInfo logInfo : logsInfo)
			{
				try
				{
					LOG.info("Marking table : " + getLogTable(logInfo) + " deleted !!");
					stmt.setShort(1, logInfo.getSrcId());
					stmt.setInt(2, logInfo.getLogId());
					stmt.executeUpdate();
					deletedLogsInfo.add(logInfo);
				} catch (SQLException ex ) {
					LOG.error("Unable to mark delete log table :" + getLogTable(logInfo) + " in bootstrap_loginfo");
					throw ex;
				}
			}
			conn.commit();
		} finally {
			DBHelper.close(stmt);
		}
		return deletedLogsInfo;
	}
	  
	private String getMarkDeletedStmt()
	{
		StringBuilder sql = new StringBuilder();

		sql.append("UPDATE bootstrap_loginfo ");
		sql.append(" set deleted = 1");
		sql.append(" where srcid = ?");
		sql.append(" and logid = ?");
		return sql.toString();
	}
	
	public void close()
	{
		_bootstrapDao.close();
	}
	
	private static class Row
	{
		private final long id;
		private final String key;
		private final long scn;
		private final DbusEvent event;
		public long getId() {
			return id;
		}
		public String getKey() {
			return key;
		}
		public long getScn() {
			return scn;
		}
		public DbusEvent getEvent() {
			return event;
		}
		public Row(long id, String key, long scn, DbusEvent event) {
			super();
			this.id = id;
			this.key = key;
			this.scn = scn;
			this.event = event;
		}
		
		@Override
		public String toString() {
			return "Row [id=" + id + ", key=" + key + ", scn=" + scn
					+ ", event=" + event + "]";
		}
	}
	
	private long  getNanoTimestampOfLastEventinLog(LogInfo logInfo)
		throws SQLException
	{
		long nanoTimestamp = Long.MAX_VALUE;
		Row r = getLastEventinLog(logInfo);
		
		if ( (null != r) && 
				(null != r.getEvent()))
		{
			nanoTimestamp = r.getEvent().timestampInNanos();
		}
		return nanoTimestamp;
	}
	
	private long  getSCNOfLastEventinLog(LogInfo logInfo)
			throws SQLException
	{
		long scn = -1;
		Row r = getLastEventinLog(logInfo);

		if ( null != r)
		{
			scn = r.getScn();
		}
		return scn;
	}
	
	private Row getLastEventinLog(LogInfo logInfo)
			throws SQLException
	{
		Statement stmt = null;
		ResultSet rs = null;
		long id = -1;
		long scn = -1;
		String key = null;
		DbusEvent event = null;
		try
		{
			String tableName = getLogTable(logInfo);
			StringBuilder sql = new StringBuilder();
			sql.append("select id, srckey, scn, val from ");
			sql.append(tableName);
			sql.append(" where id = ( select max(id) from ");
			sql.append(tableName);
			sql.append(" )");
			Connection conn = _bootstrapDao.getBootstrapConn().getDBConn();
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql.toString());
			if ( rs.next())
			{
				int i = 1;
				id = rs.getLong(i++);
				String srcKey = rs.getString(i++);
				scn = rs.getLong(i++);
				ByteBuffer tmpBuffer = ByteBuffer.wrap(rs.getBytes(i));
				LOG.info("BUFFER SIZE:" + tmpBuffer.limit());
				event = new DbusEventV1(tmpBuffer,tmpBuffer.position());
				LOG.info("Last Row for log (" + logInfo + ") - ID :" + id + ", srcKey :" + srcKey + ", SCN :" + scn + ", Event :" + event.toString());
			} else {
				LOG.error("No ResultSet for query :" + sql.toString());				
			}

		} finally {
			DBHelper.close(rs,stmt,null);
		}
		return new Row(id, key, scn, event);
	}
	
	/*
	 * Drops a table corresponding to the passed logInfo
	 *
	 * @param logInfo : LogInfo of the table to be dropped.
	 *
	 */
	private void dropTable(LogInfo logInfo)
			throws SQLException
	{
		String tableName = getLogTable(logInfo);
		String cmd = "drop table if exists " + tableName;
		Statement stmt = null;
		try
		{
			LOG.info("Dropping table :" + tableName);
			Connection conn = _bootstrapDao.getBootstrapConn().getDBConn();
			stmt = conn.createStatement();
			stmt.executeUpdate(cmd);
		} finally {
			DBHelper.close(stmt);
		}
	}
	
	
	private int optimizeTable(String tableName)
		throws SQLException
	{
		final String cmd = "optimize table " + tableName;
		Statement stmt = null;
		int ret = -1;
		try
		{
			LOG.info("Optimizing table :" + tableName + ", CMD :(" + cmd + ")");
			Connection conn = _bootstrapDao.getBootstrapConn().getDBConn();
			stmt = conn.createStatement();
			ret = stmt.executeUpdate(cmd);
		} finally {
			DBHelper.close(stmt);
		}
		return ret;
	}
	
	
	private int deleteTable(String tableName, long scn)
			throws SQLException
	{		
		final String cmd = "delete from " + tableName + " where scn <= " + scn;
		int ret = -1;
		if ( scn <= 0 )
		{
			LOG.info("Trying to delete rows whose scn <=" + scn + ", skipping !!" );
			return ret;
		}
		
		Statement stmt = null;
		try
		{
			LOG.info("Deleting events from table :" + tableName + ", CMD :(" + cmd + ")");
			Connection conn = _bootstrapDao.getBootstrapConn().getDBConn();
			stmt = conn.createStatement();
			ret = stmt.executeUpdate(cmd);
			conn.commit();
		} finally {
			DBHelper.close(stmt);
		}
		return ret;
	}


	private String getCandidateLogIdsForSrcStmt()
	{
		StringBuilder sql = new StringBuilder();
		sql.append("select logid, minwindowscn, maxwindowscn from bootstrap_loginfo ");
		sql.append("where srcid = ?");
		sql.append(" and maxwindowscn < ? ");
		sql.append(" and deleted = 0 ");
		sql.append(" and logid < ( select max(logid) from bootstrap_loginfo where srcid = ? )");
		sql.append(" order by logid desc");
		return sql.toString();
	}

	private String getMinApplierSCNLogStmt()
	{
		StringBuilder sql = new StringBuilder();
		sql.append("select logid,srcid,windowscn from bootstrap_applier_state ");
		sql.append("where srcid in (");

		int count = _sources.size();
		for (SourceStatusInfo si : _sources)
		{
			count--;
			sql.append(si.getSrcId());
			if ( count > 0)
				sql.append(",");
		}
		sql.append(") ");	  	
		sql.append("order by windowscn asc limit 1");
		return sql.toString();
	}

	private String getMinProducerSCNLogStmt()
	{
		StringBuilder sql = new StringBuilder();
		sql.append("select logid,srcid,windowscn from bootstrap_producer_state ");
		sql.append("where srcid in (");

		int count = _sources.size();
		for (SourceStatusInfo si : _sources)
		{
			count--;
			sql.append(si.getSrcId());
			if ( count > 0)
				sql.append(",");
		}
		sql.append(") ");	  	
		sql.append("order by windowscn asc limit 1");
		return sql.toString();
	}
	
	/*
	 * @param logInfo LogInfo
	 * @return the LogTable name corresponding to the passed logInfo
	 */
	private String getLogTable(LogInfo logInfo)
	{
		return "log_" + logInfo.getSrcId() + "_" + logInfo.getLogId();
	}

	
	/*
	 * 
	 * @return the SrcTable name corresponding to the passed logInfo
	 */
	private String getSrcTable(int srcid)
	{
		return "tab_" + srcid;
	}

	private String getUpdateLogStartSCNStmt()
	{
		StringBuilder sql = new StringBuilder();
		sql.append("update bootstrap_sources set logstartscn = ?");
		sql.append(" where id = ?");
		return sql.toString();
	}

	private String getFirstLogTableWithGreaterSCNStmt()
	{
		StringBuilder sql = new StringBuilder();
		sql.append("select logid, minwindowscn, maxwindowscn from bootstrap_loginfo ");
		sql.append("where srcid = ?");
		sql.append(" and minwindowscn > ? ");
		sql.append(" order by logid asc limit 1");
		return sql.toString();
	}



	public static class LogInfo
	{
		private long minWindowSCN;
		private long maxWindowSCN;
		private short srcId;
		private int   logId;

		public long getMinWindowSCN() {
			return minWindowSCN;
		}

		public void setMinWindowSCN(long minWindowSCN) {
			this.minWindowSCN = minWindowSCN;
		}

		public long getMaxWindowSCN() {
			return maxWindowSCN;
		}

		public void setMaxWindowSCN(long maxWindowSCN) {
			this.maxWindowSCN = maxWindowSCN;
		}

		public short getSrcId()
		{
			return srcId;
		}

		public void setSrcId(short srcId)
		{
			this.srcId = srcId;
		}

		public int getLogId()
		{
			return logId;
		}

		public void setLogId(int logId)
		{
			this.logId = logId;
		}

		@Override
		public String toString() {
			return "LogInfo [minWindowSCN=" + minWindowSCN + ", maxWindowSCN="
					+ maxWindowSCN + ", srcId=" + srcId + ", logId=" + logId + "]";
		}
	}
}
