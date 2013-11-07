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


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import com.linkedin.databus.bootstrap.api.BootstrapProcessingException;
import com.linkedin.databus.bootstrap.api.BootstrapProducerStatus;
import com.linkedin.databus2.core.container.request.BootstrapDBException;
import com.linkedin.databus2.core.container.request.BootstrapDatabaseTooOldException;
import com.linkedin.databus2.util.DBHelper;

/**
 *
 * BootstrapDB MetaData DAO
 *
 */
public class BootstrapDBMetaDataDAO
{
	public static final String MODULE = BootstrapDBMetaDataDAO.class.getName();
	public static final Logger LOG    = Logger.getLogger(MODULE);
	public  static final int NO_SOURCE_ID = -1;
	public static final long DEFAULT_WINDOWSCN = -1;
	public static final long INFINITY_WINDOWSCN= Long.MAX_VALUE;
	public static final String MIN_SCN_TABLE_NAME = "bootstrap_tab_minscn";
	public static final String CREATE_MINSCN_TABLE = MIN_SCN_TABLE_NAME + " (srcid int(11) NOT NULL,minscn bigint(20) NOT NULL default -1, PRIMARY KEY  (srcid)) ENGINE=InnoDB;";


	private final BootstrapConn         _bootstrapConn;
	private final DriverManagerDataSource _dataSource;

	private final String _dbHostname;
	private final String _dbUsername;
	private final String _dbPassword;
	private final String _dbName;


	public void reinitDB()
		throws SQLException
	{
		dropDB();
		createDB(_dbName, _dbUsername, _dbPassword, _dbHostname);

		_bootstrapConn.close();
		_bootstrapConn.recreateConnection();
		setupDB();
	}

	public void dropDB()
		throws SQLException
	{
		String sql = "drop database if exists " + _dbName;

		_bootstrapConn.executeDDL(sql);
	}


	public void setupDB()
		throws SQLException
	{
		LOG.info("Setting up Bootstrap DB");

		String [] sql = {
		    "CREATE TABLE if not exists " + _dbName + ".bootstrap_sources (id int(11) NOT NULL auto_increment,src varchar(255) NOT NULL,status TINYINT default 1,logstartscn bigint(20) default 0, PRIMARY KEY  (id),UNIQUE KEY src (src)) ENGINE=InnoDB;",
		    "CREATE TABLE if not exists " + _dbName + ".bootstrap_loginfo (srcid int(11) NOT NULL,logid int(11) NOT NULL default 0,minwindowscn bigint(20) NOT NULL default -1,maxwindowscn bigint(20) NOT NULL default -1,maxrid bigint(20) NOT NULL default 0,deleted TINYINT default 0,PRIMARY KEY (srcid, logid)) ENGINE=InnoDB;",
		    "CREATE TABLE if not exists " + _dbName + ".bootstrap_producer_state (srcid int(11) NOT NULL,logid int(11) NOT NULL default 0,windowscn bigint(20) NOT NULL default 0,rid bigint(20) NOT NULL default 0,PRIMARY KEY  (srcid)) ENGINE=InnoDB;",
		    "CREATE TABLE if not exists " + _dbName + ".bootstrap_applier_state (srcid int(11) NOT NULL,logid int(11) NOT NULL default 0,windowscn bigint(20) NOT NULL default 0,rid bigint(20) NOT NULL default 0,PRIMARY KEY  (srcid)) ENGINE=InnoDB;",
		    "CREATE TABLE if not exists " + _dbName + ".bootstrap_seeder_state (srcid int(11) NOT NULL,startscn bigint(20) NOT NULL default -1,endscn bigint(20) NOT NULL default -1,rid bigint(20) NOT NULL default 0,srckey varchar(255) NOT NULL default '',PRIMARY KEY  (srcid)) ENGINE=InnoDB;",
        "CREATE TABLE if not exists " + _dbName + "." + CREATE_MINSCN_TABLE,
		};

		for (int i =0; i < sql.length; i++)
			_bootstrapConn.executeDDL(sql[i]);
	}

	public void setupMinScnTable() throws SQLException
	{
	  String sql = "CREATE TABLE if not exists " + _dbName + "." + CREATE_MINSCN_TABLE;
	  _bootstrapConn.executeDDL(sql);
	}

	public static void createDB(String dbName, String dbUsername, String dbPassword, String dbHostname)
		throws SQLException
	{
		String dbSql = "CREATE DATABASE if not exists " + dbName;
		BootstrapConn bConn = new BootstrapConn();
		Statement stmt = null;
		try
		{
			LOG.info("Creating new db using SQL :" + dbSql);

			try {
				bConn.initBootstrapConn(false, dbUsername, dbPassword, dbHostname, null);
			} catch (InstantiationException e) {
				LOG.error("Got instantiation error while creating new bootstrapDB :", e);
				throw new RuntimeException(e);
			} catch (IllegalAccessException e) {
				LOG.error("Got illegal access error while creating new bootstrapDB :", e);
				throw new RuntimeException(e);
			} catch (ClassNotFoundException e) {
				LOG.error("Got class-not-found error while creating new bootstrapDB :", e);
				throw new RuntimeException(e);
			}

			stmt = bConn.getDBConn().createStatement();
			int ret = stmt.executeUpdate(dbSql);
			LOG.info("Create DB returned :" + ret);
		} catch (SQLException sqlEx) {
			LOG.error("Got error while creating new bootstrapDB :", sqlEx);
			throw sqlEx;
		} finally {
			DBHelper.close(stmt);
			bConn.close();
		}
	}



	public BootstrapDBMetaDataDAO(BootstrapConn bootstrapConn,
			String hostname,
			String dbUsername,
			String dbPassword,
			String dbName,
			boolean autoCommit)
					throws SQLException
	{
		String dbUrl =	"jdbc:mysql://" + hostname + "/"+ dbName+"?user=" + dbUsername +
				"&password=" + dbPassword;
		_dataSource = new DriverManagerDataSource();
		_dataSource.setDriverClassName("com.mysql.jdbc.Driver");
		_dataSource.setUrl(dbUrl);
		_dataSource.setUsername(dbUsername);
		_dataSource.setPassword(dbPassword);
		_dataSource.getConnection().setAutoCommit(autoCommit);

		_dbHostname = hostname;
		_dbUsername = dbUsername;
		_dbPassword = dbPassword;
		_dbName = dbName;

		_bootstrapConn = bootstrapConn;
	}

	public void addNewSourceInDB(String source, int state) throws SQLException, BootstrapDatabaseTooOldException
	{
		addNewSourceInDB(NO_SOURCE_ID, source, state);
	}



	public void addNewSourceInDB(int srcid, String source, int state) throws SQLException, BootstrapDatabaseTooOldException
	{
		Connection conn = _bootstrapConn.getDBConn();
		PreparedStatement addSrcStmt = null;
		try
		{
			int i = 1;
			try
			{
				if ( srcid == NO_SOURCE_ID)
				{
					addSrcStmt =
							conn.prepareStatement("insert into bootstrap_sources (src, status) values(?, ?) ");
				} else {
					addSrcStmt =
							conn.prepareStatement("insert into bootstrap_sources (id, src, status) values(?, ?, ?) ");
					addSrcStmt.setInt(i++, srcid);
				}

				addSrcStmt.setString(i++, source);
				addSrcStmt.setInt(i++, state);
				addSrcStmt.executeUpdate();
			} finally {
				DBHelper.close(addSrcStmt);
			}

			addSrcStmt = null;

			try
			{
				if ( srcid == NO_SOURCE_ID)
				{
					SourceStatusInfo srcIdStatus = getSrcIdStatusFromDB(source, false);
					srcid = srcIdStatus.getSrcId();
				}

				addSrcStmt =
						conn.prepareStatement("insert into bootstrap_loginfo (srcid) values(?)");
				addSrcStmt.setInt(1, srcid);
				addSrcStmt.executeUpdate();
				addSrcStmt.close();
			} finally {
				DBHelper.close(addSrcStmt);
			}

			addSrcStmt = null;

			try
			{
				addSrcStmt =
						conn.prepareStatement("insert into bootstrap_producer_state (srcid,windowscn) values(?, ?)");
				addSrcStmt.setInt(1, srcid);
				addSrcStmt.setLong(2, DEFAULT_WINDOWSCN);
				addSrcStmt.executeUpdate();
				addSrcStmt.close();
				addSrcStmt = null;

				addSrcStmt =
						conn.prepareStatement("insert into bootstrap_applier_state (srcid,windowscn) values(?,?)");
				addSrcStmt.setInt(1, srcid);
				addSrcStmt.setLong(2, DEFAULT_WINDOWSCN);
				addSrcStmt.executeUpdate();
				addSrcStmt.close();

			} finally {
				DBHelper.close(addSrcStmt);
			}

			addSrcStmt = null;

       DBHelper.commit(conn);

			createNewSrcTable(srcid);
			createNewLogTable(srcid);
		}
		catch (SQLException e)
		{
            DBHelper.rollback(conn);
			DBHelper.close(addSrcStmt);
			LOG.error("Exception encountered while adding a new source for bootstrap tracking",
					e);
			throw e;
		}
	}

	public void createNewLogTable(int srcid) throws SQLException
	{
		PreparedStatement addLogTabStmt = null;
		Connection conn = _bootstrapConn.getDBConn();
		try
		{
			String logTab = _bootstrapConn.getLogTableNameToProduce(srcid);

			StringBuilder logTabSql = new StringBuilder();
			logTabSql.append("create table if not exists ");
			logTabSql.append(logTab);
			logTabSql.append(" ( id bigint NOT NULL auto_increment,");
			logTabSql.append(" scn bigint NOT NULL, ");
			logTabSql.append(" windowscn bigint NOT NULL, ");
			logTabSql.append(" srckey varchar(255) NOT NULL,");
			logTabSql.append(" val longblob, ");
			logTabSql.append(" PRIMARY KEY (id)) ENGINE=innodb");

			final String sql = logTabSql.toString();
			addLogTabStmt = conn.prepareStatement(sql);
			addLogTabStmt.executeUpdate();

			LOG.info("Added a new log table for source : " + srcid);
		}
		catch (SQLException e)
		{
			LOG.error("Exception encountered while adding a new log table for source", e);
			throw e;
		}
		finally
		{
			if (null != addLogTabStmt)
			{
				addLogTabStmt.close();
				addLogTabStmt = null;
			}
		}
	}
	public void createNewSrcTable(int srcId) throws SQLException
	{
		PreparedStatement addSrcTabStmt = null;
		Connection conn = _bootstrapConn.getDBConn();

		try
		{
			String srcTab = _bootstrapConn.getSrcTableName(srcId);

			StringBuilder srcTabSql = new StringBuilder();
			srcTabSql.append("create table if not exists ");
			srcTabSql.append(srcTab);
			srcTabSql.append(" ( id bigint NOT NULL auto_increment,");
			srcTabSql.append(" scn bigint NOT NULL, ");
			srcTabSql.append(" srckey varchar(255) NOT NULL,");
			srcTabSql.append(" val longblob, ");
			srcTabSql.append(" PRIMARY KEY (id), UNIQUE KEY(srckey)) ENGINE=innodb");

			final String sql = srcTabSql.toString();
			LOG.info("Table Creation Command :" + sql);
			addSrcTabStmt = conn.prepareStatement(sql);
			addSrcTabStmt.executeUpdate();
		}
		catch (SQLException e)
		{
			LOG.error("Exception encountered while adding a new Source table for source: " + e.getMessage(), e);
			throw e;
		}
		finally
		{
			if (null != addSrcTabStmt)
			{
				addSrcTabStmt.close();
				addSrcTabStmt = null;
			}
		}
	}

	/**
	 * Drop all meta data and data for a source identified by source name (e.g. com.linkedin.events.example.person.Person
	 * @param sourceName
	 * @throws SQLException
	 */
	public void dropSourceInDB(String sourceName) throws SQLException
	{
	  PreparedStatement stmt = null;
    ResultSet rs = null;
    final String sql = "select id from bootstrap_sources where src=?";
    int id=-1;
    try
    {
      stmt = _bootstrapConn.getDBConn().prepareStatement(sql);
      stmt.setString(1, sourceName);
      rs = stmt.executeQuery();
      if (rs.next())
      {
        id = rs.getInt(1);
        dropSourceInDB(id);
      }
    }
    finally
    {
      DBHelper.close(rs, stmt, null);
    }

	}

	/*
	 * Drop all tables ( meta + data) for the source specified by the source Id
	 */
	public void dropSourceInDB(int srcId)
			throws SQLException
	{
		final String DELETE_SOURCES_ENTRY_PREFIX = "delete from bootstrap_sources where id = ";
		final String DELETE_SEEDER_STATE_ENTRY_PREFIX = "delete from bootstrap_seeder_state where srcid = ";
		final String DELETE_APPLIER_STATE_ENTRY_PREFIX = "delete from bootstrap_applier_state where srcid = ";
		final String DELETE_PRODUCER_STATE_ENTRY_PREFIX = "delete from bootstrap_producer_state where srcid = ";
		final String DELETE_LOGINFO_ENTRY_PREFIX = "delete from bootstrap_loginfo where srcid = ";
    final String DELETE_MINSCN_ENTRY_PREFIX = "delete from bootstrap_tab_minscn where srcid = ";
		final String DROP_TAB_TABLE_PREFIX ="drop table if exists tab_";
		final String DROP_LOG_TABLE_PREFIX = "drop table if exists log_";
		List<Integer> logIds = getAllActiveLogIdsForSource(srcId);

		_bootstrapConn.executeUpdate(DELETE_SOURCES_ENTRY_PREFIX + srcId);
		_bootstrapConn.executeUpdate(DELETE_SEEDER_STATE_ENTRY_PREFIX + srcId);
		_bootstrapConn.executeUpdate(DELETE_APPLIER_STATE_ENTRY_PREFIX + srcId);
		_bootstrapConn.executeUpdate(DELETE_PRODUCER_STATE_ENTRY_PREFIX + srcId);

		_bootstrapConn.executeUpdate(DELETE_LOGINFO_ENTRY_PREFIX + srcId);
    _bootstrapConn.executeUpdate(DELETE_MINSCN_ENTRY_PREFIX + srcId);

		for (Integer logId : logIds)
		{
			String drop = DROP_LOG_TABLE_PREFIX + srcId + "_" + logId;
			_bootstrapConn.executeUpdate(drop);
		}

		_bootstrapConn.executeUpdate(DROP_TAB_TABLE_PREFIX + srcId);
	}

	public List<Integer> getAllActiveLogIdsForSource(int srcId)
			throws SQLException
	{
		final String SELECT_LOGINFO_ENTRY_PREFIX = "select logid from bootstrap_loginfo where deleted = 0 and srcid = ";

		Statement stmt = null;
		ResultSet rs = null;
		List<Integer> result = new ArrayList<Integer>();
		try
		{
			String sql = SELECT_LOGINFO_ENTRY_PREFIX + srcId;
			stmt = _bootstrapConn.getDBConn().createStatement();
			rs = stmt.executeQuery(sql);
			while ( rs.next())
			{
				result.add(rs.getInt(1));
			}
		} finally {
			DBHelper.close(rs, stmt, null);
		}
		return result;
	}


	/*
	 * Find the id for the start of the window SCN.
	 *
	 * @return
	 *    starting row id of the window in the log table
	 *    0 if all the entries have higher SCN than the query SCN
	 */
	public long getLogRowIdForSCN(long scn, int logid, int srcid)
			throws SQLException
	{
		String table = _bootstrapConn.getLogTableName(logid, srcid);

		StringBuilder queryBuilder = new StringBuilder();

		queryBuilder.append("select max(id) from ");
		queryBuilder.append(table);
		queryBuilder.append(" where scn < ");
		queryBuilder.append(scn);

		String sql = queryBuilder.toString();
		long rowId  = _bootstrapConn.executeQueryAndGetLong(sql, -1);
		return (rowId + 1);
	}




	public List<SourceStatusInfo> getSourceIdAndStatusFromName(List<String> sourceList, boolean activeCheck)
			throws SQLException,BootstrapDatabaseTooOldException
	{
		List<SourceStatusInfo> srcInfo = new ArrayList<SourceStatusInfo>();

		try
		{
			for ( String source : sourceList)
			{
				SourceStatusInfo pair = getSrcIdStatusFromDB(source, activeCheck);
				srcInfo.add(pair);
			}
		} catch (SQLException sqlEx) {
			LOG.error("Got Exception while getting Source Status", sqlEx);
			throw sqlEx;
		}
		return srcInfo;
	}


	/**
	 * helper function to get srcids from SourceStatusInfo structure
	 * @param srcList
	 * @return
	 */
	int[] getSrcIds(List<SourceStatusInfo> srcList)
	{
	  int srcIds[] = new int[srcList.size()];
	  int i=0;
	  for (SourceStatusInfo srcStatus: srcList)
	  {
	    srcIds[i++]=srcStatus.getSrcId();
	  }
	  return srcIds;
	}

	/**
	 *
	 *
	 * @return sourceStatusInfo for all sources in a given bootstrap DB
	 * @throws SQLException
	 * @throws BootstrapDatabaseTooOldException
	 */
	public List<SourceStatusInfo> getSrcStatusInfoFromDB()
      throws SQLException, BootstrapDatabaseTooOldException
  {
    int srcid = -1;
    int status = 1;
    ResultSet rs = null;
    Connection conn = _bootstrapConn.getDBConn();
    Statement stmt = conn.createStatement();
    List<SourceStatusInfo> srcInfo = new ArrayList<SourceStatusInfo>();
    try
    {

      String sql = "SELECT id, src, status from bootstrap_sources ";
      rs=stmt.executeQuery(sql);
      while (rs.next())
      {
        srcid = rs.getInt(1);
        String src = rs.getString(2);
        status = rs.getInt(3);
        srcInfo.add(new SourceStatusInfo(src,srcid,status));
      }
    }
    catch (SQLException e)
    {
      LOG.error("Error encountered while selecting source id from bootstrap:", e);
      throw e;
    }
    finally
    {
      DBHelper.close(rs,stmt,null);
    }
    return srcInfo;
  }


	public SourceStatusInfo getSrcIdStatusFromDB(String source, boolean activeCheck)
			throws SQLException, BootstrapDatabaseTooOldException
	{
		int srcid = -1;
		int status = 1;
		PreparedStatement getSrcStmt = null;
		ResultSet rs = null;
		Connection conn = _bootstrapConn.getDBConn();
		SourceStatusInfo srcIdStatusPair = null;
		try
		{

			getSrcStmt = conn.prepareStatement("SELECT id, status from bootstrap_sources where src = ?");
			getSrcStmt.setString(1, source);
			rs=getSrcStmt.executeQuery();

			if (rs.next())
			{
				srcid = rs.getInt(1);
				status = rs.getInt(2);
			}

			//LOG.info("srcid=" + srcid + " status=" + status);

			srcIdStatusPair = new SourceStatusInfo(source, srcid, status);

			if ( activeCheck )
				validateStatus(source, status);

		}
		catch (SQLException e)
		{
			LOG.error("Error encountered while selecting source id from bootstrap:", e);
			throw e;
		} catch(BootstrapDatabaseTooOldException bde ) {
			LOG.error("Error encountered while selecting source id from bootstrap:", bde);
			throw bde;
		}
		finally
		{
			DBHelper.close(rs,getSrcStmt,null);
		}

		return srcIdStatusPair;
	}

	private void validateStatus(String source, int status) throws BootstrapDatabaseTooOldException
	{
		switch (status)
		{
		case BootstrapProducerStatus.ACTIVE:
			return;
		case BootstrapProducerStatus.FELL_OFF_RELAY:
			throw new BootstrapDatabaseTooOldException("The bootstrap database for source :" + source + " is too old!");
		case BootstrapProducerStatus.SEEDING:
			throw new BootstrapDatabaseTooOldException("The bootstrap database for source :" + source + " is being seeded!");
		case BootstrapProducerStatus.SEEDING_CATCHUP:
			throw new BootstrapDatabaseTooOldException("The bootstrap database for source :" + source + " is seeded but not yet consistent!");
		case BootstrapProducerStatus.INACTIVE:
			throw new BootstrapDatabaseTooOldException("Bootstrapping for source :" + source + " is disabled");
		default:
			// nothing to do at this point for those status
		}
	}

	public static class SourceStatusInfo
	{
		private int srcId;
		private String srcName;
		private int status;  //TODO: DDSDBUS-361 Use Enum to track BootstrapDB Status


		public String getSrcName() {
			return srcName;
		}

		public void setSrcName(String srcName) {
			this.srcName = srcName;
		}

		public int getSrcId() {
			return srcId;
		}

		public void setSrcId(int srcId) {
			this.srcId = srcId;
		}

		public int getStatus() {
			return status;
		}

		public void setStatus(int status) {
			this.status = status;
		}

		public SourceStatusInfo(String srcName, int srcId, int status) {
			super();
			this.srcName = srcName;
			this.srcId = srcId;
			this.status = status;
		}

		public SourceStatusInfo()
		{
			this.srcName = null;
			this.srcId = -1;
			this.status = -1;
		}

		public boolean isValidSource()
		{
			return srcId >= 0;
		}

		@Override
		public String toString() {
			return "SourceStatusInfo [srcId=" + srcId + ", srcName=" + srcName
					+ ", status=" + status + "]";
		}
	}


	/**
	 * Responsible for getting the min(windowscn) for the listed sources from one of the
	 *    state tables (bootstrap_producer_state, bootstrap_applier_state, bootstrap_seeder_state)
	 * @param sourceNames list of sources
	 * @param table one of (bootstrap_producer_state, bootstrap_applier_state, bootstrap_seeder_state)
	 * @return return the min(windowscn), throws Exception for any other error.
	 * @throws BootstrapDBException
	 * @throws SQLException
	 */
	public long getMinWindowSCNFromStateTable(List<String> sourceNames, String table)
			throws SQLException, BootstrapDBException
	{
		List<Integer> srcIdList = new ArrayList<Integer>();

		if ((null == sourceNames) || (sourceNames.isEmpty()))
		{
			String msg = "SourceNames is empty for getMinWindowSCNFromStateTable !!";
			LOG.error(msg);
			throw new BootstrapDBException(msg);
		}

		for (String s : sourceNames)
		{
			SourceStatusInfo info = getSrcIdStatusFromDB(s, false);
			if (info.getSrcId() <0)
			{
				String msg = "Unable to determine sourceId for sourceName :" + s + " Source List is :" + sourceNames;
				LOG.error(msg);
				throw new BootstrapDBException(msg);
			}

			srcIdList.add(info.getSrcId());
		}

		StringBuilder sql = new StringBuilder();
		sql.append("select min(windowscn) from ");
		sql.append(table);
		sql.append(" where srcid in (");

		int count = srcIdList.size();
		for ( Integer id : srcIdList)
		{
			count--;
			sql.append(id);
			if ( count > 0)
				sql.append(",");
		}
		sql.append(")");

		return _bootstrapConn.executeQueryAndGetLong(sql.toString(), -1);
	}


	String arrayToString(int[] ids)
	{
	   StringBuilder idBuilder = new StringBuilder();
	   for (int id: ids)
	   {
	      if (idBuilder.length() > 0)
	      {
	        idBuilder.append(',');
	      }
	      idBuilder.append(id);
	   }
	   return idBuilder.toString();
	}

  /**
   * Used by bootstrap applier exactly once to get minScn of tab table that has just been populated by applier
   * @param srcid
   * @param tabRid : row offset in snapshot table (tab table)
   * @param timeoutInSec: specify max timeout in sec
   * @return minScn found in rows less than or equal to tabRid ;
   * WARNING: as tabRid increases this could become an expensive query
   * @throws SQLException
   */
  public long getMinWindowScnFromSnapshot(int srcid,int tabRid,int timeoutInSec) throws SQLException
  {
    long minScn = BootstrapDBMetaDataDAO.DEFAULT_WINDOWSCN;
    if (tabRid >= 0)
    {
      String sql="select min(scn) from tab_"+srcid + " where id <= " + tabRid;
      LOG.info("Executing min scn query from snapshot table : " + sql);
      minScn = _bootstrapConn.executeQuerySafe(sql, BootstrapDBMetaDataDAO.DEFAULT_WINDOWSCN,timeoutInSec);
      LOG.info("Finished executing min scn query from snapshot table : minScn= " + minScn);
    }
    return minScn;
  }

	/**
  *
  * @param srcId
  * @return max of minscn of tab table for  given set of srcIds; safest maxscn for a given set of sources; DEFAULT_WINDOWSCN if srcid doesn't exist
  * @throws SQLException
  */
 public long getMinScnOfSnapshots(List<SourceStatusInfo> srcStatusList) throws SQLException
 {
   return getMinScnOfSnapshots(getSrcIds(srcStatusList));
 }



	/**
	 *
	 * @param srcId
	 * @return max of minscn of tab table for  given set of srcIds; safest maxscn for a given set of sources; DEFAULT_WINDOWSCN if srcid doesn't exist
	 * @throws SQLException
	 */
	public long getMinScnOfSnapshots(int ...srcIds ) throws SQLException
	{
    String minScnQuery = "select max(minscn) from bootstrap_tab_minscn where srcid in (" + arrayToString(srcIds)+ ")";
    long minScn = _bootstrapConn.executeQuerySafe(minScnQuery, DEFAULT_WINDOWSCN,0);
    if (minScn == DEFAULT_WINDOWSCN)
    {
      LOG.warn("srcid=" + Arrays.toString(srcIds) + " had no entry in bootstrap_tab_minscn. Returning " + DEFAULT_WINDOWSCN);
    }
    return minScn;
	}

	public boolean doesMinScnTableExist() throws SQLException
	{
	  Connection conn = _bootstrapConn.getDBConn();
    PreparedStatement stmt = null;
    ResultSet rs = null;
    boolean exists=false;
    try
    {
      final String query = "select table_name from  information_schema.tables where table_schema=? and table_name=?";
      stmt = conn.prepareStatement(query);
      stmt.setString(1,_dbName);
      stmt.setString(2,MIN_SCN_TABLE_NAME);
      rs=stmt.executeQuery();
      exists=rs.next();
    }
    catch (SQLException e)
    {
      LOG.warn("Error! " + e.getMessage());
    }
    finally
    {
      DBHelper.close(rs,stmt,null);
    }
    return exists;
	}

	public boolean isSeeded(int srcId) throws SQLException
	{
	  Connection conn = _bootstrapConn.getDBConn();
    PreparedStatement stmt = null;
    ResultSet rs = null;
    boolean seeded=false;
    try
    {
      String query = "select srcid from bootstrap_seeder_state where srcid=?";
      stmt = conn.prepareStatement(query);
      stmt.setInt(1, srcId);
      rs=stmt.executeQuery();
      seeded = rs.next();
    }
    finally
    {
      DBHelper.close(rs,stmt,null);
    }
    return seeded;
	}

	/**
	 *
	 * @param srcId :source-id as defined in bootstrap_sources table
	 * @param purged : has source-id tab table ever been cleaned; i.e. has cleanup mode been set to anything other than BOOTSTRAP_FULL
	 * @return minScn of the logtable - i.e. mindwindowScn of an undeleted table of source srcId ; if none exists return DEFAULT_WINDOWSCN
	 * @throws SQLException
	 */

	 public long getMinScnFromLogTable(int srcId,boolean purged) throws SQLException
	 {
	   //if it has been purged; then look for undeleted (deleted=0) else look for amongst the deleted
	   String minScnQuery;
	   if (purged)
	   {
	     //if it has been purged; then look for undeleted (deleted=0) else look for amongst the deleted
	     minScnQuery = "select min(minwindowscn) from bootstrap_loginfo where deleted=0 and srcid=" + srcId;
	   }
	   else
	   {
	     //if it hasn't been purged ; then just find the minscn; from either deleted or non-deleted table entries
	     minScnQuery = "select min(minwindowscn) from bootstrap_loginfo where srcid=" + srcId;
	   }
	   long minScn = _bootstrapConn.executeQuerySafe(minScnQuery, DEFAULT_WINDOWSCN,0);
	   if (minScn==DEFAULT_WINDOWSCN)
	   {
	     //return infinity even if sources didn't exist;
	     LOG.warn("srcid=" + srcId + " had no entry in undeleted log tables. Returning " + minScn);
	   }
	   return minScn;
	 }
	/**
	 *
	 * @param srcId
	 * @param scn - minScn
	 * update minscn of tab table of give srcId
	 * @throws SQLException
	 */
	public void updateMinScnOfSnapshot(int srcId,long scn) throws SQLException
	{
	  Connection conn = _bootstrapConn.getDBConn();
	  PreparedStatement stmt = null;
	  try
	  {
      StringBuilder minScnQuery = new StringBuilder();
      minScnQuery.append("insert into bootstrap_tab_minscn (srcid,minscn) values (?,?) ");
      minScnQuery.append(" on duplicate key update minscn=?");
      stmt = conn.prepareStatement(minScnQuery.toString());
	    stmt.setInt(1, srcId);
	    stmt.setLong(2, scn);
	    stmt.setLong(3, scn);
	    stmt.executeUpdate();
      DBHelper.commit(conn);
	    LOG.info("Set minScn for tab_" + srcId + " to " + scn);
	  }
	  finally
	  {
	    DBHelper.close(stmt);
	  }
	}

	/**
   *  Used for tests to alter seeder state to simulate seeded table
   * @throws SQLException
	 * @throws BootstrapDatabaseTooOldException
   */
	public void updateRowOfSeederState(String sourceName,long rowId) throws SQLException, BootstrapDatabaseTooOldException
	{
	  SourceStatusInfo srcInfo = getSrcIdStatusFromDB(sourceName, false);
	  updateRowOfSeederState(srcInfo.getSrcId(), rowId);
	}


	/**
	 *  Used for tests to alter seeder state to simulate seeded table
	 * @throws SQLException
	 */
	void updateRowOfSeederState(int srcId,long rowId) throws SQLException
	{
	  Connection conn = _bootstrapConn.getDBConn();
    PreparedStatement stmt = null;
    try
    {
      StringBuilder sql = new StringBuilder();
      //sql.append("insert into bootstrap_applier_state ");
      //sql.append("values (?,?,?,?) ");
      //sql.append("on duplicate key update rid = ?");
      sql.append("insert into bootstrap_seeder_state (srcid,rid,srckey) ");
      sql.append("values (?,?,'')");
      sql.append("on duplicate key update rid = ?");
      stmt = conn.prepareStatement(sql.toString());
      stmt.setInt(1, srcId);
      stmt.setLong(2, rowId);
      stmt.setLong(3, rowId);
      stmt.executeUpdate();
      DBHelper.commit(conn);
    }
    finally
    {
      DBHelper.close(stmt);
    }
	}

	public int getLogIdToCatchup(int srcId, long sinceScn)
			throws SQLException, BootstrapProcessingException
	{
		int logid = -1;
		int deleted  = 0;
		Connection conn = _bootstrapConn.getDBConn();
		PreparedStatement stmt = null;
		ResultSet rs = null;

		try
		{
			if (0 >= sinceScn)
			{ // special handling for invalid scn, e.g. 0 or -1:
				// in this case, we want to ruturn the logid corresponding to the earliest log
				stmt =
						conn.prepareStatement("select logid, deleted from bootstrap_loginfo where srcid = ? and minwindowscn = (select min(minwindowscn) from bootstrap_loginfo where srcid = ? and deleted != 1 and minwindowscn >= 0)  order by logid asc limit 1");
				stmt.setInt(1, srcId);
				stmt.setInt(2, srcId);
			}
			else
			{
				stmt =
						conn.prepareStatement("SELECT logid, deleted from bootstrap_loginfo where srcid = ? and minwindowscn <= ? and maxwindowscn >= ? order by logid asc limit 1");
				stmt.setInt(1, srcId);
				stmt.setLong(2, sinceScn);
				stmt.setLong(3, sinceScn);
			}

			rs=stmt.executeQuery();

			if (rs.next())
			{
				logid = rs.getInt(1);
				deleted = rs.getInt(2);
				LOG.info("logid for catchup:" + logid + ", Deleted :" + deleted);
			}

			if ((0 > logid) || (1 == deleted))
			{
				throw new BootstrapProcessingException("Log file with logid=" + logid + " ,srcid=" + srcId
						+ " is either deleted or not found in bootstrap_loginfo for since scn: " + sinceScn);
			}
		}
		catch (SQLException e)
		{
			LOG.error("Error encountered while selecting logid from bootstrap_loginfo", e);
			throw e;
		} finally {
			DBHelper.close(rs,stmt,null);
		}

		return logid;
	}

	public BootstrapConn getBootstrapConn()
	{
		return _bootstrapConn;
	}

	public Map<String, SourceInfo> getDBTrackedSources(Set<String> configedSources)
	{
		StringBuilder sql = new StringBuilder();
		sql.append("select s.src, s.id, p.logid, p.rid, l.minwindowscn, l.maxwindowscn, s.status ");
		sql.append("from bootstrap_sources s, bootstrap_producer_state p, bootstrap_loginfo l ");
		sql.append("where p.srcid = s.id and p.srcid = l.srcid and p.logid = l.logid");
		JdbcTemplate select = new JdbcTemplate(_dataSource);
		SourceInfoResultHandler handler = new SourceInfoResultHandler(configedSources);
		select.query(sql.toString(), handler);
		return handler.getTrackedSources();
	}

	static class SourceInfoResultHandler implements RowCallbackHandler
	{
		private final Set<String> _configedSources;
		private final Map<String, SourceInfo> _trackedSources;

		public SourceInfoResultHandler(Set<String> configedSources)
		{
			_configedSources = configedSources;
			_trackedSources = new HashMap<String, SourceInfo>();
		}

		@Override
		public void processRow(ResultSet rs) throws SQLException
		{
			String src = rs.getString(1);
			if (_configedSources.contains(src))
			{
				SourceInfo info = new SourceInfo(rs.getInt(2), rs.getInt(3), rs.getInt(4), rs.getLong(5), rs.getLong(6), rs.getInt(7));
				_trackedSources.put(src, info);
				LOG.info("SourceInfo :" + src + " :" + info);
			}
		}

		public Map<String, SourceInfo> getTrackedSources()
		{
			return _trackedSources;
		}
	}

	public void initMetadataTables(List<String> registeredSources)
	{
		// delete any existing rows if exist
		deleteExistingMetaTables();

		// insert new rows based on the registered sources
		insertAllSources(registeredSources);
	}

	public void deleteExistingMetaTables()
	{
		StringBuilder deleteBootstraSourcesSql = new StringBuilder();
		deleteBootstraSourcesSql.append("delete from bootstrap_sources");
		JdbcTemplate deleteAllTemplate = new JdbcTemplate(_dataSource);
		deleteAllTemplate.execute(deleteBootstraSourcesSql.toString());
		// TODO: need to delete other metatables
	}

	private void insertAllSources(List<String> registeredSources)
	{
		StringBuilder insertSql = new StringBuilder();
		insertSql.append("insert into bootstrap_sources (src, status) values (?, 0)");
		JdbcTemplate insertTemplate = new JdbcTemplate(_dataSource);
		insertTemplate.batchUpdate(insertSql.toString(),
				new InsertSourcesSetter(registeredSources));
	}

	public void close()
	{
		_bootstrapConn.close();
	}

	static class InsertSourcesSetter implements BatchPreparedStatementSetter
	{
		private final ArrayList<String> _registeredSources;

		public InsertSourcesSetter(List<String> registeredSources)
		{
			_registeredSources = new ArrayList<String>(registeredSources);
		}

		@Override
		public int getBatchSize()
		{
			return _registeredSources.size();
		}

		@Override
		public void setValues(PreparedStatement stmt, int index) throws SQLException
		{
			stmt.setString(1, _registeredSources.get(index));
		}
	}

	public void updateSourcesStatus(Set<String> registeredSources, int status)
	{
		StringBuilder updateSql = new StringBuilder();
		updateSql.append("update bootstrap_sources set status = ? where src = ?");
		JdbcTemplate updateTemplate = new JdbcTemplate(_dataSource);
		updateTemplate.batchUpdate(updateSql.toString(),
				new UpdateSourcesSetter(registeredSources, status));
	}

	static class UpdateSourcesSetter implements BatchPreparedStatementSetter
	{
		private final ArrayList<String> _registeredSources;
		private final int _status;

		public UpdateSourcesSetter(Set<String> registeredSources, int status)
		{
			_registeredSources = new ArrayList<String>(registeredSources);
			_status = status;
		}

		@Override
		public int getBatchSize()
		{
			return _registeredSources.size();
		}

		@Override
		public void setValues(PreparedStatement stmt, int index) throws SQLException
		{
			stmt.setInt(1, _status);
			stmt.setString(2, _registeredSources.get(index));
		}
	}
}
