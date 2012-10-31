package com.linkedin.databus.bootstrap.server;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.linkedin.databus.bootstrap.api.BootstrapProcessingException;
import com.linkedin.databus.bootstrap.common.BootstrapConn;
import com.linkedin.databus.bootstrap.common.BootstrapDBMetaDataDAO;
import com.linkedin.databus.bootstrap.common.BootstrapDBMetaDataDAO.SourceStatusInfo;
import com.linkedin.databus.bootstrap.common.BootstrapReadOnlyConfig;
import com.linkedin.databus2.core.container.request.BootstrapDatabaseTooOldException;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus2.util.DBHelper;

public class BootstrapSCNProcessor 
{
	  public static final String            MODULE               = BootstrapSCNProcessor.class.getName();
	  public static final Logger            LOG                  = Logger.getLogger(MODULE);
	  public final static long START_SCN_QUERY_WAIT_TIME = 60000; /* 1 MINUTE */
	  public final static long QUERY_WAIT_TIME_SLICE = 100; /* 100 ms */
	  
	  private BootstrapDBMetaDataDAO                 _dbDao;
	  private DbusEventsStatisticsCollector _curStatsCollector;
	  private BootstrapServerStaticConfig   _config;
	  
	  public final static String START_SCN_STMT_SQL_PREFIX 
	  						= "SELECT min(windowscn) from bootstrap_applier_state where srcid IN (";
	  public final static String START_SCN_STMT_SQL_SUFFIX = ")";
	  public final static String PRODUCER_SCN_STMT_SQL_PREFIX
	  						= "SELECT max(windowscn) from bootstrap_producer_state where srcid IN (";
	  public final static String PRODUCER_SCN_STMT_SQL_SUFFIX = ")";
	  
	  public BootstrapSCNProcessor(BootstrapServerStaticConfig config,
              DbusEventsStatisticsCollector curStatsCollector)
	  	throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException
	  {
		  _config = config;
		  _curStatsCollector = curStatsCollector;
		  BootstrapConn conn = new BootstrapConn();
		  conn.initBootstrapConn(true,
				  					config.getDb().getBootstrapDBUsername(),
				  					config.getDb().getBootstrapDBPassword(),
				  					config.getDb().getBootstrapDBHostname(),
				  					config.getDb().getBootstrapDBName());
		  _dbDao = new BootstrapDBMetaDataDAO(conn,
				  					config.getDb().getBootstrapDBHostname(),
				  					config.getDb().getBootstrapDBUsername(),
				  					config.getDb().getBootstrapDBPassword(),
				  					_config.getDb().getBootstrapDBName(),
				  					false);
		  LOG.info("BootstrapSCNProcessor: config=" + config + ", dbConn=" + conn);
	  }

	
	  /*
	   * Business Logic to bypass snapshot phase. Here are the rules that each source requested by client must
	   * satisfy:
	   * 
	   * 1. Events with windowscn = Min(sinceScn,startScn) MUST still be available in log tables (not purged).
	   * 2. Approx NumRows between sinceSCN and startSCN MUST be less than or equal to RowsThresholdForSnapshotBypass
	   * 3. startScn MAY be greater than or equal to sinceScn.
	   * 4. sinceSCN must be greater than 0.
	   */
	  public boolean shouldBypassSnapshot(long sinceScn, long startScn, List<SourceStatusInfo> srcList)
			  throws SQLException, BootstrapProcessingException
	  {		  
		  if ((srcList == null) || (srcList.isEmpty()))
			  return false;
		  
		  if ( sinceScn <= 0 )
		  {
			  // client is requesting full bootstrap. Cannot guarantee with just log tables.
			  LOG.info("Client requesting from SCN (" + sinceScn + "). Bootstrap Snapshot will not be bypassed !!");
			  return false;
		  }
		  
		  for (SourceStatusInfo pair: srcList)
		  {
			  boolean disableSnapshotBypass = _config.isBypassSnapshotDisabled(pair.getSrcName());
			  
			  if ( disableSnapshotBypass)
				  return false;
			  
			  long currRows = getRowDiff(sinceScn, startScn, pair.getSrcId());
			  
			  // catchup log could have been deleted.
			  if (currRows == Long.MAX_VALUE)
			     return false;
			  
			  long threshold = _config.getRowsThresholdForSnapshotBypass(pair.getSrcName());
			  
			  if (currRows > threshold)
			  {
				  LOG.info("Threshold check failed for source (" + pair.getSrcName() + ") Threshold:" + threshold + ",Approx Rows:" + currRows );
				  return false;
			  }
		  }
		  
		  return true;
	  }
	  
	  private long getRowDiff(long sinceScn, long startScn, int srcid)
			    throws SQLException, BootstrapProcessingException
	  {
		  boolean debugEnabled = LOG.isDebugEnabled();
		  
		  int startSCNLogId = _dbDao.getLogIdToCatchup(srcid, startScn);

		  /*
		   * Its possible that startSCN <= sinceSCN (case of slow bootstrapProducer). Encourage bypass_snapshot for this case 
		   */
		  if ( sinceScn >= startScn)
			  return 0;
		  
		  int sinceSCNLogId = -1;
		  
		  try
		  {
			  sinceSCNLogId = _dbDao.getLogIdToCatchup(srcid, sinceScn);
		  } catch (BootstrapProcessingException bpe) {
			  /*
			   * If sincSCN is too old ( not found in log), we should not bypass bootstrap_snapshot but we can still serve
			   */
			  LOG.warn("Got Bootstrap Processing exception. Will disable bypassing snapshot !!", bpe);
			  return Long.MAX_VALUE;
		  }
			  
		
		  // getLogIdToCatchup guarantees that return value is not negative.
		  
		  if (sinceSCNLogId > startSCNLogId)
		  {
			  //because of the earlier check.
			  String msg = "Internal Error. sinceSCNLogId > startSCNLogId but sinceSCN < startSCN, sinceScn:" 
			                 + sinceScn + ",startScn:" + startScn + ",sinceSCNLogId:" 
					         + sinceSCNLogId + ",startSCNLogId:" + startSCNLogId;
			  LOG.error(msg);
			  throw new BootstrapProcessingException(msg);
		  }
		  		  
		  long startRowNum = _dbDao.getLogRowIdForSCN(sinceScn, sinceSCNLogId, srcid);
		  long endRowNum = _dbDao.getLogRowIdForSCN(startScn, startSCNLogId, srcid);
		  
		  if ( debugEnabled)
		  {
			  LOG.debug("startRowNum is :" + startRowNum + ", endRowNum is :" + endRowNum 
					    + " sinceSCNLogId :" + sinceSCNLogId + ", startSCNLogId:" 
					    + startSCNLogId + ",sinceSCN :" + sinceScn + ",startSCN:" + startScn);
		  }
		  
		  long numRows = 0;
		  if ( sinceSCNLogId == startSCNLogId)
		  {
			  numRows = (endRowNum - startRowNum);
		  } else {
			  long currNumRows = _dbDao.getBootstrapConn().getMaxRowIdForLog(sinceSCNLogId, srcid);
			  
			  if ( debugEnabled)
			  {
				  LOG.debug("MaxRid for srcid :" + srcid + " and logid:" + sinceSCNLogId + " is :" + currNumRows);
			  }
			  numRows = (currNumRows - startRowNum);
			  
			  for (int i = (sinceSCNLogId + 1); i < startSCNLogId; i++)
			  {
				  currNumRows = _dbDao.getBootstrapConn().getMaxRowIdForLog(i, srcid);
				  if ( debugEnabled)
					  LOG.debug("MaxRid for srcid :" + srcid + " and logid:" + i + " is :" + currNumRows);
				  numRows += currNumRows;				  
			  }
			  
			  numRows += endRowNum;
		  }
		  
		  LOG.info("Total Rows (approx) :" + numRows + " between SCNs :" + sinceScn + " and " + startScn + " for srcid :" + srcid);
		  		  
		  return numRows;
	  }
	  

	  /**
	   * Note: for snapshoting each source, we get the min(windowscn) so that we
	   * can guarantee not to deliver events later than the scn all prior sources
	   * are consistent on. We may end up doing a bit more unnecessary catch up.
	   * But it's an optimization we can investigate later.
	   * @return startScn
	   * @throws SQLException
	   */
	  public long getMinApplierWindowScn(long sinceScn, List<SourceStatusInfo> sourceList) 
	    throws BootstrapDatabaseTooOldException, BootstrapProcessingException, SQLException
	  {
	    long terminationTime = System.currentTimeMillis() + START_SCN_QUERY_WAIT_TIME;
	    long startScn = -1;
	    long producerScn = -1;
	    ResultSet rs = null;
	    Connection conn = _dbDao.getBootstrapConn().getDBConn();
	    Statement getScnStmt = null;
	    
	  	StringBuffer buf = new StringBuffer();
	    // get src id from db - make sure BootstrapDB is not too old
		boolean first = true;


		for ( SourceStatusInfo pair : sourceList)
		{				
			if ( !pair.isValidSource())
				throw new BootstrapProcessingException("Bootstrap DB not servicing source :" + pair.getSrcId());		

			if ( ! first ) buf.append(",");
			buf.append(pair.getSrcId());
			first = false;
		}

		
		String sources = buf.toString();
	    while (producerScn < sinceScn &&
	           System.currentTimeMillis() < terminationTime)
	    {
	    
	      try
	      {
	    	                 
	        String applierSql = START_SCN_STMT_SQL_PREFIX + sources + START_SCN_STMT_SQL_SUFFIX;
	        String producerSql = PRODUCER_SCN_STMT_SQL_PREFIX + sources + PRODUCER_SCN_STMT_SQL_SUFFIX;

	        // Get Applier SCN
	        LOG.info("Executing Applier SCN Query :" + applierSql);
	        getScnStmt = conn.createStatement();        
	        getScnStmt.executeQuery(applierSql);
	        rs = getScnStmt.getResultSet();
	        if (rs.next())
	        {
	          startScn = rs.getLong(1);
	        }
	            
	        DBHelper.close(rs,getScnStmt, null);
	        rs = null;
	        getScnStmt = null;

	        // Get ProducerSCN
	        LOG.info("Executing Producer SCN Query :" + producerSql);
	        getScnStmt = conn.createStatement();        
	        getScnStmt.executeQuery(producerSql);
	        rs = getScnStmt.getResultSet();
	        if (rs.next())
	        {
	          producerScn = rs.getLong(1);
	        }
	        
	        if ( producerScn < startScn)
	        {
	        	String msg = "Bootstrap Producer has lower SCN than Applier SCN. This is unexpected !! Producer SCN :" + producerScn + ", Applier SCN :" + startScn;
	        	LOG.fatal(msg);
	        	throw new BootstrapDatabaseTooOldException(msg);
	        }
	        
	        if (producerScn < sinceScn)
	        { // bootstrap producer needs sometime to consumer events in the buffer, wait a bit.
	          LOG.warn("Bootstrap producer has not caught up to all events in its buffer yet to server client properly");
	          Thread.sleep(QUERY_WAIT_TIME_SLICE);
	        }
	      }
	      catch (InterruptedException e)
	      {
	        // keeps on sleeping until timed out
	      }
	      catch (SQLException e)
	      {
	        LOG.warn("SQLException encountered while querying for start scn", e);
	        
	      }
	      finally
	      {
	    	DBHelper.close(rs,getScnStmt, null);  
	      }
	    }
	    
	    // Slow Producer case
	    if ( producerScn < sinceScn)
	    {
	    	String msg = "Bootstrap producer is slower than the client. Client is at SCN :" + sinceScn 
	    	              + ", Producer is at SCN :" + producerScn + ", Applier is at SCN :" + startScn;
	    	LOG.error(msg);
	    	throw new BootstrapDatabaseTooOldException(msg);
	    }
	    
	    LOG.info("StartSCN Request for sources :" + sources + ",Client SCN :" + sinceScn + ",Producer SCN :" + producerScn + ", Applier SCN :" + startScn);
	    return startScn;
	  }
	  
	  
	  public long getSourceTargetScn(int srcId) 
	  		throws SQLException
	  {
	    long scn = 0;
	    ResultSet rs = null;
	    PreparedStatement targetScnStmt = getTargetScnStmt();
	    try
	    {
	      targetScnStmt.setInt(1, srcId);
	      rs = targetScnStmt.executeQuery();

	      while (rs.next ())
	      {
	        scn = rs.getLong(1);
	        LOG.info ( "target scn for source " + srcId + " is " + scn);
	      }
	      rs.close ();
	    }
	    catch(SQLException e)
	    {
	      LOG.error("Error encountered while selecting target scn for bootstrap_producer_state:", e);
	      throw e;
	    }
	    finally
	    {
	    	DBHelper.close(rs,targetScnStmt,null);
	    }
	    return scn;
	  }
	  
	  private PreparedStatement getTargetScnStmt() throws SQLException
	  {
	    Connection conn = null;
	    PreparedStatement stmt = null;

	    try
	    {
	      conn = _dbDao.getBootstrapConn().getDBConn();
	      stmt = conn.prepareStatement("SELECT windowscn FROM bootstrap_producer_state where srcid = ?");
	    }
	    catch (SQLException e)
	    {
	      LOG.error("Error occurred while creating getTargetScnStatement", e);
	      if (null != stmt)
	      {
	        stmt.close();
	        stmt = null;
	      }
	      throw e;
	    }
	    return stmt;
	  }
	  
	  public List<SourceStatusInfo> getSourceIdAndStatusFromName(List<String> sourceList)
			  	throws SQLException,BootstrapDatabaseTooOldException
	  {
		  return _dbDao.getSourceIdAndStatusFromName(sourceList,true);
	  }
	  
	  public SourceStatusInfo getSrcIdStatusFromDB(String source, boolean activeCheck) throws SQLException, BootstrapDatabaseTooOldException
	  {
		  return _dbDao.getSrcIdStatusFromDB(source, activeCheck);
	  }
	  
	  public void shutdown()
	  {

	      if (null != _dbDao)
	      {
	        _dbDao.getBootstrapConn().close();
	        _dbDao = null;
	      }
	  }
}
