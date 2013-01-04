/**
 *
 */
package com.linkedin.databus.bootstrap.producer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.linkedin.databus.bootstrap.api.BootstrapProducerStatus;
import com.linkedin.databus.bootstrap.common.BootstrapConn;
import com.linkedin.databus.bootstrap.common.BootstrapDBMetaDataDAO;
import com.linkedin.databus.bootstrap.common.BootstrapProducerStatsCollector;
import com.linkedin.databus.bootstrap.common.BootstrapProducerThreadBase;
import com.linkedin.databus.bootstrap.common.BootstrapReadOnlyConfig;
import com.linkedin.databus.bootstrap.monitoring.producer.mbean.DbusBootstrapProducerStatsMBean;
import com.linkedin.databus.core.util.RateMonitor;
import com.linkedin.databus.core.util.RngUtils;
import com.linkedin.databus2.core.BackoffTimer;
import com.linkedin.databus2.core.container.request.BootstrapDatabaseTooOldException;
import com.linkedin.databus2.util.DBHelper;

/**
 * @author ksurlake
 *
 */
public class BootstrapApplierThread 
	extends BootstrapProducerThreadBase
{

  public static final String               MODULE =
                                                      BootstrapApplierThread.class.getName();
  public static final Logger               LOG    = Logger.getLogger(MODULE);

  private static final int MAX_EVENT_WAIT_TIME = 1000;
  private static final int INITIAL_EVENT_WAIT_TIME = 5;
  private static final int DEFAULT_LOG_SAMPLING_PERCENTAGE = 2;

  private static Random _sSampler = new Random();

  private BootstrapDBMetaDataDAO                   _bootstrapDao;
  private List<String>                     _sources;
  private PreparedStatement                _tabScnStmt;
  private PreparedStatement                _getScnStmt;
  private BootstrapReadOnlyConfig          _config;
  private HashMap<String, sourcePositions> _sourcemap;

  //Log Specific
  private Map<Integer,String> _lastLogLines = null;
  private Map<Integer,Integer> _lastLogLineRepeats = null;
  private static final String APPLIER_STATE_LINE_FORMAT = "Applier state : %d %d %d %d";
  private static final int MAX_SKIPPED_LOG_LINES = 1000;

  /* Stats Specific */
  private BootstrapProducerStatsCollector _statsCollector = null;
  private RateMonitor      _srcRm  = new RateMonitor("ProducerSourceRateMonitor");
  private RateMonitor      _totalRm  = new RateMonitor("ProducerTotalRateMonitor");
  private int             _currentLogId;
  private int             _currentRowId;
  private BackoffTimer    _retryTimer;

  /**
   * @param config
   *
   */
  public BootstrapApplierThread(String name,
                                List<String> sources,
                                BootstrapReadOnlyConfig config)
  {
	  this(name,sources,config,null);
  }

  public BootstrapApplierThread(String name,
          List<String> sources,
          BootstrapReadOnlyConfig config,
          BootstrapProducerStatsCollector statsCollector)
  {
    super(name);
    _sources = sources;
    _bootstrapDao = null;
    _config = config;
    _retryTimer = new BackoffTimer("BootstrapApplier", config.getRetryConfig());
    _sourcemap = new HashMap<String, sourcePositions>();
    _lastLogLines = new HashMap<Integer,String>();
    _lastLogLineRepeats = new HashMap<Integer,Integer>();
    _statsCollector = statsCollector;
  }




  @Override
  public synchronized void start()
  {
    super.start();
  }

  @Override
  public void run()
  {
    boolean running = true;

    for (String source : _sources)
    {
      try
      {
        sourcePositions sourcePos = new sourcePositions(source);
        sourcePos.init();
        _sourcemap.put(source, sourcePos);
        if ( null != _statsCollector)
        {
        	 DbusBootstrapProducerStatsMBean stats = _statsCollector.getSourceStats(source);
        	 stats.registerBatch(0, 0, -1, sourcePos.getApplyId(), sourcePos.getLogPos());
        }
      }
      catch (Exception e)
      {
    	 if ( null != _statsCollector)
    		 _statsCollector.getTotalStats().registerSQLException();
        LOG.error("Error occurred in initializing source position", e);
        throw new RuntimeException("Error occurred while creating apply statement: ", e);

      }
    }

    int sleepTime = INITIAL_EVENT_WAIT_TIME;
    int totalRowsApplied = 0;
    while (running && !isShutdownRequested())
    {
      try
      {
  		if (isPauseRequested())
  		{
  			LOG.info("Pause requested for applier. Pausing !!");
  			signalPause();
  			LOG.info("Pausing. Waiting for resume command");
  			awaitUnPauseRequest();
  			LOG.info("Resume requested for applier. Resuming !!");
  			signalResumed();
  			LOG.info("Applier resumed !!");
  		}

    	_totalRm.start();
        Connection conn = getConnection();
        for (String source : _sources)
        {
          try
          {
            totalRowsApplied += applyLog(source);
          }
          catch (Exception e)
          {
         	 if ( null != _statsCollector)
         	 {
         		 _statsCollector.getTotalStats().registerSQLException();
         		_statsCollector.getSourceStats(source).registerSQLException();
         	 }

            LOG.error("apply error:", e);
            throw e;
          }
          conn.commit();
          _totalRm.stop();
          if (null != _statsCollector)
        	  _statsCollector.getTotalStats().registerBatch(_totalRm.getDuration()/1000000L, totalRowsApplied, -1, -1, -1);
        }

        if (0 == totalRowsApplied)
        {
          // sleep for sometime when no events found
          Thread.sleep(sleepTime);

          // increase sleep time for next round if no events are found
          sleepTime = Math.min(sleepTime * 10, MAX_EVENT_WAIT_TIME);
        }
        else
        { // reset to initial sleep time
          sleepTime = INITIAL_EVENT_WAIT_TIME;
          totalRowsApplied = 0;
        }
      }
      catch (Exception e)
      {
        LOG.error("Error occured in bootstrap applier", e);
    	 if ( null != _statsCollector)
     	 {
     		 _statsCollector.getTotalStats().registerSQLException();
     	 }

         if ( e instanceof SQLException)
         {
            if ( ! reset(true) )
            {
            	LOG.fatal("Unable to reset Bootstrap DB connections. Stopping Applier Thread !!", e);
                running = false;
            }
         }
      }
    }

    reset(false);

	doShutdownNotify();
  }

  private void closeApplyStatements() throws SQLException
  {
	 for (sourcePositions srcPos : _sourcemap.values())
	    srcPos.close();
  }


  private int applyLog(String source)
  	throws BootstrapDatabaseTooOldException, SQLException, InterruptedException
  {
    PreparedStatement stmt = null;

    sourcePositions pos = _sourcemap.get(source);
    applyBatch batch = pos.getNextApplyBatch();
    int rowsToApply = batch.getTorid() - batch.getFromrid();

    try
    {
      _srcRm.start();
      if (rowsToApply > 0)
      {
        // Apply log to the table to move it up to logScn
        stmt = pos.getApplyStmt();
        stmt.setInt(1, batch.getFromrid());
        stmt.setInt(2, batch.getTorid());
        stmt.executeUpdate();

        boolean log = (RngUtils.randomPositiveInt(_sSampler) % 100) < DEFAULT_LOG_SAMPLING_PERCENTAGE;
        if (log) LOG.info("Applied Log " + batch + " for " + source);
      }

      // we need to save state regardless if any rows are returned because
      // we could be switching logs.
      pos.save();
    }
    catch (SQLException e)
    {
      LOG.error("Error occured during apply log", e);
      throw e;
    } finally {
    	_srcRm.stop();
        if ( null != _statsCollector)
        {
       	 	DbusBootstrapProducerStatsMBean stats = _statsCollector.getSourceStats(source);
       	 	stats.registerBatch(_srcRm.getDuration()/1000000L, rowsToApply, pos.getApplyWindowSCN(), pos.getApplyId(), pos.getLogPos());
        }
    }
    return rowsToApply;
  }

  private long getWindowScnforSource(int srcid, int applyLogId, int tabRid) throws SQLException
  {
    ResultSet rs = null;
    long windowScn = 0;
    PreparedStatement stmt = null;

    try
    {
      if (0 == tabRid)
      { // get the maxscn of the prior table
        stmt = getMaxWindowScnStatement();
        stmt.setInt(1, srcid);
        stmt.setInt(2, Math.max(0, applyLogId-1));
      }
      else
      {
        stmt= getWindowScnStatement(applyLogId, srcid);
        stmt.setInt(1, tabRid);
      }

      rs = stmt.executeQuery();
      if (rs.next())
      {
        windowScn = rs.getLong(1);
      }
    }
    catch (SQLException e)
    {
      LOG.error("Error ocurred during getWindowScnforSource", e);
      throw e;
    }
    finally
    {
      if (null != rs)
      {
        rs.close();
        rs = null;
      }
      if (null != stmt)
      {
        stmt.close();
        stmt = null;
      }
    }
    return windowScn;
  }

  private void setTabPosition(int srcid, int logid, int tabRid, long windowScn) throws SQLException
  {
    PreparedStatement stmt = getTabPositionUpdateStmt();

    stmt.setInt(1, logid);
    stmt.setInt(2, tabRid);
    stmt.setLong(3, windowScn);
    stmt.setInt(4, srcid);

    stmt.executeUpdate();


    StringBuilder logLineBuilder = new StringBuilder(1024);
    Formatter logFormatter = new Formatter(logLineBuilder);
    logFormatter.format(APPLIER_STATE_LINE_FORMAT,
    					srcid,
    					logid,
    					tabRid,
    					windowScn);

    log(srcid,logFormatter);
  }

  @SuppressWarnings("unused")
  private PreparedStatement getMaxWindowScnStatement() throws SQLException
  {
    Connection conn = null;
    PreparedStatement windowScnStmt = null;
    try
    {
      conn = getConnection();
      StringBuilder sql = new StringBuilder();

      sql.append("select maxwindowscn from bootstrap_loginfo where srcid = ? and logid = ?");
      windowScnStmt = conn.prepareStatement(sql.toString());
    }
    catch (SQLException e)
    {
      DBHelper.close(windowScnStmt);
      LOG.error("error occurred during getWindowScnStatement", e);
      throw e;
    }

    return windowScnStmt;
  }

  @SuppressWarnings("unused")
  private PreparedStatement getWindowScnStatement(int applyLogId, int srcid) throws SQLException
  {
    Connection conn = null;
    PreparedStatement windowScnStmt = null;
    try
    {
      conn = getConnection();
      StringBuilder sql = new StringBuilder();

      sql.append("select windowscn from ");
      sql.append(getLogTableName(applyLogId, srcid));
      sql.append(" where id = ?");
      windowScnStmt = conn.prepareStatement(sql.toString());
    }
    catch (SQLException e)
    {
      DBHelper.close(windowScnStmt);
      LOG.error("error occurred during getWindowScnStatement", e);
      throw e;
    }

    return windowScnStmt;
  }

  private PreparedStatement getTabPositionUpdateStmt() throws SQLException
  {
    if (_tabScnStmt != null)
    {
      return _tabScnStmt;
    }

    Connection conn = null;

    try
    {
      conn = getConnection();
      StringBuilder sql = new StringBuilder();

      sql.append("update bootstrap_applier_state set logid = ?, rid = ? , windowscn = ?  where srcid = ?");
      _tabScnStmt = conn.prepareStatement(sql.toString());
    }
    catch (SQLException e)
    {
      LOG.error("Error ocurred in getTabPositionUpdateStmt", e);
      _tabScnStmt.close();
      conn.close();
      return null;
    }

    return _tabScnStmt;
  }

  private PreparedStatement getPositionsStmt() throws SQLException
  {
    if (_getScnStmt != null)
    {
      return _getScnStmt;
    }

    Connection conn = null;

    try
    {
      conn = getConnection();
      StringBuilder sql = new StringBuilder();

      sql.append("select p.logid, p.rid, a.logid, a.rid, l.maxrid, l.maxwindowscn ");
      sql.append("from bootstrap_sources s, bootstrap_producer_state p, bootstrap_applier_state a, bootstrap_loginfo l ");
      sql.append("where p.srcid = s.id and a.srcid = s.id and l.srcid = s.id and l.logid = a.logid and s.src = ?");

      _getScnStmt = conn.prepareStatement(sql.toString());
    }
    catch (Exception e)
    {
      LOG.error("Error occured in getPositionsstatement", e);
      _getScnStmt.close();
      conn.close();
      return null;
    }

    return _getScnStmt;
  }

  /*
   * Reset the Bootstrap Connection and in memory state of Applier
   */
  private boolean reset(boolean recreate)
  {
	  boolean success = false;

	  _retryTimer.reset();
	  while ( ! success )
	  {
		  try
		  {
			  _bootstrapDao.getBootstrapConn().close();

			  DBHelper.close(_getScnStmt);
			  _getScnStmt = null;

			  DBHelper.close(_tabScnStmt);
			  _tabScnStmt = null;

			  closeApplyStatements();

      		  //Close the Source Positions
      		  Set<Entry<String, sourcePositions>> entries = _sourcemap.entrySet();

      		  for(Entry<String, sourcePositions> entry : entries)
      		  {
      			 entry.getValue().close();
      		  }

			  if ( recreate)
			  {
				  getConnection();
				  _bootstrapDao.getBootstrapConn().executeDummyBootstrapDBQuery();

	      		  //Init the SourcePositions from DB
	      		  for(Entry<String, sourcePositions> entry : entries)
	      		  {
	      			 entry.getValue().init();
	      		  }
			  }

			  success = true;
		  } catch (SQLException sqlEx) {
			  LOG.error("Unable to reset DBConnections in Applier", sqlEx);
			  success = false;

			  if ( null != _statsCollector)
	          {
	        	  _statsCollector.getTotalStats().registerSQLException();
	          }

			  if ( _retryTimer.getRemainingRetriesNum() <= 0 )
			  {
				  LOG.fatal("Applier Thread reached max retries trying to reset the MySQL Connections. Stopping !!");
				  break;
			  }

			  _retryTimer.backoffAndSleep();
		  }
	  }
	  return success;
  }

  private Connection getConnection()
     throws SQLException
  {
	Connection conn = null;
	BootstrapConn bsConn = null;
    if (_bootstrapDao == null)
    {
      bsConn = new BootstrapConn();	
      try
      {
    	  bsConn.initBootstrapConn(false,
                                         java.sql.Connection.TRANSACTION_READ_COMMITTED,
                                         _config.getBootstrapDBUsername(),
                                         _config.getBootstrapDBPassword(),
                                         _config.getBootstrapDBHostname(),
                                         _config.getBootstrapDBName());
        _bootstrapDao = new BootstrapDBMetaDataDAO(bsConn,
        		_config.getBootstrapDBHostname(),
				_config.getBootstrapDBUsername(),
				_config.getBootstrapDBPassword(),
				_config.getBootstrapDBName(),
				false);
      } catch (SQLException e) {
    	LOG.fatal("Unable to get Bootstrap DB Connection", e);
        throw e;
      } catch (Exception ex) {
      	LOG.fatal("Unable to get Bootstrap DB Connection", ex);
      	return null;
      }
    }

    try
    {
    	conn = _bootstrapDao.getBootstrapConn().getDBConn();
    } catch (SQLException sqlEx) {
    	LOG.fatal("NOT able to get Bootstrap DB Connection", sqlEx);
    	throw sqlEx;
    }

    return conn;
  }

  private String getLogTableName(int applyLogId, int srcid) throws SQLException
  {
    return getBootstrapConn().getLogTableName(applyLogId, srcid);
  }

  private String getSrcTableName(int srcid) throws SQLException, BootstrapDatabaseTooOldException
  {
    return getBootstrapConn().getSrcTableName(srcid);
  }

  private BootstrapConn getBootstrapConn()
  {
    return _bootstrapDao.getBootstrapConn();
  }

  static class applyBatch
  {
    private int _logid;
    private int _fromrid;
    private int _torid;

    applyBatch(int logid, int fromrid, int torid)
    {
      _logid = logid;
      _fromrid = fromrid;
      _torid = torid;
    }

    public int getLogId()
    {
      return _logid;
    }

    public int getFromrid()
    {
      return _fromrid;
    }

    public int getTorid()
    {
      return _torid;
    }

    @Override
    public String toString()
    {
      return "LogId: " + _logid + " From: " + _fromrid + "  To: " + _torid;
    }
  }

  class sourcePositions
  {
    private int    _srcid;
    private int    _tabrid;
    private int    _logrid;
    private int    _applylogid;
    private int    _producelogid;
    private int    _logmaxrid;
    private long   _logwindowscn;
    private PreparedStatement _applyStmt;
    private long   _lastlogmaxscn;

    private String _source;

    sourcePositions(String source)
    	throws Exception
    {
      _source = source;
      init();
    }

    public void init() throws SQLException
    {
      try
      {
    	getConnection();

        BootstrapDBMetaDataDAO.SourceStatusInfo srcIdStatus = _bootstrapDao.getSrcIdStatusFromDB(_source, false);
        _srcid = srcIdStatus.getSrcId();

        if (_config.isBootstrapDBStateCheck())
        {
	        // TO allow test framework to listen to relay directly,DBStateCheck flag is used
        	if ( ! BootstrapProducerStatus.isReadyForConsumption(srcIdStatus.getStatus()))
        		throw new BootstrapDatabaseTooOldException("Bootstrap DB not ready to read events from relay, Status :" + srcIdStatus);
        }

        refresh();
        _applyStmt = createApplyStatement();
      }
      catch (BootstrapDatabaseTooOldException e)
      {
        throw new RuntimeException(e);
      }
    }

    public void close()
    {
    	DBHelper.close(_applyStmt);
    	_applyStmt = null;
    }

    void refresh() throws SQLException
    {
      ResultSet rs = null;
      try
      {
        PreparedStatement stmt = getPositionsStmt();
        stmt.setString(1, _source);
        rs = stmt.executeQuery();
        if (rs.next())
        {
          _producelogid = rs.getInt(1);
          _logrid = rs.getInt(2);
          _applylogid = rs.getInt(3);
          _tabrid = rs.getInt(4);
          _logmaxrid = rs.getInt(5);
          long currentLogMaxScn = rs.getLong(6);

          if (currentLogMaxScn > 0)
          {
            if (_lastlogmaxscn > currentLogMaxScn)
            {
              throw new RuntimeException("_lastlogmaxscn=" + _lastlogmaxscn
                                         + " currentLogMaxScn=" + currentLogMaxScn
                                         + " applylogid=" + _applylogid
                                         + " producerlogid=" + _producelogid
                                         + " tabrid=" + _tabrid
                                         + " logrid=" + _logrid
                                         + " logmaxrid=" + _logmaxrid);
            }
            _lastlogmaxscn = currentLogMaxScn;
          }
        }
        rs.close();
      }
      catch (SQLException e)
      {
        LOG.error("Error occured during refresh of sourcePositions", e);
        if (null != rs)
        {
          rs.close();
          rs = null;
        }
        throw e;
      }
    }

    @Override
    public String toString()
    {
      return "SrcId: " + _srcid + " ProducerLogId: " + _producelogid + " Logrid: "
          + _logrid + " ApplyLogId: " + _applylogid + " Tabrid: " + _tabrid
          + " LogMaxrid: " + _logmaxrid;
    }

    void save() throws SQLException
    {
      LOG.debug("Saving state " + this);

      // if tabrid is 0, we need to get max window scn from the last log table;
      // otherwise, we get the window scn of the row corresponding to _tabrid;
      // except for the initial case where nothing is inserted, (applylogid - 1) will
      // be -1 and 0 shall be used in this case.
      _logwindowscn = getWindowScnforSource(_srcid, _applylogid, _tabrid);
      setTabPosition(_srcid, _applylogid, _tabrid, _logwindowscn);
    }

    public int getTabPos()
    {
      return _tabrid;
    }

    public int getLogPos()
    {
      return _logrid;
    }

    public int getSrcId()
    {
      return _srcid;
    }

    public int getApplyId()
    {
      return _applylogid;
    }

    public long getApplyWindowSCN()
    {
    	return _logwindowscn;
    }

    public PreparedStatement getApplyStmt()
    	throws BootstrapDatabaseTooOldException, SQLException
    {
		if ( null != _applyStmt)
			return _applyStmt;
		_applyStmt = createApplyStatement();
		return _applyStmt;
	}

	public applyBatch getNextApplyBatch() throws SQLException
    {
      int _torid = _tabrid;

      // If we are applying the same log file that is currently being produced, read upto
      // next 1000 rows
      if (_applylogid == _producelogid)
      {
        // If we have caught up for this source, refresh the state
        if (_tabrid == _logrid)
        {
          refresh();
        }
        else
        {
          _torid = Math.min(_logrid, _tabrid + 1000);
        }
      }
      else
      {
        if (_logmaxrid == 0)
        {
          refresh();
        }

        // If we have finished reading this log, we can move to the next
        if (_tabrid == _logmaxrid)
        {
          _applylogid++;
          _torid = 0;
          _tabrid = 0;
          _logmaxrid = 0;
          // remove the apply statement so a new statement can be created to
          // moves rows from new log table to tab table.
          DBHelper.close(_applyStmt);
          _applyStmt = null;
        }
        else
        {
          _torid = Math.min(_logmaxrid, _tabrid + 1000);
        }
      }

      applyBatch nextBatch = new applyBatch(_applylogid, _tabrid, _torid);
      _tabrid = _torid;
      return nextBatch;
    }

	private PreparedStatement createApplyStatement( )
	    throws SQLException, BootstrapDatabaseTooOldException
	{
		 Connection conn = getConnection();
	    PreparedStatement applyStmt = null;
	    StringBuilder sql = new StringBuilder();
	    sql.append("insert into ");
	    sql.append(getSrcTableName(_srcid));
	    sql.append(" (scn, srckey, val) ");
	    sql.append("select windowscn, srckey, val from ");
	    sql.append(getLogTableName(_applylogid,_srcid) + " B ");
	    sql.append(" where B.id > ? and B.id <= ?");
	    sql.append(" on duplicate key update scn = B.windowscn, srckey=B.srckey, val=B.val");
	    applyStmt = conn.prepareStatement(sql.toString());

	    LOG.info("Created apply statement: " + sql.toString());

	    return applyStmt;
	}
  }

  private void log(int srcid, Formatter logLine)
  {
    logLine.flush();
    String newLogLine = logLine.toString();

    boolean skipLog = true;
    Integer repeat = _lastLogLineRepeats.get(srcid);
    int lastLogLineRepeat = (repeat == null) ? 0 : repeat;
    int saveLastLogLineRepeat = lastLogLineRepeat;

    String lastLogLine = _lastLogLines.get(srcid);

    if (newLogLine.equals(lastLogLine) && lastLogLineRepeat < MAX_SKIPPED_LOG_LINES)
    {
      ++lastLogLineRepeat;
    }
    else
    {
      skipLog = false;
      lastLogLine = newLogLine;
      lastLogLineRepeat = 0;
    }

    _lastLogLineRepeats.put(srcid, lastLogLineRepeat);

    if (!skipLog)
    {
        _lastLogLines.put(srcid, lastLogLine);
    	LOG.info("last line repeated: " + saveLastLogLineRepeat);
    	LOG.info(newLogLine);
    }
  }
}
