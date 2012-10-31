package com.linkedin.databus.bootstrap.monitoring.producer.mbean;

public interface DbusBootstrapProducerStatsMBean 
{
	  /**
	   * Reset object
	   */
	  void reset();

	  /**
	   *  Metric collection enabled or not
	   *   */
	  boolean isEnabled();

	  /** enable/disable metric collection */
	  void setEnabled(boolean enabled);
	  
	  /** number of times bootstrap Producer fell-off the relay */
	  long getNumErrFellOffRelay();
	  
	  /** number of erroneous requests due to sql exception */
	  long getNumErrSqlException();
	  
	  /** Latency for appending per Event Window */
	  long getLatencyPerWindow();
	  
	  /** Number of Data Events per Event Window */
	  long getNumDataEventsPerWindow();
	  
	  /** current scn seen in  bootstrap producer/applier */
	  long getCurrentSCN();

	  /** Current LogId where bootstrap producer/applier is */
	  long getCurrentLogId();
	  
	  /** Current RowId where bootstrap producer/applier is */
	  long getCurrentRowId();	  
	  
	  /** Number of Windows/batches seen by the applier thread  */
	  long getNumWindows();
	  
	  /* register calls */
	  void registerFellOffRelay() ;
	  void registerSQLException();
	  void registerBatch(long latency, long numEvents, long currentSCN, long currentLogId, long currentRowId);
}
