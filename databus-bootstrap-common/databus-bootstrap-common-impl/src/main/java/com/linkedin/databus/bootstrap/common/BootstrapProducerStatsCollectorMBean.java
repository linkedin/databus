package com.linkedin.databus.bootstrap.common;

import com.linkedin.databus.bootstrap.monitoring.producer.mbean.DbusBootstrapProducerStatsMBean;


public interface BootstrapProducerStatsCollectorMBean 
{
	  void reset();

	  boolean isEnabled();

	  void setEnabled(boolean enabled);

	  /** Obtains the mbean used to collect statistics about total bootstrap HTTP calls */
	  DbusBootstrapProducerStatsMBean getTotalStats();

	  /** Obtains the stats monitoring bean for a given peer id */
	  DbusBootstrapProducerStatsMBean getSourceStats(String peer);


	  /** set metrics pertaining to bootstrap producer/applier */
	  void registerFellOffRelay() ;
	  void registerSQLException();
	  void registerBatch(String source, long latency, long numEvents, long currentSCN, long currentLogId, long currentRowId);
	  void registerEndWindow(long latency, long numEvents, long currentSCN);
}
