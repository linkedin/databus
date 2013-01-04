package com.linkedin.databus.bootstrap.common;

import com.linkedin.databus.bootstrap.monitoring.server.mbean.DbusBootstrapHttpStatsMBean;
import com.linkedin.databus.core.Checkpoint;

public interface BootstrapHttpStatsCollectorMBean
{

  void reset();

  boolean isEnabled();

  void setEnabled(boolean enabled);

  /** Obtains the mbean used to collect statistics about total bootstrap HTTP calls */
  DbusBootstrapHttpStatsMBean getTotalStats();

  /** Obtains the stats monitoring bean for a given peer id */
  DbusBootstrapHttpStatsMBean getPeerStats(String peer);

  /**MUTATORS*/

  /** set metrics pertaining to bootstrap call */
  void registerBootStrapReq(Checkpoint cp,long latency,long size) ;
  void registerStartSCNReq(long latency);
  void registerTargetSCNReq(long latency);

  /** set metrics pertaining to err bootstrap req */
  void registerErrBootstrap();
  void registerErrStartSCN();
  void registerErrTargetSCN();
  void registerErrSqlException();
  void registerErrDatabaseTooOld();

}
