package com.linkedin.databus2.core.container.monitoring.mbean;

import com.linkedin.databus.core.monitoring.mbean.DatabusMonitoringMBean;
import com.linkedin.databus2.core.container.monitoring.events.DbusHttpTotalStatsEvent;

/**
 * Collector/accessor for total outbound traffic statistics.
 * @author cbotev
 *
 */
public interface DbusHttpTotalStatsMBean extends DatabusMonitoringMBean<DbusHttpTotalStatsEvent>
{

  // ************** GETTERS *********************

  /** Obtains the number of /sources calls */
  int getNumSourcesCalls();

  /** Obtains number of /register calls */
  int getNumRegisterCalls();

  /** Obtains the number of /stream calls */
  long getNumStreamCalls();

  /** Obtains the number of distinct peers that have ever connected */
  int getNumPeers();

  /** Obtains average time to run stream calls */
  double getLatencyStreamCalls();

  /** The stats dimension, e.g. total, peer name, source name */
  String getDimension();

  /** Obtains the minimum requested window scn in a /stream call */
  long getMinStreamWinScn();

  /** Obtains the maximum requested window scn in a /stream call */
  long getMaxStreamWinScn();

  /** get number of scn not found errors */
  long getNumScnNotFoundStream() ;

  /** number of /stream response errors */
  long getNumErrStream();

  /** number of /stream request errors */
  long getNumErrStreamReq();

  /** number of /register response errors */
  long getNumErrRegister();

  /** number of /register request errors */
  long getNumErrRegisterReq();

  /** number of /sources response errors */
  long getNumErrSources();

  /** number of /sources request errors */
  long getNumErrSourcesReq();

  /** fraction of http errors over all http calls*/
  double getHttpErrorRate();

  int getMastershipStatus();

  // ****************** MUTATORS *********************

  /** Resets the statistics. */
  @Override
  void reset();
}
