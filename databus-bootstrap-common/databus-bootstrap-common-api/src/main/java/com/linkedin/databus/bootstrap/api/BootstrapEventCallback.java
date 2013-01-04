/**
 * 
 */
package com.linkedin.databus.bootstrap.api;

import java.sql.ResultSet;

import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;

/**
 * @author lgao
 *
 */
public interface BootstrapEventCallback
{
  // it shall be called for each row fetched from database
  BootstrapEventProcessResult onEvent(ResultSet rs, DbusEventsStatisticsCollector statsCollector) throws BootstrapProcessingException;

  void onCheckpointEvent(Checkpoint ckpt, DbusEventsStatisticsCollector curStatsCollector);
}
