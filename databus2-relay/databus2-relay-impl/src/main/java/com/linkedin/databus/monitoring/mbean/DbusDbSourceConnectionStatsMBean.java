package com.linkedin.databus.monitoring.mbean;

import com.linkedin.databus.core.monitoring.mbean.DatabusMonitoringMBean;
import com.linkedin.databus.monitoring.events.DbusDbSourceConnectionStatsEvent;

/**
 * Collector/accessor for total inbound traffic statistics.
 * @author cbotev
 *
 */
public interface DbusDbSourceConnectionStatsMBean
extends DatabusMonitoringMBean<DbusDbSourceConnectionStatsEvent>
{

  // ************** GETTERS *********************

  /** Obtains the number of DB connections open */
  long getNumOpenDbConns();

  /** Obtains the number of DB connections closed */
  long getNumClosedDbConns();

  /** Obtains the timestamp of the last DB connection open operation */
  long getTimestampLastDbConnOpenMs();

  /** the timestamp of the last DB connection close operation */
  long getTimestampLastDbConnCloseMs();

  /** Obtains total lifespan of closed DB connections */
  long getTimeClosedDbConnLifeMs();

  /** Obtains total lifespan of currently open DB connections */
  long getTimeOpenDbConnLifeMs();

  /** Obtains the number of updated DB rows received */
  long getNumRowsUpdated();

  // ****************** MUTATORS *********************

  /**
   * Registers the opening of a DB connection
   * @param  timestamp      the timestamp of the operation
   */
  void registerDbConnOpen(long timestamp);

  /**
   * Registers the closing of a DB connection
   * @param  timestamp      the timestamp of the operation
   * @param  lifespanMs      the lifespan of the connection in ms
   */
  void registerDbConnClose(long timestamp, long lifespanMs);

  /**
   * Registers DB rows read
   * @param  num        the number of rows read
   * */
  void registerDbRowsRead(int num);
}
