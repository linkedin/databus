package com.linkedin.databus.monitoring.mbean;
/**
 *
 * @author snagaraj
 *Provides SCN related metrics of  source DB's of Databus2 Relays
 */

public interface DBStatisticsMBean
{

  /** Return source Mbean **/
  public String getDBSourceName();

  /** Return maximum SCN in source database */
  public long getMaxDBScn();

  /** Return per source statistics */
  public SourceDBStatisticsMBean getPerSourceStatistics(String name);

  /** Reset all statistics to zero. Typically for debugging / testing purposes. */
  public void reset();

  /** Get timestamp in seconds of last update */
  public long getTimeSinceLastUpdate();

}
