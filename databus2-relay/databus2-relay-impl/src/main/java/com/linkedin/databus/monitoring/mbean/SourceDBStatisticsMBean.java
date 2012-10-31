package com.linkedin.databus.monitoring.mbean;

/**
 *
 * @author snagaraj
 * Provides SCN related metrics of  source DB's of Databus2 Relays per logical source ; i.e. views / tables within a database
 */

public interface SourceDBStatisticsMBean
{

    /** Return source Mbean **/
    public String getSourceName();

    /** Return maximum SCN in source database */
    public long getMaxScn();

    /** Reset all statistics to zero. Typically for debugging / testing purposes. */
    public void reset();

    /** Return timestamp in seconds  of last update */
   public long getTimeSinceLastUpdate();

}
