package com.linkedin.databus2.core.mbean;

public interface DatabusReadOnlyStatusMBean
{
  /**** =============== getters ================ ******/

  /**
   * The status of the dbus component: running, paused, suspendedOnError")
   * @return status string
   */
  String getStatus();

  /**
   * The detailed message about the status
   * @return detailed status message string
   */
  String getStatusMessage();

  /**
   * The dbus component name
   * @return component name
   */
  String getComponentName();

  /** A number representation of of the status for graphing/alerts. Larger value means worse
   * */
  int getStatusCode();

  /** The number of successive errors */
  int getRetriesNum();

  /** The number of remaining retries */
  int getRemainingRetriesNum();

  /** The current retry duration */
  long getCurrentRetryLatency();

  /** Time in ms since last first retry */
  long getTotalRetryTime();

  /** The current uptime: milliseconds since last start/resume or 0 if in error state */
  long getUptimeMs();
}
