package com.linkedin.databus2.core.container.monitoring.mbean;

import java.util.List;



/**
 * MBean interface for the Container for all monitoring mbeans
 */
public interface ContainerStatisticsCollectorMBean
{

  // ************** GETTERS *********************

  /**
   * Obtains the mbean used to collect statistics about the total traffic originating from
   * remote clients.
   */
  ContainerTrafficTotalStatsMBean getOutboundTrafficTotalStats();

  /**
   * Obtains the mbean used to collect statistics about the total traffic originating from the
   * container.
   */
  ContainerTrafficTotalStatsMBean getInboundTrafficTotalStats();

  /** Checks if the stats collector is enabled */
  boolean isEnabled();

  /** Obtains the list of client ids for which there are stats accumulated */
  List<String> getOutboundClients();

  /** Obtains the stats monitoring bean for a given client id */
  ContainerTrafficTotalStats getOutboundClientStats(String client);

  // ****************** MUTATORS *********************

  /** Resets the statistics. */
  void reset();

  /**
   * Enables/disables the stats collector
   * @param enabled        true to enable, false to disable
   */
  void setEnabled(boolean enabled);
}
