package com.linkedin.databus2.core.container.monitoring.mbean;

import java.util.Collection;
import java.util.List;

import com.linkedin.databus2.core.container.request.RegisterResponseEntry;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus2.core.container.monitoring.mbean.DbusHttpTotalStatsMBean;

/**
 * MBean interface for the collector for HTTP monitoring mbeans. It provides various views over
 * the HTTP stats.
 */
public interface HttpStatisticsCollectorMBean
{

  // ************** GETTERS *********************

  String getName();

  /** Obtains the mbean used to collect statistics about total HTTP calls */
  DbusHttpTotalStatsMBean getTotalStats();

  /** Checks if the stats collector is enabled */
  boolean isEnabled();

  /** Obtains the list of source ids for which there were calls */
  List<Integer> getSources();

  /**
   * Obtains the stats monitoring bean for a given source id
   * @return the stats mbean or null
   * */
  DbusHttpTotalStatsMBean getSourceStats(int srcId);

  /** Obtains the list of pers ids for which there are stats accumulated */
  List<String> getPeers();

  /** Obtains the stats monitoring bean for a given peer id */
  DbusHttpTotalStatsMBean getPeerStats(String peer);

  // ****************** MUTATORS *********************

  /** Resets the statistics. */
  void reset();

  /**
   * Registers a /sources response
   */
  void registerSourcesCall();

  /**
   * Registers a /register response
   * @param  sources                the list of registered source ids
   */
  void registerRegisterCall(List<RegisterResponseEntry> sources);

  /**
   * Registers a /stream request
   * @param  cp              the requested checkpoint in the /stream call
   * @param  sourceIds       the source ids being streamed
   */
  void registerStreamRequest(Checkpoint cp, Collection<Integer> sourceIds);

  /**
   * Registers a /stream response
   */
  void registerStreamResponse(long duration);

  /**
   * Enables/disables the stats collector
   * @param enabled        true to enable, false to disable
   */

  /**
   * Registers an invalid /stream request
   *
   */
  void registerInvalidStreamRequest() ;

  /**
   * Registers an invalid /stream response; specifically an scn not found case
   */
  void registerScnNotFoundStreamResponse();

  /**
   * Registers an invalid /source request (request params)
   */
  void registerInvalidSourceRequest();
  /**
   * Registers an invalid /source request (request params)
   */
  void registerInvalidRegisterCall() ;

  /** Registers mastership status **/
  void registerMastershipStatus(int i);


  void setEnabled(boolean enabled);

}
