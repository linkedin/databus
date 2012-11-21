package com.linkedin.databus.client.pub;

import java.util.Map;
import java.util.Set;

import com.linkedin.databus.core.BufferInfoResponse;

/**
 * 
 * Result Container Interface for fetchMaxSCN 
 *
 */
public interface RelayFindMaxSCNResult
{
  /**
   * Per-Relay Result Code	
   */
  public enum ResultCode 
  { 
	  SUCCESS,   /** Successfully queried the maxSCN from the Relay */
	  CANNOT_CONNECT, /** Unable to connect to Relay */
	  HTTP_ERROR,  /** MaxSCN query returned error */
	  TIMEOUT,  /** Query Timed out */
  }; 
  
  /**
   * Summary Result Code
   */
  public enum SummaryCode
  {
    SUCCESS,  /** All Relays which host the partition have been successfully queried for some MaxSCN */
    PARTIAL_SUCCESS, /** atleast one relay which hosts the partition have been successfully queried for some MaxSCN */
    EMPTY_EXTERNAL_VIEW, /** External view empty either for this partition or for the whole cluster  */
    NO_ONLINE_RELAYS_IN_VIEW, /** No online relays hosting this partition */
    FAIL  /** The fetch Max SCN failed to query any of the relays hosting the partition */
  }

  /**
   * Contains SCN for each Relay that hosts the partition requested by the registration object 
   */
  public Map<DatabusServerCoordinates, BufferInfoResponse> getResultSCNMap();
  
  /**
   * Returns map contains resultCode for fetching each relay(that hosts the partition requested by the registration object )'s max scn. 
   * 
   */
  public Map<DatabusServerCoordinates, ResultCode> getRequestResultMap();
  
  /**
   * The maxScn which is the maximum available among all relays hosting the partition requested by the registration objects.
   */
  public SCN getMaxSCN();
  
  /**
   * The minScn on the relay which has the maxScn. Note that this is not the minimum of all SCNs over all the relays
   */
  public SCN getMinSCN();

  /**
   * 
   *  Set of relays which the client can point to fetch the max SCN (= getMaxSCN())
   * @return
   */
  public Set<DatabusServerCoordinates> getMaxScnRelays();
  
  /**
   * Summary Result Code for the fetchMaxScn call.
   * @return
   */
  public SummaryCode getResultSummary();
  
  
}
