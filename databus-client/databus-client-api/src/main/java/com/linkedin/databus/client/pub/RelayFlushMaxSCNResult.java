package com.linkedin.databus.client.pub;

public interface RelayFlushMaxSCNResult 
{
	/**
	 * Summary Result Code
	 */
	public enum SummaryCode
	{		
		MAXSCN_REACHED, /** Flush Succeeded */
		TIMED_OUT,  /** Flush timed out waiting to reach max SCN */
		EMPTY_EXTERNAL_VIEW, /** External view empty either for this partition or for the whole cluster  */
		NO_ONLINE_RELAYS_IN_VIEW, /** No online relays hosting this partition */
		FAIL   /** Flush failed for other reasons */
	}

	/**
	 * 
	 * Return the Requested SCN for flush to achieve and completed
	 */
	public SCN getRequestedMaxSCN();

	/**
	 * Get the current max SCN that the client is seen as part of flush call 
	 * 
	 */
	public SCN getCurrentMaxSCN(); 
	
	/**
	 * Return the summary code for the flush operation
	 */
	public SummaryCode getResultStatus();
	
	/**
	 * Return the result for fetchSCN operation associated with this flush
	 * @return
	 */
	public RelayFindMaxSCNResult getFetchSCNResult();
}
