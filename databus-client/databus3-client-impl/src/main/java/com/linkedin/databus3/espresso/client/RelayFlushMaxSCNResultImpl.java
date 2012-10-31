package com.linkedin.databus3.espresso.client;

import com.linkedin.databus.client.pub.RelayFindMaxSCNResult;
import com.linkedin.databus.client.pub.RelayFlushMaxSCNResult;
import com.linkedin.databus.client.pub.SCN;

public class RelayFlushMaxSCNResultImpl 
	implements RelayFlushMaxSCNResult 
{
	private SCN requestedMaxSCN;
	private SCN currentMaxSCN;
	private RelayFindMaxSCNResult fetchMaxSCNResult;
	private SummaryCode resultStatus;
	
	public RelayFlushMaxSCNResultImpl(RelayFindMaxSCNResult fetchMaxSCNResult)
	{
		this.fetchMaxSCNResult = fetchMaxSCNResult;
	}
		
	public RelayFlushMaxSCNResultImpl(SCN requestedMaxSCN, 
									  SCN currentMaxSCN,
									  RelayFindMaxSCNResult fetchMaxSCNResult,
									  SummaryCode resultStatus) 
	{
		super();
		this.requestedMaxSCN = requestedMaxSCN;
		this.currentMaxSCN = currentMaxSCN;
		this.fetchMaxSCNResult = fetchMaxSCNResult;
		this.resultStatus = resultStatus;
	}

	/**
	 * Used in case of returning failure from API calls
	 * @param resultStatus
	 */
	RelayFlushMaxSCNResultImpl(SummaryCode resultStatus)
	{
		super();
		this.resultStatus = resultStatus;
	}

	public void setRequestedMaxSCN(SCN requestedMaxSCN) {
		this.requestedMaxSCN = requestedMaxSCN;
	}

	public void setCurrentMaxSCN(SCN currentMaxSCN) {
		this.currentMaxSCN = currentMaxSCN;
	}

	public void setResultStatus(SummaryCode resultStatus) {
		this.resultStatus = resultStatus;
	}

	@Override
	public SCN getRequestedMaxSCN() {
		
		return requestedMaxSCN;
	}

	@Override
	public SCN getCurrentMaxSCN() {
		
		return currentMaxSCN;
	}

	@Override
	public SummaryCode getResultStatus() {
		
		return resultStatus;
	}

	@Override
	public String toString() {
		return "RelayFlushMaxSCNResultImpl [requestedMaxSCN=" + requestedMaxSCN
				+ ", currentMaxSCN=" + currentMaxSCN + ", resultStatus="
				+ resultStatus + "]";
	}

	@Override
	public RelayFindMaxSCNResult getFetchSCNResult() {
		return fetchMaxSCNResult;
	}
	
	public void getFetchSCNResult(RelayFindMaxSCNResult fetchMaxSCNResult) {
		this.fetchMaxSCNResult = fetchMaxSCNResult;
	}
}
