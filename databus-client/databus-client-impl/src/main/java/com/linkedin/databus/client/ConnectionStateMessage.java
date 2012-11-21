package com.linkedin.databus.client;

import com.linkedin.databus.client.ConnectionState.StateId;

public class ConnectionStateMessage 
{
	private final ConnectionState.StateId stateId;
	private final ConnectionState connState;
		
	public ConnectionStateMessage(StateId stateId, ConnectionState connState)
	{
		super();
		this.stateId = stateId;
		this.connState = connState;
	}
	
	public ConnectionState.StateId getStateId() {
		return stateId;
	}
	
	public ConnectionState getConnState() {
		return connState;
	}

	@Override
	public String toString() {
		return "" + stateId;
	}	
}
