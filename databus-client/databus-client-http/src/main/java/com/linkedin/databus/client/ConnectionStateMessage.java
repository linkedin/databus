package com.linkedin.databus.client;
/*
 *
 * Copyright 2013 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/


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
