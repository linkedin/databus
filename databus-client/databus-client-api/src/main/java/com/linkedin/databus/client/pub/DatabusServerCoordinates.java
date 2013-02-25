package com.linkedin.databus.client.pub;
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


import java.net.InetSocketAddress;

public class DatabusServerCoordinates
	implements Comparable<DatabusServerCoordinates>
{
	public enum StateId
	{
		UNKNOWN,
		ERROR,
		OFFLINE,
		ONLINE
	};

	/**
	 * A user-friendly name to identify the relay/bootstrep-server
	 */
	private final String            _name;

	/**
	 * The IP Address and port number
	 */
	private final InetSocketAddress _address;


	/**
	 * State of the relay/bootstrap-server ( as reported by Helix )
	 * States are Online, Offline, Error
	 */
	private StateId                 _state;

	/**
	 * Base constructor
	 *
	 * @param name
	 * @param address
	 * @param state
	 */
	public DatabusServerCoordinates(String name, InetSocketAddress address, String state)
	{
		_name = name;
		_address = address;
		try
		{
		  _state = StateId.valueOf(state);
		} catch (Exception ex ) {
			_state = StateId.UNKNOWN;
		}
	}

    public DatabusServerCoordinates(String name, InetSocketAddress address, StateId state)
    {
      _name = name;
      _address = address;
      _state = state;
    }

	/**
	 * Typically called from DatabusHttpV3ClientImpl
	 *
	 * @param id
	 * @param name
	 * @param address
	 */
	public DatabusServerCoordinates(String name, InetSocketAddress address)
	{
		this(name, address, "OFFLINE");
	}

	/**
	 * Typically called for constructing RelayCoordinates based on Client's external view
	 *
	 * @param address
	 * @param state
	 */
	public DatabusServerCoordinates(InetSocketAddress address, String state)
	{
		this("default", address, state);
	}

	public String getName()
	{
		return _name;
	}

	public InetSocketAddress getAddress()
	{
		return _address;
	}

	public void setState(StateId state)
	{
		_state = state;
	}

	public StateId getState()
	{
		return _state;
	}

	@Override
	public boolean equals(Object o)
	{
		if ( this == o)
			return true;
		if ( o == null)
			return false;
		if (getClass() != o.getClass())
			return false;

		final DatabusServerCoordinates castedObj = (DatabusServerCoordinates) o;
		if ( _address.equals(castedObj.getAddress()) && _state.equals(castedObj.getState()) )
			return true;
		else
			return false;
	}

	@Override
	public int hashCode()
	{
		return (getAddress().hashCode() << 16) + getState().hashCode();
	}

	@Override
	public int compareTo(DatabusServerCoordinates o)
	{
		InetSocketAddress addr1 = getAddress();
		InetSocketAddress addr2 = o.getAddress();

		String addrStr1 = (addr1 != null ? addr1.toString() + getState() : "");
		String addrStr2 = (addr2 != null ? addr2.toString() + getState() : "");

		return addrStr1.compareTo(addrStr2);
	}

	/**
	 * Check if the Server represented by this object is in ONLINE state
	 * @return
	 */
	public boolean isOnlineState()
	{
		return _state == StateId.ONLINE;
	}

	@Override
	public String toString() {
		return "DatabusServerCoordinates [_name=" + _name + ", _address="
				+ _address + ", _state=" + _state + "]";
	}
}
