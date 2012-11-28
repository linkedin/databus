package com.linkedin.databus.core;

import com.linkedin.databus2.core.DatabusException;

public class DispatcherRetriesExhaustedException
extends DatabusException 
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 829000999221853075L;

	public DispatcherRetriesExhaustedException()
	{
		super();
	}

	public DispatcherRetriesExhaustedException(String message, Throwable cause)
	{
		super(message, cause);
	}

	public DispatcherRetriesExhaustedException(String message)
	{
		super(message);
	}

	public DispatcherRetriesExhaustedException(Throwable cause)
	{
		super(cause);
	}
}
