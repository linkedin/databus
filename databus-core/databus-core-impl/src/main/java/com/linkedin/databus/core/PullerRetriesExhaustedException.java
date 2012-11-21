package com.linkedin.databus.core;

import com.linkedin.databus2.core.DatabusException;

public class PullerRetriesExhaustedException
extends DatabusException 
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 829000999221853075L;

	public PullerRetriesExhaustedException()
	{
		super();
	}

	public PullerRetriesExhaustedException(String message, Throwable cause)
	{
		super(message, cause);
	}

	public PullerRetriesExhaustedException(String message)
	{
		super(message);
	}

	public PullerRetriesExhaustedException(Throwable cause)
	{
		super(cause);
	}
}
