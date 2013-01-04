package com.linkedin.databus2.schemas;

import com.linkedin.databus2.core.DatabusException;

public class NoSuchSchemaException extends DatabusException
{
	private static final long serialVersionUID = 1L;

	public NoSuchSchemaException()
	{
		super();
	}

	public NoSuchSchemaException(String message, Throwable cause)
	{
		super(message, cause);
	}

	public NoSuchSchemaException(String message)
	{
		super(message);
	}

	public NoSuchSchemaException(Throwable cause)
	{
		super(cause);
	}
}
