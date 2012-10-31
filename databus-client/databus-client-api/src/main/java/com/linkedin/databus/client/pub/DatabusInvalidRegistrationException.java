package com.linkedin.databus.client.pub;

public class DatabusInvalidRegistrationException 
    extends RuntimeException 
{
	public DatabusInvalidRegistrationException()
	{
		super();
	}	
	
	public DatabusInvalidRegistrationException(String message)
	{
		super(message);
	}
	
	public DatabusInvalidRegistrationException(String message, Throwable throwable)
	{
		super(message, throwable);
	}
}
