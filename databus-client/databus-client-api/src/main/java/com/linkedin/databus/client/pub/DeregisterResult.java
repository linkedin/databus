package com.linkedin.databus.client.pub;

public interface DeregisterResult 
{
	  
	  /**
	   * Result of deregistration
	   */
	  public boolean isSuccess();
	  
	  /**
	   * Error Message when result code failed.
	   * @return null, if degregistration did not fail
	   *         error message otherwise
	   */
	  public String getErrorMessage();
	  
	  /**
	   * If the resultCode is success, then this API will tell if the connection shutdown happened or not  
	   */
	  public boolean isConnectionShutdown();
	  
	  
	  /**
	   * Exception, if any
	   * @return null, if degregistration did not fail
	   *        exception otherwise
	   */
	  public Exception getException();
}
