package com.linkedin.databus.client.pub;

public interface StartResult {
	  /**
	   * Result of start
	   */
	  public boolean getSuccess();
	  
	  /**
	   * Error Message when result code failed.
	   * @return null, if start did not fail
	   *         error message otherwise
	   */
	  public String getErrorMessage();
	  
	  /**
	   * Returns an exception in case of an error
	   * If there is no exception ( but there is an error ), can return null
	   * @return
	   */
	  public Exception getException();	  
	  
}
