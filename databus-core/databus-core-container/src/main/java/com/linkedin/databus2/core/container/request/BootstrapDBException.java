package com.linkedin.databus2.core.container.request;

public class BootstrapDBException extends Exception 
{
	  private static final long serialVersionUID = 1L;
      
	  public BootstrapDBException()
	  {
	    super();
	  }
	  
	  public BootstrapDBException(String arg0, Throwable arg1) 
	  {
	    super(arg0, arg1);
	  }
	        
	  public BootstrapDBException(String arg0) 
	  {
	    super(arg0);
	  }
	        
	  public BootstrapDBException(Throwable arg0) 
	  {
	    super(arg0);
	  }
}
