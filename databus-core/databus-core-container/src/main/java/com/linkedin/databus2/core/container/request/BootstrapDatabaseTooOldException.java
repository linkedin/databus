package com.linkedin.databus2.core.container.request;

public class BootstrapDatabaseTooOldException extends BootstrapDBException
{
  private static final long serialVersionUID = 1L;
      
  public BootstrapDatabaseTooOldException()
  {
    super();
  }
  
  public BootstrapDatabaseTooOldException(String arg0, Throwable arg1) 
  {
    super(arg0, arg1);
  }
        
  public BootstrapDatabaseTooOldException(String arg0) 
  {
    super(arg0);
  }
        
  public BootstrapDatabaseTooOldException(Throwable arg0) 
  {
    super(arg0);
  }

}
