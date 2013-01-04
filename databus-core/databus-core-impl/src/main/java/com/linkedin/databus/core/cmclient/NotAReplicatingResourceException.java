package com.linkedin.databus.core.cmclient;

public class NotAReplicatingResourceException extends Exception
{
  private static final long serialVersionUID = 1L;
  private String _resourceName;
  
  public NotAReplicatingResourceException(String resourceName)
  {
    super();
    _resourceName = resourceName;
  }
  
  public String getResourceName()
  {
	  return _resourceName;
  }

}
