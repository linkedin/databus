/*
 * $Id: DatabusException.java 168967 2011-02-25 21:56:00Z cbotev $
 */
package com.linkedin.databus2.core;

/**
 * Generic Databus exception
 */
public class DatabusException
    extends Exception
{
  private static final long serialVersionUID = 1L;

  public DatabusException()
  {
    super();
  }

  public DatabusException(String message, Throwable cause)
  {
    super(message, cause);
  }

  public DatabusException(String message)
  {
    super(message);
  }

  public DatabusException(Throwable cause)
  {
    super(cause);
  }
}
