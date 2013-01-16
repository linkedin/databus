package com.linkedin.databus.core;

/** A base class for all Databus runtime exception */
public class DatabusRuntimeException extends RuntimeException
{
  private static final long serialVersionUID = 1L;

  public DatabusRuntimeException(String message, Throwable cause)
  {
    super(message, cause);
  }

  public DatabusRuntimeException(String message)
  {
    super(message);
  }

  public DatabusRuntimeException(Throwable cause)
  {
    super(cause);
  }

}
