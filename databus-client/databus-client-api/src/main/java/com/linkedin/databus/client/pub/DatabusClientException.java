package com.linkedin.databus.client.pub;

/**
 * An base class for all Databus client library specific exceptions.
 *
 * @author cbotev
 *
 */
public class DatabusClientException extends Exception
{
  private static final long serialVersionUID = 1L;

  /**
   * Constructs an exception object with the specified error message and cause
   * @param     message         the error message
   * @param     cause           the error cause
   */
  public DatabusClientException(String message, Throwable cause)
  {
    super(message, cause);
  }

  /**
   * Constructs an exception object with the specified error message
   * @param     message         the error message
   */
  public DatabusClientException(String message)
  {
    super(message);
  }

  /**
   * Constructs an exception object with the specified error cause
   * @param     cause           the error cause
   */
  public DatabusClientException(Throwable cause)
  {
    super(cause);
  }

}
