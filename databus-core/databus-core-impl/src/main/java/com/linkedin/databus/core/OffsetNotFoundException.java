package com.linkedin.databus.core;

/** Exception usually denoting mismatch between the content in the
 * SCN index and the content in the event buffer */
public class OffsetNotFoundException extends Exception {

  private static final long serialVersionUID = 1L;

  public OffsetNotFoundException()
  {
    super();
  }

  public OffsetNotFoundException(String message, Throwable cause)
  {
    super(message, cause);
  }

  public OffsetNotFoundException(String message)
  {
    super(message);
  }

  public OffsetNotFoundException(Throwable cause)
  {
    super(cause);
  }

}
