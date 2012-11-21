/*
 * $Id: UnsupportedKeyException.java 153294 2010-12-02 20:46:45Z jwesterm $
 */
package com.linkedin.databus.core;

/**
 * Thrown when the data type of the "key" field is not a supported type.
 *
 * @author Jemiah Westerman<jwesterman@linkedin.com>
 * @version $Revision: 153294 $
 */
public class UnsupportedKeyException
extends Exception
{
  private static final long serialVersionUID = 1L;

  public UnsupportedKeyException()
  {
    super();
  }

  public UnsupportedKeyException(String message, Throwable cause)
  {
    super(message, cause);
  }

  public UnsupportedKeyException(String message)
  {
    super(message);
  }

  public UnsupportedKeyException(Throwable cause)
  {
    super(cause);
  }
}
