package com.linkedin.databus.core;

/**
 * Thrown when the Scn requested is not found in the Buffer. 
 * @author sdas
 *
 */
public class ScnNotFoundException 
extends Exception 
{

  /**
   * 
   */
  private static final long serialVersionUID = 6210518999221853075L;

  public ScnNotFoundException()
  {
    super();
  }

  public ScnNotFoundException(String message, Throwable cause)
  {
    super(message, cause);
  }

  public ScnNotFoundException(String message)
  {
    super(message);
  }

  public ScnNotFoundException(Throwable cause)
  {
    super(cause);
  }
}
