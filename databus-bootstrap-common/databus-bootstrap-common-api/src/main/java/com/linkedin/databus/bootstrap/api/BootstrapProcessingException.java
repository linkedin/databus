/**
 * 
 */
package com.linkedin.databus.bootstrap.api;

/**
 * @author lgao
 *
 */
public class BootstrapProcessingException extends Exception
{
  private static final long serialVersionUID = 1L;
  
  public BootstrapProcessingException(String arg0, Throwable arg1)
  {
    super(arg0, arg1);
  }

  public BootstrapProcessingException(String arg0)
  {
    super(arg0);
  }

  public BootstrapProcessingException(Throwable arg0)
  {
    super(arg0);
  }
}
