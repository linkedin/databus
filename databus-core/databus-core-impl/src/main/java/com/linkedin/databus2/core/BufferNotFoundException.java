/*
 * $Id: BufferNotFoundException.java 168967 2011-02-25 21:56:00Z cbotev $
 */
package com.linkedin.databus2.core;

/**
 * @author Phanindra Ganti
 */
public class BufferNotFoundException
    extends DatabusException
{
  private static final long serialVersionUID = 1L;

  public BufferNotFoundException()
  {
    super();
  }

  public BufferNotFoundException(String message)
  {
    super(message);
  }

}
