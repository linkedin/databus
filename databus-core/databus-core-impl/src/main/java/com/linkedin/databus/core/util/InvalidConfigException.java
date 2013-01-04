package com.linkedin.databus.core.util;

import com.linkedin.databus2.core.DatabusException;

public class InvalidConfigException extends DatabusException
{
  private static final long serialVersionUID = 1L;

  public InvalidConfigException(String msg)
  {
    super(msg);
  }

  public InvalidConfigException(Throwable cause)
  {
    super(cause);
  }

  public InvalidConfigException(String msg, Throwable cause)
  {
    super(msg, cause);
  }

}
