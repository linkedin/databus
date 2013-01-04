package com.linkedin.databus2.core.container.request;


public abstract class BaseBinaryCommandParser implements BinaryCommandParser
{
  protected IDatabusRequest _cmd;
  protected ErrorResponse _err;

  @Override
  public void startNew()
  {
    _cmd = null;
    _err = null;
  }

  @Override
  public IDatabusRequest getCommand()
  {
    return _cmd;
  }

  @Override
  public ErrorResponse getError()
  {
    return _err;
  }

  protected void setCommand(IDatabusRequest cmd)
  {
    _cmd = cmd;
  }

  protected void setError(ErrorResponse err)
  {
    _err = err;
  }

}
