package com.linkedin.databus2.core.container.request;

import com.linkedin.databus2.core.DatabusException;

public class UnexpectedCommand extends DatabusException
{
  private static final long serialVersionUID = 1L;
  private final byte _opcode;
  private final String _commandName;

  public UnexpectedCommand(String commandName)
  {
    super("unexpected command with name: " + commandName);
    _commandName = commandName;
    _opcode = -1;
  }

  public UnexpectedCommand(byte opcode)
  {
    super("unexpected command with opcode: " + opcode);
    _opcode = opcode;
    _commandName = null;
  }

  public byte getOpcode()
  {
    return _opcode;
  }

  public String getCommandName()
  {
    return _commandName;
  }

}
