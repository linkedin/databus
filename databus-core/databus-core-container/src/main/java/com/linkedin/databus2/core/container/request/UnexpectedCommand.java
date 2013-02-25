package com.linkedin.databus2.core.container.request;
/*
 *
 * Copyright 2013 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/


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
