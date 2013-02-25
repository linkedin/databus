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


import java.util.HashMap;
import java.util.Map;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.SimpleChannelHandler;

import com.linkedin.databus2.core.container.request.BinaryCommandParser;
import com.linkedin.databus2.core.container.request.BinaryCommandParserFactory;
import com.linkedin.databus2.core.container.request.ProcessorRegistrationConflictException;
import com.linkedin.databus2.core.container.request.RequestExecutionHandlerFactory;

/**
 * A registry that manages all factories needed to process commands through HTTP and TCP interfaces.
 *
 * <p>The implementation is not thread-safe though the factories are. All commands should be
 * registered before starting any kind of processing.
 *
 * @author cbotev
 *
 */
public class CommandsRegistry
{
  private final Map<Byte,BinaryCommandParserFactory> _binaryParsers;
  private final Map<Byte,RequestExecutionHandlerFactory> _tcpExecHandlers;
  private final Map<String, RequestExecutionHandlerFactory> _httpExecHandlers;

  public CommandsRegistry()
  {
    _binaryParsers = new HashMap<Byte, BinaryCommandParserFactory>();
    _tcpExecHandlers = new HashMap<Byte, RequestExecutionHandlerFactory>();
    _httpExecHandlers = new HashMap<String, RequestExecutionHandlerFactory>();
  }

  /**
   * Registers a new command
   * @param name                    the HTTP name of the command (null for no HTTP interface)
   * @param opcode                  the TCP opcode (null for no TCP inteface)
   * @param binaryParserFactory     the factory for binary parsers for this command to be used
   *                                by the TCP interface (can be null if opcode is null)
   * @param execHandlerFactory      the factory for ChannelHandlers to be used for executing
   *                                commands of the specified type. The factory will be used for
   *                                both HTTP and TCP commands if those interfaces are enabled.
   *                                Must not be null.
   * @throws ProcessorRegistrationConflictException
   */
  public void registerCommand(String name, Byte opcode,
                              BinaryCommandParserFactory binaryParserFactory,
                              RequestExecutionHandlerFactory execHandlerFactory)
         throws ProcessorRegistrationConflictException
  {
    if (null != opcode)
    {
      if (_binaryParsers.containsKey(opcode)) throw new ProcessorRegistrationConflictException(name);
      _binaryParsers.put(opcode, binaryParserFactory);
      _tcpExecHandlers.put(opcode, execHandlerFactory);
    }

    if (null != name)
    {
      _httpExecHandlers.put(name, execHandlerFactory);
    }
  }

  /**
   * Creates a binary parser for a TCP command
   * @param opcode          the TCP command opcode
   * @return the parser or null if no such command has been registered.
   */
  public BinaryCommandParser createParser(byte opcode, Channel channel)
  {
    BinaryCommandParserFactory factory = _binaryParsers.get(opcode);
    return (null != factory) ? factory.createParser(channel) : null;
  }

  /**
   * Creates an execution handler for a TCP command
   * @param opcode          the TCP command opcode
   * @return the execution handler or null if no such command has been registered.
   */
  public SimpleChannelHandler createExecHandler(byte opcode, Channel channel)
  {
    RequestExecutionHandlerFactory factory = _tcpExecHandlers.get(opcode);
    return (null != factory) ? factory.createHandler(channel) : null;
  }

  /**
   * Creates an execution handler for an HTTP command
   * @param name
   * @return the execution handler or null if no such command has been registered.
   */
  public SimpleChannelHandler createExecHandler(String name, Channel channel)
  {
    RequestExecutionHandlerFactory factory = _httpExecHandlers.get(name);
    return (null != factory) ? factory.createHandler(channel) : null;
  }

}
