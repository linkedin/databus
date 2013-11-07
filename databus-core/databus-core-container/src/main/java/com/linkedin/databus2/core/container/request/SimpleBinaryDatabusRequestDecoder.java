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


import java.nio.ByteOrder;

import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.ExtendedReadTimeoutHandler;

/** Decodes a binary stream with databus commands */
public class SimpleBinaryDatabusRequestDecoder extends FrameDecoder
//extends SimpleChannelUpstreamHandler
{
  public static final String MODULE = SimpleBinaryDatabusRequestDecoder.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  public static final String REQUEST_EXEC_HANDLER_NAME = "request execute hander";

  private enum State
  {
    EXPECT_COMMAND,
    INCOMPLETE_DATA,
    EXPECT_MORE_DATA,
  }

  private final CommandsRegistry _commandsRegistry;
  private final ExtendedReadTimeoutHandler _readTimeoutHandler;
  private final ByteOrder _byteOrder;
  private State _state;
  private int _curOpcode = -1;
  private BinaryCommandParser _currentParser;

  public SimpleBinaryDatabusRequestDecoder(CommandsRegistry commandsRegistry,
                                           ExtendedReadTimeoutHandler readTimeoutHandler,
                                           ByteOrder byteOrder)
  {
    _state = State.EXPECT_COMMAND;
    _commandsRegistry = commandsRegistry;
    _readTimeoutHandler = readTimeoutHandler;
    _byteOrder = byteOrder;
  }

  private void returnError(Channel responseChannel, ErrorResponse errResponse, ChannelBuffer buf,
                           State newState,
                           boolean logError,
                           boolean logBuffer)
  {
    returnError(responseChannel, errResponse, buf, logError, logBuffer);

    _state = newState;
    if (LOG.isTraceEnabled())
    {
      LOG.trace("new state after error: " + _state);
    }
  }

  public static void returnError(Channel responseChannel, ErrorResponse errResponse,
                          ChannelBuffer buf, boolean logError, boolean logBuffer)
  {
    if (logError || LOG.isDebugEnabled())
    {
      if (errResponse.isExpected())
      {
        LOG.info("Returning expected error response:" + errResponse.getErrorCode());
      }
      else
      {
        LOG.error("error " + errResponse.getErrorCode(), errResponse.getCause());
        if (null != buf && logBuffer)
        {
          buf.readerIndex(0);
          LOG.error("buffer contents:" + ChannelBuffers.hexDump(buf));
        }
      }
    }

    responseChannel.write(errResponse);

    if (null != buf)
    {
      //flush remaining bytes in the current buffer
      buf.readerIndex(buf.readerIndex() + buf.readableBytes());
    }
  }

  @Override
  protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer)
            throws Exception
  {
    int resetIndex = buffer.readerIndex();
    Object result = null;

    while (State.INCOMPLETE_DATA != _state && null == result && buffer.readable())
    {
      switch (_state)
      {
        case EXPECT_COMMAND:
        {
          byte opcode = buffer.readByte();
//          if (debugEnabled) LOG.debug("received command: " +
//                                      Integer.toHexString((opcode) & 0xFF));
          if ( opcode != _curOpcode || -1 == _curOpcode)
          {
            _currentParser = _commandsRegistry.createParser(opcode, ctx.getChannel(), _byteOrder);
            if (null == _currentParser)
            {
              _curOpcode = -1;
              returnError(ctx.getChannel(),
                          ErrorResponse.createUnknownCommandResponse(opcode),
                          buffer, State.EXPECT_COMMAND,
                          true,
                          true);
            }
            else
            {
              _curOpcode = opcode;
              _state = State.EXPECT_MORE_DATA;
              ChannelPipeline pipe = ctx.getPipeline();
              pipe.replace(REQUEST_EXEC_HANDLER_NAME, REQUEST_EXEC_HANDLER_NAME,
                           _commandsRegistry.createExecHandler(opcode, ctx.getChannel()));
              _currentParser.startNew();
              resetIndex = buffer.readerIndex();
            }
          }
          else
          {
            _state = State.EXPECT_MORE_DATA;
            _currentParser.startNew();
            resetIndex = buffer.readerIndex();
          }
          break;
        }
        case EXPECT_MORE_DATA:
        {
          if (null == _currentParser)
          {
            returnError(ctx.getChannel(),
                        ErrorResponse.createInternalServerErrorResponse(
                            new DatabusException("expecting more data but no parser")),
                            buffer, State.EXPECT_COMMAND,
                            true,
                            true);
          }
          else
          {
            try
            {
              BinaryCommandParser.ParseResult parseResult = _currentParser.parseBinary(buffer);
              switch (parseResult)
              {
                case DISCARD:  { /* do nothing */ break; }
                case INCOMPLETE_DATA: {_state = State.INCOMPLETE_DATA; break;}
                case EXPECT_MORE:  { _state = State.EXPECT_MORE_DATA; break; }
                case PASS_THROUGH: { result = buffer; break; }
                case DONE:
                  {
                    if (null == _currentParser.getError())
                    {
                      result = _currentParser.getCommand();
                      _state = State.EXPECT_COMMAND;
                    }
                    else
                    {
                      returnError(ctx.getChannel(),
                                  _currentParser.getError(),
                                  buffer, State.EXPECT_COMMAND,
                                  true,
                                  true);
                    }
                    break;
                  }
                default:
                {
                  returnError(ctx.getChannel(),
                              ErrorResponse.createInternalServerErrorResponse(
                                  new DatabusException("unknown parser return code" + parseResult)),
                              buffer, State.EXPECT_COMMAND,
                              true,
                              true);
                }
              }
            }
            catch (UnsupportedProtocolVersionException upve)
            {
              returnError(ctx.getChannel(),
                          ErrorResponse.createUnsupportedProtocolVersionResponse(upve.getProtocolVerson()),
                          buffer, State.EXPECT_COMMAND,
                          true,
                          true);
            }
            catch (Exception ex)
            {
              returnError(ctx.getChannel(),
                          ErrorResponse.createInternalServerErrorResponse(ex),
                          buffer, State.EXPECT_COMMAND,
                          true,
                          true);
            }
          }

          break;
        }
        default:
        {
          returnError(ctx.getChannel(),
                      ErrorResponse.createInternalServerErrorResponse(
                          new DatabusException("unknown state: " + _state)),
                      buffer, State.EXPECT_COMMAND,
                      true,
                      true);
        }
      }
    }

    if (State.INCOMPLETE_DATA == _state)
    {
      buffer.readerIndex(resetIndex);
      result = null;
      _state = State.EXPECT_MORE_DATA;
    }

    if (State.EXPECT_COMMAND == _state)
    {
      if (null != _readTimeoutHandler && _readTimeoutHandler.isStarted()) _readTimeoutHandler.stop();
    }
    else
    {
      if (null != _readTimeoutHandler && !_readTimeoutHandler.isStarted()) _readTimeoutHandler.start(ctx);
    }

    return result;
  }
}
