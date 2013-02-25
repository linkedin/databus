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


import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus.core.data_model.UnknownPartitionException;
import com.linkedin.databus2.core.DatabusException;

public class ErrorResponse extends SimpleDatabusResponse
{

  private final byte _errorCode;
  private final Throwable _cause;
  private final String _causeClassName;
  private final String _causeMessage;

  public ErrorResponse(byte errorCode, Throwable cause)
  {
    super((byte)1);
    _errorCode = errorCode;
    _cause = cause;
    _causeClassName = _cause.getClass().getName();
    _causeMessage = _cause.getMessage();
  }

  public ErrorResponse(byte errorCode, String causeClassName, String causeMessage)
  {
    super((byte)1);
    _errorCode = errorCode;
    _cause = null;
    _causeClassName = causeClassName;
    _causeMessage = causeMessage;
  }

  public static ErrorResponse decodeFromChannelBuffer(ChannelBuffer buffer)
  {
    if (buffer.readableBytes() < 1 + 4 + 2 + 2) return null;
    byte opcode = buffer.readByte();
    int responseLen = buffer.readInt();
    if (responseLen <= 0) throw new IllegalArgumentException("responseLen=" + responseLen);
    if (buffer.readableBytes() < responseLen) return null;

    short errorClassLen = buffer.readShort();
    if (errorClassLen <= 0) throw new IllegalArgumentException("errorClassLen=" + errorClassLen);

    byte[] classNameBytes = new byte[errorClassLen];
    buffer.readBytes(classNameBytes);

    short errorMessageLen = buffer.readShort();
    if (errorMessageLen < 0) throw new IllegalArgumentException("errorMessageLen=" + errorMessageLen);
    byte[] errorMessageBytes = (errorMessageLen > 0) ? new byte[errorMessageLen] : null;
    if (errorMessageLen > 0) buffer.readBytes(errorMessageBytes);

    ErrorResponse result = new ErrorResponse(opcode, new String(classNameBytes),
                                             null != errorMessageBytes ? new String(errorMessageBytes)
                                                                       : null);
    return result;
  }

  public byte getErrorCode()
  {
    return _errorCode;
  }

  public Throwable getCause()
  {
    return _cause;
  }

  public String getCauseClassName()
  {
    return _causeClassName;
  }

  public String getCauseMessage()
  {
    return _causeMessage;
  }

  public static ErrorResponse createUnknownCommandResponse(int opcode)
  {
    return new ErrorResponse(BinaryProtocol.RESULT_ERR_UNKNOWN_COMMAND,
                             new UnknownCommandException(Integer.toHexString(opcode & 0xFF)));
  }

  public static ErrorResponse createUnexpectedControlEventErrorResponse(String msg)
  {
    return new ErrorResponse(BinaryProtocol.RESULT_ERR_UNEXPECTED_CONTROL_EVENT,
                             new DatabusException(msg));
  }

  public static ErrorResponse createInternalServerErrorResponse(Throwable cause)
  {
    return new ErrorResponse(BinaryProtocol.RESULT_ERR_INTERNAL_SERVER_ERROR, cause);
  }

  public static ErrorResponse createUnsupportedProtocolVersionResponse(byte version)
  {
    return new ErrorResponse(BinaryProtocol.RESULT_ERR_UNSUPPORTED_PROTOCOL_VERSION,
                             new UnsupportedProtocolVersionException(version));
  }

  public static ErrorResponse createSourcesTooOldResponse(List<Short> srcIds)
  {
    return new ErrorResponse(BinaryProtocol.RESULT_ERR_SOURCES_TOO_OLD,
                             new SourcesTooOldException(srcIds));
  }

  public static ErrorResponse createSourcesTooOldResponse(int serverId)
  {
    return new ErrorResponse(BinaryProtocol.RESULT_ERR_SOURCES_TOO_OLD,
                             new SourcesTooOldException(serverId));
  }

  public static ErrorResponse createUnexpectedCommandResponse(String commandName)
  {
    return new ErrorResponse(BinaryProtocol.RESULT_ERR_UNEXPECTED_COMMAND,
                             new UnexpectedCommand(commandName));
  }

  public static ErrorResponse createUnexpectedCommandResponse(byte opcode)
  {
    return new ErrorResponse(BinaryProtocol.RESULT_ERR_UNEXPECTED_COMMAND,
                             new UnexpectedCommand(opcode));
  }

  public static ErrorResponse createInvalidRequestParam(String cmdName, String paramName,
                                                        String paramValue)
  {
    return new ErrorResponse(BinaryProtocol.RESULT_ERR_INVALID_REQ_PARAM,
                             new InvalidRequestParamValueException(cmdName, paramName, paramValue));
  }

  public static ErrorResponse createUnknownPartition(PhysicalPartition ppart)
  {
    return new ErrorResponse(BinaryProtocol.RESULT_ERR_UNKNOWN_PARTITION,
                             new UnknownPartitionException(ppart));
  }

  public static ErrorResponse createInvalidEvent(String errMsg)
  {
    return new ErrorResponse(BinaryProtocol.RESULT_ERR_INVALID_EVENT,
                             new DatabusException(errMsg));
  }

  /**
   * See MyBus protocol wiki
   * @return the binary serialization
   */
  @Override
  public ChannelBuffer serializeToBinary()
  {
    int resultSize = 1 + 4 + 2 + 2;
    int classLen = 0;
    byte[] causeClass = null;
    int messageLen = 0;
    byte[] causeMessage = null;

    if (null != _cause)
    {
      causeClass = _causeClassName.getBytes();
      classLen = Math.min(causeClass.length, BinaryProtocol.MAX_ERROR_CLASS_LEN);
      resultSize += classLen;
      if (null != _causeMessage)
      {
        causeMessage = _causeMessage.getBytes();
        messageLen = Math.min(causeMessage.length, BinaryProtocol.MAX_ERROR_MESSAGE_LEN);
        resultSize += causeMessage.length;
      }
    }

    ChannelBuffer result = ChannelBuffers.buffer(BinaryProtocol.BYTE_ORDER, resultSize);
    result.writeByte(_errorCode);
    result.writeInt(resultSize - 1 - 4);
    result.writeShort(classLen);
    result.writeBytes(causeClass, 0, classLen);
    result.writeShort(messageLen);
    if (messageLen > 0)
    {
      result.writeBytes(causeMessage, 0, messageLen);
    }

    return result;
  }

}
