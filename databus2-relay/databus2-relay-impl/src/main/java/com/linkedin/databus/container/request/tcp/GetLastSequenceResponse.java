package com.linkedin.databus.container.request.tcp;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

import com.linkedin.databus2.core.container.request.BinaryProtocol;
import com.linkedin.databus2.core.container.request.ErrorResponse;
import com.linkedin.databus2.core.container.request.SimpleDatabusResponse;

/** Contains the response to a GetLastSequence request */
public class GetLastSequenceResponse extends SimpleDatabusResponse
{
  public static class ResponseDecoderV2 extends FrameDecoder
  {
    @Override
    protected Object decode(ChannelHandlerContext ctx,
                            Channel channel,
                            ChannelBuffer buffer) throws Exception
    {
      if (buffer.readableBytes() < 8) return null;
      byte returnCode = buffer.readByte();
      if (returnCode < 0)
      {
        buffer.readerIndex(buffer.readerIndex() - 1);
        return ErrorResponse.decodeFromChannelBuffer(buffer);
      }
      return createV2FromChannelBuffer(buffer);
    }
  }

  private final long[] _lastSeqs;

  public GetLastSequenceResponse(long[] lastSeqs, byte protocolVersion)
  {
    super(protocolVersion);
    _lastSeqs = lastSeqs;
  }

  public static GetLastSequenceResponse createV2FromChannelBuffer(ChannelBuffer buffer)
  {
    /*int responseLen = */buffer.readInt();
    long binlogOfs = buffer.readLong();

    return new GetLastSequenceResponse(new long[]{binlogOfs}, (byte)2);
  }

  public long[] getLastSeqs()
  {
    return _lastSeqs;
  }

  @Override
  public ChannelBuffer serializeToBinary()
  {
    byte protoVersion = getProtocolVersion();
    if (protoVersion > 2 || protoVersion <= 0)
    {
      ErrorResponse errResponse = ErrorResponse.createUnsupportedProtocolVersionResponse(protoVersion);
      return errResponse.serializeToBinary();
    }

    ChannelBuffer responseBuffer = ChannelBuffers.buffer(BinaryProtocol.BYTE_ORDER, 1 + 4 +
                                                         _lastSeqs.length * 8);
    responseBuffer.writeByte(0);
    responseBuffer.writeInt(_lastSeqs.length * 8);
    for (long seq: _lastSeqs) responseBuffer.writeLong(seq);

    return responseBuffer;
  }
}
