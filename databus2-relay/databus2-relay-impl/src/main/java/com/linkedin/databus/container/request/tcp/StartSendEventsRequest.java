package com.linkedin.databus.container.request.tcp;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;

import com.linkedin.databus.core.data_model.LogicalSource;
import com.linkedin.databus2.core.container.request.BaseBinaryCommandParser;
import com.linkedin.databus2.core.container.request.BinaryCommandParser;
import com.linkedin.databus2.core.container.request.BinaryCommandParserFactory;
import com.linkedin.databus2.core.container.request.BinaryProtocol;
import com.linkedin.databus2.core.container.request.ErrorResponse;
import com.linkedin.databus2.core.container.request.SimpleDatabusRequest;

/**
 * Representation of the startSendEvents request
 *
 * @see <a href="https://iwww.corp.linkedin.com/wiki/cf/display/ENGS/MyBus+Protocol#MyBusProtocol-{{startSendEvents}}command">MyBusProtocol</a>
 *
 */
public class StartSendEventsRequest extends SimpleDatabusRequest
{
  private final Logger LOG;

  public static final byte OPCODE = (byte)0xA1;
  public static final short MAX_SOURCE_NAME_LENGTH = 512;

  public static class BinaryParser extends BaseBinaryCommandParser
  {
    public static final Logger LOG = Logger.getLogger(BinaryParser.class.getName());

    @Override
    public ParseResult parseBinary(ChannelBuffer buf) throws Exception
    {
      boolean debugEnabled = LOG.isDebugEnabled();
      if (buf.readableBytes() < 5) return ParseResult.INCOMPLETE_DATA;

      //read protocol version
      byte protoVersion = buf.readByte();
      if (protoVersion <= 0 || protoVersion > 3)
      {
        setError(ErrorResponse.createUnsupportedProtocolVersionResponse(protoVersion));
      }

      int serverId = 0;
      HashMap<Integer, LogicalSource> srcIdsMap = null;
      if (null == getError())
      {
        //read request params
        serverId = buf.readInt();

        if (3 == protoVersion && buf.readableBytes() < 4) return ParseResult.INCOMPLETE_DATA;

        srcIdsMap = new HashMap<Integer, LogicalSource>(32);
        if (3 == protoVersion)
        {
          int srcNum = buf.readInt();
          if (0 >= srcNum)
              setError(ErrorResponse.createInvalidRequestParam("startSendEvents",
                                                               "source_number",
                                                               String.valueOf(srcNum)));
          for (int i = 0; i < srcNum; ++i)
          {
            if (buf.readableBytes() < 4) return ParseResult.INCOMPLETE_DATA;
            Integer id = Integer.valueOf(buf.readShort());
            short nameLen = buf.readShort();
            if (0 >= id)
            {
              setError(ErrorResponse.createInvalidRequestParam("startSendEvents",
                                                               "source_id",
                                                               id.toString()));
              break;
            }
            if (0 >= nameLen)
            {
              setError(ErrorResponse.createInvalidRequestParam("startSendEvents",
                                                               "source_name_length:",
                                                               String.valueOf(nameLen)));
              break;
            }
            if (buf.readableBytes() < nameLen) return ParseResult.INCOMPLETE_DATA;
            byte[] sourceNameBytes = new byte[nameLen];
            buf.readBytes(sourceNameBytes);
            String sourceName = new String(sourceNameBytes);
            srcIdsMap.put(id, new LogicalSource(id, sourceName));
          }
        }
      }

      if (null == getError())
      {
        switch (protoVersion)
        {
          case 1: _cmd = StartSendEventsRequest.createV1(serverId); break;
          case 2: _cmd = StartSendEventsRequest.createV2(serverId); break;
          case 3: _cmd = StartSendEventsRequest.createV3(serverId, srcIdsMap); break;
          default: setError(ErrorResponse.createUnsupportedProtocolVersionResponse(protoVersion)); break;

        }

        LOG.info("startSendEventsRequest:" + _cmd);
      }

      return ParseResult.DONE;
    }

  }

  public static class BinaryParserFactory implements BinaryCommandParserFactory
  {
    @Override
    public BinaryCommandParser createParser(Channel channel)
    {
      return new BinaryParser();
    }
  }

  private final int _serverId;
  private final Map<Integer, LogicalSource> _sourceIds;

  private StartSendEventsRequest(byte protocolVersion, int serverId,
                                 Map<Integer, LogicalSource> sourceIds)
  {
    super(protocolVersion);
    _serverId = serverId;
    _sourceIds = sourceIds;
    LOG = Logger.getLogger(StartSendEventsRequest.class.getName() + ":" + _serverId);
  }

  static public StartSendEventsRequest createV1(int serverId)
  {
    return new StartSendEventsRequest((byte)1, serverId, null);
  }

  static public StartSendEventsRequest createV2(int serverId)
  {
    return new StartSendEventsRequest((byte)2, serverId, null);
  }

  static public StartSendEventsRequest createV3(int serverId, Map<Integer, LogicalSource> sourceIds)
  {
    return new StartSendEventsRequest((byte)3, serverId, sourceIds);
  }

  @Override
  public ChannelBuffer toBinaryChannelBuffer()
  {
    int bufferSize = 6;
    if (3 == _protocolVersion)
    {
      bufferSize += 4;
      for (Map.Entry<Integer, LogicalSource> entry: _sourceIds.entrySet())
      {
        bufferSize += 4 + entry.getValue().getName().getBytes().length;
      }
    }

    ChannelBuffer buffer = ChannelBuffers.buffer(BinaryProtocol.BYTE_ORDER, bufferSize);
    buffer.writeByte(OPCODE);
    buffer.writeByte(_protocolVersion);
    buffer.writeInt(_serverId);

    if (3 == _protocolVersion)
    {
      buffer.writeInt(_sourceIds.size());
      for (Map.Entry<Integer, LogicalSource> entry: _sourceIds.entrySet())
      {
        buffer.writeShort(entry.getKey().shortValue());
        byte[] sourceName = entry.getValue().getName().getBytes();
        short sourceLen = (short)Math.min(MAX_SOURCE_NAME_LENGTH, sourceName.length);
        buffer.writeShort(sourceLen);
        buffer.writeBytes(sourceName, 0, sourceLen);
      }
    }

    return buffer;
  }
  public int getServerId()
  {
    return _serverId;
  }

  public Map<Integer, LogicalSource> getSourceIds()
  {
    return _sourceIds;
  }

}
