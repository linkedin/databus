package com.linkedin.databus.container.request.tcp;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventBufferMult;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus2.core.container.request.BaseBinaryCommandParser;
import com.linkedin.databus2.core.container.request.BinaryCommandParser;
import com.linkedin.databus2.core.container.request.BinaryCommandParserFactory;
import com.linkedin.databus2.core.container.request.BinaryProtocol;
import com.linkedin.databus2.core.container.request.ErrorResponse;
import com.linkedin.databus2.core.container.request.IDatabusResponse;
import com.linkedin.databus2.core.container.request.InvalidRequestParamValueException;
import com.linkedin.databus2.core.container.request.RequestExecutionHandlerFactory;
import com.linkedin.databus2.core.container.request.SimpleDatabusRequest;
import com.linkedin.databus2.core.seq.MultiServerSequenceNumberHandler;
import com.linkedin.databus2.core.seq.ServerName;


/** Represents a GetLastSequence request */
public class GetLastSequenceRequest extends SimpleDatabusRequest
{
  public static final byte OPCODE = 0x53;

  public static class BinaryParser extends BaseBinaryCommandParser
  {

    @Override
    public ParseResult parseBinary(ChannelBuffer buf) throws Exception
    {
      if (buf.readableBytes() < 1) return ParseResult.INCOMPLETE_DATA;

      //read protocol version
      byte protoVersion = buf.readByte();
      if (protoVersion <= 0 || protoVersion > 2)
        setError(ErrorResponse.createUnsupportedProtocolVersionResponse(protoVersion));

      //GetLastSequenceRequest cmd = null;
      if (null != getError())
      {
        //nothing more to do
      }
      else if (1 == protoVersion)
      {
        if (buf.readableBytes() < 2) return ParseResult.INCOMPLETE_DATA;
        //read number of sources
        short sourcesNum = buf.readShort();
        if (sourcesNum <= 0) throw new InvalidRequestParamValueException("getLastSequence",
                                                                         "sourcesNum",
                                                                         Short.toString(sourcesNum));

        //read the sources ids
        if (buf.readableBytes() < 2 * sourcesNum) return ParseResult.INCOMPLETE_DATA;
        short[] sourceIds = new short[sourcesNum];
        for (int i = 0; i < sourcesNum; ++i) sourceIds[i] = buf.readShort();

        _cmd = GetLastSequenceRequest.createV1(sourceIds);
      }
      else if (2 == protoVersion)
      {
        if (buf.readableBytes() < 4) return ParseResult.INCOMPLETE_DATA;
        int serverId = buf.readInt();

        _cmd = GetLastSequenceRequest.createV2(serverId);
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

  public static class ExecHandler extends SimpleChannelHandler
  {
    //TODO this has to be replaced with the multi-buffer interface
    private final DbusEventBufferMult _eventBufferMult;
    /** Map from serverId to the binlog offset persistor */
    private final MultiServerSequenceNumberHandler _scnHandlers;

    public ExecHandler(DbusEventBufferMult eventBuffer,
                       MultiServerSequenceNumberHandler scnHandlers)
    {
       _eventBufferMult = eventBuffer;

       _scnHandlers = scnHandlers;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception
    {
      if (! (e.getMessage() instanceof GetLastSequenceRequest)) super.messageReceived(ctx, e);
      else
      {
        GetLastSequenceRequest req = (GetLastSequenceRequest)e.getMessage();
        IDatabusResponse result;

        if (1 == req.getProtocolVersion())
        {
          short[] srcids = req.getSourceIds();
          long[] lastSeqs = new long[srcids.length];

          for (int i = 0; i < lastSeqs.length; ++i)
          {
            PhysicalPartition ppart = _eventBufferMult.getPhysicalPartition(srcids[i]);
            if (null == ppart) lastSeqs[i] = -1;
            else
            {
              DbusEventBuffer buf = _eventBufferMult.getOneBuffer(ppart);
              if (null == buf) lastSeqs[i] = -1;
              else lastSeqs[i] = buf.lastWrittenScn() > 0 ? buf.lastWrittenScn()
                                                            : buf.getPrevScn();
            }
          }

          result = new GetLastSequenceResponse(lastSeqs, req.getProtocolVersion());
        }
        else if (2 == req.getProtocolVersion())
        {
          long binlogOfs = _scnHandlers.readLastSequenceNumber(new ServerName(req.getServerId()));
          result = new GetLastSequenceResponse(new long[]{binlogOfs}, req.getProtocolVersion());
        }
        else
        {
          result = ErrorResponse.createUnsupportedProtocolVersionResponse(req.getProtocolVersion());
        }

        ctx.getChannel().write(result);
      }
    }
  }

  public static class ExecHandlerFactory implements RequestExecutionHandlerFactory
  {
    private final DbusEventBufferMult _eventBuffer;
    /** Map from serverId to the binlog offset persistor */
    private final MultiServerSequenceNumberHandler _scnReaderWriters;

    public ExecHandlerFactory(DbusEventBufferMult eventBuffer,
                              MultiServerSequenceNumberHandler scnReaderWriters)
    {
      super();
      _eventBuffer = eventBuffer;
      _scnReaderWriters = scnReaderWriters;
    }

    @Override
    public SimpleChannelHandler createHandler(Channel channel)
    {
      return new ExecHandler(_eventBuffer, _scnReaderWriters);
    }

  }

  private final short[] _sourceIds;
  private final int _serverId;

  GetLastSequenceRequest(byte protoVersion, short[] sourceIds, int serverId)
  {
    super(protoVersion);
    _sourceIds = sourceIds;
    _serverId = serverId;
  }

  public static GetLastSequenceRequest createV1(short[] sourceIds)
  {
    return new GetLastSequenceRequest((byte)1, sourceIds, -1);
  }

  public static GetLastSequenceRequest createV2(int serverId)
  {
    return new GetLastSequenceRequest((byte)2, null, serverId);
  }

  public short[] getSourceIds()
  {
    return _sourceIds;
  }

  public int getServerId()
  {
    return _serverId;
  }

  @Override
  public ChannelBuffer toBinaryChannelBuffer()
  {
    int bufferSize = (1 == _protocolVersion) ? (1 + 1 + 2 + _sourceIds.length * 2) :
                                               (1 + 1 + 4);
    ChannelBuffer buffer = ChannelBuffers.buffer(BinaryProtocol.BYTE_ORDER,
                                                 bufferSize);
    buffer.writeByte(OPCODE);
    buffer.writeByte(_protocolVersion);

    if ( 1 == _protocolVersion)
    {
      buffer.writeShort(_sourceIds.length);
      for (short srcId: _sourceIds) buffer.writeShort(srcId);
    }
    else if (2 == _protocolVersion)
    {
      buffer.writeInt(_serverId);
    }


    return buffer;
  }


}
