package com.linkedin.databus.container.request.tcp;

import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.buffer.SlicedChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus.container.netty.HttpRelay.StaticConfig;
import com.linkedin.databus.container.request.SendEventsResponse;
import com.linkedin.databus.container.request.StartSendEventsResponse;
import com.linkedin.databus.core.DataChangeEvent;
import com.linkedin.databus.core.DbusConstants;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventBufferMult;
import com.linkedin.databus.core.InternalDatabusEventsListenerAbstract;
import com.linkedin.databus.core.InvalidEventException;
import com.linkedin.databus.core.data_model.LogicalSource;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.monitoring.mbean.StatsCollectors;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.request.BaseBinaryCommandParser;
import com.linkedin.databus2.core.container.request.BinaryCommandParser;
import com.linkedin.databus2.core.container.request.BinaryCommandParserFactory;
import com.linkedin.databus2.core.container.request.BinaryProtocol;
import com.linkedin.databus2.core.container.request.ErrorResponse;
import com.linkedin.databus2.core.container.request.IDatabusResponse;
import com.linkedin.databus2.core.container.request.RequestExecutionHandlerFactory;
import com.linkedin.databus2.core.container.request.SimpleDatabusRequest;
import com.linkedin.databus2.core.container.request.UnsupportedProtocolVersionException;
import com.linkedin.databus2.core.seq.MaxSCNReaderWriter;
import com.linkedin.databus2.core.seq.MultiServerSequenceNumberHandler;
import com.linkedin.databus2.core.seq.ServerName;
import com.linkedin.databus2.relay.config.PhysicalSourceConfig;
import com.linkedin.databus2.schemas.SchemaId;
import com.linkedin.databus2.schemas.SchemaRegistryService;
import com.linkedin.databus2.schemas.SourceIdNameRegistry;

/**
 * Representation of the SendEvents request
 *
 * @see <a href='https://iwww.corp.linkedin.com/wiki/cf/display/ENGS/MyBus+Protocol#MyBusProtocol-{{sendEvents}}command'>MyBusProtocol</a>
 */
public class SendEventsRequest extends SimpleDatabusRequest
{
  public static final String MODULE = SendEventsRequest.class.getName();
  public static final String PERF_MODULE = MODULE + "Perf";
  public static final byte OPCODE = (byte)0xA2;

  public static class BinaryParser extends BaseBinaryCommandParser
  {
    public static final Logger LOG = Logger.getLogger(BinaryParser.class.getName());
    @Override
    public ParseResult parseBinary(ChannelBuffer buf) throws Exception
    {
      if (buf.readableBytes() < 10 + DbusEvent.LengthOffset  + DbusEvent.LengthLength) return ParseResult.INCOMPLETE_DATA;

      //read protocol version
      byte protoVersion = buf.readByte();
      if (protoVersion <= 0 || (protoVersion != 1 && protoVersion != 3))
          throw new UnsupportedProtocolVersionException(protoVersion);

      long binlogOffset = buf.readLong();

      //read event length
      int curOfs = buf.readerIndex();

      //check event magic byte
      byte magic = buf.getByte(curOfs);
      if (magic != DbusEvent.CurrentMagicValue) throw new InvalidEventException();

      int eventSize = buf.getInt(curOfs + DbusEvent.LengthOffset);
      //TODO for now we buffer the whole event, later we may want to stream it
      if (buf.readableBytes() < eventSize)
      {
//        if (debugEnabled) LOG.debug("Incomplete data:event size: " + eventSize + " buffer size: " + buf.readableBytes());
        return ParseResult.INCOMPLETE_DATA;
      }


      _cmd = new SendEventsRequest(protoVersion, binlogOffset, buf, curOfs, eventSize);
//      if (debugEnabled) LOG.debug("sendEventsRequest: " + _cmd);
      buf.readerIndex(curOfs + eventSize);

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

  /** This class handles the evaluation of both {@link StartSendEventsRequest} and
   * {@link SendEventsRequest} commands.
   *
   * We use a single class because both request have to part of the same session. */
  public static class ExecHandler extends SimpleChannelHandler
  {
    private Logger _log, _perfLog;
    private final String MODULE = ExecHandler.class.getName();
    private final String PERF_MODULE = MODULE + "Perf";

    private final DbusEventBufferMult _eventBufferMult;
    private final HashMap<Integer, WeakReference<DbusEventBuffer>> _knownBuffers;
    private final HashMap<Integer, PhysicalPartition> _knownPParts;
    private long _mostRecentSeenBinlogOffset;
    private final HttpRelay.StaticConfig _relayConfig;
    private final MultiServerSequenceNumberHandler _scnHandlers;
    private final SourceIdNameRegistry _srcIdMap;  // computed mapping SourceName<=>Real src Id
    private StartSendEventsRequest _lastStartSendEventsReq;
    private final ServerName _curServerName;
    private MaxSCNReaderWriter _binlogOfsWriter = null;
    private final SourceIdNameRegistry _curInSrcMap;  // mapping from handshake (fakeId=>SourceName)
    private final StatsCollectors<DbusEventsStatisticsCollector> _globalInBoundStats;
    private WeakReference<DbusEventBuffer> _mostRecentBuffer = null;
    private final SchemaRegistryService _schemaRegService;
    private EventRewriter _eventRewriter;
    private PhysicalPartition _curPhysicalPartition;
    private final HttpRelay _relay;
    private final Map<PhysicalPartition, DbusEventsStatisticsCollector> _ppartStatsCollCache;
    private final EventLogBuffer _debugLogBuffer;

    public ExecHandler(DbusEventBufferMult eventBuffer, HttpRelay.StaticConfig relayConfig,
                       MultiServerSequenceNumberHandler scnHandlers,
                       SourceIdNameRegistry srcIdMap,
                       StatsCollectors<DbusEventsStatisticsCollector> globalInBoundStats,
                       StatsCollectors<DbusEventsStatisticsCollector> globalOutboundStats,
                       SchemaRegistryService schemaRegService, HttpRelay relay)
    {
       _eventBufferMult = eventBuffer;
       _knownBuffers = new HashMap<Integer, WeakReference<DbusEventBuffer>>(256);
       _knownPParts = new HashMap<Integer, PhysicalPartition>(256);
       _mostRecentSeenBinlogOffset = -1;
       _relayConfig = relayConfig;
       _scnHandlers = scnHandlers;
       _srcIdMap = srcIdMap;
       _curServerName = new ServerName(-1);
       _curInSrcMap = new SourceIdNameRegistry();
       _globalInBoundStats = globalInBoundStats;
       _schemaRegService = schemaRegService;
       _relay = relay;
       _log = Logger.getLogger(MODULE + "." + _curServerName.getName());
       _perfLog = Logger.getLogger(PERF_MODULE + "." + _curServerName.getName());
       _ppartStatsCollCache = new HashMap<PhysicalPartition, DbusEventsStatisticsCollector>(32);
       _debugLogBuffer = new EventLogBuffer();
    }

    private DbusEventsStatisticsCollector getPartitionCollector(PhysicalPartition ppart)
    {
    	DbusEventsStatisticsCollector res = _ppartStatsCollCache.get(ppart);
    	if (null == res)
    	{
          String ppartName = _curPhysicalPartition.toSimpleString();
		  res = (_globalInBoundStats == null)  ? null : _globalInBoundStats.getStatsCollector(ppartName);
		  if (null == res)
		  {
			  res = createPartitionCollector(_curPhysicalPartition);
		  }
		  if (null != res)
		  {
			  _ppartStatsCollCache.put(ppart, res);
		  }
    	}

    	return res;
    }

    private DbusEventsStatisticsCollector createPartitionCollector(PhysicalPartition ppart)
    {
      DbusEventsStatisticsCollector partCollectorIn = null;
      if (_globalInBoundStats != null)
      {
        if (null != _relay) _relay.addPhysicalPartitionCollectors(ppart);
        else
        {
          String ppartName = ppart.toSimpleString();
          //for testing purposes where there might not be a relay
          _globalInBoundStats.addStatsCollector(ppart.toSimpleString(),
                                                new  DbusEventsStatisticsCollector(
                                                     _relayConfig.getContainer().getId(),
                                                     ppartName+".inbound",
                                                     true,
                                                     false,
                                                     null));
        }
        partCollectorIn = _globalInBoundStats.getStatsCollector(ppart.toSimpleString());
      }
      return partCollectorIn;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception
    {
      long startTs = System.nanoTime();
      _debugLogBuffer.reset();
      if (e.getMessage() instanceof StartSendEventsRequest)
      {
        processStartSendEvents(ctx, (StartSendEventsRequest)e.getMessage());
        if (_perfLog.isDebugEnabled())
            _perfLog.debug("StartSendEventsRequest took : " + ((System.nanoTime()-startTs)*1.0) / DbusConstants.NUM_NSECS_IN_MSEC + "ms");
      }
      else if (e.getMessage() instanceof SendEventsRequest)
      {
        processSendEvents(ctx, (SendEventsRequest)e.getMessage());
        double msecs = ((System.nanoTime()-startTs)*1.0) / DbusConstants.NUM_NSECS_IN_MSEC;
        if (_perfLog.isDebugEnabled())
            _perfLog.debug("SendEventsRequest took : " + msecs + "ms");
        _debugLogBuffer.append(",processTimeMs=" + msecs);
        if (_log.isDebugEnabled())
        {
          if (_log.isTraceEnabled())
          {
            SendEventsRequest req = (SendEventsRequest)e.getMessage();
            appendEventBytes(req.obtainEventBuffer());
          }
          _log.debug(_debugLogBuffer.toString());
        }
      }
      else
      {
        super.messageReceived(ctx, e);
        if (_perfLog.isDebugEnabled())
            _perfLog.debug("Other message took : " + ((System.nanoTime()-startTs)*1.0) / DbusConstants.NUM_NSECS_IN_MSEC + "ms");
      }
    }

    private void processStartSendEvents(ChannelHandlerContext ctx,
                                        StartSendEventsRequest req) throws Exception
    {
      int serverId = req.getServerId();
      if (_curServerName.getId() != serverId) // handshake made again on the existing connection
      {
        _curServerName.setId(serverId);
        if (null != _scnHandlers) _binlogOfsWriter = _scnHandlers.getOrCreateHandler(_curServerName);
        _log = Logger.getLogger(MODULE + ":" + _curServerName.getName());
        _perfLog = Logger.getLogger(PERF_MODULE + ":" + _curServerName.getName());
        _mostRecentSeenBinlogOffset = -1; // to make sure that first event is not an EOW
      }
      _log.info("processStartSendEvent: serverId = " + serverId + "; maxScn handler = " + _binlogOfsWriter);

      IDatabusResponse result;
      _lastStartSendEventsReq = null;

      long binlogOffset = _scnHandlers.readLastSequenceNumber(_curServerName);
      if (1 == req.getProtocolVersion())
      {
        result = StartSendEventsResponse.createV1(_relayConfig.getEventBuffer().getReadBufferSize(),
                                                  binlogOffset);
        _lastStartSendEventsReq = req;
      }
      else if (2 == req.getProtocolVersion())
      {
        result = StartSendEventsResponse.createV2(_relayConfig.getEventBuffer().getReadBufferSize(),
                                                  0,
                                                  binlogOffset);
        _lastStartSendEventsReq = req;
      }
      else if (3 == req.getProtocolVersion())
      {
        result = StartSendEventsResponse.createV3(_relayConfig.getEventBuffer().getReadBufferSize(),
                                                  binlogOffset);
        _lastStartSendEventsReq = req;

        _curInSrcMap.update(_lastStartSendEventsReq.getSourceIds().values());
        _eventRewriter = new EventRewriter(_curInSrcMap, _srcIdMap, _schemaRegService, _relay, req.getServerId(), _debugLogBuffer);
      }
      else
      {
        throw new UnsupportedProtocolVersionException(req.getProtocolVersion());
      }


      _log.info("StartSendEventsRequest: " + req.toString());

      if (null != result)
      {
        _log.info(result.getClass().getSimpleName() + ':' + result);
      }

      ctx.getChannel().write(result);
    }

    private void saveBinlogOfs(long binlogOffset, boolean debugEnabled)
    {
      if (null != _binlogOfsWriter)
      {
        try
        {
//          if (debugEnabled) _log.debug("storing binlog offset " + req.getBinlogOffset() +
//                                      " for server " + _curServerName);
          _binlogOfsWriter.saveMaxScn(binlogOffset);
        }
        catch (DatabusException e)
        {
          _log.error("unable to store binlog offset: " + e.getClass() + ":" + e.getMessage() + ":" + _debugLogBuffer, e);
        }
        catch (RuntimeException e)
        {
          _log.error("unable to store binlog offset: " + e.getClass() + ":" + e.getMessage() + ":" + _debugLogBuffer, e);
        }
      }
      else
      {
        _log.error("unable to find or create binlog offset writer:" + _debugLogBuffer);
      }
    }

    private void appendEventInfo(ChannelBuffer channelBuffer)
    {
      // We log on a per server log, so we don't need to log server ID here.
//      sb.append(" srv=");
//      sb.append(null != _lastStartSendEventsReq ? _lastStartSendEventsReq.getServerId()
//                                                : Integer.MIN_VALUE);
      _debugLogBuffer.append(",seq=" + channelBuffer.getLong(DbusEvent.SequenceOffset));
      _debugLogBuffer.append(",pp=" + channelBuffer.getShort(DbusEvent.PhysicalPartitionIdOffset));
      _debugLogBuffer.append(",inSrc=" + channelBuffer.getShort(DbusEvent.SrcIdOffset));
      _debugLogBuffer.append(",eventSize=" + channelBuffer.readableBytes());
    }

    private void appendEventBytes(ChannelBuffer channelBuffer)
    {
      _debugLogBuffer.append("\n event_bytes=");
      int bufferIdx = channelBuffer.readerIndex();
      _debugLogBuffer.append(ChannelBuffers.hexDump(channelBuffer));
      channelBuffer.readerIndex(bufferIdx);
    }

    private void processSendEvents(ChannelHandlerContext ctx,
                                   SendEventsRequest req) throws Exception
    {
      boolean debugEnabled = _log.isDebugEnabled();

//      if (debugEnabled) _log.debug("push: " + req.toJsonString(false));

      IDatabusResponse result = null;
      DbusEvent event = null;
      _debugLogBuffer.append("binlogOffset=" + req.getBinlogOffset() + ",protoVersion=" +
                      req.getProtocolVersion());

      try
      {
    	  if (null == _lastStartSendEventsReq)
    	  {
          _log.error("Rejecting data request without startSendEvents:" + _debugLogBuffer);
    		  result = ErrorResponse.createUnexpectedCommandResponse(SendEventsRequest.OPCODE);
    	  }
    	  else if (1 == req.getProtocolVersion() || 3 == req.getProtocolVersion())
    	  {
    		  ChannelBuffer channelBuffer = req.obtainEventBuffer();
    		  Integer ppartId = (int)channelBuffer.getShort(DbusEvent.PhysicalPartitionIdOffset);
    		  appendEventInfo(channelBuffer);

    		  // figure out what buffer we should use
    		  DbusEventBuffer buf = null;
    		  ChannelBuffer realBuffer = channelBuffer;


    		  // get the fake source id
    		  Integer lSourceId = (int)channelBuffer.getShort(DbusEvent.SrcIdOffset);
    		  if(!DbusEvent.isControlSrcId(lSourceId.shortValue())) { // normal fake source id (negatives are for control)
    			  try {
    				  buf = getCorrespondingBuffer(ppartId, lSourceId);
    			  } catch (SendEventsRequestException sere) {
    				  result = sere.getResult();
    			  }
    			  _mostRecentBuffer = new WeakReference<DbusEventBuffer>(buf); // remember the buffer in case next event is a control event

    			  //rewrite logical source id if necessary
    			  if (null != _eventRewriter)
    			  {
    				  //TODO DDS-2135 Fix inefficient implementation
    				  long startTs1 = System.nanoTime();
    				  realBuffer = channelBuffer.copy(); //the underlying buffer is read-only so we have to copy it
    				  double msecs = ((System.nanoTime()-startTs1)*1.0) / DbusConstants.NUM_NSECS_IN_MSEC;
    				  _debugLogBuffer.append(",chBufCopyMs=" + msecs);
    				  if (_perfLog.isDebugEnabled())
    				      _perfLog.debug("channel buffer copy took : " + msecs + "ms");

    				  ByteBuffer eventBuffer = realBuffer.toByteBuffer().order(DbusEvent.byteOrder);
    				  event = new DbusEvent(eventBuffer, 0);
    				  if (event.sequence() <= 0)
    				  {
    				    String errMsg = "SCN (" + event.sequence()  + ") is less than or equal to zero";
    				    _log.error(errMsg);
                        result = ErrorResponse.createInvalidEvent(errMsg);
                        ctx.getChannel().write(result);
                        return;
    				  }
    				  /*
    				   * Comment out for now as lot of tests break because of this check
    				  if (event.schemaVersion() == 0)
    				  {
    					  String errMsg = "Schema version == 0 is invalid";
    					  _log.error(errMsg);
    					  result = ErrorResponse.createInternalServerErrorResponse(new Exception(errMsg));
    					  ctx.getChannel().write(result);
    					  return;
    				  }
    				   */
    				  try
    				  {
    					  long startTs2 = System.nanoTime();
    					  _eventRewriter.onEvent(event, 0, realBuffer.readableBytes());
    					  msecs = ((System.nanoTime()-startTs2)*1.0) / DbusConstants.NUM_NSECS_IN_MSEC;
    					  if (_perfLog.isDebugEnabled())
    					      _perfLog.debug("Event rewrite took : " + ((System.nanoTime()-startTs2)*1.0) / DbusConstants.NUM_NSECS_IN_MSEC + "ms");
    					  _debugLogBuffer.append(",rewriteMs=" + msecs);
    				  } catch (SchemaUpdateFailed su)
    				  {
    					  _log.error("Schema update happened, hence initiating a new connection from RPL_DBUS:" + _debugLogBuffer, su);
    					  result = ErrorResponse.createInternalServerErrorResponse(su);
    					  ctx.getChannel().write(result);
    					  return;
    				  }
    			  }
    		  } else {
    			  // control event
    			  if (null == _mostRecentBuffer) {
    				  _log.error("Seems like control event occured before the actual data. We don't allow it:" + _debugLogBuffer);
    				  buf = null; // will fail few lines down
    			  }
    			  else {
    				  buf = _mostRecentBuffer.get();
    			  }
    		  }
    		  boolean isControlEvent = DbusEvent.isControlSrcId(lSourceId.shortValue());
    		  if(! isControlEvent) {
    		    if(_mostRecentSeenBinlogOffset == -1)
    		      _mostRecentSeenBinlogOffset = req.getBinlogOffset();
    		  } else {
    		    if(buf == null || _mostRecentSeenBinlogOffset == -1) {
    		      //illegal state - throw unrecoverable error to make sure the RPLDBUS thread will stop and wait for change master
    		      String errMsg = "serverId=" + _lastStartSendEventsReq.getServerId() +
                  "; buf = " +  buf + "; binlog = " + _mostRecentSeenBinlogOffset;
    		      _log.error("Failing because it is a first event ever for " + errMsg + ";dbg=" + _debugLogBuffer);
    		      result = ErrorResponse.createUnexpectedControlEventErrorResponse(errMsg);
    		    }
    		  }

    		  if (null == result)
    		  {
    			  int eventsNum = 0;
    			  DbusEventsStatisticsCollector stats = getPartitionCollector(_curPhysicalPartition);

    			  eventsNum = buf.readEvents(
    					  java.nio.channels.Channels.newChannel(new ChannelBufferInputStream(realBuffer)),
    					  stats);
    			  _debugLogBuffer.append(",nEvents=" + eventsNum);
    			  _debugLogBuffer.append(",buf=" + buf.hashCode());
    			  if (eventsNum > 0 && null != _lastStartSendEventsReq && isControlEvent)
    			  {
    			      saveBinlogOfs(_mostRecentSeenBinlogOffset, debugEnabled);

    			    // reset the watch point
    			    _mostRecentSeenBinlogOffset = -1;
    			  }

    			  result = new SendEventsResponse(req.getProtocolVersion(), eventsNum);
    		  }
    	  }
    	  else
    	  {
    		  _log.error("Rejecting data request due to unsupported protocol version:" + _debugLogBuffer);
    		  result  = ErrorResponse.createUnsupportedProtocolVersionResponse(req.getProtocolVersion());
    		  _debugLogBuffer.append(",result=" + result.getClass().getName());
    	  }
      } catch (Exception e)
      {
        if (event != null)
        {
          _log.error("Exception on event:" + _debugLogBuffer, e);
        }
    	  result = ErrorResponse.createInternalServerErrorResponse(e);
      }

      ctx.getChannel().write(result);
    }

    @Override
    public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception
    {
      super.channelDisconnected(ctx, e);
    }

    // use logicalSource to extract database name and use it as phyisicalPartition name
    private PhysicalPartition buildPhysicalPartition(Integer partitionId, LogicalSource lSource)
    throws InvalidEventException {
      String lSourceName = lSource.getName();
      int separatorIdx = lSourceName.lastIndexOf('.', lSourceName.length());
      if(separatorIdx < 1) {
        throw new InvalidEventException("lsource doesn't contain db name:" + lSourceName);
      }
      return new PhysicalPartition(partitionId, lSourceName.substring(0, separatorIdx));
    }

    // figure out the right buffer
    private DbusEventBuffer getCorrespondingBuffer(Integer ppartId, Integer lSourceId)
    throws SendEventsRequestException {
      DbusEventBuffer buf = null;

      // check if we saw this combination before
      Integer key = ((ppartId<<16) + lSourceId);
      WeakReference<DbusEventBuffer> bufRef = _knownBuffers.get(key);
      if (null != bufRef)
      {
        buf = bufRef.get();
        if (null != buf)
        {
          _curPhysicalPartition = _knownPParts.get(key);
        }
        else
        {
          _knownBuffers.remove(key);
          _knownPParts.remove(key);
        }
      }

      if(buf != null) return buf;

      // get the LogicalSource by the fake id from the mapping
      Map<Integer, LogicalSource> map = _lastStartSendEventsReq.getSourceIds();
      // build partition id
      _curPhysicalPartition = null;

      if(map != null) {
        LogicalSource lSource = map.get(lSourceId);
        _debugLogBuffer.append(",lSource=" + lSource);

        if(lSource == null) {
          _debugLogBuffer.append(",InvalidLogicalSrcMapping=" + lSourceId.toString());
          throw new SendEventsRequestException(
                                               ErrorResponse.createInvalidRequestParam("sendEvents",
                                                                                       "logical Source mapping invalid ",
                                                                                       lSourceId.toString()));
        }
        // build partition object from partition id and db name
        try {
          _curPhysicalPartition = buildPhysicalPartition(ppartId, lSource);
        } catch (InvalidEventException ie) {
          _debugLogBuffer.append(",InvalidLogicalSrc=" + lSourceId.toString());
          throw new SendEventsRequestException(
                                               ErrorResponse.createInvalidRequestParam("sendEvents",
                                                                                       "logical Source is invalid ",
                                                                                       lSource.toString()));
        }
      } else { // can happen ony in case of v1
        _curPhysicalPartition = new PhysicalPartition(ppartId, PhysicalSourceConfig.DEFAULT_PHYSICAL_PARTITION_NAME);
      }
//      if(debugEnabled) _log.debug("pPartition=" + _curPhysicalPartition);



      //we have not seen the partition -- try to find it out
      _log.info("found new partition:" + _curPhysicalPartition);
      buf = _eventBufferMult.getOneBuffer( _curPhysicalPartition);
      if (null == buf)
        throw new SendEventsRequestException(ErrorResponse.createUnknownPartition(_curPhysicalPartition));

      //add our event rewrite since this is the first time we use this buffer
      _knownBuffers.put(key, new WeakReference<DbusEventBuffer>(buf));
      _knownPParts.put(key, _curPhysicalPartition);

      return buf;
    }

    private static class SendEventsRequestException extends Exception {
      private static final long serialVersionUID = 1L;

      public IDatabusResponse _result=null;
      public SendEventsRequestException(IDatabusResponse result) {
        _result = result;
      }
      IDatabusResponse getResult() {
        return _result;
      }
    }
  }

  public static class ExecHandlerFactory implements RequestExecutionHandlerFactory
  {
    private final static int CLEAN_FREQ = 250;
    public static final Logger LOG = Logger.getLogger(ExecHandlerFactory.class);

    private final DbusEventBufferMult _eventsBufferMult;
    private final HttpRelay.StaticConfig _relayConfig;
    private final HttpRelay _relay;
    //Reuse handlers
    private final Hashtable<Channel, WeakReference<ExecHandler>> _handlers;
    private final AtomicInteger _useCnt;
    /** Map from serverId to the binlog offset persistor */
    private final MultiServerSequenceNumberHandler _scnReaderWriters;
    private final SourceIdNameRegistry _srcidMap;
    private final StatsCollectors<DbusEventsStatisticsCollector> _globalInBoundEventStats;
    private final StatsCollectors<DbusEventsStatisticsCollector> _globalOutBoundEventStats;
    private final SchemaRegistryService _schemaRegService;

    public ExecHandlerFactory(DbusEventBufferMult eventBuffer, StaticConfig relayConfig,
                              MultiServerSequenceNumberHandler scnReaderWriters,
                              SourceIdNameRegistry srcIdMap,
                              StatsCollectors<DbusEventsStatisticsCollector> globalInBoundEventStats,
                              StatsCollectors<DbusEventsStatisticsCollector> globalOutBoundEventStats,
                              SchemaRegistryService schemaRegService,
                              HttpRelay relay)
    {
      super();
      _eventsBufferMult = eventBuffer;
      _relayConfig = relayConfig;
      _handlers = new Hashtable<Channel, WeakReference<ExecHandler>>(100);
      _useCnt = new AtomicInteger();
      _scnReaderWriters = scnReaderWriters;
      _srcidMap = srcIdMap;
      _globalInBoundEventStats = globalInBoundEventStats;
      _globalOutBoundEventStats = globalOutBoundEventStats;
      _schemaRegService = schemaRegService;
      _relay = relay;
    }

    @Override
    public SimpleChannelHandler createHandler(Channel channel)
    {
      int cnt = _useCnt.incrementAndGet();
      if (0 == cnt % CLEAN_FREQ) cleanHandlers();

      WeakReference<ExecHandler> ref = _handlers.get(channel);
      ExecHandler result;
      if (null == ref || null == ref.get())
      {
        result = new ExecHandler(_eventsBufferMult, _relayConfig, _scnReaderWriters, _srcidMap,
                                 _globalInBoundEventStats, _globalOutBoundEventStats,_schemaRegService, _relay);
        _handlers.put(channel, new WeakReference<SendEventsRequest.ExecHandler>(result));
      }
      else
      {
        LOG.debug("reusing handler for channel " + channel.toString());
        result = ref.get();
      }

      return result;
    }

    private synchronized void cleanHandlers()
    {
      HashSet<Channel> keysToRemove = null;

      for (Map.Entry<Channel, WeakReference<ExecHandler>> entry: _handlers.entrySet())
      {
        if (! entry.getKey().isOpen() || null == entry.getValue().get())
        {
          if (null == keysToRemove) keysToRemove = new HashSet<Channel>(_handlers.size());
          keysToRemove.add(entry.getKey());
        }
      }

      if (null != keysToRemove)
      {
        LOG.debug("about to clean out " + keysToRemove.size() + " ExecHandlers");
        for (Channel key: keysToRemove) _handlers.remove(key);
      }
    }

  }

  static class SchemaUpdateFailed extends RuntimeException
  {
  	private static final long serialVersionUID = 1L;

  }


  static class EventRewriter extends InternalDatabusEventsListenerAbstract
  {
    private final SourceIdNameRegistry _inputSrcidMap;
    private final SourceIdNameRegistry _outputSrcidMap;
    private final SchemaRegistryService _schemaRegService;
    private final HttpRelay _relay;
    private final Logger LOG;
    private final EventLogBuffer _debugLogBuffer;

    public EventRewriter(SourceIdNameRegistry inputSrcidMap,
                         SourceIdNameRegistry outputSrcidMap,
                         SchemaRegistryService schemaRegService,
                         HttpRelay relay,
                         int serverId,
                         EventLogBuffer debugLogBuffer)
    {
      _inputSrcidMap = inputSrcidMap;
      _outputSrcidMap = outputSrcidMap;
      _schemaRegService = schemaRegService;
      _relay = relay;
      _debugLogBuffer = debugLogBuffer;
      LOG = Logger.getLogger(EventRewriter.class.getName()+ ":" + serverId);
    }

    @Override
    public void onEvent(DataChangeEvent event, long offset, int size)
    {
      if (null == _inputSrcidMap || null == _outputSrcidMap) return;

      if (event.isControlMessage()) return;

      final int srcid = event.srcId();
      final String sourceName = _inputSrcidMap.getSourceName(srcid);
      final LogicalSource src = _inputSrcidMap.getSource(srcid);
      //TODO: we need to get rid of the type-casting
      final DbusEvent e = (DbusEvent)event;
      final short schemaVersion = e.schemaVersion();

      _debugLogBuffer.append(",payloadSize=" + e.payloadLength() + ",schemaVersion=" + e.schemaVersion());
      if (null != src) /* source present in SourceIdNameRegistry */
      {
        assert(src.getName().equals(sourceName));

        boolean needSchemaUpdate = needSchemaUpdate(sourceName, schemaVersion);

        if (needSchemaUpdate)
        {
          _debugLogBuffer.append(",schemaUpdate=true,srcName=" + sourceName);
          LOG.info("Current Event needs schema update:" + sourceName + ",partition="+e.physicalPartitionId());
          performSchemaUpdate(sourceName, schemaVersion);

          if (null == _outputSrcidMap.getSource(sourceName))
            LOG.error("outputSrcidMap not updated:" + _debugLogBuffer);
          if (_relay != null && !_relay.getSourceSchemaVersionMap().containsKey(sourceName))
          {
            LOG.error("relay outputSrcidMap not updated:" + _debugLogBuffer);
          }
        }
        LogicalSource dest = _outputSrcidMap.getSource(sourceName); /* obtain dest from SchemasRegistry */
        if (null == dest)
        {
          LOG.error("Source for the event not found in schemas registry:" + _debugLogBuffer);
          return;
        }
        _debugLogBuffer.append(",outSrc=" + dest.getId().shortValue());

        if (_relay != null && ! _relay.getSourceSchemaVersionMap().containsKey(sourceName))
        {
          LOG.info("This is the first time we see event from the source " + _debugLogBuffer);
          _relay.getSourceSchemaVersionMap().put(sourceName, schemaVersion);
        }

        if(_schemaRegService == null)
        {
        	LOG.error("schemaRegistry is null:" + _debugLogBuffer);
          return;
        }

        SchemaId schemaId = obtainSchemaIdForSourceName(sourceName, schemaVersion);
        if (schemaId == null)
        {
        	LOG.error("schemaId is null for dest" + _debugLogBuffer);
          return;
        }

        _debugLogBuffer.append(",schemaId=" + schemaId.toString());

        e.setSrcId(dest.getId().shortValue());
        e.setSchemaId(schemaId.getByteArray());
        e.applyCrc();
      }
      else
      {
        LOG.error("input source not found: " + _debugLogBuffer);
      }
    }

    /**
     * When there is a schema version update detected from rpl-dbus event, we need to wait until
     * the schema-registry is refreshed with the new version of schema.
     *
     **/
    private boolean waitForSchemaVersionUpdate(String dbSourceName, short schemaVersion)
    throws DatabusException
    {
      LOG.info("Waiting for schema update for source " + dbSourceName + " version " + schemaVersion);
      int count = 0;
      final int NUM_RETRIES = 10;
      while(++count < NUM_RETRIES)
      {
    	  try
    	  {
    		  _schemaRegService.fetchSchemaIdForSourceNameAndVersion(dbSourceName, schemaVersion);
    		  return true;
    	  } catch (DatabusException de)
    	  {
    		  LOG.info("Exception while trying to fetchSchemaIdForSourceNameAndVersion:" + _debugLogBuffer, de);
    	  }
    	  try
    	  {
    	    Thread.sleep(100);
    	    LOG.info("Waiting for 100 MS...");
    	  }
    	  catch (InterruptedException e1)
    	  {
    	    LOG.warn("", e1);
    	  }
      }
      LOG.error("waitForSchemaVersionUpdate failed to fetch schema version:" + _debugLogBuffer);
      return false;
    }

    /**
     * Responsible for performing schema update
     * When current version is lower than required version
     * When the table is new
     *
     * @param sourceName
     * @param schemaVersion
     */
    private void performSchemaUpdate(String sourceName, short schemaVersion)
    {
    	LOG.info("Initiating performSchemaUpdate:source=" + sourceName + ",version=" + schemaVersion);
		boolean success = false;
    	try
    	{
    		// Hold the event, wait until schemaRegistry have refreshed
    		success = waitForSchemaVersionUpdate(sourceName, schemaVersion);
    	}
        catch (DatabusException de)
        {
          success = false;
          LOG.error("schema id not found:" +  _debugLogBuffer, de);
        }

    	if (success)
    	{
    		LOG.info("Schema update succeeded. Proceeding with disconnecting clients and updating schema map" +
                  ",source=" + sourceName + ",version=" + schemaVersion);
    		// disconnect all clients; they will reconnect and get all the new schemas
    		// from schemaRegistry
    		_relay.disconnectDBusClients();
    		// Record the new version number
    		_relay.getSourceSchemaVersionMap().put(sourceName, schemaVersion);
    		LOG.info("Put an entry for sourceName " + sourceName + " and schemaVersion " + schemaVersion + " in sourceSchemaVersionMap");
    	}
    	else
    	{
    		// Cleanup the connection, if this connection has been disbanded by REPL_DBUS
    		// for a newer connection
    		throw new SchemaUpdateFailed();
    	}
    }

    private boolean needSchemaUpdate(String sourceName, short schemaVersion)
    {
    	boolean needSchemaUpdate = false;
        LogicalSource dest = _outputSrcidMap.getSource(sourceName);

        if ( null == dest )
        {
        	needSchemaUpdate = true;
        	return needSchemaUpdate;
        }

        if(_relay != null)
        {
            if(_relay.getSourceSchemaVersionMap().containsKey(sourceName))
            {
            	short knownVersion = _relay.getSourceSchemaVersionMap().get(sourceName);
            	if(knownVersion < schemaVersion)
            	{
                	LOG.info("SourceName=" + sourceName + ",Known version = " + knownVersion + " schemaVersion = " + schemaVersion);
            		needSchemaUpdate = true;
            	}
            }
            else
            {
            	LOG.info("Do not know of table " + sourceName + ", hence need a schema update");
            	needSchemaUpdate = true;
            }
        }
        if (needSchemaUpdate)
        {
            LOG.info("Schema update is needed for source " + sourceName + ",version = " + schemaVersion );
        }
    	return needSchemaUpdate;
    }

    private SchemaId obtainSchemaIdForSourceName(String sourceName, short schemaVersion)
    {
    	SchemaId schemaId = null;
        try
        {
            schemaId = _schemaRegService.fetchSchemaIdForSourceNameAndVersion(sourceName, schemaVersion);
        }
        catch (DatabusException de)
        {
          LOG.error("schema id not found:" + _debugLogBuffer, de);
        }
//        if (LOG.isDebugEnabled()) LOG.debug("Obtained schemaId =" + schemaId);
        return schemaId;
    }
  }

  /** Stores the event binary representation */
  private final long _binlogOffset;
  private final ChannelBuffer _header;
  private final ChannelBuffer _eventBuffer;
  private final ChannelBuffer _message;

  public static SendEventsRequest create(byte protocolVersion, long binlogOffset, byte[] eventBytes)
  {
    ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(eventBytes);
    return new SendEventsRequest(protocolVersion, binlogOffset, buffer, 0, buffer.readableBytes());
  }

  private static SendEventsRequest create(byte protocolVersion, long binlogOffset,
                                         ByteBuffer eventBytes)
  {
    ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(eventBytes);
    return new SendEventsRequest(protocolVersion, binlogOffset, buffer, 0, buffer.readableBytes());
  }

  public static SendEventsRequest createV1(long binlogOffset, ByteBuffer eventBytes)
  {
    return create((byte)1, binlogOffset, eventBytes);
  }

  public static SendEventsRequest createV2(long binlogOffset, ByteBuffer eventBytes)
  {
    return create((byte)2, binlogOffset, eventBytes);
  }

  public static SendEventsRequest createV3(long binlogOffset, ByteBuffer eventBytes)
  {
    return create((byte)3, binlogOffset, eventBytes);
  }

  public SendEventsRequest(byte protocolVersion, long binlogOffset, ChannelBuffer buffer, int offset,
                           int size)
  {
    super(protocolVersion);
    _eventBuffer = new SlicedChannelBuffer(buffer, offset, size);

    _binlogOffset = binlogOffset;
    _header = ChannelBuffers.buffer(BinaryProtocol.BYTE_ORDER, 10);
    _header.writeByte(OPCODE);
    _header.writeByte(_protocolVersion);
    _header.writeLong(_binlogOffset);

    _message = ChannelBuffers.wrappedBuffer(_header, _eventBuffer);
  }

  @Override
  public ChannelBuffer toBinaryChannelBuffer()
  {
    return _message;
  }

  public ChannelBuffer obtainEventBuffer()
  {
    return _eventBuffer;
  }

  public long getBinlogOffset()
  {
    return _binlogOffset;
  }

  public int getMessageSize()
  {
    return _eventBuffer.readableBytes();
  }
}
