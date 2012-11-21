package com.linkedin.databus.container.request.tcp;

import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.easymock.EasyMock;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventBuffer.DbusEventIterator;
import com.linkedin.databus.core.DbusEventBufferMult;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.data_model.LogicalSource;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus2.core.container.DummyPipelineFactory;
import com.linkedin.databus2.core.container.request.BinaryProtocol;
import com.linkedin.databus2.core.container.request.CommandsRegistry;
import com.linkedin.databus2.core.seq.MultiServerSequenceNumberHandler;
import com.linkedin.databus2.core.seq.SequenceNumberHandlerFactory;
import com.linkedin.databus2.core.seq.ServerName;
import com.linkedin.databus2.schemas.SchemaId;
import com.linkedin.databus2.schemas.SchemaRegistryService;
import com.linkedin.databus2.schemas.SourceIdNameRegistry;
import com.linkedin.databus2.test.ConditionCheck;
import com.linkedin.databus2.test.TestUtil;
import com.linkedin.databus2.test.container.SimpleTestClientConnection;
import com.linkedin.databus2.test.container.SimpleTestServerConnection;

public class TestSendEventsExecHandler2 extends ContainerTestsBase {
  SchemaRegistryService _schemaResistry;
  SimpleTestClientConnection _clientConn;
  SimpleTestServerConnection _srvConn;
  HttpRelay.StaticConfig _relayConfig;
  DbusEventBufferMult _eventBufMult;
  int _serverId;
  long _binlogOfs;

  @BeforeClass
  public void setUpClass()
  {
    TestUtil.setupLogging(true, null, Level.ERROR);
  }

  @Override
  @BeforeMethod
  public void setUp() throws Exception
  {
    DbusEvent.byteOrder = BinaryProtocol.BYTE_ORDER;

    super.setUp();

    _schemaResistry = EasyMock.createMock(SchemaRegistryService.class);
    String md5 = "1234567890123456";
    SchemaId schemaId = new SchemaId(md5.getBytes());
    EasyMock.expect(_schemaResistry.fetchSchemaIdForSourceNameAndVersion((String)EasyMock.anyObject(), EasyMock.anyInt())).andReturn(schemaId).anyTimes();
    EasyMock.replay(_schemaResistry);


    _serverId = 10;
    _binlogOfs = 665L;
    commonSetup(_binlogOfs, _serverId);

  }


  @AfterMethod
  public void tearDown() {
    if(_srvConn != null)
      _srvConn.stop();
    if(_clientConn != null)
      _clientConn.stop();
  }

  private void commonSetup(long binlogOfs, int serverId) throws Exception {
    _eventBufMult = createStandardEventBufferMult();

    HttpRelay.Config relayConfigBuilder = createStandardRelayConfigBuilder();

    _relayConfig = relayConfigBuilder.build();
    SequenceNumberHandlerFactory handlerFactory =
        _relayConfig.getDataSources().getSequenceNumbersHandler().createFactory();

    MultiServerSequenceNumberHandler multiServerSeqHandler =
        new MultiServerSequenceNumberHandler(handlerFactory);

    multiServerSeqHandler.writeLastSequenceNumber(new ServerName(serverId), binlogOfs);
    multiServerSeqHandler.writeLastSequenceNumber(new ServerName(serverId+1), binlogOfs);

    SourceIdNameRegistry sourcesReg =
        SourceIdNameRegistry.createFromIdNamePairs(_relayConfig.getSourceIds());

    SendEventsRequest.ExecHandlerFactory sendEventsExecFactory =
        new SendEventsRequest.ExecHandlerFactory(_eventBufMult, _relayConfig,
                                                 multiServerSeqHandler,
                                                 sourcesReg, null, null, _schemaResistry, null);
    CommandsRegistry cmdsRegistry = new CommandsRegistry();
    cmdsRegistry.registerCommand(null, StartSendEventsRequest.OPCODE,
                                 new StartSendEventsRequest.BinaryParserFactory(),
                                 sendEventsExecFactory);
    cmdsRegistry.registerCommand(null, SendEventsRequest.OPCODE,
                                 new SendEventsRequest.BinaryParserFactory(),
                                 sendEventsExecFactory);

    _srvConn = new SimpleTestServerConnection(DbusEvent.byteOrder);
    _srvConn.setPipelineFactory(new DummyPipelineFactory.DummyServerPipelineFactory(cmdsRegistry));
    boolean serverStarted = _srvConn.startSynchronously(104, 100);
    Assert.assertTrue(serverStarted, "server started");

    _clientConn = new SimpleTestClientConnection(DbusEvent.byteOrder);
    _clientConn.setPipelineFactory(new DummyPipelineFactory.DummyClientPipelineFactory());
    boolean clientConnected = _clientConn.startSynchronously(104, 100);
    Assert.assertTrue(clientConnected, "client connected");
  }

  private void validateStartSendEventResponse(Channel clientChannel, long expectedBinLogOfs) {
  	//check the response
  	ChannelPipeline clientPipe = clientChannel.getPipeline();
  	DummyPipelineFactory.SimpleResponseBytesAggregatorHandler respAggregator =
  		(DummyPipelineFactory.SimpleResponseBytesAggregatorHandler)clientPipe.get(
  				TestSimpleBinaryDatabusRequestDecoder.RESPONSE_AGGREGATOR_NAME);

    //verify that we get valid response
    Assert.assertTrue(null != respAggregator, "has response aggregator");
    ChannelBuffer response1 = respAggregator.getBuffer();
    Assert.assertEquals(response1.readableBytes(), 1 + 4 + 8 + 4, "correct response size");
    byte resultCode = response1.readByte();
    Assert.assertEquals(resultCode, (byte)0);
    int responseSize = response1.readInt();
    Assert.assertEquals(responseSize, 8 + 4);
    long binlogOfs = response1.readLong();
    Assert.assertEquals(binlogOfs, expectedBinLogOfs);
    int maxTransSize = response1.readInt();
    Assert.assertEquals(maxTransSize, _defaultBufferConf.getReadBufferSize());

    //send SendEvents command
    respAggregator.clear();
  }

  private void validateEventResponse(Channel clientChannel, int expectedEventNum) {
  	//check the response
    ChannelPipeline clientPipe = clientChannel.getPipeline();
    DummyPipelineFactory.SimpleResponseBytesAggregatorHandler respAggregator =
        (DummyPipelineFactory.SimpleResponseBytesAggregatorHandler)clientPipe.get(
            TestSimpleBinaryDatabusRequestDecoder.RESPONSE_AGGREGATOR_NAME);

  	ChannelBuffer response = respAggregator.getBuffer();
  	Assert.assertEquals(response.readableBytes(), 1 + 4 + 4, "correct response size");
  	byte resultCode = response.readByte();
  	Assert.assertEquals(resultCode, (byte)0, "no error: " + resultCode);
  	int responseSize = response.readInt();
  	Assert.assertEquals(responseSize, 4);
  	int eventsNum = response.readInt();
  	Assert.assertEquals(eventsNum, expectedEventNum);

  	respAggregator.clear();
  }

  private void validateErrorResponse(Channel clientChannel, byte expectedErr) {
    //check the response
    ChannelPipeline clientPipe = clientChannel.getPipeline();
    DummyPipelineFactory.SimpleResponseBytesAggregatorHandler respAggregator =
        (DummyPipelineFactory.SimpleResponseBytesAggregatorHandler)clientPipe.get(
            TestSimpleBinaryDatabusRequestDecoder.RESPONSE_AGGREGATOR_NAME);

    ChannelBuffer response = respAggregator.getBuffer();
    Assert.assertTrue(response.readableBytes() > 1 + 4 + 4, "correct response size");
    byte resultCode = response.readByte();
    Assert.assertEquals(resultCode, expectedErr, "no error: " + resultCode);
  }

  /**
   * verifying that we can ignore events with invalid SCNs (coming from REPL_DB)
   * @throws Exception
   */
  @Test
  public void testSameScnError() throws Exception {
    final Logger log = Logger.getLogger("TestSendEventsExecHandler2.testSameScnError");
    //log.setLevel(Level.DEBUG);

    int serverId = _serverId;
    long binlogOfs = _binlogOfs;

    Channel clientChannel = _clientConn.getChannel();

    //sleep to let connection happen
    try {Thread.sleep(10);} catch (InterruptedException ie){};

    //Send GetLastSequence([101])

    HashMap<Integer, LogicalSource> origSources = new HashMap<Integer, LogicalSource>();
    for (IdNamePair pair: _relayConfig.getSourceIds())
    {
      LogicalSource newPair = new LogicalSource((pair.getId().intValue() - 100), pair.getName());
      origSources.put(newPair.getId(), newPair);
    }

    long scn = 100L;
    int pPart = 1;
    int expectedEventNum = 1;
    long expectedBinlogOfs = binlogOfs-1;
    /* five rounds:
    * get events with scn 100
    * get events with scn 101
    * get events with scn 100 for a different buffer coming in - it is ok
    * get events with scn 100 from a different serverId - should be ignored
    * get events with scn 100 for the same buffer coming in - it should be ignored
    */
    long tmp = 0;
    int part1_events = 0; int part2_events=0;
    scn = 100; // we increase scn for each event
    long tmpScn=-1;
    for (int i=1; i <= 45; i ++) {
      if(i>=1 && i<=11) { scn+=i; pPart = 1; serverId = 10; expectedBinlogOfs++; part1_events+=2;} // 22(including control) events
      if(i>=12 && i<=15) { scn+=i;; pPart = 1; serverId = 10; expectedBinlogOfs++; part1_events+=2;} // 8 events
      if(i>=16 && i<=20) { scn+=i;; pPart = 2; serverId = 10; expectedBinlogOfs++; part2_events+=2;} // 10 events for part2
      if(i>=21 && i<=25) { scn+=i;; pPart = 1; serverId = 10; expectedBinlogOfs++; part1_events+=2;} // 10(including control) events
      if(i==26) {tmpScn=scn; tmp = expectedBinlogOfs; expectedBinlogOfs = binlogOfs-1;} // remember state
      if(i>=26 && i<=30) {scn = 100; pPart = 1; serverId = 10+1;  expectedBinlogOfs++;  } // OLD SCN, should be ignored
      if(i==30) {scn = tmpScn;} // restore to good SCN
      if(i>=31 && i<=35) { scn+=i;; pPart = 1; serverId = 10+1;  expectedBinlogOfs++;  part1_events+=2;} //10 more events from different server
      if(i==36) {tmpScn = scn; expectedBinlogOfs = tmp; } // restore state
      if(i>=36 && i<=40 ) { scn = 102; pPart = 1; serverId = 10; expectedBinlogOfs++;  } // OLD SCN, should be ignored
      if(i==40) scn = tmpScn; // restore to good SCN
      if(i>=41 && i<=45 ) { /* using same scn */ pPart = 1; serverId = 10; expectedBinlogOfs++;  } // SAME SCN after EOW, should be ignored

      // total number of events for part1 = 20 + 8 + 10 + 10; part2 = 10

       // create an event and write it to the channel
      StartSendEventsRequest req1 = StartSendEventsRequest.createV3(serverId, origSources);
      final ChannelFuture req1WriteFuture = req1.writeToChannelAsBinary(clientChannel);

      TestUtil.assertWithBackoff(new ConditionCheck()
      {
        @Override
        public boolean check() {return req1WriteFuture.isDone();}
      }, "write completed", 1000, null);

      Assert.assertTrue(req1WriteFuture.isSuccess(), "write successful");

      // validate the handshake
      validateStartSendEventResponse(clientChannel, expectedBinlogOfs);

      //First generate some events
      PhysicalPartition pPartition = new PhysicalPartition(pPart,"name");
      DbusEventBuffer srcBuffer = new DbusEventBuffer(_defaultBufferConf, pPartition);

      // add events
      srcBuffer.start(1);
      srcBuffer.startEvents();
      srcBuffer.appendEvent(new DbusEventKey(i), (short)pPart, (short)i,
                            System.currentTimeMillis() * 1000000,
                            (short)1, new byte[16], new byte[5], false, null);
      srcBuffer.appendEvent(new DbusEventKey(i + 1), (short)pPart, (short)(i + 1),
                            System.currentTimeMillis() * 1000000,
                            (short)1, new byte[16], new byte[5], false, null);
      srcBuffer.endEvents(scn,  null);

      DbusEventIterator eventIter = srcBuffer.acquireIterator("srcIterator");

      //skip over the first initialization event
      Assert.assertTrue(eventIter.hasNext(), "iterator has event");
      eventIter.next();

      //compare events

      while(eventIter.hasNext()) {
        DbusEvent evt = eventIter.next();
        SendEventsRequest sendEvents1 =
            SendEventsRequest.createV3(expectedBinlogOfs+1, evt.getRawBytes().order(BinaryProtocol.BYTE_ORDER));

        final ChannelFuture send1WriteFuture = sendEvents1.writeToChannelAsBinary(clientChannel);
        if ( 1 == pPart) log.debug("sending event: # " + i + " scn=" + evt.sequence() + " srcid=" +
                                   evt.srcId() + " key=" + evt.key());

        TestUtil.assertWithBackoff(new ConditionCheck()
        {
          @Override
          public boolean check() {return send1WriteFuture.isDone();}
        }, "write completed", 1000, null);
        Assert.assertTrue(send1WriteFuture.isSuccess());

        //validate
        validateEventResponse(clientChannel, expectedEventNum);
      }
    }

    //compare events
    // verify that we got event 100, 101, 102, 103 but  DIDN'T get second batch of 100 and 102
    int totalEventsExpected = -1;
    for(int i=1; i<=2; i++) {
    	if(i==1) { totalEventsExpected = part1_events; }
    	if(i==2) { totalEventsExpected = part2_events; }
    	DbusEventBuffer eventBuffer = getBufferForPartition(_eventBufMult, i, "psource");
    	final DbusEventIterator resIter = eventBuffer.acquireIterator("resIterator");

    	Assert.assertTrue(resIter.hasNext(), "has result event");
    	int totalEventsInBuffer=0;
    	long maxScnInBuffer = -1;
    	while(resIter.hasNext()) {
    		DbusEvent resEvt = resIter.next();
    		Assert.assertTrue(resEvt.isValid(), "valid event");
    		long eventScn = resEvt.sequence();
    		Assert.assertTrue(eventScn >= maxScnInBuffer, "scn(" + scn + ") >= maxScn(" + maxScnInBuffer + ") in buffer");
    		maxScnInBuffer = eventScn;
    		int srcId = resEvt.srcId();
    		if(srcId == -2)
    			continue; // ignore control event
    		Assert.assertEquals(srcId, (short)101);
    		Assert.assertEquals(resEvt.physicalPartitionId(), i);
    		totalEventsInBuffer ++;
            log.debug("checking event: # " + totalEventsInBuffer + " scn=" + eventScn + " key=" +
                      resEvt.key());
    	}
    	Assert.assertEquals(totalEventsInBuffer, totalEventsExpected);
      eventBuffer.releaseIterator(resIter);
    }

  }


  /*
   * create a buffer with 2 events and EOW. send two events and then reset the connection
   * send the EOW - verify it fails with unrecoverable error
   */
  @Test
  public void testUnexpectedEOWError() throws Exception {

    int serverId = _serverId;
    long binlogOfs = _binlogOfs;

    Channel clientChannel = _clientConn.getChannel();

    //sleep to let connection happen
    try {Thread.sleep(10);} catch (InterruptedException ie){};

    //Send GetLastSequence([101])

    HashMap<Integer, LogicalSource> origSources = new HashMap<Integer, LogicalSource>();
    for (IdNamePair pair: _relayConfig.getSourceIds())
    {
      LogicalSource newPair = new LogicalSource((pair.getId().intValue() - 100), pair.getName());
      origSources.put(newPair.getId(), newPair);
    }

    long scn = 100L;
    int pPart = 1;
    long expectedBinlogOfs = binlogOfs;
    /*
     * send valid events without EOW
    */

    int i = 1;


    // **********************
    // hand shake
    StartSendEventsRequest req1 = StartSendEventsRequest.createV3(serverId, origSources);
    final ChannelFuture req1WriteFuture = req1.writeToChannelAsBinary(clientChannel);

    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check() {return req1WriteFuture.isDone();}
    }, "write completed", 1000, null);

    Assert.assertTrue(req1WriteFuture.isSuccess(), "write successful");

    // validate the handshake
    validateStartSendEventResponse(clientChannel, expectedBinlogOfs);

    //First generate some events
    PhysicalPartition pPartition = new PhysicalPartition(pPart,"name");
    DbusEventBuffer srcBuffer = new DbusEventBuffer(_defaultBufferConf, pPartition);

    // add events
    srcBuffer.start(1);
    srcBuffer.startEvents();
    srcBuffer.appendEvent(new DbusEventKey(i), (short)pPart, (short)i,
                          System.currentTimeMillis() * 1000000,
                          (short)1, new byte[16], new byte[5], false, null);
    srcBuffer.appendEvent(new DbusEventKey(i + 1), (short)pPart, (short)(i + 1),
                          System.currentTimeMillis() * 1000000,
                          (short)1, new byte[16], new byte[5], false, null);

    srcBuffer.endEvents(scn,  null);

    DbusEventIterator eventIter = srcBuffer.acquireIterator("srcIterator");

    //skip over the first initialization event
    Assert.assertTrue(eventIter.hasNext(), "iterator has event");
    eventIter.next();

    // send events
    boolean expectError = false;
    while(eventIter.hasNext()) {
      DbusEvent evt = eventIter.next();
      if(evt.isControlMessage()) {
        // create a new connection
        _clientConn = new SimpleTestClientConnection(DbusEvent.byteOrder);
        _clientConn.setPipelineFactory(new DummyPipelineFactory.DummyClientPipelineFactory());
        boolean clientConnected = _clientConn.startSynchronously(104, 100);
        Assert.assertTrue(clientConnected, "client connected");

        clientChannel = _clientConn.getChannel();
        //new handshake
        req1 = StartSendEventsRequest.createV3(serverId, origSources);
        final ChannelFuture req2WriteFuture = req1.writeToChannelAsBinary(clientChannel);

        TestUtil.assertWithBackoff(new ConditionCheck()
        {
          @Override
          public boolean check() {return req2WriteFuture.isDone();}
        }, "write completed", 1000, null);

        Assert.assertTrue(req2WriteFuture.isSuccess(), "write of 2nd handshake successful");

        // validate the handshake
        validateStartSendEventResponse(clientChannel, expectedBinlogOfs);
        expectError = true;
      }
      SendEventsRequest sendEvents1 =
          SendEventsRequest.createV3(expectedBinlogOfs+1, evt.getRawBytes().order(BinaryProtocol.BYTE_ORDER));

      final ChannelFuture send1WriteFuture = sendEvents1.writeToChannelAsBinary(clientChannel);

      TestUtil.assertWithBackoff(new ConditionCheck()
      {
        @Override
        public boolean check() {return send1WriteFuture.isDone();}
      }, "write completed", 1000, null);
      Assert.assertTrue(send1WriteFuture.isSuccess());

      //validate
      if(expectError)
        validateErrorResponse(clientChannel, BinaryProtocol.RESULT_ERR_UNEXPECTED_CONTROL_EVENT);
      else
        validateEventResponse(clientChannel, 1); // one event at a time


    }
    srcBuffer.releaseIterator(eventIter);
}


  @Test
  public void testScnZeroError() throws Exception {

    int serverId = _serverId;
    long binlogOfs = _binlogOfs;

    Channel clientChannel = _clientConn.getChannel();

    //sleep to let connection happen
    try {Thread.sleep(10);} catch (InterruptedException ie){};

    HashMap<Integer, LogicalSource> origSources = new HashMap<Integer, LogicalSource>();
    for (IdNamePair pair: _relayConfig.getSourceIds())
    {
      LogicalSource newPair = new LogicalSource((pair.getId().intValue() - 100), pair.getName());
      origSources.put(newPair.getId(), newPair);
    }

    long scn = 0L;
    int pPart = 1;
    long expectedBinlogOfs = binlogOfs-1;
    scn=0; pPart = 1; serverId = 10; expectedBinlogOfs++;

    // total number of events for part1 = 20 + 8 + 10 + 10; part2 = 10

    // create an event and write it to the channel
    StartSendEventsRequest req1 = StartSendEventsRequest.createV3(serverId, origSources);
    final ChannelFuture req1WriteFuture = req1.writeToChannelAsBinary(clientChannel);

    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check() {return req1WriteFuture.isDone();}
    }, "write completed", 1000, null);

    Assert.assertTrue(req1WriteFuture.isSuccess(), "write successful");

    // validate the handshake
    validateStartSendEventResponse(clientChannel, expectedBinlogOfs);

    //First generate some events
    PhysicalPartition pPartition = new PhysicalPartition(pPart,"name");
    DbusEventBuffer srcBuffer = new DbusEventBuffer(_defaultBufferConf, pPartition);

    // add events
    srcBuffer.start(0);
    srcBuffer.startEvents();
    srcBuffer.appendEvent(new DbusEventKey(0L), (short)pPart, (short)1,
                          System.currentTimeMillis() * 1000000,
                          (short)1, new byte[16], new byte[5], false, null);
    srcBuffer.endEvents(scn,  null);

    DbusEventIterator eventIter = srcBuffer.acquireIterator("srcIterator");

    //skip over the first initialization event
    Assert.assertTrue(eventIter.hasNext(), "iterator has event");
    eventIter.next();

    //compare events

    if(eventIter.hasNext()) {
      DbusEvent evt = eventIter.next();
      SendEventsRequest sendEvents1 =
          SendEventsRequest.createV3(expectedBinlogOfs+1, evt.getRawBytes().order(BinaryProtocol.BYTE_ORDER));

      final ChannelFuture send1WriteFuture = sendEvents1.writeToChannelAsBinary(clientChannel);

      TestUtil.assertWithBackoff(new ConditionCheck()
      {
        @Override
        public boolean check() {return send1WriteFuture.isDone();}
      }, "write completed", 1000, null);
      Assert.assertTrue(send1WriteFuture.isSuccess());

      //validate
      validateErrorResponse(clientChannel, BinaryProtocol.RESULT_ERR_INVALID_EVENT);
    }
  }


  // verify memory gets freed
  // create a Mult buffer
  // send an event to the buffer
  // remove the buffer
  // run gc
  // make sure the buffer was collected
  @Test
  public void testMemoryCleanUp() throws Exception {
    int serverId = _serverId;
    long binlogOfs = _binlogOfs;

    // get the current buffers
    // get them as weak references - not to create additional references to the objects
    Map<Integer, WeakReference<DbusEventBuffer>> dbufs = new HashMap<Integer, WeakReference<DbusEventBuffer>>();
    for(int i=0; i<3; i++) {
      DbusEventBuffer buf = _eventBufMult.getOneBuffer(_psourcesConfig[i].getPhysicalPartition());
      dbufs.put(i, new WeakReference<DbusEventBuffer>(buf));
    }

    // should be 1 reference for each
    for(WeakReference<DbusEventBuffer> dbuf : dbufs.values()) {
      Assert.assertNotNull(dbuf);
      DbusEventBuffer buf = dbuf.get();
      System.out.println("ref counter is " + buf.getRefCount());
      Assert.assertEquals(buf.getRefCount(), 1);
    }

    // Simulate sending some events to these buffers
    sendSomeEvents(binlogOfs, serverId);

    // now ask to delete these methods (we can put fake logica source for that)
    _eventBufMult.removeBuffer(_psourcesConfig[0]);
    _eventBufMult.removeBuffer(_psourcesConfig[1]);
    _eventBufMult.removeBuffer(_psourcesConfig[2]);
    _eventBufMult.deallocateRemovedBuffers(true);

    // all buffers should be removed
    for(int i=0; i<3; i++) {
      DbusEventBuffer buf = _eventBufMult.getOneBuffer(_psourcesConfig[i].getPhysicalPartition());
      Assert.assertNull(buf);
    }
    Runtime.getRuntime().gc(); // run gc - should clean up all the buffers

    // all bufs have to be null - because GC was supposed to collect it..
    // and if there are any other references - this will fail
    for(WeakReference<DbusEventBuffer> dbuf : dbufs.values()) {
      Assert.assertNotNull(dbuf);
      DbusEventBuffer buf = dbuf.get();
      Assert.assertNull(buf);
    }


  }

  // Auxiliary method
  private void sendSomeEvents(long binlogOfs, int serverId) {
    Channel clientChannel = _clientConn.getChannel();

    //sleep to let connection happen
    try {Thread.sleep(10);} catch (InterruptedException ie){};

    long scn = 100L;
    int pPart = 1;
    int expectedEventNum = 1;
    long expectedBinlogOfs = binlogOfs;

    HashMap<Integer, LogicalSource> origSources = new HashMap<Integer, LogicalSource>();
    for (IdNamePair pair: _relayConfig.getSourceIds())
    {
      LogicalSource newPair = new LogicalSource((pair.getId().intValue() - 100), pair.getName());
      origSources.put(newPair.getId(), newPair);
    }


    // create an event and write it to the channel
    StartSendEventsRequest req1 = StartSendEventsRequest.createV3(serverId, origSources);
    final ChannelFuture req1WriteFuture = req1.writeToChannelAsBinary(clientChannel);

    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check() {return req1WriteFuture.isDone();}
    }, "write completed", 1000, null);

    Assert.assertTrue(req1WriteFuture.isSuccess(), "write successful");

    // validate the handshake
    validateStartSendEventResponse(clientChannel, expectedBinlogOfs);

    //First generate some events
    PhysicalPartition pPartition = new PhysicalPartition(pPart,"name");
    DbusEventBuffer srcBuffer = new DbusEventBuffer(_defaultBufferConf, pPartition);

    // add events
    int i = 0;
    srcBuffer.start(1);
    srcBuffer.startEvents();
    srcBuffer.appendEvent(new DbusEventKey(i), (short)pPart, (short)i,
                            System.currentTimeMillis() * 1000000,
                            (short)1, new byte[16], new byte[5], false, null);
    srcBuffer.appendEvent(new DbusEventKey(i + 1), (short)pPart, (short)(i + 1),
                          System.currentTimeMillis() * 1000000,
                          (short)1, new byte[16], new byte[5], false, null);
    srcBuffer.endEvents(scn,  null);

    DbusEventIterator eventIter = srcBuffer.acquireIterator("srcIterator");

    //skip over the first initialization event
    Assert.assertTrue(eventIter.hasNext(), "iterator has event");
    eventIter.next();

    //compare events

    while(eventIter.hasNext()) {
      DbusEvent evt = eventIter.next();
      SendEventsRequest sendEvents1 =
          SendEventsRequest.createV3(expectedBinlogOfs+1, evt.getRawBytes().order(BinaryProtocol.BYTE_ORDER));

      final ChannelFuture send1WriteFuture = sendEvents1.writeToChannelAsBinary(clientChannel);

      TestUtil.assertWithBackoff(new ConditionCheck()
      {
        @Override
        public boolean check() {return send1WriteFuture.isDone();}
      }, "write completed", 1000, null);
      Assert.assertTrue(send1WriteFuture.isSuccess());

      //validate
      validateEventResponse(clientChannel, expectedEventNum);
    }

  }
}
