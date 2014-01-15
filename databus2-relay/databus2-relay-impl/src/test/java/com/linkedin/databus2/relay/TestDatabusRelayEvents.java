package com.linkedin.databus2.relay;
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.databus.client.DatabusSourcesConnection;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventBufferAppendable;
import com.linkedin.databus.core.DbusEventBufferMult;
import com.linkedin.databus.core.DbusEventFactory;
import com.linkedin.databus.core.DbusEventInfo;
import com.linkedin.databus.core.DbusEventInternalWritable;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.DbusEventV1;
import com.linkedin.databus.core.DbusEventV2;
import com.linkedin.databus.core.DbusEventV2Factory;
import com.linkedin.databus.core.DbusOpcode;
import com.linkedin.databus.core.InvalidEventException;
import com.linkedin.databus.core.KeyTypeNotImplementedException;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus.core.util.RngUtils;
import com.linkedin.databus.core.util.Utils;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.producers.RelayEventProducer;
import com.linkedin.databus2.producers.RelayEventProducer.DatabusClientNettyThreadPools;
import com.linkedin.databus2.relay.TestDatabusRelayMain.ClientRunner;
import com.linkedin.databus2.relay.TestDatabusRelayMain.CountingConsumer;
import com.linkedin.databus2.relay.config.PhysicalSourceConfig;
import com.linkedin.databus2.relay.util.test.DatabusRelayTestUtil;
import com.linkedin.databus2.test.ConditionCheck;
import com.linkedin.databus2.test.TestUtil;

public class TestDatabusRelayEvents
{
  public static final Logger LOG = Logger.getLogger(TestDatabusRelayEvents.class);
  static DbusEventFactory _eventFactory = new DbusEventV2Factory();

  static {
    TestUtil.setupLoggingWithTimestampedFile(true, "/tmp/TestDatabusRelayEvents_", ".log", Level.INFO);
    LOG.setLevel(Level.INFO);
  }


  @Test
  /**
   * append event V2 to the buffer and stream it to the client
   * which only accepts events V1. Make sure it got converted
   */
  public void testEventConversion() throws InterruptedException, IOException, DatabusException
  {
    final Logger log = Logger.getLogger("TestDatabusRelayEvents.testEventConversion");
    log.setLevel(Level.INFO);

    DatabusRelayTestUtil.RelayRunner r1=null;
    ClientRunner cr = null;
    try
    {
      String[] srcs = { "com.linkedin.events.example.fake.FakeSchema"};

      int pId = 1;
      int srcId = 2;

      int relayPort = Utils.getAvailablePort(11994);;
      final DatabusRelayMain relay1 = createRelay(relayPort, pId, srcs);
      Assert.assertNotNull(relay1);
      r1 = new DatabusRelayTestUtil.RelayRunner(relay1);
      log.info("Relay created");

      DbusEventBufferMult bufMult = relay1.getEventBuffer();


      String pSourceName = DatabusRelayTestUtil.getPhysicalSrcName(srcs[0]);
      PhysicalPartition pPartition = new PhysicalPartition(pId, pSourceName);
      DbusEventBufferAppendable buf = bufMult.getDbusEventBufferAppendable(pPartition);
      DbusEventKey key = new DbusEventKey(123L);
      byte[] schemaId = relay1.getSchemaRegistryService().fetchSchemaIdForSourceNameAndVersion(srcs[0], 2).getByteArray();
      byte[] payload = RngUtils.randomString(100).getBytes(Charset.defaultCharset());
      DbusEventInfo eventInfo = new DbusEventInfo(DbusOpcode.UPSERT, 100L, (short)pId, (short)pId, 897L,
                                                  (short)srcId, schemaId, payload, false, true);
      eventInfo.setEventSerializationVersion(DbusEventFactory.DBUS_EVENT_V2);
      buf.startEvents();
      buf.appendEvent(key, eventInfo, null);
      buf.endEvents(100L, null);
      r1.start();
      log.info("Relay started");

      // wait until relay comes up
      TestUtil.assertWithBackoff(new ConditionCheck() {
        @Override
        public boolean check() {
          return relay1.isRunningStatus();
        }
      },"Relay hasn't come up completely ", 7000, LOG);

      // now create client:
      String srcSubscriptionString = TestUtil.join(srcs, ",");
      String serverName = "localhost:" + relayPort;
      final EventsCountingConsumer countingConsumer = new EventsCountingConsumer();

      int id = (RngUtils.randomPositiveInt() % 10000) + 1;
      DatabusSourcesConnection clientConn = RelayEventProducer
      .createDatabusSourcesConnection("testProducer", id, serverName,
          srcSubscriptionString, countingConsumer,
          1 * 1024 * 1024, 50000, 30 * 1000, 100, 15 * 1000,
          1, true, DatabusClientNettyThreadPools.createNettyThreadPools(id),
          0, DbusEventFactory.DBUS_EVENT_V1,0);

      cr = new ClientRunner(clientConn);

      cr.start();
      log.info("Consumer started");
      // wait till client gets the event
      TestUtil.assertWithBackoff(new ConditionCheck() {
        @Override
        public boolean check() {
          return countingConsumer.getNumDataEvents() == 1;
        }
      },"Consumer didn't get any events ", 64 * 1024, LOG);

      // asserts
      Assert.assertEquals(1, countingConsumer.getNumDataEvents());
      Assert.assertEquals(1, countingConsumer.getNumWindows());
      Assert.assertEquals(1, countingConsumer.getNumDataEvents(DbusEventFactory.DBUS_EVENT_V1));

    } finally {
      cleanup ( new DatabusRelayTestUtil.RelayRunner[] {r1} , cr);
    }
  }

  /**
   * create a test relay with event producer turned off
   */
  private DatabusRelayMain createRelay(int relayPort, int pId, String[] srcs) throws IOException, DatabusException {

    // create main relay with random generator
    PhysicalSourceConfig[] srcConfigs = new PhysicalSourceConfig[srcs.length];

    String pSourceName = DatabusRelayTestUtil.getPhysicalSrcName(srcs[0]);

    PhysicalSourceConfig src1 = DatabusRelayTestUtil.createPhysicalConfigBuilder(
                                                                                 (short) pId, pSourceName, "mock",
                                                                                 500, 0, srcs);

    srcConfigs[0] = src1;

    HttpRelay.Config httpRelayConfig = DatabusRelayTestUtil.createHttpRelayConfig(1002, relayPort, 3024);
    httpRelayConfig.setStartDbPuller("false"); // do not produce any events
    httpRelayConfig.getSchemaRegistry().getFileSystem().setSchemaDir("TestDatabusRelayEvents_schemas");
    final DatabusRelayMain relay1 = DatabusRelayTestUtil.createDatabusRelay(srcConfigs, httpRelayConfig);
    Assert.assertNotNull(relay1);

    return relay1;
  }

  /**
   *  adds create a bytebuffer with a serialized event and EOW event using readEvents interface
   * @param scn - scn of the event
   * @param srcId - src id of the event
   * @param pId - partition id of the event
   * @param ver - serialization version of the event (V1 or V2)
   * @return - buffer with serialized events
   * @throws KeyTypeNotImplementedException
   */
  private ByteBuffer addEvent(long scn, short srcId, byte[] schemaId,short pId, byte ver) throws KeyTypeNotImplementedException
  {
    DbusEventKey key = new DbusEventKey(123L);
    byte[] payload = RngUtils.randomString(100).getBytes(Charset.defaultCharset());
    DbusEventInfo eventInfo = new DbusEventInfo(DbusOpcode.UPSERT, scn, pId, pId, 897L,
                                                srcId, schemaId, payload, false, true);
    eventInfo.setEventSerializationVersion(ver);

    // serialize it into a buffer
    int newEventSize = DbusEventFactory.computeEventLength(key, eventInfo);

    // now create an end of window event
    DbusEventInfo eventInfoEOP = new DbusEventInfo(null, scn, pId, pId, // must be the same as physicalPartition
                                                   900L,
                                                   DbusEventInternalWritable.EOPMarkerSrcId,
                                                   DbusEventInternalWritable.emptyMd5,
                                                   DbusEventInternalWritable.EOPMarkerValue,
                                                   false, //enable tracing
                                                   true // autocommit
                                                   );
    eventInfoEOP.setEventSerializationVersion(_eventFactory.getVersion());

    // serialize it into buffer
    int newEventSizeEOP = DbusEventFactory.computeEventLength(DbusEventInternalWritable.EOPMarkerKey, eventInfoEOP);
    int totalSize = newEventSize + newEventSizeEOP;
    ByteBuffer serializationBuffer = ByteBuffer.allocate(totalSize);
    serializationBuffer.order(ByteOrder.BIG_ENDIAN);
    // event itself
    int size = DbusEventFactory.serializeEvent(key, serializationBuffer, eventInfo);
    // EOP
    int size1 = _eventFactory.serializeLongKeyEndOfPeriodMarker(serializationBuffer, eventInfoEOP);
    assert totalSize == (size+size1);
    serializationBuffer.flip();

    return serializationBuffer;
  }

  /**
   * Stuffs an event buffer with both a v1 and a v2 event, then reads the buffer two ways:
   * first accepting only v1 events (verifying conversion of the v2 event to v1); then accepting
   * both v1 and v2 events.
   *
   * Note that the version of the _EOP_ events must match the version of the event factory,
   * regardless of the versions of any preceding "real" events.  (This matches DbusEventBuffer
   * behavior; see the serializeLongKeyEndOfPeriodMarker() call in endEvents() for details.)
   */
  @Test
  public void testV2Events()
      throws KeyTypeNotImplementedException, InvalidEventException, IOException, DatabusException
  {
    final Logger log = Logger.getLogger("TestDatabusRelayEvents.testV2Events");
    log.setLevel(Level.DEBUG);

    String[] srcs = { "com.linkedin.events.example.fake.FakeSchema"};
    String pSourceName = DatabusRelayTestUtil.getPhysicalSrcName(srcs[0]);

    short srcId = 2;
    short pId = 1;
    int relayPort = Utils.getAvailablePort(11993);

    // create relay
    final DatabusRelayMain relay1 = createRelay(relayPort, pId, srcs);
    DatabusRelayTestUtil.RelayRunner r1=null;
    ClientRunner cr = null;
    try
    {
      //EventProducer[] producers = relay1.getProducers();
      r1 = new DatabusRelayTestUtil.RelayRunner(relay1);
      log.info("Relay created");

      DbusEventBufferMult bufMult = relay1.getEventBuffer();
      PhysicalPartition pPartition = new PhysicalPartition((int)pId, pSourceName);
      DbusEventBuffer buf = (DbusEventBuffer)bufMult.getDbusEventBufferAppendable(pPartition);

      log.info("create some events");
      long windowScn = 100L;
      ByteBuffer serializationBuffer = addEvent(windowScn, srcId, relay1.getSchemaRegistryService().fetchSchemaIdForSourceNameAndVersion(srcs[0], 2).getByteArray(),
          pId, DbusEventFactory.DBUS_EVENT_V2);
      ReadableByteChannel channel = Channels.newChannel(new ByteBufferInputStream(serializationBuffer));
      int readEvents = buf.readEvents(channel);
      log.info("successfully read in " + readEvents + " events ");
      channel.close();

      windowScn = 101L;
      serializationBuffer = addEvent(windowScn, srcId, relay1.getSchemaRegistryService().fetchSchemaIdForSourceNameAndVersion(srcs[0], 2).getByteArray(),
          pId, DbusEventFactory.DBUS_EVENT_V1);
      channel = Channels.newChannel(new ByteBufferInputStream(serializationBuffer));
      readEvents = buf.readEvents(channel);
      log.info("successfully read in " + readEvents + " events ");
      channel.close();

      log.info("starting relay on port " + relayPort);
      r1.start();
      //TestUtil.sleep(10*1000);

      // wait until relay comes up
      TestUtil.assertWithBackoff(new ConditionCheck() {
        @Override
        public boolean check() {
          return relay1.isRunningStatus();
        }
      },"Relay hasn't come up completely ", 30000, LOG);

      log.info("now create client");
      String srcSubscriptionString = TestUtil.join(srcs, ",");
      String serverName = "localhost:" + relayPort;
      final EventsCountingConsumer countingConsumer = new EventsCountingConsumer();

      int id = (RngUtils.randomPositiveInt() % 10000) + 1;
      DatabusSourcesConnection clientConn = RelayEventProducer
      .createDatabusSourcesConnection("testProducer", id, serverName,
          srcSubscriptionString, countingConsumer,
          1 * 1024 * 1024, 50000, 30 * 1000, 100, 15 * 1000,
          1, true, DatabusClientNettyThreadPools.createNettyThreadPools(id),
          0, DbusEventFactory.DBUS_EVENT_V1,0);

      cr = new ClientRunner(clientConn);

      log.info("starting client");
      cr.start();
      // wait till client gets the event
      TestUtil.assertWithBackoff(new ConditionCheck() {
        @Override
        public boolean check() {
          int events = countingConsumer.getNumDataEvents();
          LOG.info("client got " + events + " events");
          return events == 2;
        }
      },"Consumer didn't get 2 events ", 64 * 1024, LOG);

      // asserts
      Assert.assertEquals(countingConsumer.getNumDataEvents(), 2);
      Assert.assertEquals(countingConsumer.getNumWindows(), 2);
      Assert.assertEquals(countingConsumer.getNumDataEvents(DbusEventFactory.DBUS_EVENT_V1),2);
      log.info("shutdown first client");
      clientConn.stop();
      cr.shutdown();
      TestUtil.sleep(1000);
      cr = null;


      log.info("start another client who understands V2");
      final EventsCountingConsumer countingConsumer1 = new EventsCountingConsumer();

      clientConn = RelayEventProducer
      .createDatabusSourcesConnection("testProducer", id, serverName,
          srcSubscriptionString, countingConsumer1,
          1 * 1024 * 1024, 50000, 30 * 1000, 100, 15 * 1000,
          1, true, DatabusClientNettyThreadPools.createNettyThreadPools(id),
          0, DbusEventFactory.DBUS_EVENT_V2,0);

      cr = new ClientRunner(clientConn);

      cr.start();
      log.info("wait till client gets the event");
      TestUtil.assertWithBackoff(new ConditionCheck() {
        @Override
        public boolean check() {
          int events = countingConsumer1.getNumDataEvents();
          LOG.debug("client got " + events + " events");
          return events == 2;
        }
      },"Consumer didn't get 2 events ", 64 * 1024, LOG);

      // asserts
      Assert.assertEquals(countingConsumer1.getNumDataEvents(), 2);
      Assert.assertEquals(countingConsumer1.getNumWindows(), 2);
      Assert.assertEquals(countingConsumer1.getNumDataEvents(DbusEventFactory.DBUS_EVENT_V1), 1);
      Assert.assertEquals(countingConsumer1.getNumDataEvents(DbusEventFactory.DBUS_EVENT_V2), 1);

    } finally {
      cleanup ( new DatabusRelayTestUtil.RelayRunner[] {r1} , cr);
    }

  }

  //cleanup
  void cleanup(DatabusRelayTestUtil.RelayRunner[] relayRunners,ClientRunner clientRunner)
  {
    LOG.info("Starting cleanup");
    for (DatabusRelayTestUtil.RelayRunner r1: relayRunners)
    {
      if(null != r1)
        Assert.assertTrue(r1.shutdown(2000));
    }
    //Assert.assertNotNull(clientRunner);
    if(clientRunner != null)
      clientRunner.shutdown();
    LOG.info("Finished cleanup");
  }

  static public class EventsCountingConsumer extends  CountingConsumer {
    protected int _numEventsV1 = 0;
    protected int _numEventsV2 = 0;

    @Override
    public ConsumerCallbackResult onDataEvent(DbusEvent e,
        DbusEventDecoder eventDecoder)
    {
      if(e instanceof DbusEventV1) {
        LOG.debug("got event v=: " + ((DbusEventV1)e).getVersion() + ":"  + e);
        _numEventsV1 ++;
      } else {
        LOG.debug("got event v=: " + ((DbusEventV2)e).getVersion() + ":"  + e);
        _numEventsV2 ++;
      }
      return super.onDataEvent(e, eventDecoder);
    }
    public int getNumDataEvents(int ver) {
      if(ver == DbusEventFactory.DBUS_EVENT_V1) {
        return this._numEventsV1;
      }
      if(ver == DbusEventFactory.DBUS_EVENT_V2) {
        return this._numEventsV2;
      }
      return 0;
    }
  }

}
