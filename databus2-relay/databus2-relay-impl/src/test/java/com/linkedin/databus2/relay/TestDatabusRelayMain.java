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

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Vector;
import java.util.concurrent.Executors;

import org.apache.avro.Schema;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.DefaultChannelPipeline;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpClientCodec;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.databus.client.DatabusSourcesConnection;
import com.linkedin.databus.client.SingleSourceSCN;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DatabusCombinedConsumer;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.client.pub.SCN;
import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.data_model.LogicalSource;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsTotalStats;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus.core.util.Utils;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.DatabusHttpHeaders;
import com.linkedin.databus2.core.container.netty.ServerContainer;
import com.linkedin.databus2.core.seq.MaxSCNReaderWriter;
import com.linkedin.databus2.producers.EventProducer;
import com.linkedin.databus2.producers.RelayEventProducer;
import com.linkedin.databus2.relay.config.LogicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import com.linkedin.databus2.relay.util.test.DatabusRelayTestUtil;
import com.linkedin.databus2.test.ConditionCheck;
import com.linkedin.databus2.test.TestUtil;

public class TestDatabusRelayMain
{
  static
  {
    TestUtil.setupLoggingWithTimestampedFile(true, "/tmp/TestDatabusRelayMain_", ".log", Level.INFO);
  }

  public final static Logger LOG = Logger.getLogger(TestDatabusRelayMain.class);
  public static final String SCHEMA_REGISTRY_DIR = "TestDatabusRelayMain_schemas";

	//cleanup
	void cleanup(DatabusRelayTestUtil.RelayRunner[] relayRunners,ClientRunner clientRunner)
	{
		LOG.info("Starting cleanup");
        Assert.assertNotNull(clientRunner);
        clientRunner.shutdown();
		for (DatabusRelayTestUtil.RelayRunner r1: relayRunners)
		{
			if(null != r1)
				Assert.assertTrue(r1.shutdown(2000));
		}
		LOG.info("Finished cleanup");
	}

	protected void resetSCN(DatabusRelayMain relay)
	{
		for (PhysicalSourceStaticConfig pConfig : relay.getPhysicalSources())
		{
			MaxSCNReaderWriter scnWriters = relay.getMaxSCNReaderWriter(pConfig);
			if (scnWriters != null)
			{
				try
				{
					scnWriters.saveMaxScn(0);
				}
				catch (DatabusException e)
				{
					LOG.error("Warn: cannot clear scn" + e);
				}
			}
			else
			{
				LOG.error("Could not find scn writer for " + pConfig.getPhysicalPartition());
			}
		}
	}

    public void assertRelayRunning(final HttpRelay relay, long timeoutMs, Logger log)
    {
      TestUtil.assertWithBackoff(new ConditionCheck()
      {
        @Override
        public boolean check()
        {
          return relay.isRunningStatus();
        }
      }, "wait for relay " + relay.getContainerStaticConfig().getHttpPort() + " to start", timeoutMs, log);
    }

	@Test
	public void testRelayEventGenerator() throws InterruptedException, InvalidConfigException
	{
		DatabusRelayTestUtil.RelayRunner r1=null;
		//Logger.getRootLogger().setLevel(Level.INFO);
		final Logger log = Logger.getLogger("TestDatabusRelayMain.testRelayEventGenerator");
		//log.setLevel(Level.DEBUG);


		ClientRunner cr = null;
		try
		{
			String[][] srcNames =
			{
			{ "com.linkedin.events.example.fake.FakeSchema",
			  "com.linkedin.events.example.person.Person" }, };

			log.info("create main relay with random generator");
			PhysicalSourceConfig[] srcConfigs = new PhysicalSourceConfig[srcNames.length];
			int i = 0;
			int eventRatePerSec = 20;
			for (String[] srcs : srcNames)
			{

				PhysicalSourceConfig src1 = DatabusRelayTestUtil.createPhysicalConfigBuilder(
						(short) (i + 1), DatabusRelayTestUtil.getPhysicalSrcName(srcs[0]), "mock",
						500, eventRatePerSec, srcs);
				srcConfigs[i++] = src1;
			}
			int relayPort = Utils.getAvailablePort(11993);
			final DatabusRelayMain relay1 = DatabusRelayTestUtil.createDatabusRelayWithSchemaReg(1001, relayPort,
					10 * 1024 * 1024, srcConfigs, SCHEMA_REGISTRY_DIR);
			Assert.assertNotEquals(relay1, null);
			r1 = new DatabusRelayTestUtil.RelayRunner(relay1);

			log.info("async starts");
			r1.start();

			log.info("start client in parallel");
			String srcSubscriptionString = TestUtil.join(srcNames[0], ",");
			String serverName = "localhost:" + relayPort;
			final CountingConsumer countingConsumer = new CountingConsumer();
			DatabusSourcesConnection clientConn = RelayEventProducer
					.createDatabusSourcesConnection("testProducer", serverName,
							srcSubscriptionString, countingConsumer,
							1 * 1024 * 1024, 50000, 30 * 1000, 100, 15 * 1000,
							1, true);
			cr = new ClientRunner(clientConn);
			cr.start();
			log.info("terminating conditions");
			final DbusEventsTotalStats stats = relay1.getInboundEventStatisticsCollector().getTotalStats();
			long totalRunTime = 5000;
			long startTime = System.currentTimeMillis();
			do
			{
				log.info("numDataEvents=" + stats.getNumDataEvents()
						+ " numWindows=" + stats.getNumSysEvents() + " size="
						+ stats.getSizeDataEvents());
				Thread.sleep(1000);
			}
			while ((System.currentTimeMillis() - startTime) < totalRunTime);

			r1.pause();
			log.info("Sending pause to relay!");
			log.info("numDataEvents=" + stats.getNumDataEvents()
					+ " numWindows=" + stats.getNumSysEvents() + " size="
					+ stats.getSizeDataEvents());

			TestUtil.assertWithBackoff(new ConditionCheck()
            {
              @Override
              public boolean check()
              {
                boolean success = true;
                for (EventProducer p: relay1.getProducers())
                {
                    if (!(success = success && p.isPaused())) break;
                }
                return success;
              }
            }, "waiting for producers to pause", 4000, log);

			TestUtil.assertWithBackoff(new ConditionCheck()
            {
              @Override
              public boolean check()
              {
                log.debug("countingConsumer.getNumWindows()=" + countingConsumer.getNumWindows());
                return countingConsumer.getNumWindows() == stats.getNumSysEvents();
              }
            }, "wait until client got all events or for maxTimeout", 64 * 1024, log);

			log.info("Client stats=" + countingConsumer);
			log.info("Event windows generated=" + stats.getNumSysEvents());

            cr.shutdown(2000, log);
            log.info("Client cr stopped");
            Assert.assertEquals(countingConsumer.getNumDataEvents(), stats.getNumDataEvents());

			boolean stopped = r1.shutdown(2000);
			Assert.assertTrue(stopped);
			log.info("Relay r1 stopped");
		}
		finally
		{
			cleanup ( new DatabusRelayTestUtil.RelayRunner[] {r1} , cr);
		}

	}

	@Test
	/**
	 * Concurrent consumption of chained relay by client and by chained relay from regular relay
	 */
	public void testRelayChainingBasic() throws InterruptedException, InvalidConfigException
	{
		DatabusRelayTestUtil.RelayRunner r1=null,r2=null;
        final Logger log = Logger.getLogger("TestDatabusRelayMain.testRelayChainingBasic");
        Logger.getRootLogger().setLevel(Level.INFO);
        log.setLevel(Level.DEBUG);

        log.debug("available processors:" + Runtime.getRuntime().availableProcessors());
        log.debug("available memory:" + Runtime.getRuntime().freeMemory());
        log.debug("total memory:" + Runtime.getRuntime().totalMemory());

		ClientRunner cr = null;
		try
		{
			String[][] srcNames =
			{
	            { "com.linkedin.events.example.fake.FakeSchema",
	              "com.linkedin.events.example.person.Person" }, };

			// create main relay with random generator
			PhysicalSourceConfig[] srcConfigs = new PhysicalSourceConfig[srcNames.length];
			int i = 0;
			int eventRatePerSec = 20;
			for (String[] srcs : srcNames)
			{

				PhysicalSourceConfig src1 = DatabusRelayTestUtil.createPhysicalConfigBuilder(
						(short) (i + 1), DatabusRelayTestUtil.getPhysicalSrcName(srcs[0]), "mock",
						100, eventRatePerSec, srcs);
				srcConfigs[i++] = src1;
			}
            int relayPort = Utils.getAvailablePort(11994);
			DatabusRelayMain relay1 = DatabusRelayTestUtil.createDatabusRelayWithSchemaReg(1002, relayPort,
					10 * 1024 * 1024, srcConfigs, SCHEMA_REGISTRY_DIR);
			Assert.assertNotNull(relay1);
			r1 = new DatabusRelayTestUtil.RelayRunner(relay1);

			log.info("create chained relay");
			PhysicalSourceConfig[] chainedSrcConfigs = new PhysicalSourceConfig[srcNames.length];
			int j = 0;
			for (String[] srcs : srcNames)
			{

				PhysicalSourceConfig src1 = DatabusRelayTestUtil.createPhysicalConfigBuilder(
						(short) (j + 1), DatabusRelayTestUtil.getPhysicalSrcName(srcs[0]),
						"localhost:" + relayPort, eventRatePerSec, 50, srcs);
				chainedSrcConfigs[j++] = src1;
			}
			int chainedRelayPort = relayPort + 1000;
			DatabusRelayMain relay2 = DatabusRelayTestUtil.createDatabusRelayWithSchemaReg(1003,
					chainedRelayPort, 1 * 1024 * 1024, chainedSrcConfigs, SCHEMA_REGISTRY_DIR);
			Assert.assertNotNull(relay2);
			r2 = new DatabusRelayTestUtil.RelayRunner(relay2);

			resetSCN(relay2);

			log.info("now create client");
			String srcSubscriptionString = TestUtil.join(srcNames[0], ",");
			String serverName = "localhost:" + chainedRelayPort;
			final CountingConsumer countingConsumer = new CountingConsumer();
			//Set maxSize to 100K, maxEventSize to 50K
			DatabusSourcesConnection clientConn = RelayEventProducer
					.createDatabusSourcesConnection("testProducer", serverName,
							srcSubscriptionString, countingConsumer,
							100 * 1024 , 10000, 30 * 1000, 100, 30 * 1000,
							1, true);
			cr = new ClientRunner(clientConn);

			log.info("async starts for all components");
			r1.start();
			assertRelayRunning(r1.getRelay(), 5000, log);
			log.info("start chained relay");
			r2.start();
            assertRelayRunning(r2.getRelay(), 5000, log);

			Thread.sleep(5*1000);

			r1.pause();

      // wait for r2 to catchup with r1
      final DbusEventsTotalStats stats = relay1
          .getInboundEventStatisticsCollector().getTotalStats();
      final DbusEventsTotalStats stats2 = relay2
          .getInboundEventStatisticsCollector().getTotalStats();
      TestUtil.assertWithBackoff(new ConditionCheck()
      {

        @Override
        public boolean check()
        {
          log.debug("stats2.getNumSysEvents()=" + stats2.getNumSysEvents());
          return stats2.getNumSysEvents() == stats.getNumSysEvents();
        }
      }, "wait for chained relay to catchup", 60000, log);

      log.info("start the client");
      cr.start();

      // wait until client got all events or for maxTimeout;
      TestUtil.assertWithBackoff(new ConditionCheck()
      {

        @Override
        public boolean check()
        {
          log.debug("countingConsumer.getNumWindows()="
              + countingConsumer.getNumWindows());
          return countingConsumer.getNumWindows() == stats.getNumSysEvents();
        }
      }, "wait until client got all events", 10000, log);

			LOG.info("Client stats=" + countingConsumer);
			LOG.info("Event windows generated="
					+ stats.getNumSysEvents());
			LOG.info("numDataEvents=" + stats.getNumDataEvents()
					+ " numWindows=" + stats.getNumSysEvents() + " size="
					+ stats.getSizeDataEvents());
			LOG.info("numDataEvents2=" + stats2.getNumDataEvents()
					+ " numWindows2=" + stats2.getNumSysEvents() + " size2="
					+ stats2.getSizeDataEvents());

			Assert.assertEquals(stats.getNumDataEvents(), countingConsumer
					.getNumDataEvents());
			Assert.assertEquals(countingConsumer.getNumSources(), 2);
			Assert.assertEquals(stats.getNumSysEvents(), countingConsumer
					.getNumWindows());
		}
		finally
		{
			cleanup ( new DatabusRelayTestUtil.RelayRunner[] {r1,r2} , cr);
		}

	}

	@Test
  /**
   * Basic consumption client and by chained relay from regular relay
   * but start with small readBuffer (20K) - Set up large events
   */
  public void testDynamicBufferGrowthClient() throws InterruptedException, InvalidConfigException
  {
    DatabusRelayTestUtil.RelayRunner r1=null,r2=null;
    final Logger log = Logger.getLogger("TestDatabusRelayMain.testDynamicBufferGrowth");
    Logger.getRootLogger().setLevel(Level.INFO);

    ClientRunner cr = null;
    try
    {
      String[][] srcNames =
      {
              { "com.linkedin.events.example.Account",
                "com.linkedin.events.example.Settings" }, };

      // create main relay with random generator
      PhysicalSourceConfig[] srcConfigs = new PhysicalSourceConfig[srcNames.length];
      int i = 0;
      int eventRatePerSec = 20;
      for (String[] srcs : srcNames)
      {

        PhysicalSourceConfig src1 = DatabusRelayTestUtil.createPhysicalConfigBuilder(
            (short) (i + 1), DatabusRelayTestUtil.getPhysicalSrcName(srcs[0]), "mock",
            100, eventRatePerSec, srcs);
        srcConfigs[i++] = src1;
      }
      int relayPort = Utils.getAvailablePort(11994);
      DatabusRelayMain relay1 = DatabusRelayTestUtil.createDatabusRelayWithSchemaReg(1002, relayPort,
          10 * 1024 * 1024, srcConfigs, SCHEMA_REGISTRY_DIR);
      Assert.assertNotNull(relay1);
      r1 = new DatabusRelayTestUtil.RelayRunner(relay1);


      log.info("now create client");
      String srcSubscriptionString = TestUtil.join(srcNames[0], ",");
      String serverName = "localhost:" + relayPort;
      final CountingConsumer countingConsumer = new CountingConsumer();
      //Set maxSize to 100K, maxEventSize to 15k, set init readBufferSize to maxEventSize/2
      int maxEventSize=15*1024;
      int initReadBufferSize=maxEventSize/2;

      DatabusSourcesConnection clientConn = RelayEventProducer
          .createDatabusSourcesConnection("testProducer", serverName,
              srcSubscriptionString, countingConsumer,
              100 * 1024 , maxEventSize, 30 * 1000, 100, 30 * 1000,
              1, true,initReadBufferSize);
      cr = new ClientRunner(clientConn);

      log.info("async starts for all components");
      r1.start();
      assertRelayRunning(r1.getRelay(), 5000, log);

      Thread.sleep(5*1000);

      r1.pause();

      final DbusEventsTotalStats stats = relay1
          .getInboundEventStatisticsCollector().getTotalStats();


      log.info("start the client");
      cr.start();

      // wait until client got all events or for maxTimeout;
      TestUtil.assertWithBackoff(new ConditionCheck()
      {

        @Override
        public boolean check()
        {
          log.debug("countingConsumer.getNumWindows()="
              + countingConsumer.getNumWindows());
          return countingConsumer.getNumWindows() == stats.getNumSysEvents();
        }
      }, "wait until client got all events", 10000, log);

      LOG.info("Client stats=" + countingConsumer);
      LOG.info("Event windows generated="
          + stats.getNumSysEvents());
      LOG.info("numDataEvents=" + stats.getNumDataEvents()
          + " numWindows=" + stats.getNumSysEvents() + " size="
          + stats.getSizeDataEvents());

      Assert.assertEquals(stats.getNumDataEvents(), countingConsumer
          .getNumDataEvents());
      Assert.assertEquals(countingConsumer.getNumSources(), 2);
      Assert.assertEquals(stats.getNumSysEvents(), countingConsumer
          .getNumWindows());
    }
    finally
    {
      cleanup ( new DatabusRelayTestUtil.RelayRunner[] {r1,r2} , cr);
    }

  }

	@Test
	/** Client and chained relay start before relay, simulating unavailable relay and retries at chained relay and client
	 *
	 */
	public void testRelayChainingConnectFailure()
	{
		DatabusRelayTestUtil.RelayRunner r1=null,r2=null;
		ClientRunner cr = null;
		try
		{

      String[][] srcNames =
      {
              { "com.linkedin.events.example.fake.FakeSchema",
                "com.linkedin.events.example.person.Person" }, };

			// create main relay with random generator
			PhysicalSourceConfig[] srcConfigs = new PhysicalSourceConfig[srcNames.length];
			int i = 0;
			int eventRatePerSec = 10;
			for (String[] srcs : srcNames)
			{

				PhysicalSourceConfig src1 = DatabusRelayTestUtil.createPhysicalConfigBuilder(
						(short) (i + 1),DatabusRelayTestUtil.getPhysicalSrcName(srcs[0]), "mock",
						500, eventRatePerSec, srcs);
				srcConfigs[i++] = src1;
			}
			int relayPort = 11993;
			DatabusRelayMain relay1 = DatabusRelayTestUtil.createDatabusRelayWithSchemaReg(1004, relayPort,
					10 * 1024 * 1024, srcConfigs,SCHEMA_REGISTRY_DIR);
			Assert.assertTrue(null != relay1);
			r1 = new DatabusRelayTestUtil.RelayRunner(relay1);

			// create chained relay
			PhysicalSourceConfig[] chainedSrcConfigs = new PhysicalSourceConfig[srcNames.length];
			int j = 0;
			for (String[] srcs : srcNames)
			{

				PhysicalSourceConfig src1 = DatabusRelayTestUtil.createPhysicalConfigBuilder(
						(short) (j + 1),DatabusRelayTestUtil.getPhysicalSrcName(srcs[0]),
						"localhost:" + relayPort, eventRatePerSec, 50, srcs);
				chainedSrcConfigs[j++] = src1;
			}

			int chainedRelayPort = relayPort + 1;
			DatabusRelayMain relay2 = DatabusRelayTestUtil.createDatabusRelayWithSchemaReg(1005,
					chainedRelayPort, 10 * 1024 * 1024, chainedSrcConfigs,SCHEMA_REGISTRY_DIR);
			Assert.assertTrue(null != relay2);
			r2 = new DatabusRelayTestUtil.RelayRunner(relay2);
			resetSCN(relay2);

			// now create client:
			String srcSubscriptionString = TestUtil.join(srcNames[0], ",");
			String serverName = "localhost:" + chainedRelayPort;
			CountingConsumer countingConsumer = new CountingConsumer();
			DatabusSourcesConnection clientConn = RelayEventProducer
					.createDatabusSourcesConnection("testProducer", serverName,
							srcSubscriptionString, countingConsumer,
							1 * 1024 * 1024, 50000, 30 * 1000, 100, 15 * 1000,
							1, true);
			cr = new ClientRunner(clientConn);

			// start chained relay
			r2.start();
			// start client
			cr.start();

			// Let them try for 5seconds
			Thread.sleep(5 * 1000);

			r1.start();

			// Let the relay run for 10s
			Thread.sleep(10 * 1000);

			r1.pause();

			// wait until client got all events or for maxTimeout;
			long maxTimeOutMs = 5 * 1000;
			long startTime = System.currentTimeMillis();
			DbusEventsTotalStats stats = relay1
					.getInboundEventStatisticsCollector().getTotalStats();
			while (countingConsumer.getNumWindows() < stats.getNumSysEvents())
			{
				Thread.sleep(500);
				// LOG.info("Client stats=" + countingConsumer);
				// LOG.error("numDataEvents=" +
				// stats.getNumDataEvents() + " numWindows=" +
				// stats.getNumSysEvents() + " size=" +
				// stats.getSizeDataEvents());
				if ((System.currentTimeMillis() - startTime) > maxTimeOutMs)
				{
					break;
				}
			}

			LOG.info("Client stats=" + countingConsumer);
			LOG.info("Event windows generated="
					+ stats.getNumSysEvents());
			LOG.info("numDataEvents=" + stats.getNumDataEvents()
					+ " numWindows=" + stats.getNumSysEvents() + " size="
					+ stats.getSizeDataEvents());

			Assert.assertEquals(stats.getNumDataEvents(), countingConsumer.getNumDataEvents());
			Assert.assertEquals(countingConsumer.getNumSources(), 2);
			Assert.assertEquals(stats.getNumSysEvents(), countingConsumer.getNumWindows());

		}
		catch (Exception e)
		{
			LOG.error("Exception: " + e);
			Assert.assertTrue(false);
		}
		finally
		{
			cleanup ( new DatabusRelayTestUtil.RelayRunner[] {r1,r2} , cr);
		}

	}

	@Test
	/**
	 * Client consumes subset of sources from chained relay
	 */
	public void testRelalyChainingPartialSubscribe()
	{
		DatabusRelayTestUtil.RelayRunner r1=null,r2=null;
		ClientRunner cr = null;
		try
		{
			String[][] srcNames =
			{
			{ "com.linkedin.events.example.Account",
					"com.linkedin.events.example.Settings" }, };

			// create main relay with random generator
			PhysicalSourceConfig[] srcConfigs = new PhysicalSourceConfig[srcNames.length];
			int i = 0;
			int eventRatePerSec = 10;
			for (String[] srcs : srcNames)
			{

				PhysicalSourceConfig src1 = DatabusRelayTestUtil.createPhysicalConfigBuilder(
						(short) (i + 1),DatabusRelayTestUtil.getPhysicalSrcName(srcs[0]), "mock",
						500, eventRatePerSec, srcs);
				srcConfigs[i++] = src1;
			}
			int relayPort = 11993;
			DatabusRelayMain relay1 = DatabusRelayTestUtil.createDatabusRelayWithSchemaReg(1006, relayPort,
					10 * 1024 * 1024, srcConfigs,SCHEMA_REGISTRY_DIR);
			Assert.assertTrue(null != relay1);
			r1 = new DatabusRelayTestUtil.RelayRunner(relay1);

			// create chained relay
			PhysicalSourceConfig[] chainedSrcConfigs = new PhysicalSourceConfig[srcNames.length];
			int j = 0;
			for (String[] srcs : srcNames)
			{

				PhysicalSourceConfig src1 = DatabusRelayTestUtil.createPhysicalConfigBuilder(
						(short) (j + 1),DatabusRelayTestUtil.getPhysicalSrcName(srcs[0]),
						"localhost:" + relayPort, eventRatePerSec, 50, srcs);
				chainedSrcConfigs[j++] = src1;
			}

			int chainedRelayPort = relayPort + 1;
			DatabusRelayMain relay2 = DatabusRelayTestUtil.createDatabusRelayWithSchemaReg(1007,
					chainedRelayPort, 10 * 1024 * 1024, chainedSrcConfigs,SCHEMA_REGISTRY_DIR);
			Assert.assertTrue(null != relay2);
			resetSCN(relay2);

			r2 = new DatabusRelayTestUtil.RelayRunner(relay2);

			// now create client:
			String srcSubscriptionString = "com.linkedin.events.example.Settings";
			String serverName = "localhost:" + chainedRelayPort;
			CountingConsumer countingConsumer = new CountingConsumer();
			DatabusSourcesConnection clientConn = RelayEventProducer
					.createDatabusSourcesConnection("testProducer", serverName,
							srcSubscriptionString, countingConsumer,
							1 * 1024 * 1024, -1, 30 * 1000, 100, 15 * 1000,
							1, true);
			cr = new ClientRunner(clientConn);

			// start relay r1
			r1.start();

			// start chained relay
			r2.start();
			// start client
			cr.start();

			// Let the relay run for 10s
			Thread.sleep(10 * 1000);

			r1.pause();

			// wait until client got all events or for maxTimeout;
			long maxTimeOutMs = 5 * 1000;
			long startTime = System.currentTimeMillis();
			DbusEventsTotalStats stats = relay1
					.getInboundEventStatisticsCollector().getTotalStats();
			while (countingConsumer.getNumWindows() < stats.getNumSysEvents())
			{
				Thread.sleep(500);
				// LOG.info("Client stats=" + countingConsumer);
				// LOG.error("numDataEvents=" +
				// stats.getNumDataEvents() + " numWindows=" +
				// stats.getNumSysEvents() + " size=" +
				// stats.getSizeDataEvents());
				if ((System.currentTimeMillis() - startTime) > maxTimeOutMs)
				{
					break;
				}
			}

			LOG.info("Client stats=" + countingConsumer);
			LOG.info("Event windows generated="
					+ stats.getNumSysEvents());
			LOG.info("numDataEvents=" + stats.getNumDataEvents()
					+ " numWindows=" + stats.getNumSysEvents() + " size="
					+ stats.getSizeDataEvents());

			Assert.assertTrue(countingConsumer.getNumSources() == 1);
			Assert.assertTrue(stats.getNumSysEvents() == countingConsumer
					.getNumWindows());
			 Assert.assertTrue(stats.getNumDataEvents() == 2 * countingConsumer
	          .getNumDataEvents());


		}
		catch (Exception e)
		{
			LOG.error("Exception: " + e);
			Assert.assertTrue(false);
		}
		finally
		{
			cleanup ( new DatabusRelayTestUtil.RelayRunner[] {r1,r2} , cr);
		}

	}

	@Test
	/**
	 * Chained relay consumes a subset of parent relay
	 */
	public void testRelayChainingPartialSubscribeRelay()
	{
		DatabusRelayTestUtil.RelayRunner r1=null,r2=null;
		ClientRunner cr = null;
		try
		{
			String[][] srcNames =
			{
			{ "com.linkedin.events.example.Account",
					"com.linkedin.events.example.Settings" }, };

			// create main relay with random generator
			PhysicalSourceConfig[] srcConfigs = new PhysicalSourceConfig[srcNames.length];
			int i = 0;
			int eventRatePerSec = 10;
			for (String[] srcs : srcNames)
			{

				PhysicalSourceConfig src1 = DatabusRelayTestUtil.createPhysicalConfigBuilder(
						(short) (i + 1),DatabusRelayTestUtil.getPhysicalSrcName(srcs[0]), "mock",
						500, eventRatePerSec, srcs);
				srcConfigs[i++] = src1;
			}
			int relayPort = 11993;
			DatabusRelayMain relay1 = DatabusRelayTestUtil.createDatabusRelayWithSchemaReg(1008, relayPort,
					10 * 1024 * 1024, srcConfigs,SCHEMA_REGISTRY_DIR);
			Assert.assertTrue(null != relay1);
			r1 = new DatabusRelayTestUtil.RelayRunner(relay1);

			// create chained relay
			PhysicalSourceConfig[] chainedSrcConfigs = new PhysicalSourceConfig[srcNames.length];
			int j = 0;
			for (String[] srcs : srcNames)
			{
				String partialSrcs[] = new String[1];
				partialSrcs[0] = srcs[srcs.length - 1];
				PhysicalSourceConfig src1 = DatabusRelayTestUtil.createPhysicalConfigBuilder(
						(short) (srcs.length),DatabusRelayTestUtil.getPhysicalSrcName(srcs[0]),
						"localhost:" + relayPort, eventRatePerSec, 50,
						partialSrcs);
				chainedSrcConfigs[j++] = src1;
			}

			int chainedRelayPort = relayPort + 1;
			DatabusRelayMain relay2 = DatabusRelayTestUtil.createDatabusRelayWithSchemaReg(1009,
					chainedRelayPort, 10 * 1024 * 1024, chainedSrcConfigs,SCHEMA_REGISTRY_DIR);
			Assert.assertTrue(null != relay2);
			resetSCN(relay2);

			r2 = new DatabusRelayTestUtil.RelayRunner(relay2);

			// now create client:
			String srcSubscriptionString = "com.linkedin.events.example.Settings";
			String serverName = "localhost:" + chainedRelayPort;
			CountingConsumer countingConsumer = new CountingConsumer();
			DatabusSourcesConnection clientConn = RelayEventProducer
					.createDatabusSourcesConnection("testProducer", serverName,
							srcSubscriptionString, countingConsumer,
							1 * 1024 * 1024, 50000, 30 * 1000, 100, 15 * 1000,
							1, true);
			cr = new ClientRunner(clientConn);

			// start relay r1
			r1.start();

			// start chained relay
			r2.start();
			// start client
			cr.start();

			// Let the relay run for 10s
			Thread.sleep(10 * 1000);

			r1.pause();

			// wait until client got all events or for maxTimeout;
			long maxTimeOutMs = 5 * 1000;
			long startTime = System.currentTimeMillis();
			DbusEventsTotalStats stats = relay1
					.getInboundEventStatisticsCollector().getTotalStats();
			DbusEventsTotalStats statsChained = relay2
					.getInboundEventStatisticsCollector().getTotalStats();
			while (countingConsumer.getNumWindows() < stats.getNumSysEvents())
			{
				Thread.sleep(500);
				// LOG.info("Client stats=" + countingConsumer);
				// LOG.error("numDataEvents=" +
				// stats.getNumDataEvents() + " numWindows=" +
				// stats.getNumSysEvents() + " size=" +
				// stats.getSizeDataEvents());
				if ((System.currentTimeMillis() - startTime) > maxTimeOutMs)
				{
					break;
				}
			}

			LOG.info("Client stats=" + countingConsumer);
			LOG.info("Chained stats="
					+ statsChained.getNumSysEvents());
			LOG.info("Event windows generated="
					+ stats.getNumSysEvents());
			LOG.info("numDataEvents=" + stats.getNumDataEvents()
					+ " numWindows=" + stats.getNumSysEvents() + " size="
					+ stats.getSizeDataEvents());

			Assert.assertTrue(stats.getNumSysEvents() == statsChained
					.getNumSysEvents());
			Assert.assertTrue(statsChained.getNumDataEvents() == countingConsumer
					.getNumDataEvents());
			Assert.assertTrue(stats.getNumDataEvents() == 2 * countingConsumer
					.getNumDataEvents());
			Assert.assertTrue(countingConsumer.getNumSources() == 1);
			Assert.assertTrue(stats.getNumSysEvents() == countingConsumer
					.getNumWindows());

		}
		catch (Exception e)
		{
			LOG.error("Exception: " + e);
			Assert.assertTrue(false);
		}
		finally
		{
			cleanup ( new DatabusRelayTestUtil.RelayRunner[] {r1,r2} , cr);
		}

	}

	@Test
	/**
	 * Chained relay overwrites buffer; consumer starts late; gets only the last n events that fit.
	 * Makes sure chained relay overwrites buffer;just like main buffer
	 */
	public void testRelayChainingDelayedConsumer()
	{
		DatabusRelayTestUtil.RelayRunner r1=null,r2=null;
		ClientRunner cr = null;
		try
		{
			String[][] srcNames =
			{
			{ "com.linkedin.events.example.Account",
					"com.linkedin.events.example.Settings" }, };

			// create main relay with random generator
			PhysicalSourceConfig[] srcConfigs = new PhysicalSourceConfig[srcNames.length];
			int i = 0;
			int eventRatePerSec = 10;
			for (String[] srcs : srcNames)
			{

				PhysicalSourceConfig src1 = DatabusRelayTestUtil.createPhysicalConfigBuilder(
						(short) (i + 1),DatabusRelayTestUtil.getPhysicalSrcName(srcs[0]), "mock",
						500, eventRatePerSec, srcs);
				srcConfigs[i++] = src1;
			}
			int relayPort = 11993;
			DatabusRelayMain relay1 = DatabusRelayTestUtil.createDatabusRelayWithSchemaReg(1010, relayPort,
					10 * 1024 * 1024, srcConfigs,SCHEMA_REGISTRY_DIR);
			Assert.assertTrue(null != relay1);
			r1 = new DatabusRelayTestUtil.RelayRunner(relay1);

			// create chained relay with only 1 MB buffer
			PhysicalSourceConfig[] chainedSrcConfigs = new PhysicalSourceConfig[srcNames.length];
			int j = 0;
			for (String[] srcs : srcNames)
			{

				PhysicalSourceConfig src1 = DatabusRelayTestUtil.createPhysicalConfigBuilder(
						(short) (j + 1),DatabusRelayTestUtil.getPhysicalSrcName(srcs[0]),
						"localhost:" + relayPort, eventRatePerSec, 50, srcs);
				chainedSrcConfigs[j++] = src1;
			}
			int chainedRelayPort = relayPort + 1;
			DatabusRelayMain relay2 = DatabusRelayTestUtil.createDatabusRelayWithSchemaReg(1011,
					chainedRelayPort, 1 * 1024 * 1024, chainedSrcConfigs,SCHEMA_REGISTRY_DIR);
			Assert.assertTrue(null != relay2);
			resetSCN(relay2);

			r2 = new DatabusRelayTestUtil.RelayRunner(relay2);


			// now create client:
			String srcSubscriptionString = TestUtil.join(srcNames[0], ",");
			String serverName = "localhost:" + chainedRelayPort;
			CountingConsumer countingConsumer = new CountingConsumer();
			DatabusSourcesConnection clientConn = RelayEventProducer
					.createDatabusSourcesConnection("testProducer", serverName,
							srcSubscriptionString, countingConsumer,
							1 * 1024 * 1024, 50000, 30 * 1000, 100, 15 * 1000,
							1, true);
			cr = new ClientRunner(clientConn);

			// async starts for all components;
			r1.start();
			// start chained relay
			r2.start();

			// let it run for 10 seconds
			Thread.sleep(10 * 1000);

			r1.pause();

			// start client after the relays have finished
			cr.start();

			// wait until client got all events or for maxTimeout;
			long maxTimeOutMs = 15 * 1000;
			long startTime = System.currentTimeMillis();
			DbusEventsTotalStats stats = relay1
					.getInboundEventStatisticsCollector().getTotalStats();
			while (countingConsumer.getNumWindows() < stats.getNumSysEvents())
			{
				Thread.sleep(500);
				// LOG.info("Client stats=" + countingConsumer);
				// LOG.error("numDataEvents=" +
				// stats.getNumDataEvents() + " numWindows=" +
				// stats.getNumSysEvents() + " size=" +
				// stats.getSizeDataEvents());
				if ((System.currentTimeMillis() - startTime) > maxTimeOutMs)
				{
					break;
				}
			}

			DbusEventsTotalStats statsChained = relay2
					.getInboundEventStatisticsCollector().getTotalStats();

			LOG.info("Client stats=" + countingConsumer);
			LOG.info("Event windows generated="
					+ stats.getNumSysEvents());
			LOG.info("numDataEvents=" + stats.getNumDataEvents()
					+ " numWindows=" + stats.getNumSysEvents() + " size="
					+ stats.getSizeDataEvents());

			Assert.assertTrue(stats.getNumDataEvents() == statsChained
					.getNumDataEvents());
			Assert.assertTrue(stats.getNumSysEvents() == statsChained
					.getNumSysEvents());
			Assert.assertTrue(stats.getNumDataEvents() > countingConsumer
					.getNumDataEvents());
			Assert.assertTrue(countingConsumer.getNumSources() == 2);
			Assert.assertTrue(stats.getNumSysEvents() > countingConsumer
					.getNumWindows());

		}
		catch (Exception e)
		{
			LOG.error("Exception: " + e);
			Assert.assertTrue(false);
		}
		finally
		{
			cleanup ( new DatabusRelayTestUtil.RelayRunner[] {r1,r2} , cr);
		}

	}

	@Test
	/**
	 * Slow consumer; results in getting SCN not found exception ; as chained relay overwrites buffer .
	 * Makes sure chained relay overwrites buffer;just like main buffer
	 */
	public void testRelayChainingSlowConsumer()
	{
		DatabusRelayTestUtil.RelayRunner r1=null,r2=null;
		ClientRunner cr = null;
		try
		{
			String[][] srcNames =
			{
			{ "com.linkedin.events.example.Account",
					"com.linkedin.events.example.Settings" }, };

			// create main relay with random generator
			PhysicalSourceConfig[] srcConfigs = new PhysicalSourceConfig[srcNames.length];
			int i = 0;
			int eventRatePerSec = 10;
			for (String[] srcs : srcNames)
			{

				PhysicalSourceConfig src1 = DatabusRelayTestUtil.createPhysicalConfigBuilder(
						(short) (i + 1),DatabusRelayTestUtil.getPhysicalSrcName(srcs[0]), "mock",
						500, eventRatePerSec, srcs);
				srcConfigs[i++] = src1;
			}
			int relayPort = 11993;
			DatabusRelayMain relay1 = DatabusRelayTestUtil.createDatabusRelayWithSchemaReg(1012, relayPort,
					10 * 1024 * 1024, srcConfigs,SCHEMA_REGISTRY_DIR);
			Assert.assertTrue(null != relay1);
			r1 = new DatabusRelayTestUtil.RelayRunner(relay1);

			// create chained relay with only 1 MB buffer
			PhysicalSourceConfig[] chainedSrcConfigs = new PhysicalSourceConfig[srcNames.length];
			int j = 0;
			for (String[] srcs : srcNames)
			{

				PhysicalSourceConfig src1 = DatabusRelayTestUtil.createPhysicalConfigBuilder(
						(short) (j + 1),DatabusRelayTestUtil.getPhysicalSrcName(srcs[0]),
						"localhost:" + relayPort, eventRatePerSec, 50, srcs);
				chainedSrcConfigs[j++] = src1;
			}
			int chainedRelayPort = relayPort + 1;
			DatabusRelayMain relay2 = DatabusRelayTestUtil.createDatabusRelayWithSchemaReg(1013,
					chainedRelayPort, 1024 * 1024, chainedSrcConfigs,SCHEMA_REGISTRY_DIR);
			Assert.assertTrue(null != relay2);
			resetSCN(relay2);

			r2 = new DatabusRelayTestUtil.RelayRunner(relay2);

			// now create client:
			String srcSubscriptionString = TestUtil.join(srcNames[0], ",");
			String serverName = "localhost:" + chainedRelayPort;
			// create slow consumer with 100ms delay
			CountingConsumer countingConsumer = new CountingConsumer(500);
			DatabusSourcesConnection clientConn = RelayEventProducer
					.createDatabusSourcesConnection("testProducer", serverName,
							srcSubscriptionString, countingConsumer,
							1 * 1024 * 1024, 50000, 30 * 1000, 1000, 15 * 1000,
							1, true);
			cr = new ClientRunner(clientConn);

			// async starts for all components;
			r1.start();
			// start chained relay
			r2.start();

			// start slow client
			cr.start();

			// let it run for 20 seconds
			Thread.sleep(20 * 1000);

			r1.pause();

			// wait until client got all events or for maxTimeout;
			long maxTimeOutMs = 100 * 1000;
			long startTime = System.currentTimeMillis();
			DbusEventsTotalStats stats = relay1
					.getInboundEventStatisticsCollector().getTotalStats();
			Assert.assertTrue(stats.getNumSysEvents() > 0);
			while (countingConsumer.getNumWindows() < stats.getNumSysEvents())
			{
				Thread.sleep(500);
				if ((System.currentTimeMillis() - startTime) > maxTimeOutMs)
				{
					break;
				}
			}

			DbusEventsTotalStats statsChained = relay2
					.getInboundEventStatisticsCollector().getTotalStats();

			LOG.info("Client stats=" + countingConsumer);
			LOG.info("Event windows generated="
					+ stats.getNumSysEvents());
			LOG.info("numDataEvents=" + stats.getNumDataEvents()
					+ " numWindows=" + stats.getNumSysEvents() + " size="
					+ stats.getSizeDataEvents());

			Assert.assertTrue(stats.getMinScn() < statsChained.getMinScn());
			Assert.assertTrue(stats.getNumDataEvents() == statsChained
					.getNumDataEvents());
			Assert.assertTrue(stats.getNumSysEvents() == statsChained
					.getNumSysEvents());
			Assert.assertTrue(countingConsumer.getNumErrors() > 0);
			Assert.assertTrue(stats.getNumDataEvents() > countingConsumer
					.getNumDataEvents());
			Assert.assertTrue(countingConsumer.getNumSources() == 2);
			Assert.assertTrue(stats.getNumSysEvents() > countingConsumer
					.getNumWindows());

		}
		catch (Exception e)
		{
			LOG.error("Exception: " + e);
			Assert.assertTrue(false);
		}
		finally
		{
			cleanup ( new DatabusRelayTestUtil.RelayRunner[] {r1,r2} , cr);
		}

	}

	@Test
	/**
	 * Regular concurrent consumption amongst db-relay , chained relay and client. The only twist is that the chained relay
	 * will be paused and resumed
	 */
	public void testRelayChainingPauseResume()
	{
		DatabusRelayTestUtil.RelayRunner r1=null,r2=null;
		ClientRunner cr = null;
		try
		{
			String[][] srcNames =
			{
			{ "com.linkedin.events.example.Account",
					"com.linkedin.events.example.Settings" }, };

			// create main relay with random generator
			PhysicalSourceConfig[] srcConfigs = new PhysicalSourceConfig[srcNames.length];
			int i = 0;
			int eventRatePerSec = 10;
			for (String[] srcs : srcNames)
			{

				PhysicalSourceConfig src1 = DatabusRelayTestUtil.createPhysicalConfigBuilder(
						(short) (i + 1),DatabusRelayTestUtil.getPhysicalSrcName(srcs[0]), "mock",
						500, eventRatePerSec, srcs);
				srcConfigs[i++] = src1;
			}
			int relayPort = 11993;
			DatabusRelayMain relay1 = DatabusRelayTestUtil.createDatabusRelayWithSchemaReg(1014, relayPort,
					10 * 1024 * 1024, srcConfigs,SCHEMA_REGISTRY_DIR);
			Assert.assertTrue(null != relay1);
			r1 = new DatabusRelayTestUtil.RelayRunner(relay1);

			// create chained relay
			PhysicalSourceConfig[] chainedSrcConfigs = new PhysicalSourceConfig[srcNames.length];
			int j = 0;
			for (String[] srcs : srcNames)
			{

				PhysicalSourceConfig src1 = DatabusRelayTestUtil.createPhysicalConfigBuilder(
						(short) (j + 1),DatabusRelayTestUtil.getPhysicalSrcName(srcs[0]),
						"localhost:" + relayPort, eventRatePerSec, 50, srcs);
				chainedSrcConfigs[j++] = src1;
			}
			int chainedRelayPort = relayPort + 1;
			DatabusRelayMain relay2 = DatabusRelayTestUtil.createDatabusRelayWithSchemaReg(1015,
					chainedRelayPort, 1 * 1024 * 1024, chainedSrcConfigs,SCHEMA_REGISTRY_DIR);
			Assert.assertTrue(null != relay2);
			resetSCN(relay2);

			r2 = new DatabusRelayTestUtil.RelayRunner(relay2);

			// now create client:
			String srcSubscriptionString = TestUtil.join(srcNames[0], ",");
			String serverName = "localhost:" + chainedRelayPort;
			CountingConsumer countingConsumer = new CountingConsumer();
			DatabusSourcesConnection clientConn = RelayEventProducer
					.createDatabusSourcesConnection("testProducer", serverName,
							srcSubscriptionString, countingConsumer,
							1 * 1024 * 1024, 50000, 30 * 1000, 100, 15 * 1000,
							1, true);
			cr = new ClientRunner(clientConn);

			// async starts for all components;
			r1.start();
			// start chained relay
			r2.start();
			// start client
			cr.start();

			// let them run for 2 seconds
			Thread.sleep(2 * 1000);

			// pause chained relay
			r2.pause();

			// Sleep again for some time
			Thread.sleep(2 * 1000);

			// stop relay
			r1.pause();
			// resume chained relay
			r2.unpause();

			// wait until client got all events or for maxTimeout;
			long maxTimeOutMs = 5 * 1000;
			long startTime = System.currentTimeMillis();
			DbusEventsTotalStats stats = relay1
					.getInboundEventStatisticsCollector().getTotalStats();
			while (countingConsumer.getNumWindows() < stats.getNumSysEvents())
			{
				Thread.sleep(500);
				// LOG.info("Client stats=" + countingConsumer);
				// LOG.error("numDataEvents=" +
				// stats.getNumDataEvents() + " numWindows=" +
				// stats.getNumSysEvents() + " size=" +
				// stats.getSizeDataEvents());
				if ((System.currentTimeMillis() - startTime) > maxTimeOutMs)
				{
					break;
				}
			}

			LOG.info("Client stats=" + countingConsumer);
			LOG.info("Event windows generated="
					+ stats.getNumSysEvents());
			LOG.info("numDataEvents=" + stats.getNumDataEvents()
					+ " numWindows=" + stats.getNumSysEvents() + " size="
					+ stats.getSizeDataEvents());

			Assert.assertTrue(stats.getNumDataEvents() == countingConsumer
					.getNumDataEvents());
			Assert.assertTrue(countingConsumer.getNumSources() == 2);
			Assert.assertTrue(stats.getNumSysEvents() == countingConsumer
					.getNumWindows());
		}
		catch (Exception e)
		{
			LOG.error("Exception: " + e);
			Assert.assertTrue(false);
		}
		finally
		{
			cleanup ( new DatabusRelayTestUtil.RelayRunner[] {r1,r2} , cr);
		}

	}

	@Test
	/**
	 * Test relay chaining that utilizes restart SCN offset
	 */
	public void testRelayChainingRestartSCNOffset() throws InterruptedException, InvalidConfigException
	{
	  final Logger log = Logger.getLogger("TestDatabusRelayMain.testRelayChainingRestartSCNOffset");
	  log.info("start");
		DatabusRelayTestUtil.RelayRunner r1=null,r2=null,r3=null;
		ClientRunner cr = null;
		try
		{
			String[][] srcNames =
			{
			{ "com.linkedin.events.example.Account",
					"com.linkedin.events.example.Settings" }, };

			// create main relay with random generator
			PhysicalSourceConfig[] srcConfigs = new PhysicalSourceConfig[srcNames.length];
			int i = 0;
			int eventRatePerSec = 2;
			for (String[] srcs : srcNames)
			{

				PhysicalSourceConfig src1 = DatabusRelayTestUtil.createPhysicalConfigBuilder(
						(short) (i + 1),DatabusRelayTestUtil.getPhysicalSrcName(srcs[0]), "mock",
						500, eventRatePerSec, srcs);
				srcConfigs[i++] = src1;
			}
			int relayPort = 11993;
			DatabusRelayMain relay1 = DatabusRelayTestUtil.createDatabusRelayWithSchemaReg(1016, relayPort,
					10 * 1024 * 1024, srcConfigs,SCHEMA_REGISTRY_DIR);
			Assert.assertTrue(null != relay1);
			r1 = new DatabusRelayTestUtil.RelayRunner(relay1);

			// create chained relay
			PhysicalSourceConfig[] chainedSrcConfigs = new PhysicalSourceConfig[srcNames.length];
			int j = 0;
			for (String[] srcs : srcNames)
			{

				PhysicalSourceConfig src1 = DatabusRelayTestUtil.createPhysicalConfigBuilder(
						(short) (j + 1),DatabusRelayTestUtil.getPhysicalSrcName(srcs[0]),
						"localhost:" + relayPort, eventRatePerSec, 50, srcs);
				chainedSrcConfigs[j++] = src1;
			}
			int chainedRelayPort = relayPort + 1;
			DatabusRelayMain relay2 = DatabusRelayTestUtil.createDatabusRelayWithSchemaReg(1017,
					chainedRelayPort, 1 * 1024 * 1024, chainedSrcConfigs,SCHEMA_REGISTRY_DIR);
			Assert.assertTrue(null != relay2);
			r2 = new DatabusRelayTestUtil.RelayRunner(relay2);

			resetSCN(relay2);

			// now create client:
			String srcSubscriptionString = TestUtil.join(srcNames[0], ",");
			String serverName = "localhost:" + chainedRelayPort;
			final CountingConsumer countingConsumer = new CountingConsumer();
			DatabusSourcesConnection clientConn = RelayEventProducer
					.createDatabusSourcesConnection("testProducer", serverName,
							srcSubscriptionString, countingConsumer,
							1 * 1024 * 1024, 50000, 30 * 1000, 100, 15 * 1000,
							1, true);
			cr = new ClientRunner(clientConn);

			// async starts for all components;
			r1.start();
			// start chained relay
			r2.start();
			// start client
			Thread.sleep(500);
			cr.start();

			r1.pause();
			// let it run for 10 seconds
			Thread.sleep(10 * 1000);


			// wait until client got all events or for maxTimeout;
			final long maxTimeOutMs = 5 * 1000;
			final DbusEventsTotalStats dbRelayStats = relay1
					.getInboundEventStatisticsCollector().getTotalStats();
			TestUtil.assertWithBackoff(new ConditionCheck() {
				@Override
				public boolean check() {
					return countingConsumer.getNumWindows() == dbRelayStats.getNumSysEvents();
				}
			}, "consumer caught up", maxTimeOutMs, log);

			log.info("Client stats=" + countingConsumer);
			log.info("Event windows generated="
					+ dbRelayStats.getNumSysEvents());
			log.info("numDataEvents=" + dbRelayStats.getNumDataEvents()
					+ " numWindows=" + dbRelayStats.getNumSysEvents() + " size="
					+ dbRelayStats.getSizeDataEvents());

			Assert.assertEquals(dbRelayStats.getNumDataEvents(), countingConsumer
					.getNumDataEvents());
			Assert.assertEquals(countingConsumer.getNumSources(), 2);
			Assert.assertEquals(dbRelayStats.getNumSysEvents(), countingConsumer
					.getNumWindows());

			cr.shutdown();
			boolean s2= r2.shutdown(2000);
			Assert.assertTrue(s2);
			Assert.assertTrue(!r2.isAlive());

			//start r3; new chained relay with restart SCN offset; we get some data;
			chainedSrcConfigs[0].setRestartScnOffset(dbRelayStats.getMaxScn() - dbRelayStats.getPrevScn());
			DatabusRelayMain relay3 = DatabusRelayTestUtil.createDatabusRelayWithSchemaReg(1018,
					chainedRelayPort, 1 * 1024 * 1024, chainedSrcConfigs,SCHEMA_REGISTRY_DIR);
			r3 = new DatabusRelayTestUtil.RelayRunner(relay3);
			r3.start();

			final DbusEventsTotalStats newChainedRlyStats = relay3
					.getInboundEventStatisticsCollector().getTotalStats();

			TestUtil.assertWithBackoff(new ConditionCheck() {
				@Override
				public boolean check() {
					return newChainedRlyStats.getNumDataEvents() > 0;
				}
			}, "new chained relay running", 5000, log);

			log.info("Stats3= numDataEvents=" + newChainedRlyStats.getNumDataEvents()
					+ " numWindows=" + newChainedRlyStats.getNumSysEvents() + " size="
					+ newChainedRlyStats.getSizeDataEvents());
		}
		finally
		{
			cleanup ( new DatabusRelayTestUtil.RelayRunner[] {r1,r2,r3} , cr);
            log.info("end");
		}
	}

	@Test
	/**
	 * Test regression of SCN  of chained relay
	 */
	public void testRelayChainingSCNRegress() throws InvalidConfigException, InterruptedException
	{
      final Logger log = Logger.getLogger("TestDatabusRelayMain.testRelayChainingSCNRegress");
		DatabusRelayTestUtil.RelayRunner r1=null,r2=null,r3 = null;
		ClientRunner cr = null;
		//log.setLevel(Level.DEBUG);
		log.info("start");
		//DbusEventBuffer.LOG.setLevel(Level.DEBUG);
		try
		{
			String[][] srcNames =
			{
			{ "com.linkedin.events.example.Account",
              "com.linkedin.events.example.Settings" }, };

			// create main relay with random generator
			PhysicalSourceConfig[] srcConfigs = new PhysicalSourceConfig[srcNames.length];
			int i = 0;
			int eventRatePerSec = 10;
			int largestEventSize = 512*1024;
			long largestWindowSize = 1*1024*1024;
			for (String[] srcs : srcNames)
			{

				PhysicalSourceConfig src1 = DatabusRelayTestUtil.createPhysicalConfigBuilder(
						(short) (i + 1),DatabusRelayTestUtil.getPhysicalSrcName(srcs[0]), "mock",
						500, eventRatePerSec, srcs);
				srcConfigs[i++] = src1;
			}
			int relayPort = 11993;
			DatabusRelayMain relay1 = DatabusRelayTestUtil.createDatabusRelayWithSchemaReg(1019, relayPort,
					10 * 1024 * 1024, srcConfigs,SCHEMA_REGISTRY_DIR);
			DatabusRelayMain relay3 = DatabusRelayTestUtil.createDatabusRelayWithSchemaReg(1020, relayPort,
					10 * 1024 * 1024, srcConfigs,SCHEMA_REGISTRY_DIR);
			Assert.assertNotNull(relay1);
			Assert.assertNotNull(relay3);
			r1 = new DatabusRelayTestUtil.RelayRunner(relay1);
			final DbusEventsTotalStats stats = relay1
					.getInboundEventStatisticsCollector().getTotalStats();
			final DbusEventsTotalStats stats3 = relay3
					.getInboundEventStatisticsCollector().getTotalStats();

			// create chained relay
			PhysicalSourceConfig[] chainedSrcConfigs = new PhysicalSourceConfig[srcNames.length];
			int j = 0;
			for (String[] srcs : srcNames)
			{

				PhysicalSourceConfig src1 = DatabusRelayTestUtil.createPhysicalConfigBuilder(
						(short) (j + 1),DatabusRelayTestUtil.getPhysicalSrcName(srcs[0]),
						"localhost:" + relayPort, 500, eventRatePerSec,0,largestEventSize,largestWindowSize,srcs);
				chainedSrcConfigs[j++] = src1;
			}
			int chainedRelayPort = relayPort + 1;
			final DatabusRelayMain relay2 = DatabusRelayTestUtil.createDatabusRelayWithSchemaReg(1021,
					chainedRelayPort, 10 * 1024 * 1024, chainedSrcConfigs,SCHEMA_REGISTRY_DIR);
			Assert.assertNotNull(relay2);
			r2 = new DatabusRelayTestUtil.RelayRunner(relay2);

			resetSCN(relay2);

			// now create client:
			String srcSubscriptionString = TestUtil.join(srcNames[0], ",");
			String serverName = "localhost:" + chainedRelayPort;
			CountingConsumer countingConsumer = new CountingConsumer();
			DatabusSourcesConnection clientConn = RelayEventProducer
					.createDatabusSourcesConnection("testProducer", serverName,
							srcSubscriptionString, countingConsumer,
							1 * 1024 * 1024, largestEventSize, 30 * 1000, 100, 15 * 1000,
							1, true,largestEventSize/10);

			final DbusEventsTotalStats stats2 = relay2
					.getInboundEventStatisticsCollector().getTotalStats();

			cr = new ClientRunner(clientConn);

			// async starts for all components;
			r1.start();

			Thread.sleep(10*1000);

			// start chained relay
			r2.start();

			//start client
			cr.start();
			//Pause r1;
			r1.pause();

			Thread.sleep(1000);

			long firstGenDataEvents = stats.getNumDataEvents();
			long firstGenMinScn  = stats.getMinScn();
			long firstGenWindows = stats.getNumSysEvents();

			Assert.assertTrue(stats.getNumSysEvents() > 0);


			log.warn("numDataEvents1=" + firstGenDataEvents
					+ " numWindows1=" + firstGenWindows + " minScn="
					+ firstGenMinScn + " maxScn=" + stats.getMaxScn());


			Thread.sleep(4*1000);
			//clear the relay
			boolean s = r1.shutdown(2000);
			Assert.assertTrue(s);

			long firstGenChainWindows = stats2.getNumSysEvents();
			log.warn("numDataEvents2=" + firstGenChainWindows
					+ " numWindows2=" + stats2.getNumSysEvents() + " minScn=" + stats2.getMinScn() + " maxScn="
					+ stats2.getMaxScn());

			Thread.sleep(2*1000);

			//restart relay
			r3 = new DatabusRelayTestUtil.RelayRunner(relay3);
			r3.start();

			Thread.sleep(15*1000);

			r3.pause();

			Thread.sleep(35000);

			log.warn("numDataEvents3=" + stats3.getNumDataEvents()
					+ " numWindows3=" + stats3.getNumSysEvents() + " minScn="
					+ stats3.getMinScn() +  " maxScn= " + stats3.getMaxScn());

			log.warn("numDataEvents22=" + stats2.getNumDataEvents()
					+ " numWindows22=" + stats2.getNumSysEvents() + " minScn2="
					+ stats2.getMinScn() + " maxScn2=" + stats2.getMaxScn());

			log.warn("consumer=" + countingConsumer);


			//compare chained relays with 2 gens of tier 0 relays
			Assert.assertEquals(stats2.getMinScn(), firstGenMinScn) ;
			Assert.assertEquals(stats2.getMaxScn(), stats3.getMaxScn());
			//the total event windows seen by the chained relay will be state of consumption at first failure of relay1 minus 1 overlap window
			Assert.assertEquals(stats2.getNumSysEvents(), (firstGenChainWindows-1) + stats3.getNumSysEvents());
			Assert.assertTrue(stats2.getNumDataEvents() > stats3.getNumDataEvents());

			//compare source to final client
			Assert.assertEquals(countingConsumer.getNumSources(), 2);
			Assert.assertEquals(stats2.getNumSysEvents(), countingConsumer
					.getNumWindows());

			boolean sorted= true;
			long prev = -1;
			log.info(" scn seq on consumer=");
			for (Long l: countingConsumer.getEndScns())
			{
				sorted = sorted && (l >= prev); prev=l;
				log.info(l+ " ");
				if (!sorted) break;
			}
			Assert.assertTrue(sorted);
		}
		finally
		{
			cleanup ( new DatabusRelayTestUtil.RelayRunner[] {r1,r2,r3} , cr);
	        log.info("end");
		}
	}

  class HttpClientPipelineFactory implements ChannelPipelineFactory
  {
    private final HttpResponseHandler _handler;
    public HttpClientPipelineFactory(HttpResponseHandler handler)
    {
      _handler = handler;
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception
    {
      ChannelPipeline pipeline = new DefaultChannelPipeline();
      pipeline.addLast("codec", new HttpClientCodec());
      pipeline.addLast("aggregator", new HttpChunkAggregator( 1024 * 1024));
      pipeline.addLast("responseHandler", _handler);
      return pipeline;
    }
  }

  class HttpResponseHandler extends SimpleChannelUpstreamHandler
  {
    public String _pendingEventHeader;
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent event)
        throws Exception
    {
      LOG.error("Exception caught during finding max scn" + event.getCause());
      super.exceptionCaught(ctx, event);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent event)
    {
      try
      {
        // Default to HTTP_ERROR
        HttpResponse response = (HttpResponse) event.getMessage();
        if(response.getStatus().equals(HttpResponseStatus.OK))
        {
          ChannelBuffer content = response.getContent();
          _pendingEventHeader = response.getHeader(DatabusHttpHeaders.DATABUS_PENDING_EVENT_SIZE);
        }
        else
        {
          LOG.error("Got Http status (" + response.getStatus() + ") from remote address :" + ctx.getChannel().getRemoteAddress());
          Assert.assertTrue(false);
        }
      }
      catch(Exception ex)
      {
        LOG.error("Got exception while handling Relay MaxSCN response :", ex);
        Assert.assertTrue(false);
      }
      finally
      {
        ctx.getChannel().close();
      }
    }
  }

  private void testClient(int relayPort, int fetchSize, long scn, HttpResponseHandler handler) throws Exception
  {
    Checkpoint ckpt = Checkpoint.createOnlineConsumptionCheckpoint(scn);
    //TODO why is this needed
    //ckpt.setCatchupSource("foo");
    String uristr = "/stream?sources=105&output=json&size=" + fetchSize + "&streamFromLatestScn=false&checkPoint=" + ckpt.toString();
    ClientBootstrap bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
                                                                                      Executors.newCachedThreadPool()));
    bootstrap.setPipelineFactory(new HttpClientPipelineFactory(handler));
    ChannelFuture future = bootstrap.connect(new InetSocketAddress("localhost", relayPort));
    Channel channel = future.awaitUninterruptibly().getChannel();
    Assert.assertTrue(future.isSuccess(), "Cannot connect to relay at localhost:" + relayPort);
    HttpRequest request  = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uristr);
    request.setHeader(HttpHeaders.Names.HOST, "localhost");
    channel.write(request);
    channel.getCloseFuture().awaitUninterruptibly();
  }

  @Test
  /**
   * When the relay has no events, we should not get the x-dbus-pending-event-size even if we present a small buffer.
   * When the relay has events, we should see the header on a small buffer but see an event when the buffer
   * is large enough, and should not see the header in the large buffer case.
   */
  public void testPendingEventSize() throws Exception
  {
    DatabusRelayMain relay = null;
    try
    {
      final short srcId = 104;
      final String srcName = "foo";
      PhysicalSourceConfig pConfig = new PhysicalSourceConfig();
      pConfig.setId(srcId);
      pConfig.setName(srcName);
      pConfig.setUri("mock");
      short lid = (short) (srcId + 1);
      LogicalSourceConfig lConf = new LogicalSourceConfig();
      lConf.setId(lid);
      lConf.setName(srcName);
      // this is table name in the oracle source world
      lConf.setUri(srcName);
      lConf.setPartitionFunction("constant:1");
      pConfig.addSource(lConf);
      int relayPort = Utils.getAvailablePort(11994);
      final int relayId = 666;
      HttpRelay.Config httpRelayConfig = new HttpRelay.Config();
      ServerContainer.Config containerConfig = DatabusRelayTestUtil.createContainerConfig(relayId, relayPort);
      DbusEventBuffer.Config bufferConfig = DatabusRelayTestUtil.createBufferConfig(
          10000, 250, 100);
      httpRelayConfig.setContainer(containerConfig);
      httpRelayConfig.setEventBuffer(bufferConfig);
      httpRelayConfig.setStartDbPuller("true");
      PhysicalSourceStaticConfig[] pStaticConfigs = new PhysicalSourceStaticConfig[1];
      for (LogicalSourceConfig lsc : pConfig.getSources())
      {
        httpRelayConfig.setSourceName("" + lsc.getId(), lsc.getName());
      }
      pStaticConfigs[0] = pConfig.build();
      relay = new DatabusRelayMain(httpRelayConfig.build(), pStaticConfigs);

      relay.start();

      // Insert one event into the relay.
      LogicalSource lsrc = new LogicalSource((int)lid, srcName);
      DbusEventBuffer buf = relay.getEventBuffer().getDbusEventBuffer(lsrc);
      byte [] schema = "abcdefghijklmnop".getBytes();
      final long prevScn = 99;
      final long eventScn = 101;
      buf.start(prevScn);
      buf.startEvents();
      Assert.assertTrue(buf.appendEvent(new DbusEventKey(1),
                                        (short) 100,
                                        (short) 0,
                                        System.currentTimeMillis() * 1000000,
                                        lid,
                                        schema,
                                        new byte[100],
                                        false,
                                        null));
      buf.endEvents(eventScn,  null);


      HttpResponseHandler handler = new HttpResponseHandler();

      // On a good buffer length we should not see the extra header.
      testClient(relayPort, 1000, 100L, handler);
      Assert.assertNull(handler._pendingEventHeader, "Received pending event header on full buffer");

      // We should see the extra header when we get 0 events and the next event is too big to fit in
      testClient(relayPort, 10, 100L, handler);
      Assert.assertNotNull(handler._pendingEventHeader);
      Assert.assertEquals(Integer.valueOf(handler._pendingEventHeader).intValue(), 161);

      // But if there are no events, then we should not see the header even if buffer is very small
      handler._pendingEventHeader = null;
      testClient(relayPort, 10, 1005L, handler);
      Assert.assertNull(handler._pendingEventHeader, "Received pending event header on full buffer");
    }
    finally
    {
      relay.shutdownUninteruptibly();
    }
  }

	@Test
	/**
	 * test resetting connection when there is no events for some period of time
	 * @throws InterruptedException
	 * @throws InvalidConfigException
	 */
  public void testClientNoEventsResetConnection() throws InterruptedException, InvalidConfigException
  {
	  LOG.setLevel(Level.ALL);
    DatabusRelayTestUtil.RelayRunner r1=null;
    ClientRunner cr = null;
    try
    {
      String srcName = "com.linkedin.events.example.Settings";

      // create main relay with random generator
      int eventRatePerSec = 10;
      PhysicalSourceConfig srcConfig = DatabusRelayTestUtil.createPhysicalConfigBuilder(
            (short) 1, DatabusRelayTestUtil.getPhysicalSrcName(srcName), "mock",
            500, eventRatePerSec, new String[] {srcName});

      int relayPort = 11995;
      DatabusRelayMain relay = DatabusRelayTestUtil.createDatabusRelayWithSchemaReg(1001, relayPort,
          10 * 1024 * 1024, new PhysicalSourceConfig[] {srcConfig},SCHEMA_REGISTRY_DIR);
      Assert.assertNotEquals(relay, null);
      r1 = new DatabusRelayTestUtil.RelayRunner(relay);

      // async starts
      r1.start();
      DbusEventsTotalStats stats = relay.getInboundEventStatisticsCollector().getTotalStats();

      // start client in parallel
      String srcSubscriptionString = srcName;
      String serverName = "localhost:" + relayPort;
      ResetsCountingConsumer countingConsumer = new ResetsCountingConsumer();
      DatabusSourcesConnection clientConn = RelayEventProducer
      .createDatabusSourcesConnection("testProducer", serverName,
    		  srcSubscriptionString, countingConsumer,
    		  1 * 1024 * 1024, 50000, 30 * 1000, 100, 15 * 1000,
    		  1, true);

      cr = new ClientRunner(clientConn);
      cr.start();
      TestUtil.sleep(1000); // generate some events

      // pause event generator
      // and wait untill all the events are consumed
      // but since it is less then timeout the connection should NOT reset
      LOG.info("Sending pause to relay!");
      r1.pause();
      TestUtil.sleep(4000);

      LOG.info("no events, time less then threshold. Events=" + countingConsumer.getNumDataEvents() +
    		  "; resets = " + countingConsumer.getNumResets());
      Assert.assertEquals(countingConsumer.getNumResets(), 0);

      // generate more events, more time elapsed then the threshold, but since there are
      // events - NO reset
      r1.unpause();
      Thread.sleep(8000);
      LOG.info("some events, more time then timeout. Events=" + countingConsumer.getNumDataEvents() +
    		  "; resets = " + countingConsumer.getNumResets());
      Assert.assertEquals(countingConsumer.getNumResets(), 0);

      r1.pause(); // stop events
      //set threshold to 0 completely disabling the feature
      clientConn.getRelayPullThread().setNoEventsConnectionResetTimeSec(0);
      Thread.sleep(8000);
      LOG.info("no events, more time then timeout, but feature disabled. Events=" +
    		  countingConsumer.getNumDataEvents() + "; resets = " + countingConsumer.getNumResets());
      Assert.assertEquals(countingConsumer.getNumResets(), 0);


      // enable the feature, and sleep for timeout
      clientConn.getRelayPullThread().setNoEventsConnectionResetTimeSec(5);
      // now wait with no events
      LOG.info("pause the producer. sleep for 6 sec, should reset");
      TestUtil.sleep(6000);

      LOG.info("Client stats=" + countingConsumer);
      LOG.info("Num resets=" + countingConsumer.getNumResets());
      LOG.info("Event windows generated=" + stats.getNumSysEvents());
      Assert.assertEquals(countingConsumer.getNumResets(), 0, "0 resets");
      Assert.assertEquals(countingConsumer.getNumDataEvents(), stats.getNumDataEvents());

      boolean stopped = r1.shutdown(2000);
      Assert.assertTrue(stopped);
      LOG.info("Relay r1 stopped");
      cr.shutdown();
      LOG.info("Client cr stopped");
      Assert.assertEquals(countingConsumer.getNumDataEvents(), stats
    		  .getNumDataEvents());
    }
    finally
    {
      cleanup ( new DatabusRelayTestUtil.RelayRunner[] {r1} , cr);
    }

  }



	static public class ClientRunner extends Thread
	{

		private final DatabusSourcesConnection _conn;

		public ClientRunner(DatabusSourcesConnection conn)
		{
			_conn = conn;
		}

		@Override
        public void run()
		{
			_conn.start();
		}

		public void pause()
		{
			_conn.getConnectionStatus().pause();
		}

		public void unpause()
		{
			_conn.getConnectionStatus().resume();
		}

		public boolean shutdown()
		{
			_conn.stop();
			return true;
		}

        public boolean shutdown(long timeoutMs, final Logger log)
        {
            _conn.stop();
            TestUtil.assertWithBackoff(new ConditionCheck()
            {

              @Override
              public boolean check()
              {
                return !isShutdown();
              }
            }, "waiting for client to shutdown", timeoutMs, log);
            return isShutdown();
        }

		public boolean isShutdown()
		{
		  return _conn.isRunning();
		}

	}

	static public class ResetsCountingConsumer extends CountingConsumer {
	  private volatile int _numResets = -1; //first reset is actually the start
	  @Override
    public ConsumerCallbackResult onStartConsumption()
    {
	    _numResets ++ ;
      return ConsumerCallbackResult.SUCCESS;
    }

	   @Override
	    public ConsumerCallbackResult onStartSource(String source,
	        Schema sourceSchema)
	    {
	      return ConsumerCallbackResult.SUCCESS;
	    }

	   public int getNumResets() {
	     return _numResets;
	   }
	}

	static public class CountingConsumer implements DatabusCombinedConsumer
	{
		protected int _numWindows = 0;
		protected int _numStartWindows = 0;
		protected volatile int _numDataEvents = 0;
		protected int _numErrors = 0;
		protected int _numRollbacks = 0;
		protected final HashSet<String> _srcIds;
		protected final Vector<Long> _startScns;
		protected final Vector<Long> _endScns;
		protected long _delayInMs = 0;

		public CountingConsumer()
		{
			this(0);
		}

		public CountingConsumer(long delayInMs)
		{
			_srcIds = new HashSet<String>();
			_delayInMs = delayInMs;
			_startScns = new Vector<Long>(100);
			_endScns = new Vector<Long>(100);
		}

		@Override
		public ConsumerCallbackResult onStartConsumption()
		{
			return ConsumerCallbackResult.SUCCESS;
		}

		@Override
		public ConsumerCallbackResult onStopConsumption()
		{
			return ConsumerCallbackResult.SUCCESS;
		}

		@Override
		public ConsumerCallbackResult onStartDataEventSequence(SCN startScn)
		{
			long scn = ((SingleSourceSCN) startScn).getSequence();
			if (scn != 0)
				_numStartWindows++;
			_startScns.add(scn);
			return ConsumerCallbackResult.SUCCESS;
		}

		@Override
		public ConsumerCallbackResult onEndDataEventSequence(SCN endScn)
		{
			long scn = ((SingleSourceSCN) endScn).getSequence();
			if (scn != 0)
				_numWindows++;
			_endScns.add(scn);
			return ConsumerCallbackResult.SUCCESS;
		}

		@Override
		public ConsumerCallbackResult onRollback(SCN rollbackScn)
		{
			_numRollbacks++;
			return ConsumerCallbackResult.SUCCESS;
		}

		@Override
		public ConsumerCallbackResult onStartSource(String source,
				Schema sourceSchema)
		{
			_srcIds.add(source);
			return ConsumerCallbackResult.SUCCESS;
		}

		@Override
		public ConsumerCallbackResult onEndSource(String source,
				Schema sourceSchema)
		{
			return ConsumerCallbackResult.SUCCESS;
		}

		@Override
		public ConsumerCallbackResult onDataEvent(DbusEvent e,
				DbusEventDecoder eventDecoder)
		{
			_numDataEvents++;
			if (_delayInMs != 0)
			{
				try
				{
					Thread.sleep(_delayInMs);
				}
				catch (InterruptedException e1)
				{

				}
			}
			return ConsumerCallbackResult.SUCCESS;
		}

		@Override
		public ConsumerCallbackResult onCheckpoint(SCN checkpointScn)
		{
			return ConsumerCallbackResult.SUCCESS;
		}

		@Override
		public ConsumerCallbackResult onError(Throwable err)
		{
			_numErrors++;
			return ConsumerCallbackResult.SUCCESS;
		}

		@Override
		public ConsumerCallbackResult onStartBootstrap()
		{
			return ConsumerCallbackResult.SUCCESS;
		}

		@Override
		public ConsumerCallbackResult onStopBootstrap()
		{
			return ConsumerCallbackResult.SUCCESS;
		}

		@Override
		public ConsumerCallbackResult onStartBootstrapSequence(SCN startScn)
		{
			return onStartDataEventSequence(startScn);
		}

		@Override
		public ConsumerCallbackResult onEndBootstrapSequence(SCN endScn)
		{
			return onEndDataEventSequence(endScn);
		}

		@Override
		public ConsumerCallbackResult onStartBootstrapSource(String sourceName,
				Schema sourceSchema)
		{
			return onStartBootstrapSource(sourceName, sourceSchema);
		}

		@Override
		public ConsumerCallbackResult onEndBootstrapSource(String name,
				Schema sourceSchema)
		{
			return onEndBootstrapSource(name, sourceSchema);
		}

		@Override
		public ConsumerCallbackResult onBootstrapEvent(DbusEvent e,
				DbusEventDecoder eventDecoder)
		{
			return onDataEvent(e, eventDecoder);
		}

		@Override
		public ConsumerCallbackResult onBootstrapRollback(SCN batchCheckpointScn)
		{
			return onRollback(batchCheckpointScn);
		}

		@Override
		public ConsumerCallbackResult onBootstrapCheckpoint(SCN checkpointScn)
		{
			return onCheckpoint(checkpointScn);
		}

		@Override
		public ConsumerCallbackResult onBootstrapError(Throwable err)
		{
			return onError(err);
		}

		public Vector<Long> getStartScns()
		{
			return _startScns;
		}

		public Vector<Long> getEndScns()
		{
			return _endScns;
		}

		public int getNumWindows()
		{
			return _numWindows;
		}

		public int getNumDataEvents()
		{
			return _numDataEvents;
		}

		public int getNumErrors()
		{
			return _numErrors;
		}

		public int getNumSources()
		{
			return _srcIds.size();
		}

		public int getNumStartWindows()
		{
			return _numStartWindows;
		}

		public HashSet<String> getSources()
		{
			return _srcIds;
		}

		public int getNumRollbacks()
		{
			return _numRollbacks;
		}

		@Override
        public String toString()
		{
			StringBuilder s = new StringBuilder();
			s.append(" numWindows= ").append(_numWindows)
					.append(" numStartWindows=").append(_numStartWindows)
					.append(" numDataEvents=").append(_numDataEvents)
					.append(" numSources=").append(getNumSources())
					.append(" numErrors=").append(_numErrors)
					.append(" numRollbacks=").append(_numRollbacks);
			return s.toString();
		}

		@Override
		public boolean canBootstrap() {
			return true;
		}

	}

}
