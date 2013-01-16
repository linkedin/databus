package com.linkedin.databus2.relay;

import java.io.IOException;
import java.util.HashSet;
import java.util.Vector;
import java.util.concurrent.TimeoutException;

import org.apache.avro.Schema;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.databus.client.DatabusSourcesConnection;
import com.linkedin.databus.client.SingleSourceSCN;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DatabusCombinedConsumer;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.client.pub.SCN;
import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsTotalStats;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.BackoffTimerStaticConfigBuilder;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.netty.ServerContainer;
import com.linkedin.databus2.core.seq.MaxSCNReaderWriter;
import com.linkedin.databus2.producers.EventProducer;
import com.linkedin.databus2.producers.RelayEventProducer;
import com.linkedin.databus2.relay.config.LogicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import com.linkedin.databus2.test.ConditionCheck;
import com.linkedin.databus2.test.TestUtil;

public class TestDatabusRelayMain
{
	static
	{
		 TestUtil.setupLogging(true, "TestDatabusRelayMain.log", Level.WARN);
	}

	public final static String MODULE = TestDatabusRelayMain.class.getName();
	public final static Logger LOG = Logger.getLogger(MODULE);

	@BeforeMethod
	public void beforeMethod()
	{
	}

	@AfterMethod
	public void afterMethod()
	{
	}

	public static DbusEventBuffer.Config createBufferConfig(long eventBufferMaxSize,
			int readBufferSize, int scnIndexSize)
	{
		DbusEventBuffer.Config bconf = new DbusEventBuffer.Config();
		bconf.setMaxSize(eventBufferMaxSize);
		bconf.setReadBufferSize(readBufferSize);
		bconf.setScnIndexSize(scnIndexSize);
		bconf.setAllocationPolicy("DIRECT_MEMORY");
		return bconf;
	}

	/**
	 * @return
	 */
	public static ServerContainer.Config createContainerConfig(int id, int httpPort)
	{
		ServerContainer.Config sconf = new ServerContainer.Config();
		sconf.setHealthcheckPath("/admin");
		sconf.setHttpPort(httpPort);
		sconf.setId(id);
		sconf.getJmx().setRmiEnabled(false);
		return sconf;
	}


	public static PhysicalSourceConfig createPhysicalConfigBuilder(short id,
			String name, String uri, long pollIntervalMs, int eventRatePerSec,
			String[] logicalSources)
	{
		return createPhysicalConfigBuilder(id, name, uri, pollIntervalMs, eventRatePerSec,0,1*1024*1024,5*1024*1024,logicalSources);
	}

	public static PhysicalSourceConfig createPhysicalConfigBuilder(short id,
			String name, String uri, long pollIntervalMs, int eventRatePerSec,
			long restartScnOffset,int largestEventSize, long largestWindowSize,
			String[] logicalSources)
	{
		PhysicalSourceConfig pConfig = new PhysicalSourceConfig();
		pConfig.setId(id);
		pConfig.setName(name);
		pConfig.setUri(uri);
		pConfig.setEventRatePerSec(eventRatePerSec);
		pConfig.setRestartScnOffset(restartScnOffset);
		pConfig.setLargestEventSizeInBytes(largestEventSize);
		pConfig.setLargestWindowSizeInBytes(largestWindowSize);
		BackoffTimerStaticConfigBuilder retriesConf = new BackoffTimerStaticConfigBuilder();
		retriesConf.setInitSleep(pollIntervalMs);
		pConfig.setRetries(retriesConf);
		short lid = (short) (id + 1);
		for (String schemaName : logicalSources)
		{
			LogicalSourceConfig lConf = new LogicalSourceConfig();
			lConf.setId(lid++);
			lConf.setName(schemaName);
			// this is table name in the oracle source world
			lConf.setUri(schemaName);
			lConf.setPartitionFunction("constant:1");
			pConfig.addSource(lConf);
		}
		return pConfig;
	}

	public static DatabusRelayMain createDatabusRelay(int relayId, int httpPort,
			int maxBufferSize, PhysicalSourceConfig[] sourceConfigs)
	{
		try
		{
			HttpRelay.Config httpRelayConfig = new HttpRelay.Config();
			ServerContainer.Config containerConfig = createContainerConfig(
					relayId, httpPort);
			DbusEventBuffer.Config bufferConfig = createBufferConfig(
					maxBufferSize, maxBufferSize / 10, maxBufferSize / 10);
			httpRelayConfig.setContainer(containerConfig);
			httpRelayConfig.setEventBuffer(bufferConfig);
			httpRelayConfig.setStartDbPuller("true");
			PhysicalSourceStaticConfig[] pStaticConfigs = new PhysicalSourceStaticConfig[sourceConfigs.length];
			int i = 0;
			for (PhysicalSourceConfig pConf : sourceConfigs)
			{
				// register logical sources! I guess the physical db name is
				// prefixed to the id or what?
				for (LogicalSourceConfig lsc : pConf.getSources())
				{
					httpRelayConfig.setSourceName("" + lsc.getId(),
							lsc.getName());
				}
				pStaticConfigs[i++] = pConf.build();
			}
			DatabusRelayMain relayMain = new DatabusRelayMain(
					httpRelayConfig.build(), pStaticConfigs);
			return relayMain;
		}
		catch (IOException e)
		{
			LOG.error("IO Exception " + e);
		}
		catch (InvalidConfigException e)
		{
			LOG.error("Invalid config " + e);
		}
		catch (DatabusException e)
		{
			LOG.error("Databus Exception " + e);
		}
		return null;
	}

	public static String getPhysicalSrcName(String s)
	{
		String[] cmpt = s.split("\\.");
		String name = (cmpt.length >= 4) ? cmpt[3] : s;
		return name;
	}

	String join(String[] name, String delim)
	{
		StringBuilder joined = new StringBuilder();
		if (name.length > 0)
		{
			joined.append(name[0]);
			for (int i = 1; i < name.length; ++i)
			{
				joined.append(delim);
				joined.append(name[i]);
			}
		}
		return joined.toString();
	}

	//cleanup
	void cleanup(RelayRunner[] relayRunners,ClientRunner clientRunner)
	{
		LOG.info("Starting cleanup");
		for (RelayRunner r1: relayRunners)
		{
			if(null != r1)
				Assert.assertTrue(r1.shutdown(2000));
		}
		Assert.assertNotNull(clientRunner);
		clientRunner.shutdown();
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

	@Test
	public void testRelayEventGenerator() throws InterruptedException, InvalidConfigException
	{
		RelayRunner r1=null;
		ClientRunner cr = null;
		try
		{
			String[][] srcNames =
			{
			{ "com.linkedin.events.member2.account.MemberAccount",
					"com.linkedin.events.member2.privacy.PrivacySettings" }, };

			// create main relay with random generator
			PhysicalSourceConfig[] srcConfigs = new PhysicalSourceConfig[srcNames.length];
			int i = 0;
			int eventRatePerSec = 10;
			for (String[] srcs : srcNames)
			{

				PhysicalSourceConfig src1 = createPhysicalConfigBuilder(
						(short) (i + 1), getPhysicalSrcName(srcs[0]), "mock",
						500, eventRatePerSec, srcs);
				srcConfigs[i++] = src1;
			}
			int relayPort = 11993;
			DatabusRelayMain relay1 = createDatabusRelay(1001, relayPort,
					10 * 1024 * 1024, srcConfigs);
			Assert.assertNotEquals(relay1, null);
			r1 = new RelayRunner(relay1);

			// async starts
			r1.start();

			// start client in parallel
			String srcSubscriptionString = join(srcNames[0], ",");
			String serverName = "localhost:" + relayPort;
			CountingConsumer countingConsumer = new CountingConsumer();
			DatabusSourcesConnection clientConn = RelayEventProducer
					.createDatabusSourcesConnection("testProducer", serverName,
							srcSubscriptionString, countingConsumer,
							1 * 1024 * 1024, 50000, 30 * 1000, 100, 15 * 1000,
							1, true);
			cr = new ClientRunner(clientConn);
			cr.start();
			// terminating conditions
			DbusEventsTotalStats stats = relay1
					.getInboundEventStatisticsCollector().getTotalStats();
			long totalRunTime = 10000;
			long startTime = System.currentTimeMillis();
			do
			{
				LOG.info("numDataEvents=" + stats.getNumDataEvents()
						+ " numWindows=" + stats.getNumSysEvents() + " size="
						+ stats.getSizeDataEvents());
				Thread.sleep(1000);
			}
			while ((System.currentTimeMillis() - startTime) < totalRunTime);

			r1.pause();
			LOG.info("Sending pause to relay!");
			LOG.info("numDataEvents=" + stats.getNumDataEvents()
					+ " numWindows=" + stats.getNumSysEvents() + " size="
					+ stats.getSizeDataEvents());
			Thread.sleep(4000);
			for (EventProducer p: relay1.getProducers())
			{
				Assert.assertTrue(p.isPaused());
			}

			// wait until client got all events or for maxTimeout;
			long maxTimeOutMs = 5 * 1000;
			startTime = System.currentTimeMillis();
			while (countingConsumer.getNumWindows() <= stats.getNumSysEvents())
			{
				Thread.sleep(1000);
				if ((System.currentTimeMillis() - startTime) > maxTimeOutMs)
				{
					break;
				}
			}

			LOG.info("Client stats=" + countingConsumer);
			LOG.info("Event windows generated="
					+ stats.getNumSysEvents());

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
			cleanup ( new RelayRunner[] {r1} , cr);
		}

	}

	@Test
	/**
	 * Concurrent consumption of chained relay by client and by chained relay from regular relay
	 */
	public void testRelayChainingBasic() throws InterruptedException, InvalidConfigException
	{
		RelayRunner r1=null,r2=null;
		ClientRunner cr = null;
		try
		{
			String[][] srcNames =
			{
			{ "com.linkedin.events.member2.account.MemberAccount",
					"com.linkedin.events.member2.privacy.PrivacySettings" }, };

			// create main relay with random generator
			PhysicalSourceConfig[] srcConfigs = new PhysicalSourceConfig[srcNames.length];
			int i = 0;
			int eventRatePerSec = 2;
			for (String[] srcs : srcNames)
			{

				PhysicalSourceConfig src1 = createPhysicalConfigBuilder(
						(short) (i + 1), getPhysicalSrcName(srcs[0]), "mock",
						500, eventRatePerSec, srcs);
				srcConfigs[i++] = src1;
			}
			int relayPort = 11993;
			DatabusRelayMain relay1 = createDatabusRelay(1002, relayPort,
					10 * 1024 * 1024, srcConfigs);
			Assert.assertNotNull(relay1);
			r1 = new RelayRunner(relay1);

			// create chained relay
			PhysicalSourceConfig[] chainedSrcConfigs = new PhysicalSourceConfig[srcNames.length];
			int j = 0;
			for (String[] srcs : srcNames)
			{

				PhysicalSourceConfig src1 = createPhysicalConfigBuilder(
						(short) (j + 1), getPhysicalSrcName(srcs[0]),
						"localhost:" + relayPort, eventRatePerSec, 50, srcs);
				chainedSrcConfigs[j++] = src1;
			}
			int chainedRelayPort = relayPort + 1;
			DatabusRelayMain relay2 = createDatabusRelay(1003,
					chainedRelayPort, 1 * 1024 * 1024, chainedSrcConfigs);
			Assert.assertNotNull(relay2);
			r2 = new RelayRunner(relay2);

			resetSCN(relay2);

			// now create client:
			String srcSubscriptionString = join(srcNames[0], ",");
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
			Thread.sleep(5*1000);
			cr.start();

			r1.pause();
			// let it run for 10 seconds
			Thread.sleep(10 * 1000);


			// wait until client got all events or for maxTimeout;
			long maxTimeOutMs = 5 * 1000;
			long startTime = System.currentTimeMillis();
			DbusEventsTotalStats stats = relay1
					.getInboundEventStatisticsCollector().getTotalStats();
			DbusEventsTotalStats stats2 = relay2
					.getInboundEventStatisticsCollector().getTotalStats();
			while (countingConsumer.getNumWindows() < stats.getNumSysEvents())
			{
				Thread.sleep(500);
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
			cleanup ( new RelayRunner[] {r1,r2} , cr);
		}

	}



	@Test
	/** Client and chained relay start before relay, simulating unavailable relay and retries at chained relay and client
	 *
	 */
	public void testRelayChainingConnectFailure()
	{
		RelayRunner r1=null,r2=null;
		ClientRunner cr = null;
		try
		{
			String[][] srcNames =
			{
			{ "com.linkedin.events.member2.account.MemberAccount",
					"com.linkedin.events.member2.privacy.PrivacySettings" }, };

			// create main relay with random generator
			PhysicalSourceConfig[] srcConfigs = new PhysicalSourceConfig[srcNames.length];
			int i = 0;
			int eventRatePerSec = 10;
			for (String[] srcs : srcNames)
			{

				PhysicalSourceConfig src1 = createPhysicalConfigBuilder(
						(short) (i + 1), getPhysicalSrcName(srcs[0]), "mock",
						500, eventRatePerSec, srcs);
				srcConfigs[i++] = src1;
			}
			int relayPort = 11993;
			DatabusRelayMain relay1 = createDatabusRelay(1004, relayPort,
					10 * 1024 * 1024, srcConfigs);
			Assert.assertTrue(null != relay1);
			r1 = new RelayRunner(relay1);

			// create chained relay
			PhysicalSourceConfig[] chainedSrcConfigs = new PhysicalSourceConfig[srcNames.length];
			int j = 0;
			for (String[] srcs : srcNames)
			{

				PhysicalSourceConfig src1 = createPhysicalConfigBuilder(
						(short) (j + 1), getPhysicalSrcName(srcs[0]),
						"localhost:" + relayPort, eventRatePerSec, 50, srcs);
				chainedSrcConfigs[j++] = src1;
			}

			int chainedRelayPort = relayPort + 1;
			DatabusRelayMain relay2 = createDatabusRelay(1005,
					chainedRelayPort, 10 * 1024 * 1024, chainedSrcConfigs);
			Assert.assertTrue(null != relay2);
			r2 = new RelayRunner(relay2);
			resetSCN(relay2);

			// now create client:
			String srcSubscriptionString = join(srcNames[0], ",");
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
			cleanup ( new RelayRunner[] {r1,r2} , cr);
		}

	}

	@Test
	/**
	 * Client consumes subset of sources from chained relay
	 */
	public void testRelayChainingPartialSubscribe()
	{
		RelayRunner r1=null,r2=null;
		ClientRunner cr = null;
		try
		{
			String[][] srcNames =
			{
			{ "com.linkedin.events.member2.account.MemberAccount",
					"com.linkedin.events.member2.privacy.PrivacySettings" }, };

			// create main relay with random generator
			PhysicalSourceConfig[] srcConfigs = new PhysicalSourceConfig[srcNames.length];
			int i = 0;
			int eventRatePerSec = 10;
			for (String[] srcs : srcNames)
			{

				PhysicalSourceConfig src1 = createPhysicalConfigBuilder(
						(short) (i + 1), getPhysicalSrcName(srcs[0]), "mock",
						500, eventRatePerSec, srcs);
				srcConfigs[i++] = src1;
			}
			int relayPort = 11993;
			DatabusRelayMain relay1 = createDatabusRelay(1006, relayPort,
					10 * 1024 * 1024, srcConfigs);
			Assert.assertTrue(null != relay1);
			r1 = new RelayRunner(relay1);

			// create chained relay
			PhysicalSourceConfig[] chainedSrcConfigs = new PhysicalSourceConfig[srcNames.length];
			int j = 0;
			for (String[] srcs : srcNames)
			{

				PhysicalSourceConfig src1 = createPhysicalConfigBuilder(
						(short) (j + 1), getPhysicalSrcName(srcs[0]),
						"localhost:" + relayPort, eventRatePerSec, 50, srcs);
				chainedSrcConfigs[j++] = src1;
			}

			int chainedRelayPort = relayPort + 1;
			DatabusRelayMain relay2 = createDatabusRelay(1007,
					chainedRelayPort, 10 * 1024 * 1024, chainedSrcConfigs);
			Assert.assertTrue(null != relay2);
			resetSCN(relay2);

			r2 = new RelayRunner(relay2);

			// now create client:
			String srcSubscriptionString = "com.linkedin.events.member2.privacy.PrivacySettings";
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
			cleanup ( new RelayRunner[] {r1,r2} , cr);
		}

	}

	@Test
	/**
	 * Chained relay consumes a subset of parent relay
	 */
	public void testRelayChainingPartialSubscribeRelay()
	{
		RelayRunner r1=null,r2=null;
		ClientRunner cr = null;
		try
		{
			String[][] srcNames =
			{
			{ "com.linkedin.events.member2.account.MemberAccount",
					"com.linkedin.events.member2.privacy.PrivacySettings" }, };

			// create main relay with random generator
			PhysicalSourceConfig[] srcConfigs = new PhysicalSourceConfig[srcNames.length];
			int i = 0;
			int eventRatePerSec = 10;
			for (String[] srcs : srcNames)
			{

				PhysicalSourceConfig src1 = createPhysicalConfigBuilder(
						(short) (i + 1), getPhysicalSrcName(srcs[0]), "mock",
						500, eventRatePerSec, srcs);
				srcConfigs[i++] = src1;
			}
			int relayPort = 11993;
			DatabusRelayMain relay1 = createDatabusRelay(1008, relayPort,
					10 * 1024 * 1024, srcConfigs);
			Assert.assertTrue(null != relay1);
			r1 = new RelayRunner(relay1);

			// create chained relay
			PhysicalSourceConfig[] chainedSrcConfigs = new PhysicalSourceConfig[srcNames.length];
			int j = 0;
			for (String[] srcs : srcNames)
			{
				String partialSrcs[] = new String[1];
				partialSrcs[0] = srcs[srcs.length - 1];
				PhysicalSourceConfig src1 = createPhysicalConfigBuilder(
						(short) (srcs.length), getPhysicalSrcName(srcs[0]),
						"localhost:" + relayPort, eventRatePerSec, 50,
						partialSrcs);
				chainedSrcConfigs[j++] = src1;
			}

			int chainedRelayPort = relayPort + 1;
			DatabusRelayMain relay2 = createDatabusRelay(1009,
					chainedRelayPort, 10 * 1024 * 1024, chainedSrcConfigs);
			Assert.assertTrue(null != relay2);
			resetSCN(relay2);

			r2 = new RelayRunner(relay2);

			// now create client:
			String srcSubscriptionString = "com.linkedin.events.member2.privacy.PrivacySettings";
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
			cleanup ( new RelayRunner[] {r1,r2} , cr);
		}

	}

	@Test
	/**
	 * Chained relay overwrites buffer; consumer starts late; gets only the last n events that fit.
	 * Makes sure chained relay overwrites buffer;just like main buffer
	 */
	public void testRelayChainingDelayedConsumer()
	{
		RelayRunner r1=null,r2=null;
		ClientRunner cr = null;
		try
		{
			String[][] srcNames =
			{
			{ "com.linkedin.events.member2.account.MemberAccount",
					"com.linkedin.events.member2.privacy.PrivacySettings" }, };

			// create main relay with random generator
			PhysicalSourceConfig[] srcConfigs = new PhysicalSourceConfig[srcNames.length];
			int i = 0;
			int eventRatePerSec = 10;
			for (String[] srcs : srcNames)
			{

				PhysicalSourceConfig src1 = createPhysicalConfigBuilder(
						(short) (i + 1), getPhysicalSrcName(srcs[0]), "mock",
						500, eventRatePerSec, srcs);
				srcConfigs[i++] = src1;
			}
			int relayPort = 11993;
			DatabusRelayMain relay1 = createDatabusRelay(1010, relayPort,
					10 * 1024 * 1024, srcConfigs);
			Assert.assertTrue(null != relay1);
			r1 = new RelayRunner(relay1);

			// create chained relay with only 1 MB buffer
			PhysicalSourceConfig[] chainedSrcConfigs = new PhysicalSourceConfig[srcNames.length];
			int j = 0;
			for (String[] srcs : srcNames)
			{

				PhysicalSourceConfig src1 = createPhysicalConfigBuilder(
						(short) (j + 1), getPhysicalSrcName(srcs[0]),
						"localhost:" + relayPort, eventRatePerSec, 50, srcs);
				chainedSrcConfigs[j++] = src1;
			}
			int chainedRelayPort = relayPort + 1;
			DatabusRelayMain relay2 = createDatabusRelay(1011,
					chainedRelayPort, 1 * 1024 * 1024, chainedSrcConfigs);
			Assert.assertTrue(null != relay2);
			resetSCN(relay2);

			r2 = new RelayRunner(relay2);


			// now create client:
			String srcSubscriptionString = join(srcNames[0], ",");
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
			cleanup ( new RelayRunner[] {r1,r2} , cr);
		}

	}

	@Test
	/**
	 * Slow consumer; results in getting SCN not found exception ; as chained relay overwrites buffer .
	 * Makes sure chained relay overwrites buffer;just like main buffer
	 */
	public void testRelayChainingSlowConsumer()
	{
		RelayRunner r1=null,r2=null;
		ClientRunner cr = null;
		try
		{
			String[][] srcNames =
			{
			{ "com.linkedin.events.member2.account.MemberAccount",
					"com.linkedin.events.member2.privacy.PrivacySettings" }, };

			// create main relay with random generator
			PhysicalSourceConfig[] srcConfigs = new PhysicalSourceConfig[srcNames.length];
			int i = 0;
			int eventRatePerSec = 10;
			for (String[] srcs : srcNames)
			{

				PhysicalSourceConfig src1 = createPhysicalConfigBuilder(
						(short) (i + 1), getPhysicalSrcName(srcs[0]), "mock",
						500, eventRatePerSec, srcs);
				srcConfigs[i++] = src1;
			}
			int relayPort = 11993;
			DatabusRelayMain relay1 = createDatabusRelay(1012, relayPort,
					10 * 1024 * 1024, srcConfigs);
			Assert.assertTrue(null != relay1);
			r1 = new RelayRunner(relay1);

			// create chained relay with only 1 MB buffer
			PhysicalSourceConfig[] chainedSrcConfigs = new PhysicalSourceConfig[srcNames.length];
			int j = 0;
			for (String[] srcs : srcNames)
			{

				PhysicalSourceConfig src1 = createPhysicalConfigBuilder(
						(short) (j + 1), getPhysicalSrcName(srcs[0]),
						"localhost:" + relayPort, eventRatePerSec, 50, srcs);
				chainedSrcConfigs[j++] = src1;
			}
			int chainedRelayPort = relayPort + 1;
			DatabusRelayMain relay2 = createDatabusRelay(1013,
					chainedRelayPort, 1024 * 1024, chainedSrcConfigs);
			Assert.assertTrue(null != relay2);
			resetSCN(relay2);

			r2 = new RelayRunner(relay2);

			// now create client:
			String srcSubscriptionString = join(srcNames[0], ",");
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
			cleanup ( new RelayRunner[] {r1,r2} , cr);
		}

	}

	@Test
	/**
	 * Regular concurrent consumption amongst db-relay , chained relay and client. The only twist is that the chained relay
	 * will be paused and resumed
	 */
	public void testRelayChainingPauseResume()
	{
		RelayRunner r1=null,r2=null;
		ClientRunner cr = null;
		try
		{
			String[][] srcNames =
			{
			{ "com.linkedin.events.member2.account.MemberAccount",
					"com.linkedin.events.member2.privacy.PrivacySettings" }, };

			// create main relay with random generator
			PhysicalSourceConfig[] srcConfigs = new PhysicalSourceConfig[srcNames.length];
			int i = 0;
			int eventRatePerSec = 10;
			for (String[] srcs : srcNames)
			{

				PhysicalSourceConfig src1 = createPhysicalConfigBuilder(
						(short) (i + 1), getPhysicalSrcName(srcs[0]), "mock",
						500, eventRatePerSec, srcs);
				srcConfigs[i++] = src1;
			}
			int relayPort = 11993;
			DatabusRelayMain relay1 = createDatabusRelay(1014, relayPort,
					10 * 1024 * 1024, srcConfigs);
			Assert.assertTrue(null != relay1);
			r1 = new RelayRunner(relay1);

			// create chained relay
			PhysicalSourceConfig[] chainedSrcConfigs = new PhysicalSourceConfig[srcNames.length];
			int j = 0;
			for (String[] srcs : srcNames)
			{

				PhysicalSourceConfig src1 = createPhysicalConfigBuilder(
						(short) (j + 1), getPhysicalSrcName(srcs[0]),
						"localhost:" + relayPort, eventRatePerSec, 50, srcs);
				chainedSrcConfigs[j++] = src1;
			}
			int chainedRelayPort = relayPort + 1;
			DatabusRelayMain relay2 = createDatabusRelay(1015,
					chainedRelayPort, 1 * 1024 * 1024, chainedSrcConfigs);
			Assert.assertTrue(null != relay2);
			resetSCN(relay2);

			r2 = new RelayRunner(relay2);

			// now create client:
			String srcSubscriptionString = join(srcNames[0], ",");
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
			cleanup ( new RelayRunner[] {r1,r2} , cr);
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
		RelayRunner r1=null,r2=null,r3=null;
		ClientRunner cr = null;
		try
		{
			String[][] srcNames =
			{
			{ "com.linkedin.events.member2.account.MemberAccount",
					"com.linkedin.events.member2.privacy.PrivacySettings" }, };

			// create main relay with random generator
			PhysicalSourceConfig[] srcConfigs = new PhysicalSourceConfig[srcNames.length];
			int i = 0;
			int eventRatePerSec = 2;
			for (String[] srcs : srcNames)
			{

				PhysicalSourceConfig src1 = createPhysicalConfigBuilder(
						(short) (i + 1), getPhysicalSrcName(srcs[0]), "mock",
						500, eventRatePerSec, srcs);
				srcConfigs[i++] = src1;
			}
			int relayPort = 11993;
			DatabusRelayMain relay1 = createDatabusRelay(1016, relayPort,
					10 * 1024 * 1024, srcConfigs);
			Assert.assertTrue(null != relay1);
			r1 = new RelayRunner(relay1);

			// create chained relay
			PhysicalSourceConfig[] chainedSrcConfigs = new PhysicalSourceConfig[srcNames.length];
			int j = 0;
			for (String[] srcs : srcNames)
			{

				PhysicalSourceConfig src1 = createPhysicalConfigBuilder(
						(short) (j + 1), getPhysicalSrcName(srcs[0]),
						"localhost:" + relayPort, eventRatePerSec, 50, srcs);
				chainedSrcConfigs[j++] = src1;
			}
			int chainedRelayPort = relayPort + 1;
			DatabusRelayMain relay2 = createDatabusRelay(1017,
					chainedRelayPort, 1 * 1024 * 1024, chainedSrcConfigs);
			Assert.assertTrue(null != relay2);
			r2 = new RelayRunner(relay2);

			resetSCN(relay2);

			// now create client:
			String srcSubscriptionString = join(srcNames[0], ",");
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
			DatabusRelayMain relay3 = createDatabusRelay(1018,
					chainedRelayPort, 1 * 1024 * 1024, chainedSrcConfigs);
			r3 = new RelayRunner(relay3);
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
			cleanup ( new RelayRunner[] {r1,r2,r3} , cr);
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
		RelayRunner r1=null,r2=null,r3 = null;
		ClientRunner cr = null;
		//log.setLevel(Level.DEBUG);
		log.info("start");
		//DbusEventBuffer.LOG.setLevel(Level.DEBUG);
		try
		{
			String[][] srcNames =
			{
			{ "com.linkedin.events.member2.account.MemberAccount",
              "com.linkedin.events.member2.privacy.PrivacySettings" }, };

			// create main relay with random generator
			PhysicalSourceConfig[] srcConfigs = new PhysicalSourceConfig[srcNames.length];
			int i = 0;
			int eventRatePerSec = 10;
			int largestEventSize = 15*1024;
			long largestWindowSize = 100*1024;
			for (String[] srcs : srcNames)
			{

				PhysicalSourceConfig src1 = createPhysicalConfigBuilder(
						(short) (i + 1), getPhysicalSrcName(srcs[0]), "mock",
						500, eventRatePerSec, srcs);
				srcConfigs[i++] = src1;
			}
			int relayPort = 11993;
			DatabusRelayMain relay1 = createDatabusRelay(1019, relayPort,
					10 * 1024 * 1024, srcConfigs);
			DatabusRelayMain relay3 = createDatabusRelay(1020, relayPort,
					10 * 1024 * 1024, srcConfigs);
			Assert.assertNotNull(relay1);
			Assert.assertNotNull(relay3);
			r1 = new RelayRunner(relay1);
			final DbusEventsTotalStats stats = relay1
					.getInboundEventStatisticsCollector().getTotalStats();
			final DbusEventsTotalStats stats3 = relay3
					.getInboundEventStatisticsCollector().getTotalStats();

			// create chained relay
			PhysicalSourceConfig[] chainedSrcConfigs = new PhysicalSourceConfig[srcNames.length];
			int j = 0;
			for (String[] srcs : srcNames)
			{

				PhysicalSourceConfig src1 = createPhysicalConfigBuilder(
						(short) (j + 1), getPhysicalSrcName(srcs[0]),
						"localhost:" + relayPort, 500, eventRatePerSec,0,largestEventSize,largestWindowSize,srcs);
				chainedSrcConfigs[j++] = src1;
			}
			int chainedRelayPort = relayPort + 1;
			final DatabusRelayMain relay2 = createDatabusRelay(1021,
					chainedRelayPort, 10 * 1024 * 1024, chainedSrcConfigs);
			Assert.assertNotNull(relay2);
			r2 = new RelayRunner(relay2);

			resetSCN(relay2);

			// now create client:
			String srcSubscriptionString = join(srcNames[0], ",");
			String serverName = "localhost:" + chainedRelayPort;
			CountingConsumer countingConsumer = new CountingConsumer();
			DatabusSourcesConnection clientConn = RelayEventProducer
					.createDatabusSourcesConnection("testProducer", serverName,
							srcSubscriptionString, countingConsumer,
							1 * 1024 * 1024, 50000, 30 * 1000, 100, 15 * 1000,
							1, true);

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


			log.info("numDataEvents1=" + firstGenDataEvents
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
			r3 = new RelayRunner(relay3);
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
			cleanup ( new RelayRunner[] {r1,r2,r3} , cr);
	        log.info("end");
		}
	}

	static public class RelayRunner extends Thread
	{
		private final DatabusRelayMain _relay;

		public RelayRunner(DatabusRelayMain relay)
		{
			_relay = relay;
			Assert.assertTrue(_relay != null);
		}

		@Override
		public void run()
		{
			try
			{
				_relay.initProducers();
			}
			catch (Exception e)
			{
				LOG.error("Exception " + e);
				return;
			}
			_relay.startAsynchronously();
		}

		public void pause()
		{
			_relay.pause();
		}

		public void unpause()
		{
			_relay.resume();
		}

		public boolean shutdown(int timeoutMs)
		{
			_relay.shutdown();
			try
			{
				if (timeoutMs > 0)
				{
					_relay.awaitShutdown(timeoutMs);
				}
				else
				{
					_relay.awaitShutdown();
				}
			}
			catch (TimeoutException e)
			{
				LOG.error("Not shutdown in " + timeoutMs + " ms");
				return false;
			}
			catch (InterruptedException e)
			{
				LOG.info("Interrupted before " + timeoutMs + " ms");
				return true;
			}
			return true;
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

	}

	static public class CountingConsumer implements DatabusCombinedConsumer
	{
		private int _numWindows = 0;
		private int _numStartWindows = 0;
		private int _numDataEvents = 0;
		private int _numErrors = 0;
		private int _numRollbacks = 0;
		private final HashSet<String> _srcIds;
		private final Vector<Long> _startScns;
		private final Vector<Long> _endScns;
		private long _delayInMs = 0;

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
