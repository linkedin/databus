/*
 * $Id: RelayStartable.java 267534 2011-05-06 02:16:55Z cbotev $
 */
package com.linkedin.databus2.relayli;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.xeril.util.Shutdownable;
import org.xeril.util.Startable;
import org.xeril.util.TimeOutException;

import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus.container.netty.HttpRelay.StaticConfig;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus2.relay.DatabusRelayMain;
import com.linkedin.databus2.relay.config.LogicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;

/**
 * @author Jemiah Westerman<jwesterman@linkedin.com>
 * @version $Revision: 267534 $
 */
public class RelayStartable implements Startable, Shutdownable {
	private DatabusRelayMain _relay;
	private PhysicalSourceConfig[] _physicalSourceConfigs;
	private Logger _log = Logger.getLogger(getClass());

	private void init(Properties properties, Properties override,
			PhysicalSourceConfig[] physicalSourceConfigs) throws Exception {
		// Merge the default properties and override properties into a single
		// new properties object
		Properties props = new Properties();
		for (Map.Entry<Object, Object> entry : properties.entrySet()) {
			props.setProperty((String) entry.getKey(),
					(String) entry.getValue());
		}
		if (override != null) {
			for (Map.Entry<Object, Object> entry : override.entrySet()) {
				props.setProperty((String) entry.getKey(),
						(String) entry.getValue());
			}
		}

		if (override != null) {
			_log.info("properties: " + properties + "; override: " + override
					+ "; merged props: " + props + "; physicalSourceConfig: "
					+ Arrays.toString(physicalSourceConfigs));
		} else {
			_log.info("properties: " + properties + "; merged props: " + props
					+ "; physicalSourceConfig: "
					+ Arrays.toString(physicalSourceConfigs));
		}
		// Make sure the PhysicalSourceConfig is properly initialized
		for (PhysicalSourceConfig pConfig : physicalSourceConfigs)
			pConfig.checkForNulls();

		_physicalSourceConfigs = physicalSourceConfigs;

		// Load the configuration
		HttpRelay.Config config = new HttpRelay.Config();
		ConfigLoader<StaticConfig> staticConfigLoader = new ConfigLoader<StaticConfig>(
				"databus.relay.", config);

		// Register all sources with the static config
		PhysicalSourceStaticConfig[] pStaticConfigs = new PhysicalSourceStaticConfig[_physicalSourceConfigs.length];
		int i = 0;
		for (PhysicalSourceConfig pConfig : physicalSourceConfigs) {
			for (LogicalSourceConfig lsc : pConfig.getSources()) {
				config.setSourceName("" + lsc.getId(), lsc.getName());
			}
			// build all the static versions
			pStaticConfigs[i++] = pConfig.build();
		}

		// load the static config
		StaticConfig staticConfig = staticConfigLoader.loadConfig(props);

		_log.info("Creating server container");
		_relay = new DatabusRelayMain(staticConfig, pStaticConfigs);
		_log.info("Initializing server container");
		_relay.initProducers();
		_log.info("server container initialization done.");
	}

	public RelayStartable(Properties properties, Properties override,
			PhysicalSourceConfig[] physicalSourceConfigs) throws Exception {
		init(properties, override, physicalSourceConfigs);
	}

	public RelayStartable(Properties properties,
			ArrayList<Map<String, Object>> physicalSourceConfigs)
			throws Exception {
		Iterator<Map<String, Object>> iter = physicalSourceConfigs.iterator();
		PhysicalSourceConfig[] pconfigList = new PhysicalSourceConfig[physicalSourceConfigs
				.size()];
		int count = 0;
		while (iter.hasNext()) {
			Map<String, Object> map = iter.next();
			pconfigList[count++] = PhysicalSourceConfig.fromMap(map);

		}
		init(properties, null, pconfigList);
	}

	/*
	 * @see org.xeril.util.Startable#start()
	 */
	@Override
	public void start() throws Exception {
		_log.info("Starting relay");
		_relay.start();
		_log.info("Starting relay: out");
	}

	/*
	 * @see org.xeril.util.Shutdownable#shutdown()
	 */
	@Override
	public void shutdown() {
		_log.info("Relay shutdown requested");
		_relay.shutdown();
		_log.info("Relay is shutdown.");
	}

	/*
	 * @see org.xeril.util.Shutdownable#waitForShutdown()
	 */
	@Override
	public void waitForShutdown() throws InterruptedException,
			IllegalStateException {
		_log.info("Waiting for relay shutdown ...");
		_relay.awaitShutdown();
		_log.info("Relay is shutdown!");
	}

	/*
	 * @see org.xeril.util.Shutdownable#waitForShutdown(long)
	 */
	@Override
	public void waitForShutdown(long timeout) throws InterruptedException,
			IllegalStateException, TimeOutException {
		// TODO add timeout
		_log.info("Waiting for relay shutdown ...");
		_relay.awaitShutdown();
		_log.info("Relay is shutdown!");
	}

	public DatabusRelayMain getRelay() {
		return _relay;
	}

	public PhysicalSourceConfig[] getPhysicalSourceConfig() {
		return _physicalSourceConfigs;
	}

	public Logger getLog() {
		return _log;
	}
}
