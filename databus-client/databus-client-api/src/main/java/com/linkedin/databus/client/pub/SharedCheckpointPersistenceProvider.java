package com.linkedin.databus.client.pub;
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
import java.util.List;

import org.apache.log4j.Logger;

import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.util.ConfigApplier;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;

public class SharedCheckpointPersistenceProvider extends CheckpointPersistenceProviderAbstract
{

	public static final String MODULE = SharedCheckpointPersistenceProvider.class.getName();
	public static final Logger LOG = Logger.getLogger(MODULE);

	/** The static configuration */
	private final StaticConfig _staticConfig;

	private final DatabusClientGroupMember _groupMember;
	private int _numWrites = 0;


	public SharedCheckpointPersistenceProvider(DatabusClientGroupMember node) throws InvalidConfigException
	{
		this (node,new Config());

	}

	public StaticConfig getStaticConfig() {
		return _staticConfig;
	}

	public SharedCheckpointPersistenceProvider(DatabusClientGroupMember groupMember , Config config) throws InvalidConfigException
	{
		this(groupMember, config.build());
	}

	public SharedCheckpointPersistenceProvider(DatabusClientGroupMember node, StaticConfig config) throws InvalidConfigException
	{
		_staticConfig  = config;
		_groupMember = node;
		if (_groupMember==null) {
			throw new InvalidConfigException("Cannot initialize shared checkpoint with null cluster group member! Check if cluster node has been enabled");
		}

	}

	@Override
	public void storeCheckpoint(List<String> sourceNames, Checkpoint checkpoint) throws IOException
	{
		String key = makeKey(sourceNames);
		_numWrites++;
		if (_numWrites > _staticConfig.getMaxNumWritesSkipped())
		{
			_numWrites=0;
			if (!_groupMember.writeSharedData(key,checkpoint)) {
				LOG.info("Write failed in store checkpoint; Assuming no connection to zookeeper; throwing IO Exception: key=" + key);
				throw new IOException("Write failed in store checkpoint; Assuming no connection to zookeeper; throwing IO Exception: key=" + key);
			}
		}
	}

	protected String makeKey(List<String> srcs)
	{
		StringBuilder k = new StringBuilder();
		for (String s: srcs)
		{
			k.append("_");
			k.append(s);
		}
		return k.toString();
	}

	@Override
	public Checkpoint loadCheckpoint(List<String> sourceNames)
	{
		String key = makeKey(sourceNames);
		return (Checkpoint) _groupMember.readSharedData(key);
	}

	@Override
	public void removeCheckpoint(List<String> sourceNames)
	{
		String key = makeKey(sourceNames);
		_groupMember.removeSharedData(key);
	}

	/** Static configuration for the shared checkpoint persistence provider.
	 *
	 * @see SharedCheckpointPersistenceProvider
	 */
	public static class StaticConfig
	{
		private final int _maxNumWritesSkipped;

		public StaticConfig(int maxNumWritesSkipped)
		{
			_maxNumWritesSkipped = maxNumWritesSkipped;
		}

		public int getMaxNumWritesSkipped()
		{
			return _maxNumWritesSkipped;
		}

		@Override
		public String toString() {
			return "StaticConfig [_maxNumWritesSkipped=" + _maxNumWritesSkipped + "]";
		}


	}

	public static class Config implements ConfigBuilder<StaticConfig>
	{

		public Config()
		{
		}

		private   int _maxNumWritesSkipped=0;

		@Override
		public StaticConfig build() throws InvalidConfigException
		{
			return new StaticConfig(_maxNumWritesSkipped);

		}

		public int getMaxNumWritesSkipped()
		{
			return _maxNumWritesSkipped;
		}



		public void setMaxNumWritesSkipped(int frequencyOfWritesInEvents)
		{
			_maxNumWritesSkipped = frequencyOfWritesInEvents;
		}

	}

	public static class RuntimeConfig implements ConfigApplier<RuntimeConfig>
	{

		@Override
		public void applyNewConfig(RuntimeConfig oldConfig)
		{
			// No shared state to change; runtime config settings are used on demand

		}

		@Override
		public boolean equalsConfig(RuntimeConfig otherConfig)
		{
			if (null == otherConfig) return false;
			return true;
		}

	}

	public static class RuntimeConfigBuilder implements ConfigBuilder<RuntimeConfig>
	{

		private SharedCheckpointPersistenceProvider _managedInstance = null;
		@Override
		public RuntimeConfig build() throws InvalidConfigException
		{
			if (null == _managedInstance)
			{
				throw new InvalidConfigException("No associated managed instance for runtime config");
			}
			return new RuntimeConfig();
		}

		public void setManagedInstance(SharedCheckpointPersistenceProvider persistenceProvider)
		{
			_managedInstance = persistenceProvider;
		}

		public Object getManagedInstance()
		{
			return _managedInstance;
		}

	}
}
