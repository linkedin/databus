package com.linkedin.databus.bootstrap.common;
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


import java.util.Map;


public class BootstrapCleanerStaticConfig 
{
	public static enum RetentionType
	{
		NO_CLEANUP,
		RETENTION_SECONDS,
		RETENTION_LOGS
	};

	public static enum BootstrapDBType
	{
		BOOTSTRAP_CATCHUP_APPLIER_RUNNING,
		BOOTSTRAP_CATCHUP_APPLIER_NOT_RUNNING,
		BOOTSTRAP_FULL;
		
		public boolean isApplierRunning()
		{
			if (this == BOOTSTRAP_CATCHUP_APPLIER_NOT_RUNNING)
				return false;
			return true;
		}
		
		public boolean isCatchupMode()
		{
			if (this == BOOTSTRAP_FULL)
				return false;
			return true;
		}
	};
	
	private final Map<String,RetentionStaticConfig> retentionOverride;
	private final RetentionStaticConfig retentionDefault;
	private final Map<String,BootstrapDBType> bootstrapTypeOverride;
	private final BootstrapDBType bootstrapTypeDefault;
	private final Map<String,Boolean> enableOptimizeTable;
	private final Boolean enableOptimizeTableDefault;
	private final Map<String,Boolean> enableForceTabTableCleanup;
	private final Boolean enableForceTabTableCleanupDefault;
	private final boolean enable;
	private final DiskSpaceTriggerConfig diskSpaceTrigger;
	private final PeriodicTriggerConfig periodicTrigger;
	
	public Map<String, RetentionStaticConfig> getRetentionOverride() {
		return retentionOverride;
	}

	public RetentionStaticConfig getRetentionDefault() {
		return retentionDefault;
	}
			
	public RetentionStaticConfig getRetentionConfig(String sourceName)
	{
		RetentionStaticConfig c = retentionOverride.get(sourceName);

		if ( null == c)
			c = retentionDefault;

		return c;
	}

	public Boolean forceTabTableCleanup(String sourceName)
	{
		Boolean c = enableForceTabTableCleanup.get(sourceName);

		if ( null == c)
			c = enableForceTabTableCleanupDefault;

		return c;
	}
	
	public Map<String, Boolean> getEnableOptimizeTable() {
		return enableOptimizeTable;
	}


	public Boolean isOptimizeTableEnabled(String sourceName)
	{
		Boolean c = enableOptimizeTable.get(sourceName);

		if ( null == c)
			c = enableOptimizeTableDefault;

		return c;
	}
	
	public Boolean getEnableOptimizeTableeDefault() {
		return enableOptimizeTableDefault;
	}
	
	public Map<String, BootstrapDBType> getBootstrapTypeOverride() {
		return bootstrapTypeOverride;
	}


	public BootstrapDBType getBootstrapType(String sourceName)
	{
		BootstrapDBType c = bootstrapTypeOverride.get(sourceName);

		if ( null == c)
			c = bootstrapTypeDefault;

		return c;
	}
	
	public BootstrapDBType getBootstrapTypeDefault() {
		return bootstrapTypeDefault;
	}

	public boolean isEnable() {
		return enable;
	}


	public DiskSpaceTriggerConfig getDiskSpaceTrigger() {
		return diskSpaceTrigger;
	}

	public PeriodicTriggerConfig getPeriodSpaceTrigger() {
		return periodicTrigger;
	}
	
	public BootstrapCleanerStaticConfig(
			Map<String, RetentionStaticConfig> retentionOverride,
			RetentionStaticConfig retentionDefault,
			Map<String, BootstrapDBType> bootstrapTypeOverride,
			BootstrapDBType bootstrapTypeDefault,
			Map<String, Boolean> enableOptimizeTable,
			boolean enableOptimizeTableDefault, 
			Map<String,Boolean> enableForceTabTableCleanup,
			boolean enableForceTabTableCleanupDefault,
			boolean enable,
			DiskSpaceTriggerConfig diskSpaceTrigger,
			PeriodicTriggerConfig periodicTrigger) {
		super();
		this.retentionOverride = retentionOverride;
		this.retentionDefault = retentionDefault;
		this.bootstrapTypeOverride = bootstrapTypeOverride;
		this.bootstrapTypeDefault = bootstrapTypeDefault;
		this.enableOptimizeTable = enableOptimizeTable;
		this.enableOptimizeTableDefault = enableOptimizeTableDefault;
		this.enableForceTabTableCleanup = enableForceTabTableCleanup;
		this.enableForceTabTableCleanupDefault = enableForceTabTableCleanupDefault;
		this.enable = enable;
		this.diskSpaceTrigger = diskSpaceTrigger;
		this.periodicTrigger = periodicTrigger;
	}

	@Override
	public String toString() {
		return "BootstrapCleanerStaticConfig [retentionOverride="
				+ retentionOverride + ", retentionDefault=" + retentionDefault
				+ ", bootstrapTypeOverride=" + bootstrapTypeOverride
				+ ", bootstrapTypeDefault=" + bootstrapTypeDefault
				+ ", enableOptimizeTable=" + enableOptimizeTable
				+ ", enableOptimizeTableDefault=" + enableOptimizeTableDefault
				+ ", enableForceTabTableCleanup=" + enableForceTabTableCleanup
				+ ", enableForceTabTableCleanupDefault="
				+ enableForceTabTableCleanupDefault + ", enable=" + enable
				+ ", diskSpaceTrigger=" + diskSpaceTrigger
				+ ", periodicTrigger=" + periodicTrigger + "]";
	}

	public static class RetentionStaticConfig
	{
		private final RetentionType retentiontype;
		private final long retentionQuantity;

		public RetentionStaticConfig(RetentionType retentiontype,
				long retentionQuantity) {
			super();
			this.retentiontype = retentiontype;
			this.retentionQuantity = retentionQuantity;
		}

		public RetentionType getRetentiontype() {
			return retentiontype;
		}

		public long getRetentionQuantity() {
			return retentionQuantity;
		}

		@Override
		public String toString() {
			return "RetentionStaticConfig [retentiontype=" + retentiontype
					+ ", retentionQuantity=" + retentionQuantity + "]";
		}		
	}
	
	
	public static class PeriodicTriggerConfig
	{
		private final long runIntervalSeconds; 
		private final boolean enable;
		private final boolean runOnStart;
		
		public long getRunIntervalSeconds() {
			return runIntervalSeconds;
		}
		
		public boolean isEnable() {
			return enable;
		}
		
		public boolean isRunOnStart() {
			return runOnStart;
		}
		
		@Override
		public String toString() {
			return "PeriodicTriggerConfig [runIntervalSeconds="
					+ runIntervalSeconds + ", enable=" + enable
					+ ", runOnStart=" + runOnStart + "]";
		}
		
		public PeriodicTriggerConfig(long runIntervalSeconds, boolean enable,
				boolean runOnStart) {
			super();
			this.runIntervalSeconds = runIntervalSeconds;
			this.enable = enable;
			this.runOnStart = runOnStart;
		}
	}
	
	
	public static class DiskSpaceTriggerConfig
	{
		private final boolean enable;
		private final long runIntervalSeconds;
		private final double availableThresholdPercent;
		private final String bootstrapDBDrive;
		


		public DiskSpaceTriggerConfig(boolean enable, long runIntervalSeconds,
				double availableThresholdPercent, String bootstrapDBDrive) {
			super();
			this.enable = enable;
			this.runIntervalSeconds = runIntervalSeconds;
			this.availableThresholdPercent = availableThresholdPercent;
			this.bootstrapDBDrive = bootstrapDBDrive;
		}

		public boolean isEnable() {
			return enable;
		}

		public long getRunIntervalSeconds() {
			return runIntervalSeconds;
		}

		public double getAvailableThresholdPercent() {
			return availableThresholdPercent;
		}

		public String getBootstrapDBDrive() {
			return bootstrapDBDrive;
		}

		@Override
		public String toString() {
			return "DiskSpaceTriggerConfig [enable=" + enable
					+ ", runIntervalSeconds=" + runIntervalSeconds
					+ ", availableThresholdPercent="
					+ availableThresholdPercent + ", bootstrapDBDrive="
					+ bootstrapDBDrive + "]";
		}
		
	}
}


