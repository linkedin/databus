package com.linkedin.databus.bootstrap.common;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.linkedin.databus.bootstrap.common.BootstrapCleanerStaticConfig.BootstrapDBType;
import com.linkedin.databus.bootstrap.common.BootstrapCleanerStaticConfig.DiskSpaceTriggerConfig;
import com.linkedin.databus.bootstrap.common.BootstrapCleanerStaticConfig.PeriodicTriggerConfig;
import com.linkedin.databus.bootstrap.common.BootstrapCleanerStaticConfig.RetentionStaticConfig;
import com.linkedin.databus.bootstrap.common.BootstrapCleanerStaticConfig.RetentionType;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;

public class BootstrapCleanerConfig
	implements ConfigBuilder<BootstrapCleanerStaticConfig>
{

	private boolean enable;
	private Map<String,RetentionConfig> retentionOverride;
	private RetentionConfig retentionDefault;
	private Map<String,String> bootstrapTypeOverride;		
	private String bootstrapTypeDefault;
	private Map<String, Boolean> enableOptimizeTable;
	private Boolean enableOptimizeTableDefault;
	private Map<String, Boolean> enableForceTabTableCleanup;
	private Boolean enableForceTabTableCleanupDefault;
	private DiskSpaceTriggerConfigBuilder diskSpaceTrigger;
	private PeriodicTriggerConfigBuilder periodicTrigger;

	public BootstrapCleanerConfig()
	{
		retentionOverride = new HashMap<String, RetentionConfig>();
		retentionDefault = new RetentionConfig();
		bootstrapTypeOverride = new HashMap<String, String>();
		bootstrapTypeDefault = BootstrapDBType.BOOTSTRAP_FULL.toString();
		enable = true;
		enableOptimizeTable = new HashMap<String, Boolean>();
		enableOptimizeTableDefault = Boolean.FALSE;		
		enableForceTabTableCleanup = new HashMap<String, Boolean>();
		enableForceTabTableCleanupDefault = Boolean.FALSE;		
		diskSpaceTrigger = new DiskSpaceTriggerConfigBuilder();
		periodicTrigger = new PeriodicTriggerConfigBuilder();
	}

	@Override
	public BootstrapCleanerStaticConfig build() throws InvalidConfigException
	{
		Map<String, RetentionStaticConfig> retOverrides = new HashMap<String, RetentionStaticConfig>();
		for (Entry<String, RetentionConfig> e : retentionOverride.entrySet())
		{
			retOverrides.put(e.getKey(), e.getValue().build());
		}

		Map<String, BootstrapDBType> bsTypeOverrides = new HashMap<String, BootstrapDBType>();
		for (Entry<String, String> e : bootstrapTypeOverride.entrySet())
		{
			bsTypeOverrides.put(e.getKey(), BootstrapDBType.valueOf(e.getValue()));
		}
		
		return new BootstrapCleanerStaticConfig(retOverrides,
								retentionDefault.build(),
								bsTypeOverrides,
								BootstrapDBType.valueOf(bootstrapTypeDefault),
								enableOptimizeTable,
								enableOptimizeTableDefault,
								enableForceTabTableCleanup,
								enableForceTabTableCleanupDefault,
								enable,
								diskSpaceTrigger.build(),
								periodicTrigger.build());
	}

	public RetentionConfig getRetentionOverride(String source) 
	{
		RetentionConfig r = retentionOverride.get(source);

		if (null == r)
		{
			r = new RetentionConfig();
			retentionOverride.put(source, r);				
		}

		return r;
	}

	public Boolean getEnableOptimizeTable(String source) 
	{
		return enableOptimizeTable.get(source);
	}
	
	public void setEnableOptimizeTable(String source, Boolean enable) 
	{
		this.enableOptimizeTable.put(source, enable);
	}
	
	public Boolean getEnableOptimizeTableDefault() 
	{
		return enableOptimizeTableDefault;
	}

	public void setEnableOptimizeTableDefault(Boolean enable) 
	{
		this.enableOptimizeTableDefault = enable;
	}
	
	public Boolean getEnableForceTabTableCleanup(String source) 
	{
		return enableForceTabTableCleanup.get(source);
	}
	
	public void setEnableForceTabTableCleanup(String source, Boolean enable) 
	{
		this.enableForceTabTableCleanup.put(source, enable);
	}
	
	public Boolean getEnableForceTabTableCleanupDefault() 
	{
		return enableForceTabTableCleanupDefault;
	}

	public void setEnableForceTabTableCleanupDefault(Boolean enable) 
	{
		this.enableForceTabTableCleanupDefault = enable;
	}
	
	
	public String getBootstrapTypeOverride(String source) 
	{
		return bootstrapTypeOverride.get(source);
	}
	
	public void setBootstrapTypeOverride(String source, String bootstrapTypeOverride) 
	{
		this.bootstrapTypeOverride.put(source, bootstrapTypeOverride);
	}
	
	public void setRetentionOverride(String source, RetentionConfig retentionOverride) 
	{
		this.retentionOverride.put(source, retentionOverride);
	}

	public RetentionConfig getRetentionDefault() 
	{
		return retentionDefault;
	}

	public void setRetentionDefault(RetentionConfig retentionDefault) 
	{
		this.retentionDefault = retentionDefault;
	}

	public String getBootstrapTypeDefault() 
	{
		return bootstrapTypeDefault;
	}

	public void setBootstrapTypeDefault(String bootstrapTypeDefault) 
	{
		this.bootstrapTypeDefault = bootstrapTypeDefault;
	}   

	public boolean getEnable()
	{
		return enable;
	}

	public void setEnable(boolean e)
	{
		enable = e;
	}
	
	public PeriodicTriggerConfigBuilder getPeriodicTrigger() {
		return periodicTrigger;
	}

	public void setPeriodicTrigger(PeriodicTriggerConfigBuilder periodicTrigger) {
		this.periodicTrigger = periodicTrigger;
	}

	public DiskSpaceTriggerConfigBuilder getDiskSpaceTrigger() {
		return diskSpaceTrigger;
	}

	public void setDiskSpaceTrigger(DiskSpaceTriggerConfigBuilder diskSpaceTrigger) {
		this.diskSpaceTrigger = diskSpaceTrigger;
	}
	
	public static class RetentionConfig implements ConfigBuilder<RetentionStaticConfig>
	{
		private static final String DEFAULT_RETENTION_TYPE = RetentionType.RETENTION_SECONDS.toString();
		private static final long DEFAULT_RETENTION_QTY = 14 * 24 * 60 * 60; // 2 weeks

		private String retentionType;
		private long retentionQuantity;

		public RetentionConfig()
		{
			retentionType = DEFAULT_RETENTION_TYPE;
			retentionQuantity = DEFAULT_RETENTION_QTY;
		}

		public String getRetentionType() {
			return retentionType;
		}

		public void setRetentionType(String retentionType) {
			this.retentionType = retentionType;
		}

		public long getRetentionQuantity() {
			return retentionQuantity;
		}

		public void setRetentionQuantity(long retentionQuantity) {
			this.retentionQuantity = retentionQuantity;
		}
		
		@Override
		public RetentionStaticConfig build() throws InvalidConfigException 
		{
			try
			{
				RetentionType type = RetentionType.valueOf(retentionType);
				return new RetentionStaticConfig(type, retentionQuantity);
			} catch (Exception ex) {
				throw new InvalidConfigException(ex);
			}
		}		  
	}
	
	
	public static class DiskSpaceTriggerConfigBuilder
		implements ConfigBuilder<DiskSpaceTriggerConfig>
	{
		public static final double DEFAULT_AVAILABLE_THRESHOLD_PERCENT = 25.0;
		public static final boolean DEFAULT_ENABLE = true;
		public static final long DEFAULT_RUN_INTERVAL_SECONDS = 3600; //every hour
		public static final String DEFAULT_BOOTSTRAP_DB_DRIVE = "/mnt/u001";

		private boolean enable;
		private long runIntervalSeconds;
		private double availableThresholdPercent;
		private String bootstrapDBDrive;

		public DiskSpaceTriggerConfigBuilder()
		{
			enable = DEFAULT_ENABLE;
			runIntervalSeconds = DEFAULT_RUN_INTERVAL_SECONDS;
			availableThresholdPercent = DEFAULT_AVAILABLE_THRESHOLD_PERCENT;
			bootstrapDBDrive = DEFAULT_BOOTSTRAP_DB_DRIVE;
		}

		public boolean getEnable() {
			return enable;
		}

		public void setEnable(boolean enable) {
			this.enable = enable;
		}

		public long getRunIntervalSeconds() {
			return runIntervalSeconds;
		}

		public void setRunIntervalSeconds(long runIntervalSeconds) {
			this.runIntervalSeconds = runIntervalSeconds;
		}

		public double getAvailableThresholdPercent() {
			return availableThresholdPercent;
		}

		public void setAvailableThresholdPercent(double availableThresholdPercent) {
			this.availableThresholdPercent = availableThresholdPercent;
		}

		@Override
		public DiskSpaceTriggerConfig build() 
				throws InvalidConfigException 
		{
			if (availableThresholdPercent > 100 || 
					availableThresholdPercent < 0)
				throw new InvalidConfigException("availableThresholdPercent must be between 0-100");

			return new DiskSpaceTriggerConfig(enable, runIntervalSeconds, availableThresholdPercent, bootstrapDBDrive);
		}

		public String getBootstrapDBDrive() {
			return bootstrapDBDrive;
		}

		public void setBootstrapDBDrive(String bootstrapDBDrive) {
			this.bootstrapDBDrive = bootstrapDBDrive;
		}						
	}
	
	public static class PeriodicTriggerConfigBuilder
		implements ConfigBuilder<PeriodicTriggerConfig>
	{
		public static final long DEFAULT_RUN_INTERVAL_SECONDS = 24*60*60; //every day		  

		private long runIntervalSeconds; 
		private boolean enable;
		private boolean runOnStart;
		
		public PeriodicTriggerConfigBuilder()
		{
			runIntervalSeconds = DEFAULT_RUN_INTERVAL_SECONDS;
			enable = true;
			runOnStart = false;
		}
		
		public long getRunIntervalSeconds() {
			return runIntervalSeconds;
		}
		
		public void setRunIntervalSeconds(long runIntervalSeconds) {
			this.runIntervalSeconds = runIntervalSeconds;
		}
		
		public boolean getEnable() {
			return enable;
		}
		
		public void setEnable(boolean enable) {
			this.enable = enable;
		}
		
		public boolean isRunOnStart() {
			return runOnStart;
		}
		
		public void setRunOnStart(boolean runOnStart) {
			this.runOnStart = runOnStart;
		}

		@Override
		public PeriodicTriggerConfig build() 
				throws InvalidConfigException 
		{
			return new PeriodicTriggerConfig(runIntervalSeconds, enable, runOnStart);
		}
	}
}
