package com.linkedin.databus.bootstrap.producer;

import java.io.File;

import org.apache.log4j.Logger;

import com.linkedin.databus.bootstrap.common.BootstrapDBCleaner;
import com.linkedin.databus.bootstrap.common.BootstrapProducerThreadBase;
import com.linkedin.databus.bootstrap.common.BootstrapCleanerStaticConfig.DiskSpaceTriggerConfig;


public class BootstrapDBDiskSpaceTriggerThread 
	extends BootstrapProducerThreadBase
{
	public static final String MODULE = BootstrapDBDiskSpaceTriggerThread.class.getName();
	public static final Logger LOG = Logger.getLogger(MODULE);
	
	private final BootstrapDBCleaner cleaner;
	private final DiskSpaceTriggerConfig config;
	
	public BootstrapDBDiskSpaceTriggerThread(BootstrapDBCleaner cleaner, DiskSpaceTriggerConfig config)
	{
		super("DiskSpaceTrigger");
		this.cleaner = cleaner;
		this.config = config;
	}
		
	@Override
	public void run()
	{
		LOG.info("DiskSpaceTrigger Config :" + config );
		
		if ( ! config.isEnable())
		{
			LOG.info("DiskSpace Trigger not enabled !!");
			return;
		}
		
		File bootstrapDBDrive = new File(config.getBootstrapDBDrive());
		
		if ( ! bootstrapDBDrive.exists())
		{
			LOG.error("Bootstrap Drive (" + bootstrapDBDrive.getAbsolutePath() + ") not found. Disabling DiskSpaceTrigger !!");
			return;
		}
		
		long run = 0;
		while(! isShutdownRequested())
		{
			run++;
			LOG.info("DiskSpace Trigger run :" + run);
			long total = bootstrapDBDrive.getTotalSpace();
			long available = bootstrapDBDrive.getUsableSpace();
			double percentAvailable = ((available * 100.0)/total);

			
			LOG.info("BootstrapDB Drive(" + bootstrapDBDrive.getAbsolutePath() 
					      + ") : Total bytes :" + total  + 
					      ", Avalable bytes :" + available +
					      ", Percent Available :" + percentAvailable);
			
			if (percentAvailable < config.getAvailableThresholdPercent())
			{
				LOG.info("Available Space (" + percentAvailable 
						   + ") less than threshold (" + config.getAvailableThresholdPercent() 
						   + "). Triggering cleaner !!");
			
				synchronized(cleaner)
				{
					if ( ! cleaner.isCleanerRunning())
					{
						cleaner.doClean();						
					} else {
						LOG.info("Skipping as cleaner is already running !!");
					}			
				}
			}	
				
			LOG.info("Sleeping for :" + config.getRunIntervalSeconds() + " seconds !!");
			try
			{
				Thread.sleep(config.getRunIntervalSeconds() * 1000);
			} catch (InterruptedException ie) {
				LOG.info("Got interrupted while sleeping for :" + config.getRunIntervalSeconds() + " seconds !!");
			}
		}
		doShutdownNotify();
	}			
}
