package com.linkedin.databus.bootstrap.producer;

import org.apache.log4j.Logger;

import com.linkedin.databus.bootstrap.common.BootstrapDBCleaner;
import com.linkedin.databus.bootstrap.common.BootstrapProducerThreadBase;
import com.linkedin.databus.bootstrap.common.BootstrapCleanerStaticConfig.PeriodicTriggerConfig;

public class BootstrapDBPeriodicTriggerThread extends
		BootstrapProducerThreadBase 
{
	public static final String MODULE = BootstrapDBPeriodicTriggerThread.class.getName();
	public static final Logger LOG = Logger.getLogger(MODULE);
	
	public static final long MILLISEC_IN_SECONDS = 1000;

	
	private final BootstrapDBCleaner _cleaner;
	private final PeriodicTriggerConfig _config;
	
	public BootstrapDBPeriodicTriggerThread(BootstrapDBCleaner cleaner, PeriodicTriggerConfig config)
	{
		super("BootstrapDBPeriodicTriggerThread");
		this._cleaner = cleaner;
		this._config = config;
	}
	
	@Override
	public void run()
	{
		long runNumber = 0;
		long roundBeginTime = System.currentTimeMillis();
		long roundEndTime = roundBeginTime;
		long timeToSleep = _config.getRunIntervalSeconds() * 1000;
		
		if ( ! _config.isRunOnStart())
		{
			LOG.info("Sleeping for :" + (timeToSleep/MILLISEC_IN_SECONDS) + " seconds !!");
			try
			{
				Thread.sleep(timeToSleep);
			} catch (InterruptedException ie) {
				LOG.info("Got interrupted while sleeping for :" + timeToSleep);
			}
		}
		
		while ( !isShutdownRequested())
		{
			runNumber++;
			LOG.info("Run : " + runNumber);
			
			synchronized(_cleaner)
			{
				if ( ! _cleaner.isCleanerRunning())
				{
					roundBeginTime = System.currentTimeMillis();
					_cleaner.doClean();
					roundEndTime = System.currentTimeMillis();
				} else {
					LOG.info("Skipping this round as cleaner is already running !!");
				}
			}
			
			long timeTakenMs = roundEndTime - roundBeginTime;
			LOG.info("Round (" + runNumber + ") complete. Took around " + (timeTakenMs/1000) + " seconds !!");
			timeToSleep = (_config.getRunIntervalSeconds() * MILLISEC_IN_SECONDS) - timeTakenMs;
			LOG.info("Sleeping for :" + (timeToSleep/MILLISEC_IN_SECONDS) + " seconds !!");
			
			try
			{
				Thread.sleep(timeToSleep);
			} catch (InterruptedException ie) {
				LOG.info("Got interrupted while sleeping for :" + timeToSleep + " ms");
			}
		}
		doShutdownNotify();
	}
}
