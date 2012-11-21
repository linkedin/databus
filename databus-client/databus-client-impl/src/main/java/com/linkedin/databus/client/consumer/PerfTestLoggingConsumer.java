package com.linkedin.databus.client.consumer;

import org.apache.log4j.Logger;

import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.client.pub.SCN;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.util.RngUtils;

public class PerfTestLoggingConsumer extends DelegatingDatabusCombinedConsumer
{
	private long _busyLoadTimeMs = 0;
	private long _idleLoadTimeMs = 0;

	public static final String MODULE = PerfTestLoggingConsumer.class.getName();
	private static final Logger LOG = Logger.getLogger(MODULE);

	
	public PerfTestLoggingConsumer()
	{
		super (null,null,null);
	}
	
	
	public PerfTestLoggingConsumer(LoggingConsumer loggingConsumer)
	{
		super(loggingConsumer,loggingConsumer,LOG);
	}

	private void simulateLoad(long timeElapsedMs)
	{
		if ((_busyLoadTimeMs - timeElapsedMs) > 0)
		{
			simulateBusyLoad(_busyLoadTimeMs - timeElapsedMs);
		}
		if (_idleLoadTimeMs > 0)
		{
			simulateIdleLoad(_idleLoadTimeMs);
		}
	}

	private void simulateIdleLoad(long idleLoadTimeMs)
	{
		try
		{
			Thread.sleep(idleLoadTimeMs);
		}
		catch (InterruptedException e)
		{
			LOG.info("Logger interrupted while simulating idleLoad for "
					+ idleLoadTimeMs + " ms");
		}
	}

	private void simulateBusyLoad(long timeMs)
	{
		long startTime = System.currentTimeMillis();
		long endTime = startTime + timeMs;
		while (System.currentTimeMillis() <= endTime)
		{
			long n1 = RngUtils.randomPositiveLong(2, 100000);
			long n2 = RngUtils.randomPositiveLong(2, 100000);
			// find gcd
			if (n2 > n1)
			{
				long t = n1;
				n1 = n2;
				n2 = t;
			}
			long rem = 0;
			while ((rem = n1 % n2) != 0)
			{
				n1 = n2;
				n2 = rem;
			}
		}
		LOG.debug("Simulated load for " + timeMs + " ms");
	}

	public long getIdleLoadTimeMs()
	{
		return _idleLoadTimeMs;
	}

	public void setIdleLoadTimeMs(long idleLoadTimeMs)
	{
		_idleLoadTimeMs = idleLoadTimeMs;
	}

	public long getBusyLoadTimeMs()
	{
		return _busyLoadTimeMs;
	}

	public void setBusyLoadTimeMs(long busyLoadTimeMs)
	{
		_busyLoadTimeMs = busyLoadTimeMs;
	}

	@Override
	public ConsumerCallbackResult onEndDataEventSequence(SCN endScn)
	{
		long startTimeMs = System.currentTimeMillis();
		ConsumerCallbackResult res = super.onEndDataEventSequence(endScn);
		if (res == ConsumerCallbackResult.SUCCESS)
		{
			simulateLoad(System.currentTimeMillis()-startTimeMs);
		}
		return res;
	}

	
	@Override
	public ConsumerCallbackResult onDataEvent(DbusEvent e,
			DbusEventDecoder eventDecoder)
	{
		long startTimeMs = System.currentTimeMillis();
		ConsumerCallbackResult res = super.onDataEvent(e,eventDecoder);
		if (res == ConsumerCallbackResult.SUCCESS)
		{
			simulateLoad(System.currentTimeMillis()-startTimeMs);
		}
		return res;
	}


	@Override
	public ConsumerCallbackResult onEndBootstrapSequence(SCN endScn)
	{
		long startTimeMs = System.currentTimeMillis();
		ConsumerCallbackResult res = super.onEndBootstrapSequence(endScn);
		if (res == ConsumerCallbackResult.SUCCESS)
		{
			simulateLoad(System.currentTimeMillis()-startTimeMs);
		}
		return res;
	}


	@Override
	public ConsumerCallbackResult onBootstrapEvent(DbusEvent e,
			DbusEventDecoder eventDecoder)
	{
		long startTimeMs = System.currentTimeMillis();
		ConsumerCallbackResult res = super.onBootstrapEvent(e,eventDecoder);
		if (res == ConsumerCallbackResult.SUCCESS)
		{
			simulateLoad(System.currentTimeMillis()-startTimeMs);
		}
		return res;
	}

}
