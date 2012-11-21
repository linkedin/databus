package com.linkedin.databus.container.request;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelFuture;

import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus2.core.container.request.DatabusRequest;
import com.linkedin.databus2.core.container.request.RequestProcessingException;
import com.linkedin.databus2.core.container.request.RequestProcessor;

public class ChannelCloseFaultInjectionRequestProcessor implements
		RequestProcessor {
	  public static final String MODULE = ChannelCloseFaultInjectionRequestProcessor.class.getName();
	  public static final Logger LOG = Logger.getLogger(MODULE);

	private final ExecutorService _executorService;
	private final HttpRelay _relay;
	
	public ChannelCloseFaultInjectionRequestProcessor( ExecutorService executorService,
													   HttpRelay relay)
	{
		super();
		_relay = relay;
		_executorService = executorService;
	}
	
	@Override
	public DatabusRequest process(DatabusRequest request) throws IOException,
			RequestProcessingException {
		// Close the channel
		
		LOG.debug("Waiting for raw channel to close");

		ChannelFuture future = request.getResponseContent().getRawChannel().close().awaitUninterruptibly();
		
		
		try {
			future.await();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		LOG.debug("Done waiting for raw channel to close");

		return request;
	}

	@Override
	public ExecutorService getExecutorService() {
		// TODO Auto-generated method stub
		return _executorService;
	}

}
