package com.linkedin.databus.container.request;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;

import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus.core.BufferInfoResponse;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventBufferMult;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus2.core.BufferNotFoundException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.request.AbstractRequestProcesser;
import com.linkedin.databus2.core.container.request.ContainerStatsRequestProcessor;
import com.linkedin.databus2.core.container.request.DatabusRequest;
import com.linkedin.databus2.core.container.request.InvalidRequestParamValueException;
import com.linkedin.databus2.core.container.request.RequestProcessingException;
import com.linkedin.databus2.core.container.request.RequestProcessor;
import com.linkedin.databus2.core.seq.MultiServerSequenceNumberHandler;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;

public class BufferInfoRequestProcessor 
	extends AbstractRequestProcesser 
{
	public static final String MODULE = BufferInfoRequestProcessor.class.getName();
	public static final Logger LOG = Logger.getLogger(MODULE);

	public static final String COMMAND_NAME = "bufferInfo";
	private final static String INBOUND_VIEW = "inbound/";
	
	private final ExecutorService _executorService;
    private final DbusEventBufferMult _eventBufferMult;
    
    
	public BufferInfoRequestProcessor(ExecutorService executorService,
								   DbusEventBufferMult eventBuffer)
	{
		super();
		_executorService = executorService;
		_eventBufferMult = eventBuffer;
	}
	
	@Override
	public DatabusRequest process(DatabusRequest request) throws IOException,
			RequestProcessingException, DatabusException 
	{
	    String category = request.getParams().getProperty(DatabusRequest.PATH_PARAM_NAME);

	    if (null == category)
	    {
	      throw new InvalidRequestParamValueException(COMMAND_NAME, "category", "null");
	    }
	    
	    if ( category.startsWith(INBOUND_VIEW))
	    {
	        String sourceIdStr = category.substring(INBOUND_VIEW.length());
	        sourceIdStr = sourceIdStr.replace('/', ':');
	        PhysicalPartition pPartition = PhysicalPartition.parsePhysicalPartitionString(sourceIdStr, ":");
	        processInboundRequest(request,pPartition);
	    } else {
	    	throw new InvalidRequestParamValueException(COMMAND_NAME, "category", category);
	    }
	    
		return request;
	}

	private void processInboundRequest(DatabusRequest request, PhysicalPartition pPart)
		throws IOException, DatabusException
	{        
		DbusEventBuffer evb = _eventBufferMult.getOneBuffer(pPart);
				
		if ( null == evb)
		{
			LOG.error("BufferInfoRequest : Buffer not available for physical partition :" + pPart);
	    	throw new BufferNotFoundException("Buffer not available for partition :" + pPart);
		}
		
		BufferInfoResponse response = new BufferInfoResponse();
		response.setMinScn(evb.getMinScn());
		response.setMaxScn(evb.lastWrittenScn());
		response.setTimestampFirstEvent(evb.getTimestampOfFirstEvent());
		response.setTimestampLatestEvent(evb.getTimestampOfLatestDataEvent());
		
	    writeJsonObjectToResponse(response, request);
	}
	
	@Override
	public ExecutorService getExecutorService() {
		return _executorService;
	}
}
