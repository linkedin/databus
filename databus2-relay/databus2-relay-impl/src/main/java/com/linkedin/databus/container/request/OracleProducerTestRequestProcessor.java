package com.linkedin.databus.container.request;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;

import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.request.DatabusRequest;
import com.linkedin.databus2.core.container.request.RequestProcessingException;
import com.linkedin.databus2.core.container.request.RequestProcessor;
import com.linkedin.databus2.producers.db.OracleTxlogEventReader;

public class OracleProducerTestRequestProcessor implements RequestProcessor 
{
	public static final String MODULE = OracleProducerTestRequestProcessor.class.getName();
	public static final Logger LOG = Logger.getLogger(MODULE);

	private OracleTxlogEventReader _producer = null;
	private ExecutorService _service = null;

	public static final String RESET_CATCHUPSCN_COMMAND = "resetCatchupScn";
	public static final String COMMAND_NAME = "testOracleProducer";

	public OracleProducerTestRequestProcessor(OracleTxlogEventReader producer,
			ExecutorService service)
	{
		_producer = producer;
		_service = service;
	}

	@Override
	public DatabusRequest process(DatabusRequest request) throws IOException,
	RequestProcessingException, DatabusException 
	{
	    String command = request.getParams().getProperty(DatabusRequest.PATH_PARAM_NAME, "");

		if ( command.equalsIgnoreCase(RESET_CATCHUPSCN_COMMAND))
		{
			LOG.info("Resetting Catchup Target Max Scn !!");
			_producer.setCatchupTargetMaxScn(-1);
			String result = "{ \"command\" : \"" + command + "\", \"result\" : \"true\" }";			
			request.getResponseContent().write(ByteBuffer.wrap(result.getBytes()));
		}  else {
			throw new DatabusException("Unknown command :" + command);
		}

		return request;
	}

	@Override
	public ExecutorService getExecutorService() 
	{
		return _service;
	}

}
