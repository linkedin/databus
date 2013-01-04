package com.linkedin.databus2.core.container.request;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.request.DatabusRequest;

/**
 * A processor for {@link DatabusRequest} commands. 
 * @author cbotev
 *
 */
public interface RequestProcessor {

  /**
   * Processes the specified databus request
   * @param  request
   * @return the buffer with the response
   * @throws IOException
   */
   DatabusRequest process(DatabusRequest request) throws IOException, RequestProcessingException, DatabusException; 

  /**
   * Obtains the executor service to use for scheduling command for this processor
   * @return	the service
   */
  ExecutorService getExecutorService();
}
