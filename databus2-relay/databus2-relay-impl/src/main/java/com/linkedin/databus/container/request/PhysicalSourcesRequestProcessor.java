package com.linkedin.databus.container.request;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus2.core.container.monitoring.mbean.HttpStatisticsCollector;
import com.linkedin.databus2.core.container.request.DatabusRequest;
import com.linkedin.databus2.core.container.request.RequestProcessingException;
import com.linkedin.databus2.core.container.request.RequestProcessor;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;

public class PhysicalSourcesRequestProcessor implements RequestProcessor
{

  public static final String MODULE = SourcesRequestProcessor.class.getName();
  public static Logger LOG = Logger.getLogger(MODULE);
  public final static String COMMAND_NAME = "physicalSources";
  
  private final ExecutorService _executorService;
  private final HttpRelay _relay;
  
  public PhysicalSourcesRequestProcessor(ExecutorService executorService,
                                 HttpRelay relay)
  {
    super();
    _executorService = executorService;
    _relay = relay;
  }
  
  @Override
  public ExecutorService getExecutorService()
  {
    return _executorService;
  }

  @Override
  public DatabusRequest process(DatabusRequest request) throws IOException,
      RequestProcessingException
  {
    ObjectMapper mapper = new ObjectMapper();
    StringWriter out = new StringWriter(10240);
    List<PhysicalSourceStaticConfig> sources = _relay.getPhysicalSources();
    if(sources.isEmpty())
      mapper.writeValue(out, new ArrayList<PhysicalSourceStaticConfig>());
    else 
      mapper.writeValue(out,sources);
    
    byte[] resultBytes = out.toString().getBytes();
    request.getResponseContent().write(ByteBuffer.wrap(resultBytes));
    
    HttpStatisticsCollector relayStatsCollector = _relay.getHttpStatisticsCollector();
    if (null != relayStatsCollector) 
    {
      HttpStatisticsCollector connStatsCollector = (HttpStatisticsCollector)
          request.getParams().get(relayStatsCollector.getName());
      if (null != connStatsCollector)
      {
        connStatsCollector.registerSourcesCall();
      }
      else
      {
        relayStatsCollector.registerSourcesCall();
      }
    }
    
    return request;
  }

}
