package com.linkedin.databus.container.request;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;

import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus.core.DbusEventBufferMult;
import com.linkedin.databus.core.DbusEventBufferMult.PhysicalPartitionKey;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus.core.data_model.PhysicalSource;
import com.linkedin.databus2.core.container.request.DatabusRequest;
import com.linkedin.databus2.core.container.request.RequestProcessingException;
import com.linkedin.databus2.core.container.request.RequestProcessor;

public class PhysicalBuffersRequestProcessor implements RequestProcessor
{

  public static final String MODULE = SourcesRequestProcessor.class.getName();
  public static Logger LOG = Logger.getLogger(MODULE);
  public final static String COMMAND_NAME = "physicalBuffers";
  
  private final ExecutorService _executorService;
  private final HttpRelay _relay;
  
  public PhysicalBuffersRequestProcessor(ExecutorService executorService,
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
    boolean pretty = request.getParams().getProperty("pretty") != null;
    
    // create pretty or regular writer
    ObjectWriter writer = pretty ? mapper.defaultPrettyPrintingWriter() : mapper.writer();
    StringWriter out = new StringWriter(10240);
    
    DbusEventBufferMult multBuf = _relay.getEventBuffer();
    Set<PhysicalPartitionKey> keys = multBuf.getAllPhysicalPartitionKeys();
    // creat map to output partId=>PhysicalSources...
    Map<PhysicalPartition, Set<PhysicalSource>> map = 
        new HashMap<PhysicalPartition, Set<PhysicalSource>>(keys.size());
    for(PhysicalPartitionKey key: keys) {
      Set<PhysicalSource> set = multBuf.getPhysicalSourcesForPartition(key.getPhysicalPartition());
      map.put(key.getPhysicalPartition(), set);
    }
    
    if(keys.isEmpty()) {
      writer.writeValue(out, new HashSet<PhysicalPartition>());
    } else {
      writer.writeValue(out,map);
    }
    
    byte[] resultBytes = out.toString().getBytes();
    
    request.getResponseContent().write(ByteBuffer.wrap(resultBytes));
        
    return request;
  }

}
