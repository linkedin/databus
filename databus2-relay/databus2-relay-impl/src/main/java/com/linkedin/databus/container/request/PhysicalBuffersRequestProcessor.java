package com.linkedin.databus.container.request;
/*
 *
 * Copyright 2013 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/


import java.io.IOException;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
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
    
    byte[] resultBytes = out.toString().getBytes(Charset.defaultCharset());
    
    request.getResponseContent().write(ByteBuffer.wrap(resultBytes));
        
    return request;
  }

}
