/*
 * $Id: ControlSourceEventsRequestProcessor.java 169055 2011-02-26 01:01:19Z cbotev $
 */
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
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus2.core.container.request.DatabusRequest;
import com.linkedin.databus2.core.container.request.InvalidRequestParamValueException;
import com.linkedin.databus2.core.container.request.RequestProcessingException;
import com.linkedin.databus2.core.container.request.RequestProcessor;
import com.linkedin.databus2.producers.EventProducer;

/**
 * @author Jemiah Westerman<jwesterman@linkedin.com>
 * @version $Revision: 169055 $
 */
public class ControlSourceEventsRequestProcessor
implements RequestProcessor
{
  private final ExecutorService _executorService;
  private List<EventProducer> _eventProducers;

  public static final String COMMAND_NAME = "controlSources";
  public static final String PARAM_SOURCE_NAMES = "sources";
  public static final String PARAM_SCN = "scn";

  public ControlSourceEventsRequestProcessor(ExecutorService executorService,
                                            HttpRelay relay,
                                            EventProducer eventProducer)
  {
    _executorService = executorService;
    _eventProducers = new ArrayList<EventProducer>();
    _eventProducers.add(eventProducer);
  }

  public ControlSourceEventsRequestProcessor(ExecutorService executorService,
                                             HttpRelay relay,
                                             List<EventProducer> eventProducers)
   {
     _executorService = executorService;
     _eventProducers = new ArrayList<EventProducer>();
     _eventProducers.addAll(eventProducers);
   }

  /** add more producers */
  public void addEventProducers(List<EventProducer> eventProducers) {
    _eventProducers.addAll(eventProducers);
  }
  
  /** remove some producers */
  public void removeEventProducers(List<EventProducer> eventProducers) {
    _eventProducers.removeAll(eventProducers);
  }
  
  private enum Actions
  {
    STATUS, START, PAUSE, UNPAUSE, SHUTDOWN;
  }

  /*
   * @see com.linkedin.databus.container.request.RequestProcessor#getExecutorService()
   */
  @Override
  public ExecutorService getExecutorService()
  {
    return _executorService;
  }

  /*
   * @see com.linkedin.databus.container.request.RequestProcessor#process(com.linkedin.databus.container.request.DatabusRequest)
   */
  @Override
  public DatabusRequest process(DatabusRequest request)
      throws IOException, RequestProcessingException
  {
    // Read the action from the request
    Actions action;
    try
    {
      String strAction = request.getParams().getProperty(DatabusRequest.PATH_PARAM_NAME, "");
      action = Actions.valueOf(strAction.toUpperCase());
    }
    catch(Exception ex)
    {
      throw new InvalidRequestParamValueException(COMMAND_NAME, "request path", request.getParams().getProperty(DatabusRequest.PATH_PARAM_NAME, ""));
    }

    switch(action)
    {
    case STATUS:
      doStatus(request);
      break;
    case PAUSE:
      doPause(request);
      break;
    case SHUTDOWN:
      doShutdown(request);
      break;
    case START:
      doStart(request);
      break;
    case UNPAUSE:
      doUnpause(request);
      break;
    }
    return null;
  }

  private void doStatus(DatabusRequest request)
  throws IOException
  {
    Set<String> sources = getSourcesParam(request);
    for(EventProducer producer : _eventProducers)
    {
      if(sources == null || sources.contains(producer.getName()))
      {
        String state;
        if(producer.isRunning())
        {
          state = "running";
        }
        else if(producer.isPaused())
        {
          state = "paused";
        }
        else
        {
          state = "shutdown";
        }
        write(request, String.format("{\"name\" : \"%s\", \"status\" : \"%s\", \"SCN\" : %d}", producer.getName(), state, producer.getSCN()));
      }
    }
  }

  private void doPause(DatabusRequest request)
  throws IOException
  {
    Set<String> sources = getSourcesParam(request);
    for(EventProducer producer : _eventProducers)
    {
      if(sources == null || sources.contains(producer.getName()))
      {
        producer.pause();
      }
    }
    doStatus(request);
  }

  private void doUnpause(DatabusRequest request)
  throws IOException
  {
    Set<String> sources = getSourcesParam(request);
    for(EventProducer producer : _eventProducers)
    {
      if(sources == null || sources.contains(producer.getName()))
      {
        producer.unpause();
      }
    }
    doStatus(request);
  }

  private void doShutdown(DatabusRequest request)
  throws IOException
  {
    Set<String> sources = getSourcesParam(request);
    for(EventProducer producer : _eventProducers)
    {
      if(sources == null || sources.contains(producer.getName()))
      {
        producer.shutdown();
      }
    }
    doStatus(request);
  }

  private void doStart(DatabusRequest request)
  throws IOException, RequestProcessingException
  {
    Set<String> sources = getSourcesParam(request);
    if(sources == null || sources.size() != 1)
    {
      throw new RequestProcessingException("start requires exactly one source be specified");
    }
    long scn = request.getOptionalLongParam(PARAM_SCN, -1L);

    for(EventProducer producer : _eventProducers)
    {
      if(sources.contains(producer.getName()))
      {
        producer.start(scn);
        write(request, String.format("{\"name\" : \"%s\", \"status\" : \"%s\", \"SCN\" : %d}", producer.getName(), "running", producer.getSCN()));
      }
    }
  }

  private Set<String> getSourcesParam(DatabusRequest request)
  {
    String s = request.getParams().getProperty(PARAM_SOURCE_NAMES);
    if(s == null || s.length() == 0)
    {
      return null;
    }

    String[] sources = s.split(",");
    Set<String> sourceSet = new HashSet<String>(sources.length);
    sourceSet.addAll(Arrays.asList(sources));
    return sourceSet;
  }

  private void write(DatabusRequest request, String str)
  throws IOException
  {
    request.getResponseContent().write(ByteBuffer.wrap(str.getBytes(Charset.defaultCharset())));
  }
}
