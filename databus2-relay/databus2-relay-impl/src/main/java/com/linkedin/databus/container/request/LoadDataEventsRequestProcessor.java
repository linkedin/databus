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


import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventBufferMult;
import com.linkedin.databus.core.DbusEventV1;
import com.linkedin.databus.core.InvalidEventException;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus2.core.container.request.DatabusRequest;
import com.linkedin.databus2.core.container.request.RequestProcessingException;
import com.linkedin.databus2.core.container.request.RequestProcessor;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import org.apache.log4j.Logger;

public class LoadDataEventsRequestProcessor implements RequestProcessor
{
  public static final Logger LOG = Logger.getLogger(LoadDataEventsRequestProcessor.class.getName());

  public static final String COMMAND_NAME = "loadDataEvents";
  public static final String FILE_PATH_PARAM = "file";
  public static final String START_WINDOW_PARAM = "startWindow";
  public static final String PHYSICAL_PARTITION_ID_PARAM = "physicalPartionId";

  private final ExecutorService _executorService;
  private final HttpRelay _relay;
  private final DbusEventBufferMult _eventBuffer;

  public LoadDataEventsRequestProcessor(ExecutorService executorService,
                                        HttpRelay relay)
  {
    _executorService = executorService;
    _relay = relay;
    _eventBuffer = _relay.getEventBuffer();
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
    String fileStr = request.getRequiredStringParam(FILE_PATH_PARAM);
    String startWinStr = request.getParams().getProperty(START_WINDOW_PARAM, "false");
    String physicalParitionParameter = request.getParams().getProperty(PHYSICAL_PARTITION_ID_PARAM);

    LOG.info(PHYSICAL_PARTITION_ID_PARAM + "=" + physicalParitionParameter);
    PhysicalPartition pPartition = PhysicalSourceStaticConfig.getDefaultPhysicalPartition();
    if(physicalParitionParameter != null) {
      // physical partition parameter format is PPName_PPId
      pPartition = PhysicalPartition.parsePhysicalPartitionString(physicalParitionParameter, "_");
    }

    boolean startWin = Boolean.valueOf(startWinStr);

    BufferedReader in = new BufferedReader(new FileReader(fileStr));

    try
    {
      //PhysicalPartition pPartition = new PhysicalPartition(physicalPartitionId);
      // TODO this should actually use DbusEventBufferAppendable (DDSDBUS-78)
      DbusEventBuffer buf = (DbusEventBuffer)_eventBuffer.getDbusEventBufferAppendable(pPartition);
      if(buf == null)
        throw new RequestProcessingException("cannot find buffer for ph. partion " + pPartition);

      if ((buf.getMinScn() < 0) && (buf.getPrevScn() < 0)) buf.start(0);

      try
      {
        DbusEventsStatisticsCollector statsCollector = _relay.getInBoundStatsCollectors().getStatsCollector(pPartition.toSimpleString());

        int eventsAppended = 0;
        if (!((eventsAppended = DbusEventV1.appendToEventBuffer(in, buf, statsCollector,
                                                              startWin))>0))
        {
          throw new RequestProcessingException("event loading failed");
        }

        StringBuilder res = new StringBuilder(20);
        res.append("{\"eventsAppended\":");
        res.append(eventsAppended);
        res.append("}");

        request.getResponseContent().write(ByteBuffer.wrap(res.toString().getBytes()));
      }
      catch (InvalidEventException iee)
      {
        throw new RequestProcessingException(iee);
      }
      catch (RuntimeException re)
      {
        LOG.error("runttime excception: " + re.getMessage(), re);
        throw re;
      }
    }
    finally
    {
      in.close();
    }

    return request;
  }

}
