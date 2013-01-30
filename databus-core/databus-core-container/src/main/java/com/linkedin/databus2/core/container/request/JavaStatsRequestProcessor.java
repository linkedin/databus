package com.linkedin.databus2.core.container.request;
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
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;

import com.linkedin.databus2.core.container.request.AbstractStatsRequestProcessor;
import com.linkedin.databus2.core.container.request.DatabusRequest;
import com.linkedin.databus2.core.container.request.RequestProcessingException;

public class JavaStatsRequestProcessor extends AbstractStatsRequestProcessor
{
  public static final String MODULE = JavaStatsRequestProcessor.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);
  
  public static final String COMMAND_NAME = "javaStats";
  
  public JavaStatsRequestProcessor(ExecutorService executorService)
  {
    super(COMMAND_NAME, executorService);
  }


  @Override
  public boolean doProcess(String category, DatabusRequest request) 
         throws IOException, RequestProcessingException
  {
    boolean success = true;
    
    if (category.equals("memory/heap"))
    {
      processHeapMemoryStats(request);
    }
    else if (category.equals("memory/nonheap"))
    {
      processNonHeapMemoryStats(request);
    }
    else if (category.equals("memory"))
    {
      processAllMemoryStats(request);
    }
    else if (category.equals("gc"))
    {
      processGcStats(request);
    }
    else if (category.equals("os"))
    {
      processOsStats(request);
    }
    else
    {
      success = false;
    }
    
    return success;
  }
  
  private void processAllMemoryStats(DatabusRequest request) throws IOException
  {
    MemoryMXBean memStats = ManagementFactory.getMemoryMXBean();
    writeJsonObjectToResponse(memStats, request);
  }

  private void processOsStats(DatabusRequest request) throws IOException
  {
    OperatingSystemMXBean osStats = ManagementFactory.getOperatingSystemMXBean();
    writeJsonObjectToResponse(osStats, request);
  }

  private void processGcStats(DatabusRequest request) throws IOException
  {
    List<GarbageCollectorMXBean> gcStats = ManagementFactory.getGarbageCollectorMXBeans();
    writeJsonObjectToResponse(gcStats, request);
  }

  private void processNonHeapMemoryStats(DatabusRequest request) throws IOException
  {
    MemoryUsage memStats = ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage();
    writeJsonObjectToResponse(memStats, request);
  }

  private void processHeapMemoryStats(DatabusRequest request) throws IOException
  {
    MemoryUsage memStats = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
    writeJsonObjectToResponse(memStats, request);
  }
  
}
