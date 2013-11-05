/**
 *
 */
package com.linkedin.databus.bootstrap.server;
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
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;

import com.linkedin.databus.bootstrap.api.BootstrapProcessingException;
import com.linkedin.databus.bootstrap.common.BootstrapHttpStatsCollector;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DatabusComponentStatus;
import com.linkedin.databus.core.DbusClientMode;
import com.linkedin.databus.core.Encoding;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.monitoring.mbean.StatsCollectors;
import com.linkedin.databus2.core.container.DatabusHttpHeaders;
import com.linkedin.databus2.core.container.request.BootstrapDBException;
import com.linkedin.databus2.core.container.request.BootstrapDatabaseTooOldException;
import com.linkedin.databus2.core.container.request.DatabusRequest;
import com.linkedin.databus2.core.container.request.RequestProcessingException;
import com.linkedin.databus2.core.filter.DbusFilter;
import com.linkedin.databus2.core.filter.DbusKeyFilter;
import com.linkedin.databus2.core.filter.KeyFilterConfigJSONFactory;


/**
 * @author lgao
 *
 */
public class BootstrapRequestProcessor extends BootstrapRequestProcessorBase
{
  public static final String       MODULE           = BootstrapRequestProcessor.class.getName();
  public static final Logger       LOG              = Logger.getLogger(MODULE);

  public final static String       COMMAND_NAME     = "bootstrap";
  public final static String       ACTION_PARAM     = "action";
  public final static String       CHECKPOINT_PARAM = "checkPoint";
  public final static String       BATCHSIZE_PARAM  = "batchSize";
  public final static String       PARTITION_INFO_PARAM = "filter";
  public final static String       OUTPUT_PARAM = "output";
  public static final int          DEFAULT_BUFFER_MARGIN_SPACE = 900;
  // To keep track of JSON format overhead characters like ( {, }, [, ], ", :)
  public static final int          DEFAULT_JSON_OVERHEAD_BYTES = 10;

  private final DatabusComponentStatus _componentStatus;

  public BootstrapRequestProcessor(ExecutorService executorService,
                                   BootstrapServerStaticConfig config,
                                   BootstrapHttpServer bootstrapServer) throws InstantiationException,
                                   IllegalAccessException,
                                   ClassNotFoundException,
                                   SQLException
                                   {
	super(executorService,config,bootstrapServer);
    _componentStatus = bootstrapServer.getComponentStatus();
                                   }

  /*
   * (non-Javadoc)
   *
   * @see
   * com.linkedin.databus.container.request.RequestProcessor#process(com.linkedin.databus
   * .container.request.DatabusRequest)
   */
  @Override
  protected DatabusRequest doProcess(DatabusRequest request) throws IOException,
  RequestProcessingException
  {
    BootstrapProcessor processor = null;
    BootstrapHttpStatsCollector bootstrapStatsCollector = _bootstrapServer.getBootstrapStatsCollector();
    long startTime = System.currentTimeMillis();
    boolean isDebug = LOG.isDebugEnabled();

    try
    {
      try
      {
        String threadName = Thread.currentThread().getName();
        DbusEventsStatisticsCollector threadCollector = _bootstrapServer.getOutBoundStatsCollectors().getStatsCollector(threadName);
        if (null == threadCollector)
        {
            threadCollector = new DbusEventsStatisticsCollector(_bootstrapServer.getContainerStaticConfig().getId(),
                    threadName,
                    true,
                    false,
                    _bootstrapServer.getMbeanServer());
        	StatsCollectors<DbusEventsStatisticsCollector> ds = _bootstrapServer.getOutBoundStatsCollectors();
            ds.addStatsCollector(threadName, threadCollector);
        }
        processor = new BootstrapProcessor(_config, threadCollector);
    }
      catch (Exception e)
      {
        if (null != bootstrapStatsCollector)
        {
          bootstrapStatsCollector.registerErrBootstrap();
        }

        throw new RequestProcessingException(e);
      }

      DatabusComponentStatus componentStatus = _componentStatus.getStatusSnapshot();
      if (!componentStatus.isRunningStatus())
      {
        if (null != bootstrapStatsCollector)
          bootstrapStatsCollector.registerErrBootstrap();

   		throw new RequestProcessingException(componentStatus.getMessage());
    	}

    	String partitionInfoString = request.getParams().getProperty(PARTITION_INFO_PARAM);

    	DbusKeyFilter keyFilter = null;
    	if ( (null != partitionInfoString) && (!partitionInfoString.isEmpty()))
    	{
    		try
    		{
    			keyFilter = KeyFilterConfigJSONFactory.parseDbusKeyFilter(partitionInfoString);
    			if ( isDebug) LOG.debug("ServerSideFilter is :" + keyFilter);
    		} catch ( Exception ex) {
    			String msg = "Unable to parse partitionInfo from request. PartitionInfo was :" + partitionInfoString;
    			LOG.error(msg,ex);
    			throw new RequestProcessingException(msg,ex);
    		}
    	}

    	String outputFormat = request.getParams().getProperty(OUTPUT_PARAM);
    	Encoding enc = Encoding.BINARY;

    	if ( null != outputFormat)
    	{
    		try
    		{
    			enc = Encoding.valueOf(outputFormat.toUpperCase());
    		} catch (Exception ex) {
    			LOG.error("Unable to find the output format for bootstrap request for " + outputFormat + ". Using Binary!!", ex);
    		}
    	}

      processor.setKeyFilter(keyFilter);
      String checkpointString = request.getRequiredStringParam(CHECKPOINT_PARAM);

    	int bufferMarginSpace = DEFAULT_BUFFER_MARGIN_SPACE;
    	if ( null != _serverHostPort)
    	{
    		bufferMarginSpace = Math.max(bufferMarginSpace, (_serverHostPort.length() + Checkpoint.BOOTSTRAP_SERVER_INFO.length() + DEFAULT_JSON_OVERHEAD_BYTES));
    	}

    	int clientFreeBufferSize = request.getRequiredIntParam(BATCHSIZE_PARAM) - checkpointString.length() - bufferMarginSpace;

        BootstrapEventWriter writer = null;
        if(_config.getPredicatePushDown())
          writer = createEventWriter(request, clientFreeBufferSize, null, enc);
        else
          writer = createEventWriter(request, clientFreeBufferSize, keyFilter, enc);

        Checkpoint cp = new Checkpoint(checkpointString);


      DbusClientMode consumptionMode = cp.getConsumptionMode();

      LOG.info("Bootstrap request received: " +
          "fetchSize=" + clientFreeBufferSize +
          ", consumptionMode=" + consumptionMode +
          ", checkpoint=" + checkpointString +
          ", predicatePushDown=" + _config.getPredicatePushDown()
          );

      try
      {
        boolean phaseCompleted = false;
        switch (consumptionMode)
        {
        case BOOTSTRAP_SNAPSHOT:
          phaseCompleted = processor.streamSnapShotRows(new Checkpoint(
              checkpointString), writer);
          break;
        case BOOTSTRAP_CATCHUP:
          phaseCompleted = processor.streamCatchupRows(new Checkpoint(
              checkpointString), writer);
          break;
        default:
          if (null != bootstrapStatsCollector)
            bootstrapStatsCollector.registerErrBootstrap();

          throw new RequestProcessingException("Unexpected mode: "
              + consumptionMode);
        }

        if (null != bootstrapStatsCollector)
          bootstrapStatsCollector.registerBootStrapReq(cp, System.currentTimeMillis()-startTime, clientFreeBufferSize);

        if (writer.getNumRowsWritten() == 0 && writer.getSizeOfPendingEvent() > 0)
        {
          // Append a header to indicate to the client that we do have at least one event to
          // send, but it is too large to fit into client's offered buffer.
          request.getResponseContent().addMetadata(DatabusHttpHeaders.DATABUS_PENDING_EVENT_SIZE,
                                                   writer.getSizeOfPendingEvent());
          if (isDebug)
          {
            LOG.debug("Returning 0 events but have pending event of size " + writer.getSizeOfPendingEvent());
          }
        }
        if (phaseCompleted)
        {
          request.getResponseContent().setMetadata(BootstrapProcessor.PHASE_COMPLETED_HEADER_NAME, BootstrapProcessor.PHASE_COMPLETED_HEADER_TRUE);
        }

      }
      catch (BootstrapDatabaseTooOldException e)
      {
        if (null != bootstrapStatsCollector)
          bootstrapStatsCollector.registerErrDatabaseTooOld();

        LOG.error("Bootstrap database is too old!", e);
        throw new RequestProcessingException(e);
      }
      catch (BootstrapDBException e)
      {
        if (null != bootstrapStatsCollector)
          bootstrapStatsCollector.registerErrBootstrap();
        throw new RequestProcessingException(e);
      }
      catch (SQLException e)
      {
        if (null != bootstrapStatsCollector)
          bootstrapStatsCollector.registerErrSqlException();

        throw new RequestProcessingException(e);
      }
      catch (BootstrapProcessingException e)
      {
        if (null != bootstrapStatsCollector)
          bootstrapStatsCollector.registerErrBootstrap();

        throw new RequestProcessingException(e);
      }
    }
    finally
    {
      if ( null != processor)
        processor.shutdown();
    }

    return request;
  }

  protected BootstrapEventWriter createEventWriter(DatabusRequest request, long clientFreeBufferSize, DbusFilter keyFilter, Encoding enc)
  {
  	BootstrapEventWriter writer = new BootstrapEventWriter(request.getResponseContent(), clientFreeBufferSize, keyFilter, enc);
  	return writer;
  }
}
