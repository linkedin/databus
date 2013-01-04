package com.linkedin.databus2.core.container.request;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonGenerator.Feature;
import org.codehaus.jackson.ObjectCodec;
import org.codehaus.jackson.impl.WriterBasedGenerator;
import org.codehaus.jackson.io.IOContext;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.util.BufferRecycler;

import com.linkedin.databus2.core.container.request.InvalidRequestParamValueException;
import com.linkedin.databus.core.monitoring.mbean.DatabusMonitoringMBean;

public abstract class AbstractStatsRequestProcessor extends AbstractRequestProcesser
{

  public static final String MODULE = AbstractStatsRequestProcessor.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);
  public static final String RESET_PARAM = "reset";
  public static final String ENABLED_PARAM = "enabled";


  private final ExecutorService _executorService;
  private final String _commandName;

  public AbstractStatsRequestProcessor(String commandName, ExecutorService executorService)
  {
    super();
    _executorService = executorService;
    _commandName = commandName;
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
    String category = request.getParams().getProperty(DatabusRequest.PATH_PARAM_NAME);
    if (null == category)
    {
      throw new InvalidRequestParamValueException(_commandName, "category", "null");
    }

    if (! doProcess(category, request))
    {
      throw new InvalidRequestParamValueException(_commandName, "category", category);
    }

    return request;
  }

  protected abstract boolean doProcess(String  category, DatabusRequest request)
                     throws IOException, RequestProcessingException;

  protected<T> void enableOrResetStatsMBean(DatabusMonitoringMBean<T> bean, DatabusRequest request)
  {
    String enabledStr = request.getParams().getProperty(ENABLED_PARAM);
    if (null != enabledStr)
    {
      boolean newEnabled = Boolean.parseBoolean(enabledStr);
      bean.setEnabled(newEnabled);
    }

    String resetStr = request.getParams().getProperty(RESET_PARAM);
    if (null != resetStr)
    {
      bean.reset();
    }
  }

}
