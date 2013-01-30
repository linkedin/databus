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


import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus2.core.container.ChunkedWritableByteChannel;
import com.linkedin.databus2.core.container.netty.ServerContainer;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

/**
 * The main class to represent a RESTful request. The request can be associated with a
 * {@link RequestProcessor} which knows how to interpret the request parameters and how to process
 * them. Such a request can be called (implements {@link java.util.concurrent.Callable}). The
 * object also implements {@link java.util.concurrent.Future} so it can be returned directly by
 * {@link RequestProcessorRegistry#run(DatabusRequest)} if there is no associated
 * {@link java.util.concurrent.ExecutorService} with the {@link RequestProcessor}. This saves us
 * a memory allocation for a wrapper object.
 *
 * @author cbotev
 *
 */
public class DatabusRequest implements Callable<DatabusRequest>, Future<DatabusRequest> {

	public static final String MODULE = DatabusRequest.class.getName();
	public static final Logger LOG = Logger.getLogger(MODULE);

	public static final String DATA_PARAM_NAME = "reqdata";
	public static final String PATH_PARAM_NAME = "reqpath";
    public static final String DATABUS_ERROR_CLASS_HEADER = "x-dbus-error";
    public static final String DATABUS_ERROR_MESSAGE_HEADER = "x-dbus-error-message";
    public static final String DATABUS_ERROR_CAUSE_CLASS_HEADER = "x-dbus-error-cause";
    public static final String DATABUS_ERROR_CAUSE_MESSAGE_HEADER = "x-dbus-error-cause-message";
    public static final String DATABUS_REQUEST_ID_HEADER = "x-dbus-req-id";
    public static final String DATABUS_REQUEST_LATENCY_HEADER = "x-dbus-req-latency";

	private final static String ERROR_MESSAGE_PREFIX = "";
	private final static byte[] FALLBACK_ERROR_MESSAGE_BYTES = "{\"class\":\"unknown\"}".getBytes();
	private final static String ERROR_MESSAGE_SUFFIX = "\r\n";

	private final long _id;

    private final String _name;
	private final HttpMethod _requestType;
	private final Properties _params;
	private final ServerContainer.RuntimeConfig _config;
	private final long _createTimestampMs;
	private final SocketAddress _remoteAddress;

  /**
   * _cursorPartition has the last partition from which an event was sent (could be partial or full window)
   * to the receiver over a channel. For now, this is a context maintained in the server when serving events
   * from multiple partitions over a single channel (connection). Keeping this context allows the server to
   * cycle through partitions across responses while not starving out some partitions. Eventually we will move
   * this object to be sent by the client in the CheckpointMult object as a hint.
   *
   * The cursorPartition is only used as a hint, and its absense will not affect the correctness. Specifically,
   * it is expected that the cursorPartition is ignored if any one of the checkpoints indicates that a partial
   * window was sent.
   *
   * DatabusRequest is merely used as a holder of the server context information since the request processor does
   * not have access to the netty ChannelContext.
   */
  private PhysicalPartition _cursorPartition = null;

  private ChunkedWritableByteChannel _responseContent = null;

	private Throwable _responseThrowable = null;
	private RequestProcessor _processor = null;

	//For debugging purposes
	private static AtomicLong IdCounter = new AtomicLong(1);

	public DatabusRequest(String name, HttpMethod requestType, SocketAddress remoteAddress,
	                      Properties params, ServerContainer.RuntimeConfig config)
	{
		super();
		_name = name;
		_params = params;
		_requestType = requestType;
		_config = config;
		_id = IdCounter.getAndIncrement();
		_createTimestampMs = System.currentTimeMillis();
		_remoteAddress = remoteAddress;
	}

	public DatabusRequest(String name, HttpMethod requestType, SocketAddress remoteAddress,
	                      ServerContainer.RuntimeConfig config)
	{
		this(name, requestType, remoteAddress, new Properties(), config);
	}

	public String getName()
	{
		return _name;
	}

	public Properties getParams()
	{
		return _params;
	}

	@Override
  public String toString()
	{
		StringBuilder res = new StringBuilder();
		res.append("{\"name\":\"");
		res.append(_name);
		res.append("\", \"method\":\"");
		res.append(_requestType);
		res.append("\" ");

		for (Object key:_params.keySet())
		{
			Object value = _params.get(key);
			res.append(", \"");
			res.append(key.toString());
			res.append("\":\"");
			res.append(null != value ? value.toString() : "null");
			res.append("\"");
		}
		res.append("}");

		return res.toString();
	}

	/**
	 * Obtain the response body to be returned to the user. If {@link DatabusRequest#getResponseThrowable()}
	 * is not null, the body contains the error description.
	 * @return the response body content
	 */
	public ChunkedWritableByteChannel getResponseContent()
	{
		return _responseContent;
	}

	/**
	 * Obtains the error that resulted from the processing of the request if any.
	 * @return the error or null
	 */
	public Throwable getResponseThrowable()
	{
		return _responseThrowable;
	}

	@Override
	public DatabusRequest call()
	{
		if (null != _processor)
		{
			try
			{
			  if (LOG.isDebugEnabled())
			  {
			    LOG.debug(_name + ": start processing");
			  }
			  _processor.process(this);
              if (LOG.isDebugEnabled())
              {
                LOG.debug(_name + ": end processing");
              }
			}
			catch (Exception e)
			{
              StringWriter traceStr = new StringWriter();
              PrintWriter errTrace = new PrintWriter(traceStr);
              e.printStackTrace(errTrace);
              errTrace.close();
              LOG.error(getName() + " error:" + traceStr.toString());

              setError(e);
			}
		}

		return this;
	}

	public RequestProcessor getProcessor()
	{
		return _processor;
	}

	public void setProcessor(RequestProcessor processor)
	{
		_processor = processor;
	}

	/**
	 * Specify that the request processing resulted in an error
	 * @param throwable		the throwable describing the error
	 */
	public void setError(Throwable throwable)
	{
		_responseThrowable = throwable;
		if (null != _responseThrowable)
		{
		  //LOG.error("Response error: " + _responseThrowable);
          _responseContent.addMetadata(DATABUS_ERROR_CLASS_HEADER, _responseThrowable.getClass().getName());
          Throwable cause = _responseThrowable.getCause();
          if (null != cause)
          {
            //LOG.error("Response error caused by: " + cause);
            _responseContent.addMetadata(DATABUS_ERROR_CAUSE_CLASS_HEADER, cause.getClass().getName());
          }

          _responseContent.setResponseCode(HttpResponseStatus.INTERNAL_SERVER_ERROR);


          HashMap<String, String> exceptionInfo = new HashMap<String, String>();
          exceptionInfo.put("error", _responseThrowable.getClass().getName());
          exceptionInfo.put("message", _responseThrowable.getMessage());
          ObjectMapper mapper = new ObjectMapper();
          mapper.getJsonFactory().configure(org.codehaus.jackson.JsonParser.Feature.AUTO_CLOSE_SOURCE, false);

          StringWriter out = new StringWriter(10240);

          out.write(ERROR_MESSAGE_PREFIX);

          byte[] dataBytes;
          try
          {
              mapper.writeValue(out, exceptionInfo);
              out.write(ERROR_MESSAGE_SUFFIX);
              out.close();
              dataBytes = out.toString().getBytes();
          }
          catch (IOException e)
          {
            dataBytes = FALLBACK_ERROR_MESSAGE_BYTES;
          }

          try
          {
              _responseContent.write(ByteBuffer.wrap(dataBytes));
          }
          catch (IOException ioe)
          {
              LOG.error("Can't serialize exception " + ioe.toString());
          }
		}
	}

	public PhysicalPartition getCursorPartition()
	{
		return _cursorPartition;
	}

	public void setCursorPartition(PhysicalPartition cursorPartition)
	{
		_cursorPartition = cursorPartition;
	}

	public HttpMethod getRequestType()
	{
		return _requestType;
	}

	public ServerContainer.RuntimeConfig getConfig()
	{
		return _config;
	}

	public int getRequiredIntParam(String paramName) throws InvalidRequestParamValueException
	{
	  String paramStr = getParams().getProperty(paramName);
	  if (null == paramStr)
	  {
	    throw new InvalidRequestParamValueException(getName(), paramName, "null");
	  }
	  try
	  {
	    return Integer.parseInt(paramStr);
	  }
	  catch (NumberFormatException nfe)
	  {
	    throw new InvalidRequestParamValueException(getName(), paramName, paramStr);
	  }
	}

    public int getOptionalIntParam(String paramName, int defaultValue) throws InvalidRequestParamValueException
    {
      String paramStr = getParams().getProperty(paramName);
      if (null == paramStr)
      {
        return defaultValue;
      }
      try
      {
        return Integer.parseInt(paramStr);
      }
      catch (NumberFormatException nfe)
      {
        throw new InvalidRequestParamValueException(getName(), paramName, paramStr);
      }
    }

    public long getRequiredLongParam(String paramName) throws InvalidRequestParamValueException
    {
      String paramStr = getParams().getProperty(paramName);
      if (null == paramStr)
      {
        throw new InvalidRequestParamValueException(getName(), paramName, "null");
      }
      try
      {
        return Long.parseLong(paramStr);
      }
      catch (NumberFormatException nfe)
      {
        throw new InvalidRequestParamValueException(getName(), paramName, paramStr);
      }
    }

    public long getOptionalLongParam(String paramName, long defaultValue) throws InvalidRequestParamValueException
    {
      String paramStr = getParams().getProperty(paramName);
      if (null == paramStr)
      {
        return defaultValue;
      }
      try
      {
        return Long.parseLong(paramStr);
      }
      catch (NumberFormatException nfe)
      {
        throw new InvalidRequestParamValueException(getName(), paramName, paramStr);
      }
    }

    public String getRequiredStringParam(String paramName) throws InvalidRequestParamValueException
    {
      String paramValue = getParams().getProperty(paramName);
      if (null == paramValue)
      {
        throw new InvalidRequestParamValueException(getName(), paramName, "null");
      }

      return paramValue;
    }

    @Override
    public boolean cancel(boolean arg0)
    {
      return false;
    }

    @Override
    public DatabusRequest get() throws InterruptedException,
        ExecutionException
    {
      return this;
    }

    @Override
    public DatabusRequest get(long arg0, TimeUnit arg1) throws InterruptedException,
        ExecutionException,
        TimeoutException
    {
      return this;
    }

    @Override
    public boolean isCancelled()
    {
      return false;
    }

    @Override
    public boolean isDone()
    {
      return true;
    }

	public void setResponseContent(ChunkedWritableByteChannel responseContent) {
		_responseContent = responseContent;
	}

    public long getId()
    {
      return _id;
    }

    public long getCreateTimestampMs()
    {
      return _createTimestampMs;
    }

    public SocketAddress getRemoteAddress()
    {
      return _remoteAddress;
    }
}
