package com.linkedin.databus.container.netty;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import com.linkedin.util.CallTracker;
import com.linkedin.util.CallTrackerImpl;

public class NettyStats
{
  public static final String MODULE = NettyStats.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private final CallTracker _pipelineFactory_GetPipelineCallTracker = 
      new CallTrackerImpl(new CallTrackerImpl.Config());
  private final CallTracker _requestHandler_messageRecieved =
      new CallTrackerImpl(new CallTrackerImpl.Config());
  private final CallTracker _requestHandler_processRequest =
    new CallTrackerImpl(new CallTrackerImpl.Config());
  private final CallTracker _requestHandler_writeResponse =
    new CallTrackerImpl(new CallTrackerImpl.Config());
  private AtomicBoolean _enabled = new AtomicBoolean(true);
  
  public NettyStats()
  {
  }

  public void reset()
  {
    boolean enabled = isEnabled();
    if (enabled)
    {
      setEnabled(false);
    }
    
    _pipelineFactory_GetPipelineCallTracker.reset();
    _requestHandler_messageRecieved.reset();
    _requestHandler_processRequest.reset();
    _requestHandler_writeResponse.reset();
    
    if (enabled)
    {
      setEnabled(true);
    }
  }
  
  public boolean isEnabled()
  {
    return _enabled.get();
  }

  public void setEnabled(boolean enabled)
  {
    _enabled.set(enabled);
  }

  public CallTracker getPipelineFactory_GetPipelineCallTracker()
  {
    return _pipelineFactory_GetPipelineCallTracker;
  }

  public CallTracker getRequestHandler_messageRecieved()
  {
    return _requestHandler_messageRecieved;
  }

  public CallTracker getRequestHandler_processRequest()
  {
    return _requestHandler_processRequest;
  }

  public CallTracker getRequestHandler_writeResponse()
  {
    return _requestHandler_writeResponse;
  }
  
}
