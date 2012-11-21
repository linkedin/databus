package com.linkedin.databus2.core.container;

import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.LifeCycleAwareChannelHandler;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.timeout.ReadTimeoutException;
import org.jboss.netty.util.ExternalResourceReleasable;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.Timer;
import org.jboss.netty.util.TimerTask;

/**
 * Base class for read timeout handlers.
 *
 * The handler can be temporarily suspended to model the case where one has multiple request/
 * response pairs over a persistent connection. We want the timeout not to be enforced when there
 * is no communication.
 *
 * @author cbotev
 *
 */
public class ExtendedReadTimeoutHandler extends SimpleChannelUpstreamHandler
                                        implements LifeCycleAwareChannelHandler,
                                                       ExternalResourceReleasable
{
  public static final String MODULE = ExtendedReadTimeoutHandler.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  private final String _name;
  private final boolean _closeOnTimeout;
  private final Timer _timer;
  private final long _timeoutMs;
  private final boolean _ownTimer;
  private volatile long _lastReadTs; //-1 means not started
  private volatile Timeout _timeout;
  private volatile ReadTimeoutTask _timeoutTask;

  public ExtendedReadTimeoutHandler(String name, Timer timer, long timeoutMs, boolean closeOnTimeout)
  {
    _name = name;
    _closeOnTimeout = closeOnTimeout;
    _timeoutMs = timeoutMs;
    _lastReadTs = -1;
    if (null != timer)
    {
      _timer = timer;
      _ownTimer = false;
    }
    else
    {
      _timer = new HashedWheelTimer(timeoutMs, TimeUnit.MILLISECONDS, 10);
      _ownTimer = true;
    }
  }

  public void start(ChannelHandlerContext ctx)
  {
    updateLastReadTime();
    _timeoutTask = new ReadTimeoutTask(ctx);
    createTimeout(_timeoutTask, _timeoutMs);
  }

  public void stop()
  {
    _lastReadTs = -1;
    if (null != _timeout)  _timeout.cancel();
    _timeoutTask = null;
    _timeout = null;
  }

  public void destroy()
  {
    stop();
    if (_ownTimer)
    {
      LOG.info("stopping timeout timer");
      _timer.stop();
    }
  }

  public boolean isStarted()
  {
    return null != _timeoutTask;
  }

  @Override
  public void beforeAdd(ChannelHandlerContext arg0) throws Exception
  {
    //Nothing to do
  }

  @Override
  public void afterAdd(ChannelHandlerContext arg0) throws Exception
  {
    //Nothing to do
  }

  @Override
  public void beforeRemove(ChannelHandlerContext arg0) throws Exception
  {
    destroy();
  }

  @Override
  public void afterRemove(ChannelHandlerContext arg0) throws Exception
  {
    //Nothing to do
  }

  @Override
  public void releaseExternalResources()
  {
    destroy();
  }

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception
  {
    if (! isStarted())
    {
      //start automatically on first received message
      //this is useful for servers for which we don't know when to expect the first message
      start(ctx);
    }
    else
    {
      updateLastReadTime();
    }
    super.messageReceived(ctx, e);
  }

  private void updateLastReadTime()
  {
    _lastReadTs = System.currentTimeMillis();
  }

  private void createTimeout(ReadTimeoutTask task, long timeoutMs)
  {
    if (timeoutMs > 0)
    {
      _timeout = _timer.newTimeout(task, timeoutMs, TimeUnit.MILLISECONDS);
    }
  }

  private void readTimedOut(ChannelHandlerContext ctx)
  {
    Channels.fireExceptionCaught(ctx, new ReadTimeoutException(_name));
    if (_closeOnTimeout) ctx.getChannel().close(); //close the channel asynchronously
  }

  private final class ReadTimeoutTask implements TimerTask
  {
    private final ChannelHandlerContext _ctx;

    ReadTimeoutTask(ChannelHandlerContext ctx)
    {
      _ctx = ctx;
    }

    @Override
    public void run(Timeout timeout) throws Exception
    {
      if (timeout.isCancelled() || !_ctx.getChannel().isOpen() || -1 == _lastReadTs) return;

      long now = System.currentTimeMillis();
      long nextTimeout = _timeoutMs - (now - _lastReadTs);
      if (nextTimeout <= 0)
      {
        //Timeout
        try
        {
          readTimedOut(_ctx);
        }
        catch (Throwable t)
        {
          Channels.fireExceptionCaught(_ctx, t);
        }
      }
      else
      {
        //Read occurred before the timeout - set a new timeout with shorter delay.
        createTimeout(this, nextTimeout);
      }
    }
  }

}
