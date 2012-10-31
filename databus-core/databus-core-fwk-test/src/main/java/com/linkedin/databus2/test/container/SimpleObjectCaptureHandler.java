package com.linkedin.databus2.test.container;

import java.util.List;
import java.util.Vector;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

/**
 * A simple channel handler which records all message that  pass through it. The class is meant
 * mostly for testing purposes.*/
public class SimpleObjectCaptureHandler extends SimpleChannelUpstreamHandler
{

  private final List<Object> _messages;
  private final Lock _msgLock = new ReentrantLock();
  private final Condition _newMsgCondition = _msgLock.newCondition();

  public SimpleObjectCaptureHandler()
  {
    _messages = new Vector<Object>();
  }

  public void clear()
  {
    _msgLock.lock();
    try
    {
      _messages.clear();
    }
    finally
    {
      _messages.clear();
    }
  }

  public List<Object> getMessages()
  {
    return _messages;
  }

  public boolean waitForMessage(long timeoutMs, int size)
  {
    _msgLock.lock();
    try
    {
      boolean interrupted = false;
      while (!interrupted && size >= _messages.size())
      {
        try
        {
          if (timeoutMs > 0)
          {
            _newMsgCondition.await(timeoutMs, TimeUnit.MILLISECONDS);
          }
          else
          {
            _newMsgCondition.await();
          }
        }
        catch (InterruptedException e)
        {
          interrupted = true;
        }
      }
      return size < _messages.size();
    }
    finally
    {
      _msgLock.unlock();
    }
  }

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception
  {
    _msgLock.lock();
    try
    {
      _messages.add(e.getMessage());
      _newMsgCondition.signalAll();
    }
    finally
    {
      _msgLock.unlock();
    }
    super.messageReceived(ctx, e);
  }

}
