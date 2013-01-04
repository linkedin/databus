package com.linkedin.databus2.test.container;

import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.DirectChannelBufferFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.local.DefaultLocalClientChannelFactory;
import org.jboss.netty.channel.local.LocalAddress;

/** Simple network client for unit tests. One has to specify the channel pipeline to use. */
public class SimpleTestClientConnection
{
  private Channel _channel;
  private Thread _thread;
  private final ClientBootstrap _clientBootstrap;
  private final Lock _lock = new ReentrantLock(true);
  private boolean _connected;
  private boolean _shutdownRequested;
  private boolean _shutdown;
  private final Condition _connectedCondition = _lock.newCondition();
  private final Condition _shutdownReqCondition = _lock.newCondition();
  private final Condition _shutdownCondition = _lock.newCondition();

  public SimpleTestClientConnection(ByteOrder bufferByteOrder)
  {
    _clientBootstrap = new ClientBootstrap(new DefaultLocalClientChannelFactory());
    _clientBootstrap.setOption("bufferFactory",
                               DirectChannelBufferFactory.getInstance(bufferByteOrder));
  }

  public void setPipelineFactory(ChannelPipelineFactory pipelineFactory)
  {
    _clientBootstrap.setPipelineFactory(pipelineFactory);
  }

  public void start(final int serverAddr)
  {
    _shutdownRequested = false;
    _shutdown = false;
    _connected = false;
    _thread = new Thread(new Runnable()
        {

          @Override
          public void run()
          {
            //System.err.println("Client running on thread: " + Thread.currentThread());
            ChannelFuture connectFuture = _clientBootstrap.connect(new LocalAddress(serverAddr));
            connectFuture.awaitUninterruptibly();
            _channel = connectFuture.getChannel();
            _lock.lock();
            try
            {
              _connected = connectFuture.isSuccess();
              _connectedCondition.signalAll();
              while (!_shutdownRequested)
              {
                try
                {
                  _shutdownReqCondition.await();
                }
                catch (InterruptedException ie) {}
              }
              _shutdown = true;
              _shutdownCondition.signalAll();
            }
            finally
            {
              _lock.unlock();
            }
          }
        });
    _thread.setDaemon(true);
    _thread.start();
  }

  public boolean startSynchronously(final int serverAddr, long timeoutMillis)
  {
    start(serverAddr);
    try {awaitConnected(timeoutMillis); } catch (InterruptedException ie){};
    return isConnected();
  }

  public void awaitConnected(long timeoutMillis) throws InterruptedException
  {
    _lock.lock();
    try
    {
      if (!_connected) _connectedCondition.await(timeoutMillis, TimeUnit.MILLISECONDS);
    }
    finally
    {
      _lock.unlock();
    }
  }

  public boolean isConnected()
  {
    _lock.lock();
    try
    {
      return _connected;
    }
    finally
    {
      _lock.unlock();
    }

  }

  public void stop()
  {
    _lock.lock();
    try
    {
      _shutdownRequested = true;
      _shutdownReqCondition.signalAll();
      while (! _shutdown)
      {
        try
        {
          _shutdownCondition.await();
        }
        catch (InterruptedException ie) {}
      }
    }
    finally
    {
      _lock.unlock();
    }

    ChannelFuture closeFuture = _channel.close();
    closeFuture.awaitUninterruptibly();
    _clientBootstrap.releaseExternalResources();
  }

  public Channel getChannel()
  {
    return _channel;
  }

}
