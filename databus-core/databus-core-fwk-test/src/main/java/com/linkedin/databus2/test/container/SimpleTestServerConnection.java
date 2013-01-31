package com.linkedin.databus2.test.container;
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


import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.DirectChannelBufferFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ChildChannelStateEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.local.DefaultLocalServerChannelFactory;
import org.jboss.netty.channel.local.LocalAddress;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.channel.socket.oio.OioServerSocketChannelFactory;
import org.testng.Assert;

/** Simple network server for unit tests. One has to specify the channel pipeline to use. */
public class SimpleTestServerConnection
{
  public enum ServerType
  {
    NIO,
    LOCAL,
    OIO
  }

  private Channel _channel;
  private Thread _thread;
  private final ServerBootstrap _srvBootstrap;
  private final Lock _lock = new ReentrantLock(true);
  private boolean _shutdownRequested;
  private boolean _shutdown;
  private boolean _started;
  private final Condition _startedCondition = _lock.newCondition();
  private final Condition _shutdownReqCondition = _lock.newCondition();
  private final Condition _shutdownCondition = _lock.newCondition();
  private Channel _lastConnChannel;
  private final Map<SocketAddress, Channel> _childrenChannels =
		  new ConcurrentHashMap<SocketAddress, Channel>();
  private final ServerType _serverType;

  public SimpleTestServerConnection(ByteOrder bufferByteOrder)
  {
    this(bufferByteOrder, ServerType.LOCAL);
  }

  public SimpleTestServerConnection(ByteOrder bufferByteOrder, ServerType serverType)
  {
    ChannelFactory channelFactory;
    _serverType = serverType;
    switch (serverType)
    {
    case LOCAL: channelFactory = new DefaultLocalServerChannelFactory(); break;
    case NIO:
    {
      channelFactory =
          new NioServerSocketChannelFactory(Executors.newCachedThreadPool(),
                                            Executors.newCachedThreadPool());
      break;
    }
    case OIO:
    {
      channelFactory = new OioServerSocketChannelFactory(Executors.newCachedThreadPool(),
                                                         Executors.newCachedThreadPool());
      break;
    }
    default: throw new RuntimeException("unsupported server type: " + serverType );
    }
    _srvBootstrap = new ServerBootstrap(channelFactory);
    _srvBootstrap.setOption("child.bufferFactory",
                            DirectChannelBufferFactory.getInstance(bufferByteOrder));
    _srvBootstrap.setParentHandler(new ChildChannelTracker());
  }

  public void setPipelineFactory(ChannelPipelineFactory pipelineFactory)
  {
    _srvBootstrap.setPipelineFactory(pipelineFactory);
  }

  public void start(final int localAddr)
  {
    _shutdownRequested = false;
    _shutdown = false;
    _thread = new Thread(new Runnable()
        {

          @Override
          public void run()
          {
            SocketAddress serverAddr = (ServerType.LOCAL == _serverType) ?
                new LocalAddress(localAddr) :
                new InetSocketAddress(localAddr);
            //System.err.println("Server running on thread: " + Thread.currentThread());
            _channel = _srvBootstrap.bind(serverAddr);
            _lock.lock();
            try
            {
              _started = true;
              _startedCondition.signalAll();

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

  public boolean startSynchronously(final int localAddr, long timeoutMillis)
  {
    start(localAddr);
    try {awaitStarted(timeoutMillis);} catch (InterruptedException ie) {};
    return isStarted();
  }

  public boolean isStarted()
  {
    _lock.lock();
    try
    {
      return _started;
    }
    finally
    {
      _lock.unlock();
    }
  }

  public void awaitStarted(long timeoutMillis) throws InterruptedException
  {
    _lock.lock();
    try
    {
      if (!_started) _startedCondition.await(timeoutMillis, TimeUnit.MILLISECONDS);
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
    _srvBootstrap.releaseExternalResources();
  }

  public Channel getChannel()
  {
    return _channel;
  }

  class ChildChannelTracker extends SimpleChannelUpstreamHandler
  {

	@Override
    public synchronized void childChannelOpen(ChannelHandlerContext ctx, ChildChannelStateEvent e)
           throws Exception
    {
      _lastConnChannel = e.getChildChannel();
      e.getChildChannel().getPipeline().addFirst("childChannelMapHandler", new ChildChannelMapHandler());
      super.childChannelOpen(ctx, e);
    }

	class ChildChannelMapHandler extends SimpleChannelUpstreamHandler
	{
	    @Override
        public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception
        {
	      SocketAddress remoteAddr = e.getChannel().getRemoteAddress();
	      _childrenChannels.put(remoteAddr, e.getChannel());
          super.channelConnected(ctx, e);
        }

      @Override
	    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e)
	           throws Exception
	    {
	        SocketAddress remoteAddr = e.getChannel().getRemoteAddress();
	        _childrenChannels.remove(remoteAddr);
	        super.channelClosed(ctx, e);
	    }
	}

  }

  public Channel getLastConnChannel()
  {
    return _lastConnChannel;
  }

  public Channel getChildChannel(SocketAddress clientAddr)
  {
    return _childrenChannels.get(clientAddr);
  }

  public void sendServerResponse(SocketAddress clientAddr, Object response, long timeoutMillis)
  {
    Channel childChannel = getChildChannel(clientAddr);
    Assert.assertNotEquals(childChannel, null);
    ChannelFuture writeFuture = childChannel.write(response);
    if (timeoutMillis > 0)
    {
      try
      {
        writeFuture.await(timeoutMillis);
      }
      catch (InterruptedException e)
      {
        //NOOP
      }
      Assert.assertTrue(writeFuture.isDone());
      Assert.assertTrue(writeFuture.isSuccess());
    }
  }

  public void sendServerClose(SocketAddress clientAddr, long timeoutMillis)
  {
    Channel childChannel = getChildChannel(clientAddr);
    Assert.assertNotEquals(childChannel, null);
    ChannelFuture closeFuture = childChannel.close();
    if (timeoutMillis > 0)
    {
      try
      {
        closeFuture.await(timeoutMillis);
      }
      catch (InterruptedException e)
      {
        //NOOP
      }
      Assert.assertTrue(closeFuture.isDone());
      Assert.assertTrue(closeFuture.isSuccess());
    }
  }

}
