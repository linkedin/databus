package com.linkedin.databus2.core.container.netty;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelConfig;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.nio.NioSocketChannelConfig;
import org.jboss.netty.handler.codec.http.DefaultHttpChunk;
import org.jboss.netty.handler.codec.http.DefaultHttpChunkTrailer;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpChunkTrailer;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import com.linkedin.databus2.core.container.ChunkedWritableByteChannel;

public class ChunkedBodyWritableByteChannel implements ChunkedWritableByteChannel
{
  public static final String MODULE = ChunkedBodyWritableByteChannel.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  public static final String RESPONSE_CODE_FOOTER_NAME = "x-databus-response-code";

  private final Channel _channel;

  /** Temporarily stores the HTTP response headers until the first byte of the body is sent.
   * This lets response producers to set headers. If set to null, the response headers have been
   * sent.
   */
  private HttpResponse _response;
  private HttpResponseStatus _responseCode;
  private boolean _open = true;
  private HttpChunk _chunkReuse = null;
  private HttpChunkTrailer _trailer = null;


  public ChunkedBodyWritableByteChannel(Channel channel, HttpResponse response)
  {
    _channel = channel;
    _responseCode = response.getStatus();
    _response = response;
    ChannelConfig channelConfig = _channel.getConfig();
    if (channelConfig instanceof NioSocketChannelConfig)
    {
      NioSocketChannelConfig nioSocketConfig = (NioSocketChannelConfig)channelConfig;
      //FIXME: add a config for the high water-mark
      nioSocketConfig.setWriteBufferLowWaterMark(16 * 1024);
      nioSocketConfig.setWriteBufferHighWaterMark(64 * 1024);
    }
  }

  @Override
  public int write(ByteBuffer buffer) throws IOException
  {
    if (null != _response)
    {
      _response.setHeader(HttpHeaders.Names.TRANSFER_ENCODING, HttpHeaders.Values.CHUNKED);
      _response.setChunked(true);
      writeToChannel(_response);
      _response = null;
    }

    //We need to lie to netty that the buffer is BIG_ENDIAN event if it is LITTLE_ENDIAN (see DDS-1212)
    ByteOrder bufferOrder = buffer.order();
    ByteBuffer realBuffer = bufferOrder == ByteOrder.BIG_ENDIAN
        ? buffer : buffer.slice().order(ByteOrder.BIG_ENDIAN);
    if (null == _chunkReuse)
    {
      _chunkReuse = new DefaultHttpChunk(ChannelBuffers.wrappedBuffer(realBuffer));
    }
    else
    {
      _chunkReuse.setContent(ChannelBuffers.wrappedBuffer(realBuffer));
    }

    writeToChannel(_chunkReuse);
    //DDS-1212
    if (bufferOrder == ByteOrder.LITTLE_ENDIAN) buffer.position(realBuffer.position());

    return realBuffer.remaining();
  }

  @Override
  public void close() throws IOException
  {
    if (_open)
    {
      _open = false;
      if (null != _response)
      {
        _response.setHeader(HttpHeaders.Names.CONTENT_LENGTH,
                            Long.valueOf(_response.getContent().readableBytes()));

        writeToChannel(_response, 10);
        _response = null;
      }
      else
      {
        if (null != _trailer)
        {
          writeToChannel(_trailer, 1);
          _trailer = null;
        }
        else
        {
          writeToChannel(HttpChunk.LAST_CHUNK, 1);
        }
      }
    }
  }

  @Override
  public void addMetadata(String name, String value)
  {
    if (null != _response)
    {
      _response.addHeader(name, value);
    }
    else
    {
      if (null == _trailer)
      {
        _trailer = new DefaultHttpChunkTrailer();
      }

      _trailer.addHeader(name, value);
    }
  }

  @Override
  public void setMetadata(String name, String value)
  {
    if (null != _response)
    {
      _response.setHeader(name, value);
    }
    else
    {
      if (null == _trailer)
      {
        _trailer = new DefaultHttpChunkTrailer();
      }

      _trailer.setHeader(name, value);
    }
  }

  @Override
  public void removeMetadata(String name)
  {
    if (null != _response)
    {
      _response.removeHeader(name);
    }
    else if (null != _trailer)
    {
      _trailer.removeHeader(name);
    }
  }
  private void writeToChannel(Object o, int flushSize) throws IOException
  {
    ChannelFuture channelFuture = _channel.write(o);
    if (flushSize > 0 && !_channel.isWritable())
    {
      ChannelConfig channelConfig = _channel.getConfig();
      if (channelConfig instanceof NioSocketChannelConfig)
      {
        NioSocketChannelConfig nioSocketConfig = (NioSocketChannelConfig)channelConfig;
        nioSocketConfig.setWriteBufferLowWaterMark(flushSize);
        nioSocketConfig.setWriteBufferHighWaterMark(flushSize);
      }
    }
    awaitChannelFuture(channelFuture);
    if (! channelFuture.isSuccess())
    {
      throw new IOException(channelFuture.getCause());
    }
  }

  private void writeToChannel(Object o) throws IOException
  {
    writeToChannel(o, 0);
  }

  private void awaitChannelFuture(ChannelFuture channelFuture)
  {
    boolean done = channelFuture.isDone();
    while (!done)
    {
      try
      {
        channelFuture.await();
        done = channelFuture.isDone();
      }
      catch (InterruptedException ie)
      {//do nothing
      }
    }
}

  @Override
  public boolean isOpen()
  {
    return _open;
  }


  @Override
  public HttpResponseStatus getResponseCode()
  {
    return _responseCode;
  }

  @Override
  public void setResponseCode(HttpResponseStatus responseCode)
  {
    _responseCode = responseCode;

    if (null != _response)
    {
      _response.setStatus(responseCode);
    }
    else
    {
      setMetadata(RESPONSE_CODE_FOOTER_NAME, Integer.toString(responseCode.getCode()));
    }
  }

  @Override
  public Channel getRawChannel() 
  {
	return _channel;
  }
}
