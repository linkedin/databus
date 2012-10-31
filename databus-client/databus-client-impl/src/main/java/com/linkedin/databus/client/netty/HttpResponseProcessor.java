package com.linkedin.databus.client.netty;

import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpChunkTrailer;
import org.jboss.netty.handler.codec.http.HttpResponse;

/**
 * A callback for processing HTTP responses. The sequence of events is startResponse -> ( addChunk*
 * -> addTrailer?)? -> finishResponse
 * @author cbotev
 *
 */
public interface HttpResponseProcessor
{
  public void startResponse(HttpResponse response) throws Exception;
  public void addChunk(HttpChunk chunk) throws Exception;
  public void addTrailer(HttpChunkTrailer trailer) throws Exception;
  public void finishResponse() throws Exception;
  public void channelException(Throwable cause);
  public void channelClosed();
}
