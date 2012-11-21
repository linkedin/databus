package com.linkedin.databus.core.test.netty;

import java.util.List;
import java.util.Map.Entry;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.frame.TooLongFrameException;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpChunkTrailer;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMessage;

/**
 * Based on netty's HttpChunkAggregator but can handle footers
 * @author cbotev
 *
 */
public class FooterAwareHttpChunkAggregator extends SimpleChannelUpstreamHandler {

  private final int maxContentLength;
  private HttpMessage currentMessage;

  /**
   * Creates a new instance.
   *
   * @param maxContentLength
   *        the maximum length of the aggregated content.
   *        If the length of the aggregated content exceeds this value,
   *        a {@link TooLongFrameException} will be raised.
   */
  public FooterAwareHttpChunkAggregator(int maxContentLength) {
      if (maxContentLength <= 0) {
          throw new IllegalArgumentException(
                  "maxContentLength must be a positive integer: " +
                  maxContentLength);
      }
      this.maxContentLength = maxContentLength;
  }

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
          throws Exception {

      Object msg = e.getMessage();
      HttpMessage currentMessage = this.currentMessage;

      if (msg instanceof HttpMessage) {
          HttpMessage m = (HttpMessage) msg;
          if (m.isChunked()) {
              // A chunked message - remove 'Transfer-Encoding' header,
              // initialize the cumulative buffer, and wait for incoming chunks.
              List<String> encodings = m.getHeaders(HttpHeaders.Names.TRANSFER_ENCODING);
              encodings.remove(HttpHeaders.Values.CHUNKED);
              if (encodings.isEmpty()) {
                  m.removeHeader(HttpHeaders.Names.TRANSFER_ENCODING);
              }
              m.setContent(ChannelBuffers.dynamicBuffer(e.getChannel().getConfig().getBufferFactory()));
              this.currentMessage = m;
          } else {
              // Not a chunked message - pass through.
              this.currentMessage = null;
              ctx.sendUpstream(e);
          }
      } else if (msg instanceof HttpChunk) {
          // Sanity check
          if (currentMessage == null) {
              throw new IllegalStateException(
                      "received " + HttpChunk.class.getSimpleName() +
                      " without " + HttpMessage.class.getSimpleName());
          }

          // Merge the received chunk into the content of the current message.
          HttpChunk chunk = (HttpChunk) msg;
          ChannelBuffer content = currentMessage.getContent();

          if (content.readableBytes() > maxContentLength - chunk.getContent().readableBytes()) {
              throw new TooLongFrameException(
                      "HTTP content length exceeded " + maxContentLength +
                      " bytes.");
          }

          content.writeBytes(chunk.getContent());
          if (chunk.isLast()) {
              this.currentMessage = null;
              currentMessage.setHeader(
                      HttpHeaders.Names.CONTENT_LENGTH,
                      String.valueOf(content.readableBytes()));
              
              if (chunk instanceof HttpChunkTrailer)
              {
                HttpChunkTrailer chunkTrailer = (HttpChunkTrailer)chunk;
                for(Entry<String, String> footer: chunkTrailer.getHeaders())
                {
                  currentMessage.setHeader(footer.getKey(), footer.getValue());
                }
              }
              
              Channels.fireMessageReceived(ctx, currentMessage, e.getRemoteAddress());
          }
      } else {
          // Neither HttpMessage or HttpChunk
          ctx.sendUpstream(e);
      }
  }
}
