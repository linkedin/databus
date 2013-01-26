package com.linkedin.databus.client.netty;

import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventV1;
import com.linkedin.databus.core.Encoding;
import com.linkedin.databus.core.OffsetNotFoundException;
import com.linkedin.databus.core.ScnNotFoundException;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus2.core.container.request.RegisterResponseEntry;
import com.linkedin.databus2.core.filter.AllowAllDbusFilter;
import com.linkedin.databus2.test.container.SimpleObjectCaptureHandler;
import com.linkedin.databus2.test.container.SimpleTestServerConnection;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.SocketAddress;
import java.nio.channels.WritableByteChannel;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import junit.framework.Assert;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;

public class NettyTestUtils
{

  public static void sendServerResponses(SimpleTestServerConnection srv,
                                         SocketAddress clientAddr,
                                         HttpResponse sourcesResp,
                                         HttpChunk body,
                                         long timeout)
  {
    srv.sendServerResponse(clientAddr, sourcesResp, timeout);
    srv.sendServerResponse(clientAddr, body, timeout);
    srv.sendServerResponse(clientAddr, HttpChunk.LAST_CHUNK, timeout);
  }

  public static void sendServerResponses(SimpleTestServerConnection srv,
                                         SocketAddress clientAddr,
                                         HttpResponse sourcesResp,
                                         HttpChunk body)
  {
    sendServerResponses(srv, clientAddr, sourcesResp, body, 1000);
  }

  public static String generateRegisterResponse(RegisterResponseEntry... entries)
      throws JsonGenerationException, JsonMappingException, IOException
  {
    ObjectMapper mapper = new ObjectMapper();
    StringWriter w = new StringWriter();
    mapper.writeValue(w, entries);
    w.close();
    return w.toString();
  }

  public static ChannelBuffer streamToChannelBuffer(DbusEventBuffer buf, Checkpoint cp,
                                                    int maxSize,
                                                    DbusEventsStatisticsCollector stats)
      throws ScnNotFoundException, OffsetNotFoundException, IOException
  {
    ChannelBuffer tmpBuf = ChannelBuffers.buffer(DbusEventV1.byteOrder, maxSize);
    OutputStream tmpOS = new ChannelBufferOutputStream(tmpBuf);
    WritableByteChannel tmpChannel = java.nio.channels.Channels.newChannel(tmpOS);
    buf.streamEvents(cp, false, maxSize, tmpChannel, Encoding.BINARY, new AllowAllDbusFilter(),
                     stats);
    tmpChannel.close();
    return tmpBuf;
  }

  public static Matcher waitForHttpRequest(SimpleObjectCaptureHandler objCapture,
                                           String regex,
                                           long timeout)
  {
    Pattern pattern = Pattern.compile(regex);

    Assert.assertTrue(objCapture.waitForMessage(timeout, 0));
    Object msgObj = objCapture.getMessages().get(0);
    Assert.assertTrue(msgObj instanceof HttpRequest);
    HttpRequest msgReq = (HttpRequest)msgObj;
    Matcher result = pattern.matcher(msgReq.getUri());
    Assert.assertTrue(result.matches());

    return result;
  }

}
