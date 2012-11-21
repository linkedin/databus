package com.linkedin.databus2.core.container;

import java.nio.channels.ClosedChannelException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.timeout.ReadTimeoutException;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus2.test.container.ExceptionListenerTestHandler;
import com.linkedin.databus2.test.container.SimpleTestClientConnection;
import com.linkedin.databus2.test.container.SimpleTestMessageReader;
import com.linkedin.databus2.test.container.SimpleTestServerConnection;


/** Tests {@link ExtendedReadTimeoutHandler} */
public class TestExtendedReadTimeoutHandler
{
  static final long CONNECT_TIMEOUT_MS = 1000;

  @Test
  /**
   * Simulates the follow communication with no induced timeouts
   *
   * <table>
   * <th><td>client</td><td>server</td></th>
   * <tr><td>send "hello"</td><td></td></tr>
   * <tr><td></td><td>start read timeout</td></tr>
   * <tr><td>send "eom"</td><td></td></tr>
   * <tr><td>start read timeout</td><td></td></tr>
   * <tr><td></td><td>stop read timeout</td></tr>
   * <tr><td></td>send "hi there"<td></td></tr>
   * <tr><td></td>send "eom"<td></td></tr>
   * <tr><td>stop read timeout</td><td></td></tr>
   * </table>
   */
  public void testServerSimpleRequestResponse()
  {
    SimpleTestServerConnection srvConn = new SimpleTestServerConnection(DbusEvent.byteOrder);
    srvConn.setPipelineFactory(new SimpleServerPipelineFactory());
    boolean serverStarted = srvConn.startSynchronously(1, CONNECT_TIMEOUT_MS);
    Assert.assertTrue(serverStarted, "server started");

    SimpleTestClientConnection clientConn = new SimpleTestClientConnection(DbusEvent.byteOrder);
    clientConn.setPipelineFactory(new SimpleClientPipelineFactory());
    boolean clientConnected = clientConn.startSynchronously(1, CONNECT_TIMEOUT_MS);
    Assert.assertTrue(clientConnected, "client connected");

    //hook in to key places in the server pipeline
    ChannelPipeline lastSrvConnPipeline = srvConn.getLastConnChannel().getPipeline();
    ExtendedReadTimeoutHandler srvTimeoutHandler =
        (ExtendedReadTimeoutHandler)lastSrvConnPipeline.get(
            ExtendedReadTimeoutHandler.class.getSimpleName());
    SimpleTestMessageReader srvMsgReader = (SimpleTestMessageReader)lastSrvConnPipeline.get(
        SimpleTestMessageReader.class.getSimpleName());
    ExceptionListenerTestHandler srvExceptionListener =
        (ExceptionListenerTestHandler)lastSrvConnPipeline.get(
            ExceptionListenerTestHandler.class.getSimpleName());

    //hook in to key places in the client pipeline
    ChannelPipeline clientPipeline = clientConn.getChannel().getPipeline();
    ExtendedReadTimeoutHandler clientTimeoutHandler =
        (ExtendedReadTimeoutHandler)clientPipeline.get(
            ExtendedReadTimeoutHandler.class.getSimpleName());
    SimpleTestMessageReader clientMsgReader = (SimpleTestMessageReader)clientPipeline.get(
        SimpleTestMessageReader.class.getSimpleName());
    ExceptionListenerTestHandler clientExceptionListener =
        (ExceptionListenerTestHandler)clientPipeline.get(
            ExceptionListenerTestHandler.class.getSimpleName());

    //send a request
    ChannelBuffer msg = ChannelBuffers.wrappedBuffer("hello".getBytes());
    clientConn.getChannel().write(msg);

    //wait for the request to propagate
    try {Thread.sleep(10);} catch (InterruptedException ie){};

    Assert.assertNull(srvExceptionListener.getLastException(), "no server read timeout");
    Assert.assertNull(clientExceptionListener.getLastException(), "no client read timeout");
    Assert.assertEquals(srvMsgReader.getMsg(), "hello", "message read");

    //start server read timeout
    srvTimeoutHandler.start(lastSrvConnPipeline.getContext(srvTimeoutHandler));

    ChannelBuffer msg2 = ChannelBuffers.wrappedBuffer("eom".getBytes());
    clientConn.getChannel().write(msg2);

    //start the client timeout handler
    clientTimeoutHandler.start(clientPipeline.getContext(clientTimeoutHandler));

    Assert.assertNull(srvExceptionListener.getLastException(), "no server read timeout");
    Assert.assertNull(clientExceptionListener.getLastException(), "no client read timeout");
    Assert.assertEquals(srvMsgReader.getMsg(), "eom", "message read");

    //stop server read timeout
    srvTimeoutHandler.stop();

    ChannelBuffer resp = ChannelBuffers.wrappedBuffer("hi there".getBytes());
    lastSrvConnPipeline.getChannel().write(resp);

    //wait for the response to propagate
    try {Thread.sleep(10);} catch (InterruptedException ie){};

    Assert.assertNull(srvExceptionListener.getLastException(), "no server read timeout");
    Assert.assertNull(clientExceptionListener.getLastException(), "no client read timeout");
    Assert.assertEquals(clientMsgReader.getMsg(), "hi there", "response read");

    ChannelBuffer resp2 = ChannelBuffers.wrappedBuffer("eom".getBytes());
    lastSrvConnPipeline.getChannel().write(resp2);

    //wait for the response to propagate
    try {Thread.sleep(10);} catch (InterruptedException ie){};

    Assert.assertNull(srvExceptionListener.getLastException(), "no server read timeout");
    Assert.assertNull(clientExceptionListener.getLastException(), "no client read timeout");
    Assert.assertEquals(clientMsgReader.getMsg(), "eom", "response read");

    //stop client read timeout
    clientTimeoutHandler.stop();

    clientConn.stop();
    srvConn.stop();
  }

  @Test
  /**
   * Simulates the follow communication with no induced timeouts
   *
   * <table>
   * <th><td>client</td><td>server</td></th>
   * <tr><td>send "hello1"</td><td></td></tr>
   * <tr><td></td><td>start read timeout</td></tr>
   * <tr><td>send "hello2"</td><td></td></tr>
   * <tr><td>:</td><td></td></tr>
   * <tr><td>:</td><td></td></tr>
   * <tr><td>send "hello50"</td><td></td></tr>
   * <tr><td>send "eom"</td><td></td></tr>
   * <tr><td>start read timeout</td><td></td></tr>
   * <tr><td></td><td>stop read timeout</td></tr>
   * <tr><td></td>send "hi there1"<td></td></tr>
   * <tr><td></td>send ":"<td></td></tr>
   * <tr><td></td>send "hi there100"<td></td></tr>
   * <tr><td></td>send "eom"<td></td></tr>
   * <tr><td>stop read timeout</td><td></td></tr>
   * </table>
   */
  public void testServerComplexRequestResponse()
  {
    SimpleTestServerConnection srvConn = new SimpleTestServerConnection(DbusEvent.byteOrder);
    srvConn.setPipelineFactory(new SimpleServerPipelineFactory());
    boolean serverStarted = srvConn.startSynchronously(2, CONNECT_TIMEOUT_MS);
    Assert.assertTrue(serverStarted, "server started");

    SimpleTestClientConnection clientConn = new SimpleTestClientConnection(DbusEvent.byteOrder);
    clientConn.setPipelineFactory(new SimpleClientPipelineFactory());
    boolean clientConnected = clientConn.startSynchronously(2, CONNECT_TIMEOUT_MS);
    Assert.assertTrue(clientConnected, "client connected");

    //hook in to key places in the server pipeline
    ChannelPipeline lastSrvConnPipeline = srvConn.getLastConnChannel().getPipeline();
    ExtendedReadTimeoutHandler srvTimeoutHandler =
        (ExtendedReadTimeoutHandler)lastSrvConnPipeline.get(
            ExtendedReadTimeoutHandler.class.getSimpleName());
    SimpleTestMessageReader srvMsgReader = (SimpleTestMessageReader)lastSrvConnPipeline.get(
        SimpleTestMessageReader.class.getSimpleName());
    ExceptionListenerTestHandler srvExceptionListener =
        (ExceptionListenerTestHandler)lastSrvConnPipeline.get(
            ExceptionListenerTestHandler.class.getSimpleName());

    //hook in to key places in the client pipeline
    ChannelPipeline clientPipeline = clientConn.getChannel().getPipeline();
    ExtendedReadTimeoutHandler clientTimeoutHandler =
        (ExtendedReadTimeoutHandler)clientPipeline.get(
            ExtendedReadTimeoutHandler.class.getSimpleName());
    SimpleTestMessageReader clientMsgReader = (SimpleTestMessageReader)clientPipeline.get(
        SimpleTestMessageReader.class.getSimpleName());
    ExceptionListenerTestHandler clientExceptionListener =
        (ExceptionListenerTestHandler)clientPipeline.get(
            ExceptionListenerTestHandler.class.getSimpleName());

    for (int i = 0; i < 50; ++i)
    {
      //send a request
      ChannelBuffer msg = ChannelBuffers.wrappedBuffer(("hello" + i).getBytes());
      clientConn.getChannel().write(msg);

      //wait for the request to propagate
      try {Thread.sleep(10);} catch (InterruptedException ie){};

      Assert.assertNull(srvExceptionListener.getLastException(), "no server read timeout: " + i);
      Assert.assertNull(clientExceptionListener.getLastException(), "no client read timeout: " + i);
      Assert.assertEquals(srvMsgReader.getMsg(), "hello" + i, "message read: " + i);

      //start server read timeout after the first message
      if (0 == i) srvTimeoutHandler.start(lastSrvConnPipeline.getContext(srvTimeoutHandler));
    }

    ChannelBuffer msg2 = ChannelBuffers.wrappedBuffer("eom".getBytes());
    clientConn.getChannel().write(msg2);

    //start the client timeout handler
    clientTimeoutHandler.start(clientPipeline.getContext(clientTimeoutHandler));

    Assert.assertNull(srvExceptionListener.getLastException(), "no server read timeout");
    Assert.assertNull(clientExceptionListener.getLastException(), "no client read timeout");
    Assert.assertEquals(srvMsgReader.getMsg(), "eom", "message read");

    //stop server read timeout
    srvTimeoutHandler.stop();

    for (int  i = 0; i < 100; ++i)
    {
      String responseString = "hi there " + i;
      ChannelBuffer resp = ChannelBuffers.wrappedBuffer(responseString.getBytes());
      lastSrvConnPipeline.getChannel().write(resp);

      //wait for the response to propagate
      try {Thread.sleep(10);} catch (InterruptedException ie){};

      Assert.assertNull(srvExceptionListener.getLastException(), "no server read timeout: " + i);
      Assert.assertNull(clientExceptionListener.getLastException(), "no client read timeout: " + i);
      Assert.assertEquals(clientMsgReader.getMsg(), responseString, "response read: " + i);
    }

    ChannelBuffer resp2 = ChannelBuffers.wrappedBuffer("eom".getBytes());
    lastSrvConnPipeline.getChannel().write(resp2);

    //wait for the response to propagate
    try {Thread.sleep(10);} catch (InterruptedException ie){};

    Assert.assertNull(srvExceptionListener.getLastException(), "no server read timeout");
    Assert.assertNull(clientExceptionListener.getLastException(), "no client read timeout");
    Assert.assertEquals(clientMsgReader.getMsg(), "eom", "response read");

    //stop client read timeout
    clientTimeoutHandler.stop();

    clientConn.stop();
    srvConn.stop();
  }

  @Test
  /**
   * Simulates the follow communication with no request timeout
   *
   * <table>
   * <th><td>client</td><td>server</td></th>
   * <tr><td>send "hello"</td><td></td></tr>
   * <tr><td></td><td>start read timeout</td></tr>
   * <tr><td>induce timeout</td><td></td></tr>
   * <tr><td>generate read timeout</td><td></td></tr>
   * <tr><td></td><td>disconnect client</td></tr>
   * <tr><td>send "eom"</td><td></td></tr>
   * <tr><td>detect it has been disconnected</td><td></td></tr>
   * </table>
   */
  public void testServerSimpleRequestTimeout()
  {
    SimpleTestServerConnection srvConn = new SimpleTestServerConnection(DbusEvent.byteOrder);
    srvConn.setPipelineFactory(new SimpleServerPipelineFactory());
    boolean serverStarted = srvConn.startSynchronously(3, CONNECT_TIMEOUT_MS);
    Assert.assertTrue(serverStarted, "server started");

    SimpleTestClientConnection clientConn = new SimpleTestClientConnection(DbusEvent.byteOrder);
    clientConn.setPipelineFactory(new SimpleClientPipelineFactory());
    boolean clientConnected = clientConn.startSynchronously(3, CONNECT_TIMEOUT_MS);
    Assert.assertTrue(clientConnected, "client connected");

    //hook in to key places in the server pipeline
    ChannelPipeline lastSrvConnPipeline = srvConn.getLastConnChannel().getPipeline();
    ExtendedReadTimeoutHandler srvTimeoutHandler =
        (ExtendedReadTimeoutHandler)lastSrvConnPipeline.get(
            ExtendedReadTimeoutHandler.class.getSimpleName());
    SimpleTestMessageReader srvMsgReader = (SimpleTestMessageReader)lastSrvConnPipeline.get(
        SimpleTestMessageReader.class.getSimpleName());
    ExceptionListenerTestHandler srvExceptionListener =
        (ExceptionListenerTestHandler)lastSrvConnPipeline.get(
            ExceptionListenerTestHandler.class.getSimpleName());

    //hook in to key places in the client pipeline
    ChannelPipeline clientPipeline = clientConn.getChannel().getPipeline();
    ExtendedReadTimeoutHandler clientTimeoutHandler =
        (ExtendedReadTimeoutHandler)clientPipeline.get(
            ExtendedReadTimeoutHandler.class.getSimpleName());
    ExceptionListenerTestHandler clientExceptionListener =
        (ExceptionListenerTestHandler)clientPipeline.get(
            ExceptionListenerTestHandler.class.getSimpleName());

    //send a request
    ChannelBuffer msg = ChannelBuffers.wrappedBuffer("hello".getBytes());
    ChannelFuture writeFuture = clientConn.getChannel().write(msg);

    //wait for the request to propagate
    try {writeFuture.await(10);} catch (InterruptedException ie){};
    Assert.assertTrue(writeFuture.isDone(), "write completed");
    Assert.assertTrue(writeFuture.isSuccess(), "write successful");

    Assert.assertNull(srvExceptionListener.getLastException(), "no server read timeout");
    Assert.assertNull(clientExceptionListener.getLastException(), "no client read timeout");
    Assert.assertEquals(srvMsgReader.getMsg(), "hello", "message read");

    //start server read timeout
    srvTimeoutHandler.start(lastSrvConnPipeline.getContext(srvTimeoutHandler));

    //Timeout
    try {Thread.sleep(300);} catch (InterruptedException ie){};

    ChannelBuffer msg2 = ChannelBuffers.wrappedBuffer("eom".getBytes());
    writeFuture = clientConn.getChannel().write(msg2);

    //wait for the respomse to propagate
    try {writeFuture.await(10);} catch (InterruptedException ie){};

    //start the client timeout handler
    clientTimeoutHandler.start(clientPipeline.getContext(clientTimeoutHandler));

    Assert.assertTrue(srvExceptionListener.getLastException() instanceof ReadTimeoutException,
                      "server read timeout");
    Assert.assertTrue(clientExceptionListener.getLastException() instanceof ClosedChannelException,
                      "failed write");
    Assert.assertTrue(! lastSrvConnPipeline.getChannel().isConnected(), "client has been disconnected");
    Assert.assertTrue(! clientPipeline.getChannel().isConnected(), "disconnected from server");

    //stop server read timeout
    srvTimeoutHandler.stop();

    //stop client read timeout
    clientTimeoutHandler.stop();

    clientConn.stop();
    srvConn.stop();
  }

  @Test
  /**
   * Simulates the follow communication with no request timeout
   *
   * <table>
   * <th><td>client</td><td>server</td></th>
   * <tr><td>send "hello"</td><td></td></tr>
   * <tr><td></td><td>start read timeout</td></tr>
   * <tr><td>generate read timeout</td><td></td></tr>
   * <tr><td></td><td>induce timeout</td></tr>
   * <tr><td>disconnect from server</td><td></td></tr>
   * <tr><td></td><td>send "hi there"</td></tr>
   * <tr><td></td><td>detect it has been disconnected</td></tr>
   * </table>
   */
  public void testServerSimpleResponseTimeout()
  {
    SimpleTestServerConnection srvConn = new SimpleTestServerConnection(DbusEvent.byteOrder);
    srvConn.setPipelineFactory(new SimpleServerPipelineFactory());
    boolean serverStarted = srvConn.startSynchronously(4, CONNECT_TIMEOUT_MS);
    Assert.assertTrue(serverStarted, "server started");

    SimpleTestClientConnection clientConn = new SimpleTestClientConnection(DbusEvent.byteOrder);
    clientConn.setPipelineFactory(new SimpleClientPipelineFactory());
    boolean clientConnected = clientConn.startSynchronously(4, CONNECT_TIMEOUT_MS);
    Assert.assertTrue(clientConnected, "client connected");

    //hook in to key places in the server pipeline
    ChannelPipeline lastSrvConnPipeline = srvConn.getLastConnChannel().getPipeline();
    ExtendedReadTimeoutHandler srvTimeoutHandler =
        (ExtendedReadTimeoutHandler)lastSrvConnPipeline.get(
            ExtendedReadTimeoutHandler.class.getSimpleName());
    SimpleTestMessageReader srvMsgReader = (SimpleTestMessageReader)lastSrvConnPipeline.get(
        SimpleTestMessageReader.class.getSimpleName());
    ExceptionListenerTestHandler srvExceptionListener =
        (ExceptionListenerTestHandler)lastSrvConnPipeline.get(
            ExceptionListenerTestHandler.class.getSimpleName());

    //hook in to key places in the client pipeline
    ChannelPipeline clientPipeline = clientConn.getChannel().getPipeline();
    ExtendedReadTimeoutHandler clientTimeoutHandler =
        (ExtendedReadTimeoutHandler)clientPipeline.get(
            ExtendedReadTimeoutHandler.class.getSimpleName());
    ExceptionListenerTestHandler clientExceptionListener =
        (ExceptionListenerTestHandler)clientPipeline.get(
            ExceptionListenerTestHandler.class.getSimpleName());

    //send a request
    ChannelBuffer msg = ChannelBuffers.wrappedBuffer("hello".getBytes());
    clientConn.getChannel().write(msg);

    //wait for the request to propagate
    try {Thread.sleep(10);} catch (InterruptedException ie){};

    Assert.assertNull(srvExceptionListener.getLastException(), "no server read timeout");
    Assert.assertNull(clientExceptionListener.getLastException(), "no client read timeout");
    Assert.assertEquals(srvMsgReader.getMsg(), "hello", "message read");

    //start server read timeout
    srvTimeoutHandler.start(lastSrvConnPipeline.getContext(srvTimeoutHandler));

    ChannelBuffer msg2 = ChannelBuffers.wrappedBuffer("eom".getBytes());
    clientConn.getChannel().write(msg2);

    //start the client timeout handler
    clientTimeoutHandler.start(clientPipeline.getContext(clientTimeoutHandler));

    Assert.assertNull(srvExceptionListener.getLastException(), "no server read timeout");
    Assert.assertNull(clientExceptionListener.getLastException(), "no client read timeout");
    Assert.assertEquals(srvMsgReader.getMsg(), "eom", "message read");

    //stop server read timeout
    srvTimeoutHandler.stop();

    ChannelBuffer resp = ChannelBuffers.wrappedBuffer("hi there".getBytes());

    //Induce timeout
    try {Thread.sleep(500);} catch (InterruptedException ie){};

    lastSrvConnPipeline.getChannel().write(resp);

    //wait for the response to propagate
    try {Thread.sleep(10);} catch (InterruptedException ie){};

    Assert.assertTrue(srvExceptionListener.getLastException() instanceof ClosedChannelException,
                      "no server read timeout but client has disconnected");
    Assert.assertTrue(clientExceptionListener.getLastException() instanceof ReadTimeoutException,
                      "client read timeout");
    Assert.assertTrue(! lastSrvConnPipeline.getChannel().isConnected(), "client has disconnected");
    Assert.assertTrue(! clientPipeline.getChannel().isConnected(), "closed connection to server");

    //stop client read timeout
    clientTimeoutHandler.stop();

    clientConn.stop();
    srvConn.stop();
  }
}


class SimpleServerPipelineFactory implements ChannelPipelineFactory
{

  @Override
  public ChannelPipeline getPipeline() throws Exception
  {
    ChannelPipeline newPipeline = Channels.pipeline();
    newPipeline.addLast(ExtendedReadTimeoutHandler.class.getSimpleName(),
                        new ExtendedReadTimeoutHandler("test", null,
                                                       100,
                                                       true));
    newPipeline.addLast(ExceptionListenerTestHandler.class.getSimpleName(),
                        new ExceptionListenerTestHandler());
    newPipeline.addLast(SimpleTestMessageReader.class.getSimpleName(),
                        new SimpleTestMessageReader());

    return newPipeline;
  }

}

class SimpleClientPipelineFactory implements ChannelPipelineFactory
{

  @Override
  public ChannelPipeline getPipeline() throws Exception
  {
    ChannelPipeline newPipeline = Channels.pipeline();
    newPipeline.addLast(ExtendedReadTimeoutHandler.class.getSimpleName(),
                        new ExtendedReadTimeoutHandler("test", null,
                                                       100,
                                                       true));
    newPipeline.addLast(ExceptionListenerTestHandler.class.getSimpleName(),
                        new ExceptionListenerTestHandler());
    newPipeline.addLast(SimpleTestMessageReader.class.getSimpleName(),
                        new SimpleTestMessageReader());

    return newPipeline;
  }

}
