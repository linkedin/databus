package com.linkedin.databus.client.netty;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.HeapChannelBufferFactory;
import org.jboss.netty.handler.codec.http.DefaultHttpChunk;
import org.jboss.netty.handler.codec.http.DefaultHttpChunkTrailer;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpChunkTrailer;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.testng.annotations.Test;

import com.linkedin.databus.client.ChunkedBodyReadableByteChannel;
import com.linkedin.databus.client.DatabusBootstrapConnection;
import com.linkedin.databus.client.DatabusBootstrapConnectionStateMessage;
import com.linkedin.databus.client.DatabusRelayConnectionStateMessage;
import com.linkedin.databus.client.netty.TestResponseProcessors.TestRemoteExceptionHandler.ExceptionType;
import com.linkedin.databus.client.pub.ServerInfo;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DbusClientMode;
import com.linkedin.databus.core.async.ActorMessageQueue;
import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus2.core.container.request.BootstrapDatabaseTooOldException;
import com.linkedin.databus2.core.container.request.DatabusRequest;
import com.linkedin.databus2.core.container.request.RegisterResponseEntry;

public class TestResponseProcessors
{
  @Test
  public void testStreamResponseProcessors()
  	throws Exception
  {
	  // happy Path - Chunked
	  {
		  TestAbstractQueue queue = new TestAbstractQueue();
		  TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
		  HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		  StreamHttpResponseProcessor  processor = new StreamHttpResponseProcessor(queue, stateMsg, null);
		  ChannelBuffer buf = HeapChannelBufferFactory.getInstance().getBuffer(100);
		  HttpChunk httpChunk = new DefaultHttpChunk(buf);
		  HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

		  processor.startResponse(httpResponse);
		  processor.addChunk(httpChunk);
		  processor.addTrailer(httpChunkTrailer);
		  processor.finishResponse();
		  processor.channelClosed();

		  Assert.assertEquals("Error Handled", false, processor._errorHandled);
		  Assert.assertEquals("Processor Response State", HttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
		  Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
		  Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
		  TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
		  Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.STREAM_RESPONSE_SUCCESS, gotMsg._state);
		  Assert.assertEquals("No More CHunks",true, stateMsg._channel.hasNoMoreChunks());
	  }

	  // happy Path - Non - Chunked
	  {
		  TestAbstractQueue queue = new TestAbstractQueue();
		  TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
		  HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		  StreamHttpResponseProcessor  processor = new StreamHttpResponseProcessor(queue, stateMsg, null);

		  processor.startResponse(httpResponse);
		  processor.finishResponse();
		  processor.channelClosed();


		  Assert.assertEquals("Error Handled", false, processor._errorHandled);
		  Assert.assertEquals("Processor Response State", HttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
		  Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
		  Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
		  TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
		  Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.STREAM_RESPONSE_SUCCESS, gotMsg._state);
		  Assert.assertEquals("No More CHunks",true, stateMsg._channel.hasNoMoreChunks());
	  }

	  // Exception at the start
	  {
		  TestAbstractQueue queue = new TestAbstractQueue();
		  TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
		  HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		  StreamHttpResponseProcessor  processor = new StreamHttpResponseProcessor(queue, stateMsg, null);
		  ChannelBuffer buf = HeapChannelBufferFactory.getInstance().getBuffer(100);
		  HttpChunk httpChunk = new DefaultHttpChunk(buf);
		  HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

		  processor.channelException(new Exception("dummy exception"));
		  processor.startResponse(httpResponse);
		  processor.addChunk(httpChunk);
		  processor.addTrailer(httpChunkTrailer);
		  processor.finishResponse();

		  Assert.assertEquals("Error Handled", true, processor._errorHandled);
		  Assert.assertEquals("Processor Response State", HttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
		  Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
		  Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
		  TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
		  Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.STREAM_RESPONSE_ERROR, gotMsg._state);
		  Assert.assertEquals("No More CHunks",true, (null == stateMsg._channel));
	  }

	  //Exception after Start - Case 1
	  {
		  TestAbstractQueue queue = new TestAbstractQueue();
		  TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
		  HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		  StreamHttpResponseProcessor  processor = new StreamHttpResponseProcessor(queue, stateMsg, null);
		  ChannelBuffer buf = HeapChannelBufferFactory.getInstance().getBuffer(100);
		  HttpChunk httpChunk = new DefaultHttpChunk(buf);
		  HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

		  processor.startResponse(httpResponse);
		  processor.channelException(new Exception("dummy exception"));
		  processor.addChunk(httpChunk);
		  processor.addTrailer(httpChunkTrailer);
		  processor.finishResponse();

		  Assert.assertEquals("Error Handled", true, processor._errorHandled);
		  Assert.assertEquals("Processor Response State", HttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
		  Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
		  Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
		  TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
		  Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.STREAM_RESPONSE_SUCCESS, gotMsg._state);
		  Assert.assertEquals("No More CHunks",true, stateMsg._channel.hasNoMoreChunks());
	  }

	  //Exception after Start - Case 2
	  {
		  TestAbstractQueue queue = new TestAbstractQueue();
		  TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
		  HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		  StreamHttpResponseProcessor  processor = new StreamHttpResponseProcessor(queue, stateMsg, null);
		  ChannelBuffer buf = HeapChannelBufferFactory.getInstance().getBuffer(100);
		  HttpChunk httpChunk = new DefaultHttpChunk(buf);
		  HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

		  processor.startResponse(httpResponse);
		  processor.addChunk(httpChunk);
		  processor.channelException(new Exception("dummy exception"));
		  processor.addTrailer(httpChunkTrailer);
		  processor.finishResponse();

		  Assert.assertEquals("Error Handled", true, processor._errorHandled);
		  Assert.assertEquals("Processor Response State", HttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
		  Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
		  Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
		  TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
		  Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.STREAM_RESPONSE_SUCCESS, gotMsg._state);
		  Assert.assertEquals("No More CHunks",true, stateMsg._channel.hasNoMoreChunks());
	  }

	  //Exception after Start - Case 3
	  {
		  TestAbstractQueue queue = new TestAbstractQueue();
		  TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
		  HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		  StreamHttpResponseProcessor  processor = new StreamHttpResponseProcessor(queue, stateMsg, null);
		  ChannelBuffer buf = HeapChannelBufferFactory.getInstance().getBuffer(100);
		  HttpChunk httpChunk = new DefaultHttpChunk(buf);
		  HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

		  processor.startResponse(httpResponse);
		  processor.addChunk(httpChunk);
		  processor.addTrailer(httpChunkTrailer);
		  processor.channelException(new Exception("dummy exception"));
		  processor.finishResponse();

		  Assert.assertEquals("Error Handled", true, processor._errorHandled);
		  Assert.assertEquals("Processor Response State", HttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
		  Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
		  Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
		  TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
		  Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.STREAM_RESPONSE_SUCCESS, gotMsg._state);
		  Assert.assertEquals("No More CHunks",true, stateMsg._channel.hasNoMoreChunks());
	  }

	  //Exception after finishResponse
	  {
		  TestAbstractQueue queue = new TestAbstractQueue();
		  TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
		  HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		  StreamHttpResponseProcessor  processor = new StreamHttpResponseProcessor(queue, stateMsg, null);
		  ChannelBuffer buf = HeapChannelBufferFactory.getInstance().getBuffer(100);
		  HttpChunk httpChunk = new DefaultHttpChunk(buf);
		  HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

		  processor.startResponse(httpResponse);
		  processor.addChunk(httpChunk);
		  processor.addTrailer(httpChunkTrailer);
		  processor.finishResponse();
		  processor.channelException(new Exception("dummy exception"));

		  Assert.assertEquals("Error Handled", true, processor._errorHandled);
		  Assert.assertEquals("Processor Response State", HttpResponseProcessorDecorator.ResponseStatus.CHUNKS_EXCEPTION,processor._responseStatus);
		  Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
		  Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
		  TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
		  Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.STREAM_RESPONSE_SUCCESS, gotMsg._state);
		  Assert.assertEquals("No More CHunks",true, stateMsg._channel.hasNoMoreChunks());
	  }

	  // Both Exception and ChannelClose getting triggered
	  {
		  TestAbstractQueue queue = new TestAbstractQueue();
		  TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
		  StreamHttpResponseProcessor  processor = new StreamHttpResponseProcessor(queue, stateMsg, null);

		  processor.channelException(new Exception("dummy exception"));
		  boolean isException = false;
		  try
		  {
			  processor.channelClosed();
		  } catch (Exception ex){
			  isException = true;
		  }
		  Assert.assertEquals("Is Exception", true,isException);
		  Assert.assertEquals("Error Handled", true, processor._errorHandled);
		  Assert.assertEquals("Processor Response State", HttpResponseProcessorDecorator.ResponseStatus.CHUNKS_EXCEPTION,processor._responseStatus);
		  Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
		  Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
		  TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
		  Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.STREAM_RESPONSE_ERROR, gotMsg._state);
		  Assert.assertEquals("No More CHunks",true, null == stateMsg._channel);
	  }
  }


  @Test
  public void testRegisterResponseProcessors()
  	throws Exception
  {
	  // happy Path - Chunked
	  {
		  TestAbstractQueue queue = new TestAbstractQueue();
		  TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
		  DefaultHttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		  RegisterHttpResponseProcessor  processor = new RegisterHttpResponseProcessor(queue, stateMsg, null);
		  ChannelBuffer buf = getRegisterResponse();
		  HttpChunk httpChunk = new DefaultHttpChunk(buf);
		  HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

		  processor.startResponse(httpResponse);
		  processor.addChunk(httpChunk);
		  processor.addTrailer(httpChunkTrailer);
		  processor.finishResponse();
		  processor.channelClosed();

		  Assert.assertEquals("Error Handled", false, processor._errorHandled);
		  Assert.assertEquals("Processor Response State", HttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
		  Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
		  Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
		  TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
		  Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.REGISTER_SUCCESS, gotMsg._state);
		  Assert.assertEquals("Register Response Id Check",true, stateMsg._registerResponse.containsKey(new Long(202)));
		  Assert.assertEquals("Register Response Id Check",1, stateMsg._registerResponse.size());

	  }

	  // happy Path - Non - Chunked
	  {
		  TestAbstractQueue queue = new TestAbstractQueue();
		  TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
		  DefaultHttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		  RegisterHttpResponseProcessor  processor = new RegisterHttpResponseProcessor(queue, stateMsg, null);
		  ChannelBuffer buf = getRegisterResponse();
		  httpResponse.setContent(buf);
		  httpResponse.setHeader(HttpHeaders.Names.CONTENT_LENGTH,"1000");
		  processor.startResponse(httpResponse);
		  processor.finishResponse();
		  processor.channelClosed();


		  Assert.assertEquals("Error Handled", false, processor._errorHandled);
		  Assert.assertEquals("Processor Response State", HttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
		  Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
		  Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
		  TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
		  System.out.println("Long Max is :" + Long.MAX_VALUE);
		  Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.REGISTER_SUCCESS, gotMsg._state);
		  Assert.assertEquals("Register Response Id Check",1, stateMsg._registerResponse.size());
	  }

	  // Exception at the start
	  {
		  TestAbstractQueue queue = new TestAbstractQueue();
		  TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
		  HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		  RegisterHttpResponseProcessor  processor = new RegisterHttpResponseProcessor(queue, stateMsg, null);
		  ChannelBuffer buf = getRegisterResponse();
		  HttpChunk httpChunk = new DefaultHttpChunk(buf);
		  HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

		  processor.channelException(new Exception("dummy exception"));
		  processor.startResponse(httpResponse);
		  processor.addChunk(httpChunk);
		  processor.addTrailer(httpChunkTrailer);
		  processor.finishResponse();

		  Assert.assertEquals("Error Handled", true, processor._errorHandled);
		  Assert.assertEquals("Processor Response State", HttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
		  Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
		  Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
		  TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
		  Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.REGISTER_RESPONSE_ERROR, gotMsg._state);
		  Assert.assertEquals("No More CHunks",true, (null == stateMsg._registerResponse));
	  }

	  //Exception after Start - Case 1
	  {
		  TestAbstractQueue queue = new TestAbstractQueue();
		  TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
		  HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		  RegisterHttpResponseProcessor  processor = new RegisterHttpResponseProcessor(queue, stateMsg, null);
		  ChannelBuffer buf = getRegisterResponse();
		  HttpChunk httpChunk = new DefaultHttpChunk(buf);
		  HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

		  processor.startResponse(httpResponse);
		  processor.channelException(new Exception("dummy exception"));
		  processor.addChunk(httpChunk);
		  processor.addTrailer(httpChunkTrailer);
		  processor.finishResponse();

		  Assert.assertEquals("Error Handled", true, processor._errorHandled);
		  Assert.assertEquals("Processor Response State", HttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
		  Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
		  Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
		  TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
		  Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.REGISTER_RESPONSE_ERROR, gotMsg._state);
		  Assert.assertEquals("No More CHunks",true, (null == stateMsg._registerResponse));
	  }

	  //Exception after Start - Case 2
	  {
		  TestAbstractQueue queue = new TestAbstractQueue();
		  TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
		  HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		  RegisterHttpResponseProcessor  processor = new RegisterHttpResponseProcessor(queue, stateMsg, null);
		  ChannelBuffer buf = getRegisterResponse();
		  HttpChunk httpChunk = new DefaultHttpChunk(buf);
		  HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

		  processor.startResponse(httpResponse);
		  processor.addChunk(httpChunk);
		  processor.channelException(new Exception("dummy exception"));
		  processor.addTrailer(httpChunkTrailer);
		  processor.finishResponse();

		  Assert.assertEquals("Error Handled", true, processor._errorHandled);
		  Assert.assertEquals("Processor Response State", HttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
		  Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
		  Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
		  TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
		  Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.REGISTER_RESPONSE_ERROR, gotMsg._state);
		  Assert.assertEquals("No More CHunks",true, (null == stateMsg._registerResponse));
	  }

	  //Exception after Start - Case 3
	  {
		  TestAbstractQueue queue = new TestAbstractQueue();
		  TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
		  HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		  RegisterHttpResponseProcessor  processor = new RegisterHttpResponseProcessor(queue, stateMsg, null);
		  ChannelBuffer buf = getRegisterResponse();
		  HttpChunk httpChunk = new DefaultHttpChunk(buf);
		  HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

		  processor.startResponse(httpResponse);
		  processor.addChunk(httpChunk);
		  processor.addTrailer(httpChunkTrailer);
		  processor.channelException(new Exception("dummy exception"));
		  processor.finishResponse();

		  Assert.assertEquals("Error Handled", true, processor._errorHandled);
		  Assert.assertEquals("Processor Response State", HttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
		  Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
		  Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
		  TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
		  Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.REGISTER_RESPONSE_ERROR, gotMsg._state);
		  Assert.assertEquals("No More CHunks",true, (null == stateMsg._registerResponse));
	  }

	  //Exception after finishResponse
	  {
		  TestAbstractQueue queue = new TestAbstractQueue();
		  TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
		  HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		  RegisterHttpResponseProcessor  processor = new RegisterHttpResponseProcessor(queue, stateMsg, null);
		  ChannelBuffer buf = getRegisterResponse();
		  HttpChunk httpChunk = new DefaultHttpChunk(buf);
		  HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

		  processor.startResponse(httpResponse);
		  processor.addChunk(httpChunk);
		  processor.addTrailer(httpChunkTrailer);
		  processor.finishResponse();
		  processor.channelException(new Exception("dummy exception"));

		  Assert.assertEquals("Error Handled", true, processor._errorHandled);
		  Assert.assertEquals("Processor Response State", HttpResponseProcessorDecorator.ResponseStatus.CHUNKS_EXCEPTION,processor._responseStatus);
		  Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
		  Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
		  TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
		  Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.REGISTER_SUCCESS, gotMsg._state);
		  Assert.assertEquals("Register Response Id Check",true, stateMsg._registerResponse.containsKey(new Long(202)));
		  Assert.assertEquals("Register Response Id Check",1, stateMsg._registerResponse.size());
	  }

	  // Both Exception and ChannelClose getting triggered
	  {
		  TestAbstractQueue queue = new TestAbstractQueue();
		  TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
		  RegisterHttpResponseProcessor  processor = new RegisterHttpResponseProcessor(queue, stateMsg, null);

		  processor.channelException(new Exception("dummy exception"));
		  boolean isException = false;
		  try
		  {
			  processor.channelClosed();
		  } catch (Exception ex){
			  isException = true;
		  }
		  Assert.assertEquals("Is Exception", true,isException);
		  Assert.assertEquals("Error Handled", true, processor._errorHandled);
		  Assert.assertEquals("Processor Response State", HttpResponseProcessorDecorator.ResponseStatus.CHUNKS_EXCEPTION,processor._responseStatus);
		  Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
		  Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
		  TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
		  Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.REGISTER_RESPONSE_ERROR, gotMsg._state);
		  Assert.assertEquals("No More CHunks",true, null == stateMsg._registerResponse);
	  }
  }

  @Test
  public void testSourceResponseProcessors()
  	throws Exception
  {
	  // happy Path - Chunked
	  {
		  TestAbstractQueue queue = new TestAbstractQueue();
		  TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
		  DefaultHttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		  SourcesHttpResponseProcessor<DatabusRelayConnectionStateMessage>  processor = new SourcesHttpResponseProcessor<DatabusRelayConnectionStateMessage>(queue, stateMsg, null);
		  ChannelBuffer buf = getSourcesResponse();
		  HttpChunk httpChunk = new DefaultHttpChunk(buf);
		  HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

		  processor.startResponse(httpResponse);
		  processor.addChunk(httpChunk);
		  processor.addTrailer(httpChunkTrailer);
		  processor.finishResponse();
		  processor.channelClosed();

		  Assert.assertEquals("Error Handled", false, processor._errorHandled);
		  Assert.assertEquals("Processor Response State", HttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
		  Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
		  Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
		  TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
		  Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.SOURCES_SUCCESS, gotMsg._state);
		  Assert.assertEquals("Register Response Id Check",1, stateMsg._sourcesResponse.size());
		  Assert.assertEquals("Register Response Id Check",new Long(301), stateMsg._sourcesResponse.get(0).getId());


	  }

	  // happy Path - Non - Chunked
	  {
		  TestAbstractQueue queue = new TestAbstractQueue();
		  TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
		  DefaultHttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		  SourcesHttpResponseProcessor<DatabusRelayConnectionStateMessage>  processor = new SourcesHttpResponseProcessor<DatabusRelayConnectionStateMessage>(queue, stateMsg, null);
		  ChannelBuffer buf = getSourcesResponse();
		  httpResponse.setContent(buf);
		  httpResponse.setHeader(HttpHeaders.Names.CONTENT_LENGTH,"1000");
		  processor.startResponse(httpResponse);
		  processor.finishResponse();
		  processor.channelClosed();


		  Assert.assertEquals("Error Handled", false, processor._errorHandled);
		  Assert.assertEquals("Processor Response State", HttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
		  Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
		  Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
		  TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
		  System.out.println("Long Max is :" + Long.MAX_VALUE);
		  Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.SOURCES_SUCCESS, gotMsg._state);
		  Assert.assertEquals("Register Response Id Check",1, stateMsg._sourcesResponse.size());
		  Assert.assertEquals("Register Response Id Check",new Long(301), stateMsg._sourcesResponse.get(0).getId());
	  }

	  // Exception at the start
	  {
		  TestAbstractQueue queue = new TestAbstractQueue();
		  TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
		  HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		  SourcesHttpResponseProcessor<DatabusRelayConnectionStateMessage>  processor = new SourcesHttpResponseProcessor<DatabusRelayConnectionStateMessage>(queue, stateMsg, null);
		  ChannelBuffer buf = getSourcesResponse();
		  HttpChunk httpChunk = new DefaultHttpChunk(buf);
		  HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

		  processor.channelException(new Exception("dummy exception"));
		  processor.startResponse(httpResponse);
		  processor.addChunk(httpChunk);
		  processor.addTrailer(httpChunkTrailer);
		  processor.finishResponse();

		  Assert.assertEquals("Error Handled", true, processor._errorHandled);
		  Assert.assertEquals("Processor Response State", HttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
		  Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
		  Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
		  TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
		  Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.SOURCES_RESPONSE_ERROR, gotMsg._state);
		  Assert.assertEquals("No More CHunks",true, (null == stateMsg._sourcesResponse));
	  }

	  //Exception after Start - Case 1
	  {
		  TestAbstractQueue queue = new TestAbstractQueue();
		  TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
		  HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		  SourcesHttpResponseProcessor<DatabusRelayConnectionStateMessage>  processor = new SourcesHttpResponseProcessor<DatabusRelayConnectionStateMessage>(queue, stateMsg, null);
		  ChannelBuffer buf = getSourcesResponse();
		  HttpChunk httpChunk = new DefaultHttpChunk(buf);
		  HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

		  processor.startResponse(httpResponse);
		  processor.channelException(new Exception("dummy exception"));
		  processor.addChunk(httpChunk);
		  processor.addTrailer(httpChunkTrailer);
		  processor.finishResponse();

		  Assert.assertEquals("Error Handled", true, processor._errorHandled);
		  Assert.assertEquals("Processor Response State", HttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
		  Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
		  Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
		  TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
		  Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.SOURCES_RESPONSE_ERROR, gotMsg._state);
		  Assert.assertEquals("No More CHunks",true, (null == stateMsg._sourcesResponse));
	  }

	  //Exception after Start - Case 2
	  {
		  TestAbstractQueue queue = new TestAbstractQueue();
		  TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
		  HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		  SourcesHttpResponseProcessor<DatabusRelayConnectionStateMessage>  processor = new SourcesHttpResponseProcessor<DatabusRelayConnectionStateMessage>(queue, stateMsg, null);
		  ChannelBuffer buf = getSourcesResponse();
		  HttpChunk httpChunk = new DefaultHttpChunk(buf);
		  HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

		  processor.startResponse(httpResponse);
		  processor.addChunk(httpChunk);
		  processor.channelException(new Exception("dummy exception"));
		  processor.addTrailer(httpChunkTrailer);
		  processor.finishResponse();

		  Assert.assertEquals("Error Handled", true, processor._errorHandled);
		  Assert.assertEquals("Processor Response State", HttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
		  Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
		  Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
		  TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
		  Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.SOURCES_RESPONSE_ERROR, gotMsg._state);
		  Assert.assertEquals("No More CHunks",true, (null == stateMsg._sourcesResponse));
	  }

	  //Exception after Start - Case 3
	  {
		  TestAbstractQueue queue = new TestAbstractQueue();
		  TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
		  HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		  SourcesHttpResponseProcessor<DatabusRelayConnectionStateMessage>  processor = new SourcesHttpResponseProcessor<DatabusRelayConnectionStateMessage>(queue, stateMsg, null);
		  ChannelBuffer buf = getSourcesResponse();
		  HttpChunk httpChunk = new DefaultHttpChunk(buf);
		  HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

		  processor.startResponse(httpResponse);
		  processor.addChunk(httpChunk);
		  processor.addTrailer(httpChunkTrailer);
		  processor.channelException(new Exception("dummy exception"));
		  processor.finishResponse();

		  Assert.assertEquals("Error Handled", true, processor._errorHandled);
		  Assert.assertEquals("Processor Response State", HttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
		  Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
		  Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
		  TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
		  Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.SOURCES_RESPONSE_ERROR, gotMsg._state);
		  Assert.assertEquals("No More CHunks",true, (null == stateMsg._sourcesResponse));
	  }

	  //Exception after finishResponse
	  {
		  TestAbstractQueue queue = new TestAbstractQueue();
		  TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
		  HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		  SourcesHttpResponseProcessor<DatabusRelayConnectionStateMessage>  processor = new SourcesHttpResponseProcessor<DatabusRelayConnectionStateMessage>(queue, stateMsg, null);
		  ChannelBuffer buf = getSourcesResponse();
		  HttpChunk httpChunk = new DefaultHttpChunk(buf);
		  HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

		  processor.startResponse(httpResponse);
		  processor.addChunk(httpChunk);
		  processor.addTrailer(httpChunkTrailer);
		  processor.finishResponse();
		  processor.channelException(new Exception("dummy exception"));

		  Assert.assertEquals("Error Handled", true, processor._errorHandled);
		  Assert.assertEquals("Processor Response State", HttpResponseProcessorDecorator.ResponseStatus.CHUNKS_EXCEPTION,processor._responseStatus);
		  Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
		  Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
		  TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
		  Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.SOURCES_SUCCESS, gotMsg._state);
		  Assert.assertEquals("Register Response Id Check",1, stateMsg._sourcesResponse.size());
		  Assert.assertEquals("Register Response Id Check",new Long(301), stateMsg._sourcesResponse.get(0).getId());
	  }

	  // Both Exception and ChannelClose getting triggered
	  {
		  TestAbstractQueue queue = new TestAbstractQueue();
		  TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
		  SourcesHttpResponseProcessor<DatabusRelayConnectionStateMessage>  processor = new SourcesHttpResponseProcessor<DatabusRelayConnectionStateMessage>(queue, stateMsg, null);

		  processor.channelException(new Exception("dummy exception"));
		  boolean isException = false;
		  try
		  {
			  processor.channelClosed();
		  } catch (Exception ex){
			  isException = true;
		  }
		  Assert.assertEquals("Is Exception", true,isException);
		  Assert.assertEquals("Error Handled", true, processor._errorHandled);
		  Assert.assertEquals("Processor Response State", HttpResponseProcessorDecorator.ResponseStatus.CHUNKS_EXCEPTION,processor._responseStatus);
		  Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
		  Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
		  TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
		  Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.SOURCES_RESPONSE_ERROR, gotMsg._state);
		  Assert.assertEquals("No More CHunks",true, null == stateMsg._sourcesResponse);
	  }
  }


  @Test
  public void testStartSCNResponseProcessors()
  	throws Exception
  {
	  // happy Path - Chunked
	  {
		  TestAbstractQueue queue = new TestAbstractQueue();
		  TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
		  DefaultHttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		  TestRemoteExceptionHandler remoteExHandler = new TestRemoteExceptionHandler();
		  Checkpoint cp = new Checkpoint();
		  cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
		  BootstrapStartScnHttpResponseProcessor  processor = new BootstrapStartScnHttpResponseProcessor(queue, stateMsg, cp, remoteExHandler, null);
		  ChannelBuffer buf = getScnResponse();
		  HttpChunk httpChunk = new DefaultHttpChunk(buf);
		  HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

		  processor.startResponse(httpResponse);
		  processor.addChunk(httpChunk);
		  processor.addTrailer(httpChunkTrailer);
		  processor.finishResponse();
		  processor.channelClosed();

		  Assert.assertEquals("Error Handled", false, processor._errorHandled);
		  Assert.assertEquals("Processor Response State", HttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
		  Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
		  Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
		  TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
		  Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.STARTSCN_RESPONSE_SUCCESS, gotMsg._state);
		  Assert.assertEquals("StartSCN Response Id Check",new Long(5678912),cp.getBootstrapStartScn());

	  }

	  // happy Path - Non - Chunked
	  {
		  TestAbstractQueue queue = new TestAbstractQueue();
		  TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
		  DefaultHttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		  TestRemoteExceptionHandler remoteExHandler = new TestRemoteExceptionHandler();
		  Checkpoint cp = new Checkpoint();
		  cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
		  BootstrapStartScnHttpResponseProcessor  processor = new BootstrapStartScnHttpResponseProcessor(queue, stateMsg, cp, remoteExHandler, null);
		  ChannelBuffer buf = getScnResponse();
		  httpResponse.setContent(buf);
		  httpResponse.setHeader(HttpHeaders.Names.CONTENT_LENGTH,"1000");
		  processor.startResponse(httpResponse);
		  processor.finishResponse();
		  processor.channelClosed();


		  Assert.assertEquals("Error Handled", false, processor._errorHandled);
		  Assert.assertEquals("Processor Response State", HttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
		  Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
		  Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
		  TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
		  System.out.println("Long Max is :" + Long.MAX_VALUE);
		  Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.STARTSCN_RESPONSE_SUCCESS, gotMsg._state);
		  Assert.assertEquals("StartSCN Response Id Check",new Long(5678912),cp.getBootstrapStartScn());
	  }


	  // Bootstrap Too Old Exception in Response
	  {
		  TestAbstractQueue queue = new TestAbstractQueue();
		  TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
		  DefaultHttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		  TestRemoteExceptionHandler remoteExHandler = new TestRemoteExceptionHandler();
		  remoteExHandler._exType= ExceptionType.BOOTSTRAP_TOO_OLD_EXCEPTION;
		  Checkpoint cp = new Checkpoint();
		  cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
		  BootstrapStartScnHttpResponseProcessor  processor = new BootstrapStartScnHttpResponseProcessor(queue, stateMsg, cp, remoteExHandler, null);
		  ChannelBuffer buf = getScnResponse();
		  HttpChunk httpChunk = new DefaultHttpChunk(buf);
		  HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

		  processor.startResponse(httpResponse);
		  processor.addChunk(httpChunk);
		  processor.addTrailer(httpChunkTrailer);
		  processor.finishResponse();
		  processor.channelClosed();

		  Assert.assertEquals("Error Handled", false, processor._errorHandled);
		  Assert.assertEquals("Processor Response State", HttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
		  Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
		  Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
		  TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
		  Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.STARTSCN_RESPONSE_ERROR, gotMsg._state);
		  Assert.assertEquals("No response",true, (null == stateMsg._cp));
	  }

	  // Other Exception in Response
	  {
		  TestAbstractQueue queue = new TestAbstractQueue();
		  TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
		  DefaultHttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		  httpResponse.setHeader(DatabusRequest.DATABUS_ERROR_CAUSE_CLASS_HEADER, "Other Dummy Error");
		  TestRemoteExceptionHandler remoteExHandler = new TestRemoteExceptionHandler();
		  remoteExHandler._exType= ExceptionType.OTHER_EXCEPTION;
		  Checkpoint cp = new Checkpoint();
		  cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
		  BootstrapStartScnHttpResponseProcessor  processor = new BootstrapStartScnHttpResponseProcessor(queue, stateMsg, cp, remoteExHandler, null);
		  ChannelBuffer buf = getScnResponse();
		  HttpChunk httpChunk = new DefaultHttpChunk(buf);
		  HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

		  processor.startResponse(httpResponse);
		  processor.addChunk(httpChunk);
		  processor.addTrailer(httpChunkTrailer);
		  processor.finishResponse();
		  processor.channelClosed();

		  Assert.assertEquals("Error Handled", false, processor._errorHandled);
		  Assert.assertEquals("Processor Response State", HttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
		  Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
		  Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
		  TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
		  Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.STARTSCN_RESPONSE_ERROR, gotMsg._state);
		  Assert.assertEquals("No response",true, (null == stateMsg._cp));
	  }

	  // Exception at the start
	  {
		  TestAbstractQueue queue = new TestAbstractQueue();
		  TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
		  HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		  TestRemoteExceptionHandler remoteExHandler = new TestRemoteExceptionHandler();
		  Checkpoint cp = new Checkpoint();
		  cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
		  BootstrapStartScnHttpResponseProcessor  processor = new BootstrapStartScnHttpResponseProcessor(queue, stateMsg, cp, remoteExHandler, null);
		  ChannelBuffer buf = getScnResponse();
		  HttpChunk httpChunk = new DefaultHttpChunk(buf);
		  HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

		  processor.channelException(new Exception("dummy exception"));
		  processor.startResponse(httpResponse);
		  processor.addChunk(httpChunk);
		  processor.addTrailer(httpChunkTrailer);
		  processor.finishResponse();

		  Assert.assertEquals("Error Handled", true, processor._errorHandled);
		  Assert.assertEquals("Processor Response State", HttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
		  Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
		  Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
		  TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
		  Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.STARTSCN_RESPONSE_ERROR, gotMsg._state);
		  Assert.assertEquals("No response",true, (null == stateMsg._cp));
	  }

	  //Exception after Start - Case 1
	  {
		  TestAbstractQueue queue = new TestAbstractQueue();
		  TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
		  HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		  TestRemoteExceptionHandler remoteExHandler = new TestRemoteExceptionHandler();
		  Checkpoint cp = new Checkpoint();
		  cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
		  BootstrapStartScnHttpResponseProcessor  processor = new BootstrapStartScnHttpResponseProcessor(queue, stateMsg, cp, remoteExHandler, null);
		  ChannelBuffer buf = getScnResponse();
		  HttpChunk httpChunk = new DefaultHttpChunk(buf);
		  HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

		  processor.startResponse(httpResponse);
		  processor.channelException(new Exception("dummy exception"));
		  processor.addChunk(httpChunk);
		  processor.addTrailer(httpChunkTrailer);
		  processor.finishResponse();

		  Assert.assertEquals("Error Handled", true, processor._errorHandled);
		  Assert.assertEquals("Processor Response State", HttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
		  Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
		  Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
		  TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
		  Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.STARTSCN_RESPONSE_ERROR, gotMsg._state);
		  Assert.assertEquals("No response",true, (null == stateMsg._cp));
	  }

	  //Exception after Start - Case 2
	  {
		  TestAbstractQueue queue = new TestAbstractQueue();
		  TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
		  HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		  TestRemoteExceptionHandler remoteExHandler = new TestRemoteExceptionHandler();
		  Checkpoint cp = new Checkpoint();
		  cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
		  BootstrapStartScnHttpResponseProcessor  processor = new BootstrapStartScnHttpResponseProcessor(queue, stateMsg, cp, remoteExHandler, null);
		  ChannelBuffer buf = getScnResponse();
		  HttpChunk httpChunk = new DefaultHttpChunk(buf);
		  HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

		  processor.startResponse(httpResponse);
		  processor.addChunk(httpChunk);
		  processor.channelException(new Exception("dummy exception"));
		  processor.addTrailer(httpChunkTrailer);
		  processor.finishResponse();

		  Assert.assertEquals("Error Handled", true, processor._errorHandled);
		  Assert.assertEquals("Processor Response State", HttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
		  Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
		  Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
		  TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
		  Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.STARTSCN_RESPONSE_ERROR, gotMsg._state);
		  Assert.assertEquals("No response",true, (null == stateMsg._cp));
	  }

	  //Exception after Start - Case 3
	  {
		  TestAbstractQueue queue = new TestAbstractQueue();
		  TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
		  HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		  TestRemoteExceptionHandler remoteExHandler = new TestRemoteExceptionHandler();
		  Checkpoint cp = new Checkpoint();
		  cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
		  BootstrapStartScnHttpResponseProcessor  processor = new BootstrapStartScnHttpResponseProcessor(queue, stateMsg, cp, remoteExHandler, null);
		  ChannelBuffer buf = getScnResponse();
		  HttpChunk httpChunk = new DefaultHttpChunk(buf);
		  HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

		  processor.startResponse(httpResponse);
		  processor.addChunk(httpChunk);
		  processor.addTrailer(httpChunkTrailer);
		  processor.channelException(new Exception("dummy exception"));
		  processor.finishResponse();

		  Assert.assertEquals("Error Handled", true, processor._errorHandled);
		  Assert.assertEquals("Processor Response State", HttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
		  Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
		  Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
		  TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
		  Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.STARTSCN_RESPONSE_ERROR, gotMsg._state);
		  Assert.assertEquals("No response",true, (null == stateMsg._cp));
	  }

	  //Exception after finishResponse
	  {
		  TestAbstractQueue queue = new TestAbstractQueue();
		  TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
		  HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		  TestRemoteExceptionHandler remoteExHandler = new TestRemoteExceptionHandler();
		  Checkpoint cp = new Checkpoint();
		  cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
		  BootstrapStartScnHttpResponseProcessor  processor = new BootstrapStartScnHttpResponseProcessor(queue, stateMsg, cp, remoteExHandler, null);
		  ChannelBuffer buf = getScnResponse();
		  HttpChunk httpChunk = new DefaultHttpChunk(buf);
		  HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

		  processor.startResponse(httpResponse);
		  processor.addChunk(httpChunk);
		  processor.addTrailer(httpChunkTrailer);
		  processor.finishResponse();
		  processor.channelException(new Exception("dummy exception"));

		  Assert.assertEquals("Error Handled", true, processor._errorHandled);
		  Assert.assertEquals("Processor Response State", HttpResponseProcessorDecorator.ResponseStatus.CHUNKS_EXCEPTION,processor._responseStatus);
		  Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
		  Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
		  TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
		  Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.STARTSCN_RESPONSE_SUCCESS, gotMsg._state);
		  Assert.assertEquals("StartSCN Response Id Check",new Long(5678912),cp.getBootstrapStartScn());
	  }

	  // Both Exception and ChannelClose getting triggered
	  {
		  TestAbstractQueue queue = new TestAbstractQueue();
		  TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
		  TestRemoteExceptionHandler remoteExHandler = new TestRemoteExceptionHandler();
		  Checkpoint cp = new Checkpoint();
		  cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
		  BootstrapStartScnHttpResponseProcessor  processor = new BootstrapStartScnHttpResponseProcessor(queue, stateMsg, cp, remoteExHandler, null);
		  processor.channelException(new Exception("dummy exception"));
		  boolean isException = false;
		  try
		  {
			  processor.channelClosed();
		  } catch (Exception ex){
			  isException = true;
		  }
		  Assert.assertEquals("Is Exception", true,isException);
		  Assert.assertEquals("Error Handled", true, processor._errorHandled);
		  Assert.assertEquals("Processor Response State", HttpResponseProcessorDecorator.ResponseStatus.CHUNKS_EXCEPTION,processor._responseStatus);
		  Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
		  Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
		  TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
		  Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.STARTSCN_RESPONSE_ERROR, gotMsg._state);
		  Assert.assertEquals("No response",true, (null == stateMsg._cp));
	  }
  }

  @Test
  public void testTargetSCNResponseProcessors()
  	throws Exception
  {
	  // happy Path - Chunked
	  {
		  TestAbstractQueue queue = new TestAbstractQueue();
		  TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
		  DefaultHttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		  TestRemoteExceptionHandler remoteExHandler = new TestRemoteExceptionHandler();
		  Checkpoint cp = new Checkpoint();
		  cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_CATCHUP);
		  BootstrapTargetScnHttpResponseProcessor  processor = new BootstrapTargetScnHttpResponseProcessor(queue, stateMsg, cp, remoteExHandler, null);
		  ChannelBuffer buf = getScnResponse();
		  HttpChunk httpChunk = new DefaultHttpChunk(buf);
		  HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

		  processor.startResponse(httpResponse);
		  processor.addChunk(httpChunk);
		  processor.addTrailer(httpChunkTrailer);
		  processor.finishResponse();
		  processor.channelClosed();

		  Assert.assertEquals("Error Handled", false, processor._errorHandled);
		  Assert.assertEquals("Processor Response State", HttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
		  Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
		  Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
		  TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
		  Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.TARGETSCN_RESPONSE_SUCCESS, gotMsg._state);
		  Assert.assertEquals("StartSCN Response Id Check",new Long(5678912),cp.getBootstrapTargetScn());
	  }

	  // happy Path - Non - Chunked
	  {
		  TestAbstractQueue queue = new TestAbstractQueue();
		  TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
		  DefaultHttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		  TestRemoteExceptionHandler remoteExHandler = new TestRemoteExceptionHandler();
		  Checkpoint cp = new Checkpoint();
		  cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_CATCHUP);
		  BootstrapTargetScnHttpResponseProcessor  processor = new BootstrapTargetScnHttpResponseProcessor(queue, stateMsg, cp, remoteExHandler, null);
		  ChannelBuffer buf = getScnResponse();
		  httpResponse.setContent(buf);
		  httpResponse.setHeader(HttpHeaders.Names.CONTENT_LENGTH,"1000");
		  processor.startResponse(httpResponse);
		  processor.finishResponse();
		  processor.channelClosed();


		  Assert.assertEquals("Error Handled", false, processor._errorHandled);
		  Assert.assertEquals("Processor Response State", HttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
		  Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
		  Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
		  TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
		  System.out.println("Long Max is :" + Long.MAX_VALUE);
		  Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.TARGETSCN_RESPONSE_SUCCESS, gotMsg._state);
		  Assert.assertEquals("StartSCN Response Id Check",new Long(5678912),cp.getBootstrapTargetScn());
	  }


	  // Bootstrap Too Old Exception in Response
	  {
		  TestAbstractQueue queue = new TestAbstractQueue();
		  TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
		  DefaultHttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		  TestRemoteExceptionHandler remoteExHandler = new TestRemoteExceptionHandler();
		  remoteExHandler._exType= ExceptionType.BOOTSTRAP_TOO_OLD_EXCEPTION;
		  Checkpoint cp = new Checkpoint();
		  cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_CATCHUP);
		  BootstrapTargetScnHttpResponseProcessor  processor = new BootstrapTargetScnHttpResponseProcessor(queue, stateMsg, cp, remoteExHandler, null);
		  ChannelBuffer buf = getScnResponse();
		  HttpChunk httpChunk = new DefaultHttpChunk(buf);
		  HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

		  processor.startResponse(httpResponse);
		  processor.addChunk(httpChunk);
		  processor.addTrailer(httpChunkTrailer);
		  processor.finishResponse();
		  processor.channelClosed();

		  Assert.assertEquals("Error Handled", false, processor._errorHandled);
		  Assert.assertEquals("Processor Response State", HttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
		  Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
		  Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
		  TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
		  Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.TARGETSCN_RESPONSE_ERROR, gotMsg._state);
		  Assert.assertEquals("No response",true, (null == stateMsg._cp));
	  }

	  // Other Exception in Response
	  {
		  TestAbstractQueue queue = new TestAbstractQueue();
		  TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
		  DefaultHttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		  httpResponse.setHeader(DatabusRequest.DATABUS_ERROR_CAUSE_CLASS_HEADER, "Other Dummy Error");
		  TestRemoteExceptionHandler remoteExHandler = new TestRemoteExceptionHandler();
		  remoteExHandler._exType= ExceptionType.OTHER_EXCEPTION;
		  Checkpoint cp = new Checkpoint();
		  cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_CATCHUP);
		  BootstrapTargetScnHttpResponseProcessor  processor = new BootstrapTargetScnHttpResponseProcessor(queue, stateMsg, cp, remoteExHandler, null);
		  ChannelBuffer buf = getScnResponse();
		  HttpChunk httpChunk = new DefaultHttpChunk(buf);
		  HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

		  processor.startResponse(httpResponse);
		  processor.addChunk(httpChunk);
		  processor.addTrailer(httpChunkTrailer);
		  processor.finishResponse();
		  processor.channelClosed();

		  Assert.assertEquals("Error Handled", false, processor._errorHandled);
		  Assert.assertEquals("Processor Response State", HttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
		  Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
		  Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
		  TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
		  Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.TARGETSCN_RESPONSE_ERROR, gotMsg._state);
		  Assert.assertEquals("No response",true, (null == stateMsg._cp));
	  }

	  // Exception at the start
	  {
		  TestAbstractQueue queue = new TestAbstractQueue();
		  TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
		  HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		  TestRemoteExceptionHandler remoteExHandler = new TestRemoteExceptionHandler();
		  Checkpoint cp = new Checkpoint();
		  cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_CATCHUP);
		  BootstrapTargetScnHttpResponseProcessor  processor = new BootstrapTargetScnHttpResponseProcessor(queue, stateMsg, cp, remoteExHandler, null);
		  ChannelBuffer buf = getScnResponse();
		  HttpChunk httpChunk = new DefaultHttpChunk(buf);
		  HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

		  processor.channelException(new Exception("dummy exception"));
		  processor.startResponse(httpResponse);
		  processor.addChunk(httpChunk);
		  processor.addTrailer(httpChunkTrailer);
		  processor.finishResponse();

		  Assert.assertEquals("Error Handled", true, processor._errorHandled);
		  Assert.assertEquals("Processor Response State", HttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
		  Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
		  Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
		  TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
		  Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.TARGETSCN_RESPONSE_ERROR, gotMsg._state);
		  Assert.assertEquals("No response",true, (null == stateMsg._cp));
	  }

	  //Exception after Start - Case 1
	  {
		  TestAbstractQueue queue = new TestAbstractQueue();
		  TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
		  HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		  TestRemoteExceptionHandler remoteExHandler = new TestRemoteExceptionHandler();
		  Checkpoint cp = new Checkpoint();
		  cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_CATCHUP);
		  BootstrapTargetScnHttpResponseProcessor  processor = new BootstrapTargetScnHttpResponseProcessor(queue, stateMsg, cp, remoteExHandler, null);
		  ChannelBuffer buf = getScnResponse();
		  HttpChunk httpChunk = new DefaultHttpChunk(buf);
		  HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

		  processor.startResponse(httpResponse);
		  processor.channelException(new Exception("dummy exception"));
		  processor.addChunk(httpChunk);
		  processor.addTrailer(httpChunkTrailer);
		  processor.finishResponse();

		  Assert.assertEquals("Error Handled", true, processor._errorHandled);
		  Assert.assertEquals("Processor Response State", HttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
		  Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
		  Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
		  TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
		  Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.TARGETSCN_RESPONSE_ERROR, gotMsg._state);
		  Assert.assertEquals("No response",true, (null == stateMsg._cp));
	  }

	  //Exception after Start - Case 2
	  {
		  TestAbstractQueue queue = new TestAbstractQueue();
		  TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
		  HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		  TestRemoteExceptionHandler remoteExHandler = new TestRemoteExceptionHandler();
		  Checkpoint cp = new Checkpoint();
		  cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_CATCHUP);
		  BootstrapTargetScnHttpResponseProcessor  processor = new BootstrapTargetScnHttpResponseProcessor(queue, stateMsg, cp, remoteExHandler, null);
		  ChannelBuffer buf = getScnResponse();
		  HttpChunk httpChunk = new DefaultHttpChunk(buf);
		  HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

		  processor.startResponse(httpResponse);
		  processor.addChunk(httpChunk);
		  processor.channelException(new Exception("dummy exception"));
		  processor.addTrailer(httpChunkTrailer);
		  processor.finishResponse();

		  Assert.assertEquals("Error Handled", true, processor._errorHandled);
		  Assert.assertEquals("Processor Response State", HttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
		  Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
		  Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
		  TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
		  Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.TARGETSCN_RESPONSE_ERROR, gotMsg._state);
		  Assert.assertEquals("No response",true, (null == stateMsg._cp));
	  }

	  //Exception after Start - Case 3
	  {
		  TestAbstractQueue queue = new TestAbstractQueue();
		  TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
		  HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		  TestRemoteExceptionHandler remoteExHandler = new TestRemoteExceptionHandler();
		  Checkpoint cp = new Checkpoint();
		  cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_CATCHUP);
		  BootstrapTargetScnHttpResponseProcessor  processor = new BootstrapTargetScnHttpResponseProcessor(queue, stateMsg, cp, remoteExHandler, null);
		  ChannelBuffer buf = getScnResponse();
		  HttpChunk httpChunk = new DefaultHttpChunk(buf);
		  HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

		  processor.startResponse(httpResponse);
		  processor.addChunk(httpChunk);
		  processor.addTrailer(httpChunkTrailer);
		  processor.channelException(new Exception("dummy exception"));
		  processor.finishResponse();

		  Assert.assertEquals("Error Handled", true, processor._errorHandled);
		  Assert.assertEquals("Processor Response State", HttpResponseProcessorDecorator.ResponseStatus.CHUNKS_FINISHED,processor._responseStatus);
		  Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
		  Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
		  TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
		  Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.TARGETSCN_RESPONSE_ERROR, gotMsg._state);
		  Assert.assertEquals("No response",true, (null == stateMsg._cp));
	  }

	  //Exception after finishResponse
	  {
		  TestAbstractQueue queue = new TestAbstractQueue();
		  TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
		  HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		  TestRemoteExceptionHandler remoteExHandler = new TestRemoteExceptionHandler();
		  Checkpoint cp = new Checkpoint();
		  cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_CATCHUP);
		  BootstrapTargetScnHttpResponseProcessor  processor = new BootstrapTargetScnHttpResponseProcessor(queue, stateMsg, cp, remoteExHandler, null);
		  ChannelBuffer buf = getScnResponse();
		  HttpChunk httpChunk = new DefaultHttpChunk(buf);
		  HttpChunkTrailer httpChunkTrailer = new DefaultHttpChunkTrailer();

		  processor.startResponse(httpResponse);
		  processor.addChunk(httpChunk);
		  processor.addTrailer(httpChunkTrailer);
		  processor.finishResponse();
		  processor.channelException(new Exception("dummy exception"));

		  Assert.assertEquals("Error Handled", true, processor._errorHandled);
		  Assert.assertEquals("Processor Response State", HttpResponseProcessorDecorator.ResponseStatus.CHUNKS_EXCEPTION,processor._responseStatus);
		  Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
		  Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
		  TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
		  Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.TARGETSCN_RESPONSE_SUCCESS, gotMsg._state);
		  Assert.assertEquals("StartSCN Response Id Check",new Long(5678912),cp.getBootstrapTargetScn());
	  }

	  // Both Exception and ChannelClose getting triggered
	  {
		  TestAbstractQueue queue = new TestAbstractQueue();
		  TestConnectionStateMessage stateMsg = new TestConnectionStateMessage();
		  TestRemoteExceptionHandler remoteExHandler = new TestRemoteExceptionHandler();
		  Checkpoint cp = new Checkpoint();
		  cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_CATCHUP);
		  BootstrapTargetScnHttpResponseProcessor  processor = new BootstrapTargetScnHttpResponseProcessor(queue, stateMsg, cp, remoteExHandler, null);
		  processor.channelException(new Exception("dummy exception"));
		  boolean isException = false;
		  try
		  {
			  processor.channelClosed();
		  } catch (Exception ex){
			  isException = true;
		  }
		  Assert.assertEquals("Is Exception", true,isException);
		  Assert.assertEquals("Error Handled", true, processor._errorHandled);
		  Assert.assertEquals("Processor Response State", HttpResponseProcessorDecorator.ResponseStatus.CHUNKS_EXCEPTION,processor._responseStatus);
		  Assert.assertEquals("Actor Queue Size", 1, queue.getMessages().size());
		  Assert.assertEquals("Expected ConnectionStateMessage","TestConnectionStateMessage", queue.getMessages().get(0).getClass().getSimpleName());
		  TestConnectionStateMessage gotMsg = (TestConnectionStateMessage)(queue.getMessages().get(0));
		  Assert.assertEquals("Expected ConnectionStateMessage State", TestConnectionStateMessage.State.TARGETSCN_RESPONSE_ERROR, gotMsg._state);
		  Assert.assertEquals("No response",true, (null == stateMsg._cp));
	  }
  }
  public static class TestAbstractQueue
     implements ActorMessageQueue
  {
	private List<Object> _msgList = new ArrayList<Object>();

	@Override
	public void enqueueMessage(Object message) {
		_msgList.add(message);
	}

	public void clear()
	{
		_msgList.clear();
	}

	public List<Object> getMessages()
	{
		return _msgList;
	}
  }

  public static class TestConnectionStateMessage
  	implements DatabusRelayConnectionStateMessage, DatabusBootstrapConnectionStateMessage
  {
	  public enum State
	  {
		  STREAM_REQUEST_SENT,
		  STREAM_REQUEST_ERROR,
		  STREAM_RESPONSE_ERROR,
		  STREAM_RESPONSE_SUCCESS,
		  SOURCES_REQUEST_ERROR,
		  SOURCES_RESPONSE_ERROR,
		  SOURCES_REQUEST_SENT,
		  SOURCES_RESPONSE_SUCCESS,
		  SOURCES_SUCCESS,
		  REGISTER_REQUEST_ERROR,
		  REGISTER_RESPONSE_ERROR,
		  REGISTER_REQUEST_SENT,
		  REGISTER_RESPONSE_SUCCESS,
		  REGISTER_SUCCESS,
		  BOOTSTRAP_REQUESTED,
		  STARTSCN_REQUEST_ERROR,
		  STARTSCN_RESPONSE_ERROR,
		  STARTSCN_REQUEST_SENT,
		  STARTSCN_RESPONSE_SUCCESS,
		  TARGETSCN_REQUEST_ERROR,
		  TARGETSCN_RESPONSE_ERROR,
		  TARGETSCN_REQUEST_SENT,
		  TARGETSCN_RESPONSE_SUCCESS,
	  };

	  protected State _state;
	  protected ChunkedBodyReadableByteChannel _channel;
	  protected Map<Long, List<RegisterResponseEntry>> _registerResponse;
	  protected List<IdNamePair> _sourcesResponse;
	  protected Checkpoint _cp;
	  protected ServerInfo _currServer;

	@Override
	public void switchToStreamRequestError() {
		_state = State.STREAM_REQUEST_ERROR;
	}

	@Override
	public void switchToStreamResponseError() {
		_state = State.STREAM_RESPONSE_ERROR;
	}

	@Override
	public void switchToStreamSuccess(ChunkedBodyReadableByteChannel result)
	{
		_state = State.STREAM_RESPONSE_SUCCESS;
		_channel = result;
	}

	@Override
	public void switchToSourcesRequestError() {
		_state = State.SOURCES_REQUEST_ERROR;
	}

	@Override
	public void switchToSourcesRequestSent() {
		_state = State.SOURCES_REQUEST_SENT;
	}

	@Override
	public void switchToSourcesResponseError() {
		_state = State.SOURCES_RESPONSE_ERROR;
	}

	@Override
	public void switchToSourcesSuccess(List<IdNamePair> result) {
		_state = State.SOURCES_SUCCESS;
		_sourcesResponse = result;
	}

	@Override
	public void switchToRegisterRequestError() {
		_state = State.REGISTER_REQUEST_ERROR;
	}

	@Override
	public void swichToRegisterRequestSent() {
		_state = State.REGISTER_REQUEST_SENT;
	}

	@Override
	public void switchToRegisterResponseError() {
		_state = State.REGISTER_RESPONSE_ERROR;
	}

	@Override
	public void switchToRegisterSuccess(
			Map<Long, List<RegisterResponseEntry>> result) {
		_state = State.REGISTER_SUCCESS;
		_registerResponse = result;

	}

	@Override
	public void switchToStreamRequestSent() {
		_state = State.STREAM_REQUEST_SENT;

	}

	@Override
	public void switchToBootstrapRequested() {
		_state = State.BOOTSTRAP_REQUESTED;
	}

	@Override
	public void switchToStartScnRequestError() {
		_state = State.STARTSCN_REQUEST_ERROR;
	}

	@Override
	public void switchToStartScnResponseError() {
		_state = State.STARTSCN_RESPONSE_ERROR;
	}

	@Override
	public void switchToStartScnSuccess(Checkpoint cp,
			DatabusBootstrapConnection bootstrapConnection,
			ServerInfo serverInfo) {
		_state = State.STARTSCN_RESPONSE_SUCCESS;
		_cp = cp;
		_currServer = serverInfo;
	}

	@Override
	public void switchToStartScnRequestSent() {
		_state = State.STARTSCN_REQUEST_SENT;
	}

	@Override
	public void switchToTargetScnRequestError() {
		_state = State.TARGETSCN_REQUEST_ERROR;
	}

	@Override
	public void switchToTargetScnResponseError() {
		_state = State.TARGETSCN_RESPONSE_ERROR;
	}

	@Override
	public void switchToTargetScnSuccess() {
		_state = State.TARGETSCN_RESPONSE_SUCCESS;
	}

	@Override
	public void switchToTargetScnRequestSent() {
		_state = State.TARGETSCN_REQUEST_SENT;
	}

	@Override
	public void switchToBootstrapDone() {
		// TODO Auto-generated method stub

	}

  public State getState()
  {
    return _state;
  }

  @Override
  public String toString()
  {
    return "TestConnectionStateMessage:" + getState();
  }
  }


  public static class TestRemoteExceptionHandler
     extends RemoteExceptionHandler
  {
	  public static enum ExceptionType
	  {
		  NO_EXCEPTION,
		  BOOTSTRAP_TOO_OLD_EXCEPTION,
		  OTHER_EXCEPTION
	  };

	  protected ExceptionType _exType = ExceptionType.NO_EXCEPTION;

	  public TestRemoteExceptionHandler()
	  {
		  super(null,null);
	  }

	  @Override
	  public Throwable getException(ChunkedBodyReadableByteChannel readChannel)
	  {
		  switch(_exType)
		  {
		  case NO_EXCEPTION: return null;
		  case BOOTSTRAP_TOO_OLD_EXCEPTION: return new BootstrapDatabaseTooOldException();
		  default: return new Exception();
		  }
	  }
  }

  private ChannelBuffer getRegisterResponse()
  {
	  String result =
		  "[{\"id\":202,\"version\":1}]";
	  byte[] b = result.getBytes();
	  ByteBuffer buf = ByteBuffer.wrap(b);
	  ChannelBuffer c = HeapChannelBufferFactory.getInstance().getBuffer(buf);
	  return c;
  }

  private ChannelBuffer getSourcesResponse()
  {
	  String result =
		  "[{\"name\":\"com.linkedin.events.company.Companies\",\"id\":301}]";
	  byte[] b = result.getBytes();
	  ByteBuffer buf = ByteBuffer.wrap(b);
	  ChannelBuffer c = HeapChannelBufferFactory.getInstance().getBuffer(buf);
	  return c;
  }

  private ChannelBuffer getScnResponse()
  {
	  String result =
		  "5678912";
	  byte[] b = result.getBytes();
	  ByteBuffer buf = ByteBuffer.wrap(b);
	  ChannelBuffer c = HeapChannelBufferFactory.getInstance().getBuffer(buf);
	  return c;
  }
}
