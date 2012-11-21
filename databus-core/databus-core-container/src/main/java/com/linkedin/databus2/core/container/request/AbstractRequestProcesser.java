package com.linkedin.databus2.core.container.request;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.ByteBuffer;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.ObjectCodec;
import org.codehaus.jackson.JsonGenerator.Feature;
import org.codehaus.jackson.impl.WriterBasedGenerator;
import org.codehaus.jackson.io.IOContext;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.util.BufferRecycler;

public abstract class AbstractRequestProcesser
  implements RequestProcessor
{
  public static final String PRETTY_PRINT_PARAM = "pretty";

  protected<T> String makeJsonResponse(T obj, DatabusRequest request)
  throws IOException
  {
    StringWriter out = new StringWriter(102400);
    ObjectMapper mapper = new ObjectMapper();
    JsonGenerator jsonGen = createJsonGenerator(mapper, out,
                                                null != request.getParams().getProperty(PRETTY_PRINT_PARAM));
    mapper.writeValue(jsonGen, obj);
    return out.toString();
  }

  protected<T> void writeJsonObjectToResponse(T obj, DatabusRequest request) throws IOException
  {
    String out = makeJsonResponse(obj, request);
    byte[] dataBytes = out.getBytes();
    request.getResponseContent().write(ByteBuffer.wrap(dataBytes));
  }

  protected JsonGenerator createJsonGenerator(ObjectCodec codec, Writer writer, boolean prettyPrint)
  {
    IOContext ioCtx = new IOContext(new BufferRecycler(), null, true);
    WriterBasedGenerator result = new WriterBasedGenerator(ioCtx, 0, codec, writer);
    result.configure(Feature.QUOTE_FIELD_NAMES, true);
    if (prettyPrint) result.useDefaultPrettyPrinter();

    return result;
  }
}
