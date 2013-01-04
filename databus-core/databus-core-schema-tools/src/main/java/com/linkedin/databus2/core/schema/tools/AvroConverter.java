package com.linkedin.databus2.core.schema.tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

public class AvroConverter
{

  public static enum AvroFormat
  {
    JSON,
    JSON_LINES,
    BINARY,
  }

  private final AvroFormat _inputFormat;
  private final AvroFormat _outputFormat;
  private final Schema _outputSchema;
  private final Schema _inputSchema;

  public AvroConverter(AvroFormat inputFormat, AvroFormat outputFormat,
                       Schema inputSchema, Schema outputSchema)
  {
    _inputFormat = inputFormat;
    _outputFormat = outputFormat;
    _inputSchema = inputSchema;
    _outputSchema = outputSchema;
  }

  public List<GenericRecord> convert(InputStream in) throws IOException
  {

    Decoder inputDecoder = (_inputFormat == AvroFormat.BINARY) ?
        DecoderFactory.defaultFactory().createBinaryDecoder(in, null) :
        (AvroFormat.JSON == _inputFormat) ?
            new JsonDecoder(_inputSchema, in) :
            null;

    ArrayList<GenericRecord> result = new ArrayList<GenericRecord>();
    GenericDatumReader<GenericRecord> genericReader = _inputSchema != _outputSchema ?
        new GenericDatumReader<GenericRecord>(_inputSchema, _outputSchema) :
        new GenericDatumReader<GenericRecord>(_inputSchema);
    switch (_inputFormat)
    {
      case BINARY:
      case JSON:
      {
        GenericRecord r = genericReader.read(null, inputDecoder);
        result.add(r);
        break;
      }
      case JSON_LINES:
      {
        InputStreamReader inReader = new InputStreamReader(in);
        try
        {
          BufferedReader lineIn = new BufferedReader(inReader);
          try
          {
            String line;
            while (null != (line = lineIn.readLine()))
            {
              inputDecoder =  new JsonDecoder(_inputSchema, line);
              GenericRecord r = genericReader.read(null, inputDecoder);
              result.add(r);
              break;
            }
          }
          finally
          {
            lineIn.close();
          }
        }
        finally
        {
          inReader.close();
        }
      }
      default:
      {
        throw new RuntimeException("Unimplemented input format: " + _inputFormat);
      }
    }

    return result;
  }

  public void convert(InputStream in, OutputStream out) throws IOException
  {
    JsonGenerator jsonGenerator = (new JsonFactory()).createJsonGenerator(new OutputStreamWriter(out));
    if (AvroFormat.JSON == _outputFormat) jsonGenerator.useDefaultPrettyPrinter();

    List<GenericRecord> result = convert(in);
    Encoder outputEncoder = (AvroFormat.BINARY == _outputFormat) ?
        new BinaryEncoder(out) :
        new JsonEncoder(_outputSchema, jsonGenerator);

    GenericDatumWriter<GenericRecord> genericWriter = new GenericDatumWriter<GenericRecord>(_outputSchema);
    for (GenericRecord r: result)
    {
      genericWriter.write(r, outputEncoder);
    }
    outputEncoder.flush();
    out.flush();
  }

}
