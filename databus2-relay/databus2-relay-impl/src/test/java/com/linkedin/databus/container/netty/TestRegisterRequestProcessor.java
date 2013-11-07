package com.linkedin.databus.container.netty;


import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import junit.framework.Assert;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.testng.annotations.Test;
import com.linkedin.databus.container.request.RegisterRequestProcessor;
import com.linkedin.databus.core.data_model.LogicalSource;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.ChunkedWritableByteChannel;
import com.linkedin.databus2.core.container.DatabusHttpHeaders;
import com.linkedin.databus2.core.container.request.DatabusRequest;
import com.linkedin.databus2.core.container.request.InvalidRequestParamValueException;
import com.linkedin.databus2.core.container.request.RegisterResponseEntry;
import com.linkedin.databus2.core.container.request.RegisterResponseMetadataEntry;
import com.linkedin.databus2.core.container.request.RequestProcessingException;
import com.linkedin.databus2.schemas.SchemaId;
import com.linkedin.databus2.schemas.SchemaRegistryService;
import com.linkedin.databus2.schemas.SourceIdNameRegistry;
import com.linkedin.databus2.schemas.VersionedSchemaSet;


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
public class TestRegisterRequestProcessor
{
  public static final String MODULE = TestRegisterRequestProcessor.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);
  private String makeMetadataSchema(int version)
  {
    // Introduce extra spaces in the schema to ensure that we preserve the original schema string.
    return "{   \"name\":   \"metadata\", \"type\":\"record\",\"doc\":\"Dummy metadata\",\"namespace\":\"com.linkedin.events.espresso\",\"version\":"+version+",\"fields\":[{\"name\":\"etag\",\"type\":\"string\"}]}";
  }

  // Test the happy path where there are 2 versions of document schema of a table and one version of metadata
  // schema in the registry.
  @Test
  public void testV4RegisterRequestProcessor() throws Exception
  {
    Properties params = new Properties();
    final int protoVersion = 4;
    final int srcId1 = 101;
    final String srcName1 = "source-101";
    final String docSchema1 = "docSchema1";
    final String docSchema2 = "docSchema2";
    final String metadataSchema1 =  makeMetadataSchema(1);
    final String metadataSchema2 =  makeMetadataSchema(2);
    final byte[] metaSchemaDigest1 = new byte[] {32, 33, 34, 35};
    final byte[] metaSchemaDigest2 = new byte[] {35, 34, 33, 32};
    final short docSchemaV1 = 1;
    final short docSchemaV2 = 2;
    final short metaSchemaV1 = 1;
    final short metaSchemaV2 = 2;

    params.setProperty(DatabusHttpHeaders.PROTOCOL_VERSION_PARAM, Integer.toString(protoVersion));
    params.setProperty(RegisterRequestProcessor.SOURCES_PARAM, Integer.toString(srcId1));

    final StringBuilder responseStr = new StringBuilder();
    ChunkedWritableByteChannel chunkedWritableByteChannel = EasyMock.createMock(ChunkedWritableByteChannel.class);
    chunkedWritableByteChannel.addMetadata(EasyMock.eq(DatabusHttpHeaders.DBUS_CLIENT_RELAY_PROTOCOL_VERSION_HDR),
                                           EasyMock.eq(protoVersion));
    chunkedWritableByteChannel.write(EasyMock.anyObject(ByteBuffer.class));
    EasyMock.expectLastCall().andAnswer(new IAnswer<Object>()
    {
      @Override
      public Object answer()
          throws Throwable
      {
        Charset charset = Charset.forName("UTF-8");
        CharsetDecoder decoder = charset.newDecoder();
        responseStr.append(decoder.decode((ByteBuffer)EasyMock.getCurrentArguments()[0]));
        return responseStr.length();
      }
    });
    EasyMock.replay(chunkedWritableByteChannel);


    DatabusRequest mockReq = EasyMock.createMock(DatabusRequest.class);
    EasyMock.expect(mockReq.getParams()).andReturn(params).anyTimes();
    EasyMock.expect(mockReq.getResponseContent()).andReturn(chunkedWritableByteChannel);
    EasyMock.replay(mockReq);

    LogicalSource lsrc1 = new LogicalSource(srcId1, srcName1);
    SourceIdNameRegistry mockSrcIdReg = EasyMock.createMock(SourceIdNameRegistry.class);
    EasyMock.expect(mockSrcIdReg.getSource(srcId1)).andReturn(lsrc1).anyTimes();
    EasyMock.replay(mockSrcIdReg);

    Map<Short, String> srcSchemaVersions = new HashMap<Short, String>();
    srcSchemaVersions.put(docSchemaV1, docSchema1);
    srcSchemaVersions.put(docSchemaV2, docSchema2);
    VersionedSchemaSet metadataSchemaSet = new VersionedSchemaSet();
    metadataSchemaSet.add(SchemaRegistryService.DEFAULT_METADATA_SCHEMA_SOURCE,
                          metaSchemaV1,
                          new SchemaId(metaSchemaDigest1),
                          metadataSchema1,
                          true);
    metadataSchemaSet.add(SchemaRegistryService.DEFAULT_METADATA_SCHEMA_SOURCE,
                          metaSchemaV2,
                          new SchemaId(metaSchemaDigest2),
                          metadataSchema2,
                          true);

    SchemaRegistryService mockSchemaReg = EasyMock.createMock(SchemaRegistryService.class);
    EasyMock.expect(mockSchemaReg.fetchAllSchemaVersionsBySourceName(srcName1)).andReturn(srcSchemaVersions).anyTimes();
    EasyMock.expect(mockSchemaReg.fetchAllMetadataSchemaVersions()).andReturn(metadataSchemaSet).anyTimes();
    EasyMock.replay(mockSchemaReg);

    HttpRelay mockRelay = EasyMock.createMock(HttpRelay.class);
    EasyMock.expect(mockRelay.getHttpStatisticsCollector()).andReturn(null).anyTimes();
    EasyMock.expect(mockRelay.getSourcesIdNameRegistry()).andReturn(mockSrcIdReg).anyTimes();
    EasyMock.expect(mockRelay.getSchemaRegistryService()).andReturn(mockSchemaReg).anyTimes();
    EasyMock.replay(mockRelay);

    RegisterRequestProcessor reqProcessor = new RegisterRequestProcessor(null, mockRelay);
    reqProcessor.process(mockReq);

    // Decode
    ObjectMapper mapper = new ObjectMapper();
    HashMap<String, List<Object>> responseMap =
        mapper.readValue(responseStr.toString(), new TypeReference<HashMap<String, List<Object>>>() {});

    Map<Long, List<RegisterResponseEntry>> sourcesSchemasMap = RegisterResponseEntry.createFromResponse(responseMap,
                                                                                                        RegisterResponseEntry.SOURCE_SCHEMAS_KEY,
                                                                                                        false);
    // There should be one entry in the map, which is a  list.
    Assert.assertEquals(1, sourcesSchemasMap.size());
    Assert.assertEquals(2, sourcesSchemasMap.get(new Long(srcId1)).size());
    for (RegisterResponseEntry r : sourcesSchemasMap.get(new Long(srcId1)))
    {
      Assert.assertEquals(srcId1, r.getId());
      if (r.getVersion() == docSchemaV1)
      {
        Assert.assertEquals(docSchema1, r.getSchema());
      }
      else
      {
        Assert.assertEquals(docSchema2, r.getSchema());
      }
    }
    Map<Long, List<RegisterResponseEntry>> keysSchemasMap = RegisterResponseEntry.createFromResponse(responseMap,
                                                                                                     RegisterResponseEntry.KEY_SCHEMAS_KEY,
                                                                                                     true);
    Assert.assertNull(keysSchemasMap);

    List<RegisterResponseMetadataEntry> metadataSchemasList = RegisterResponseMetadataEntry.createFromResponse(responseMap,
                                                                                                               RegisterResponseMetadataEntry.METADATA_SCHEMAS_KEY,
                                                                                                               true);
    // The response should contain the exact string that the schema registry has.
    Assert.assertEquals(2, metadataSchemasList.size());
    for (RegisterResponseMetadataEntry r : metadataSchemasList)
    {
      if (r.getVersion() == 1)
      {
        Assert.assertEquals(metadataSchema1, r.getSchema());
        Assert.assertTrue(Arrays.equals(metaSchemaDigest1, r.getCrc32()));
      }
      else
      {
        Assert.assertEquals(metadataSchema2, r.getSchema());
        Assert.assertTrue(Arrays.equals(metaSchemaDigest2, r.getCrc32()));
      }
    }

    EasyMock.verify(mockRelay);
    EasyMock.verify(mockReq);
    EasyMock.verify(mockSchemaReg);
    EasyMock.verify(mockSrcIdReg);
  }

  // Test of happy path when the protocol version is specified as 2 or 3,
  // or not specified at all.
  // We should send out the source schemas only, and that too as a list.
  private void testRegisterReqProcessorVx(final int protoVersion) throws Exception
  {
    LOG.info("Verifying happy path with protocol version: " + protoVersion);

    Properties params = new Properties();
    final int srcId1 = 101;
    final String srcName1 = "source-101";
    final String docSchema1 = "docSchema1";
    final String docSchema2 = "docSchema2";
    final short docSchemaV1 = 1;
    final short docSchemaV2 = 2;

    if (protoVersion != 0)
    {
      params.setProperty(DatabusHttpHeaders.PROTOCOL_VERSION_PARAM, Integer.toString(protoVersion));
    }
    params.setProperty(RegisterRequestProcessor.SOURCES_PARAM, Integer.toString(srcId1));

    final StringBuilder responseStr = new StringBuilder();
    ChunkedWritableByteChannel chunkedWritableByteChannel = EasyMock.createMock(ChunkedWritableByteChannel.class);
    // We should write out proto-version as 3 if none was specified in the input, otherwise match the proto version
    chunkedWritableByteChannel.addMetadata(EasyMock.eq(DatabusHttpHeaders.DBUS_CLIENT_RELAY_PROTOCOL_VERSION_HDR),
                                           protoVersion != 0 ? EasyMock.eq(protoVersion): EasyMock.eq(3));
    EasyMock.expectLastCall().times(1);
    chunkedWritableByteChannel.write(EasyMock.anyObject(ByteBuffer.class));
    EasyMock.expectLastCall().andAnswer(new IAnswer<Object>()
    {
      @Override
      public Object answer()
          throws Throwable
      {
        Charset charset = Charset.forName("UTF-8");
        CharsetDecoder decoder = charset.newDecoder();
        responseStr.append(decoder.decode((ByteBuffer)EasyMock.getCurrentArguments()[0]));
        return responseStr.length();
      }
    });
    EasyMock.replay(chunkedWritableByteChannel);


    DatabusRequest mockReq = EasyMock.createMock(DatabusRequest.class);
    EasyMock.expect(mockReq.getParams()).andReturn(params).anyTimes();
    EasyMock.expect(mockReq.getResponseContent()).andReturn(chunkedWritableByteChannel);
    EasyMock.replay(mockReq);

    LogicalSource lsrc1 = new LogicalSource(srcId1, srcName1);
    SourceIdNameRegistry mockSrcIdReg = EasyMock.createMock(SourceIdNameRegistry.class);
    EasyMock.expect(mockSrcIdReg.getSource(srcId1)).andReturn(lsrc1).anyTimes();
    EasyMock.replay(mockSrcIdReg);

    Map<Short, String> srcSchemaVersions = new HashMap<Short, String>();
    srcSchemaVersions.put(docSchemaV1, docSchema1);
    srcSchemaVersions.put(docSchemaV2, docSchema2);

    SchemaRegistryService mockSchemaReg = EasyMock.createMock(SchemaRegistryService.class);
    EasyMock.expect(mockSchemaReg.fetchAllSchemaVersionsBySourceName(srcName1)).andReturn(srcSchemaVersions).anyTimes();
    EasyMock.replay(mockSchemaReg);

    HttpRelay mockRelay = EasyMock.createMock(HttpRelay.class);
    EasyMock.expect(mockRelay.getHttpStatisticsCollector()).andReturn(null).anyTimes();
    EasyMock.expect(mockRelay.getSourcesIdNameRegistry()).andReturn(mockSrcIdReg).anyTimes();
    EasyMock.expect(mockRelay.getSchemaRegistryService()).andReturn(mockSchemaReg).anyTimes();
    EasyMock.replay(mockRelay);

    RegisterRequestProcessor reqProcessor = new RegisterRequestProcessor(null, mockRelay);
    reqProcessor.process(mockReq);

    ObjectMapper mapper = new ObjectMapper();
    List<RegisterResponseEntry> schemasList =
        mapper.readValue(responseStr.toString(), new TypeReference<List<RegisterResponseEntry>>() {});

    Map<Long, List<RegisterResponseEntry>> sourcesSchemasMap = RegisterResponseEntry.convertSchemaListToMap(schemasList);

    // There should be 1 entry in the map.
    Assert.assertEquals(1, sourcesSchemasMap.size());
    Assert.assertEquals(2, sourcesSchemasMap.get(new Long(srcId1)).size());
    for (RegisterResponseEntry r : sourcesSchemasMap.get(new Long(srcId1)))
    {
      Assert.assertEquals(srcId1, r.getId());
      if (r.getVersion() == docSchemaV1)
      {
        Assert.assertEquals(docSchema1, r.getSchema());
      }
      else
      {
        Assert.assertEquals(docSchema2, r.getSchema());
      }
    }
    EasyMock.verify(mockRelay);
    EasyMock.verify(mockReq);
    EasyMock.verify(mockSchemaReg);
    EasyMock.verify(mockSrcIdReg);
  }

  @Test
  public void testRegisterRequestProcessorOldProtos() throws Exception
  {
    testRegisterReqProcessorVx(2);
    testRegisterReqProcessorVx(3);
    testRegisterReqProcessorVx(0);
  }

  public void testBadProtoVersion(final String protoVersion) throws Exception
  {
    LOG.info("Verifying unhappy path with bad protocol version: " + protoVersion);

    Properties params = new Properties();
    final int srcId1 = 101;

    params.setProperty(DatabusHttpHeaders.PROTOCOL_VERSION_PARAM, protoVersion);
    params.setProperty(RegisterRequestProcessor.SOURCES_PARAM, Integer.toString(srcId1));

    DatabusRequest mockReq = EasyMock.createMock(DatabusRequest.class);
    EasyMock.expect(mockReq.getParams()).andReturn(params).anyTimes();
    EasyMock.replay(mockReq);

    HttpRelay mockRelay = EasyMock.createMock(HttpRelay.class);
    EasyMock.expect(mockRelay.getHttpStatisticsCollector()).andReturn(null).anyTimes();
    EasyMock.replay(mockRelay);

    RegisterRequestProcessor reqProcessor = new RegisterRequestProcessor(null, mockRelay);
    boolean exceptionCaught = false;
    try
    {
      reqProcessor.process(mockReq);
    }
    catch(InvalidRequestParamValueException e)
    {
      Assert.assertEquals(RegisterRequestProcessor.COMMAND_NAME, e.getCmdName());
      Assert.assertEquals(DatabusHttpHeaders.PROTOCOL_VERSION_PARAM, e.getParamName());
      Assert.assertEquals(protoVersion, e.getParamValue());
      exceptionCaught = true;
    }
    Assert.assertTrue("Expected exception not raised", exceptionCaught);

    EasyMock.verify(mockRelay);
    EasyMock.verify(mockReq);
  }

  @Test
  public void testBadProtoVersions() throws Exception
  {
    testBadProtoVersion("abc");
    testBadProtoVersion("1");
    testBadProtoVersion("1.1");
    testBadProtoVersion("5");
  }

  private void testDatabusExceptionInGetSchemas(final int protoVersion) throws Exception
  {
    LOG.info("Testing DatabusException in getSchemas() call with protocol version " + protoVersion);
    Properties params = new Properties();
    final int srcId1 = 101;
    final String srcName1 = "source-101";
    final String docSchema1 = "docSchema1";
    final String docSchema2 = "docSchema2";
    final short docSchemaV1 = 1;
    final short docSchemaV2 = 2;

    if (protoVersion != 0)
    {
      params.setProperty(DatabusHttpHeaders.PROTOCOL_VERSION_PARAM, Integer.toString(protoVersion));
    }
    params.setProperty(RegisterRequestProcessor.SOURCES_PARAM, Integer.toString(srcId1));

    final StringBuilder responseStr = new StringBuilder();
    ChunkedWritableByteChannel chunkedWritableByteChannel = EasyMock.createMock(ChunkedWritableByteChannel.class);
    chunkedWritableByteChannel.addMetadata(EasyMock.anyObject(String.class), EasyMock.anyInt());
    EasyMock.expectLastCall().times(1);
    chunkedWritableByteChannel.write(EasyMock.anyObject(ByteBuffer.class));
    EasyMock.expectLastCall().andAnswer(new IAnswer<Object>()
    {
      @Override
      public Object answer()
          throws Throwable
      {
        Charset charset = Charset.forName("UTF-8");
        CharsetDecoder decoder = charset.newDecoder();
        responseStr.append(decoder.decode((ByteBuffer)EasyMock.getCurrentArguments()[0]));
        return responseStr.length();
      }
    });
    EasyMock.replay(chunkedWritableByteChannel);


    DatabusRequest mockReq = EasyMock.createMock(DatabusRequest.class);
    EasyMock.expect(mockReq.getParams()).andReturn(params).anyTimes();
    EasyMock.replay(mockReq);

    LogicalSource lsrc1 = new LogicalSource(srcId1, srcName1);
    SourceIdNameRegistry mockSrcIdReg = EasyMock.createMock(SourceIdNameRegistry.class);
    EasyMock.expect(mockSrcIdReg.getSource(srcId1)).andReturn(lsrc1).anyTimes();
    EasyMock.replay(mockSrcIdReg);

    Map<Short, String> srcSchemaVersions = new HashMap<Short, String>();
    srcSchemaVersions.put(docSchemaV1, docSchema1);
    srcSchemaVersions.put(docSchemaV2, docSchema2);

    DatabusException expectedCause = new DatabusException("FakeException");
    SchemaRegistryService mockSchemaReg = EasyMock.createMock(SchemaRegistryService.class);
    EasyMock.expect(mockSchemaReg.fetchAllSchemaVersionsBySourceName(srcName1)).andThrow(expectedCause);
                                                                                         EasyMock.replay(mockSchemaReg);

    HttpRelay mockRelay = EasyMock.createMock(HttpRelay.class);
    EasyMock.expect(mockRelay.getHttpStatisticsCollector()).andReturn(null).anyTimes();
    EasyMock.expect(mockRelay.getSourcesIdNameRegistry()).andReturn(mockSrcIdReg).anyTimes();
    EasyMock.expect(mockRelay.getSchemaRegistryService()).andReturn(mockSchemaReg).anyTimes();
    EasyMock.replay(mockRelay);

    RegisterRequestProcessor reqProcessor = new RegisterRequestProcessor(null, mockRelay);
    boolean exceptionCaught = false;
    try
    {
      reqProcessor.process(mockReq);
    }
    catch (RequestProcessingException e)
    {
      Assert.assertEquals(expectedCause, e.getCause());
    }

    EasyMock.verify(mockRelay);
    EasyMock.verify(mockReq);
    EasyMock.verify(mockSchemaReg);
    EasyMock.verify(mockSrcIdReg);
  }

  @Test
  public void testDatabusExceptionInGetSchemas() throws Exception
  {
    testDatabusExceptionInGetSchemas(0);
    testDatabusExceptionInGetSchemas(2);
    testDatabusExceptionInGetSchemas(4);
  }

  private void testNullSchemasInGetSchemas(final int protoVersion) throws Exception
  {
    LOG.info("Testing null return from fetchAllSchemaVersionsBySourceName() with protoversion " + protoVersion);
    Properties params = new Properties();
    final int srcId1 = 101;
    final String srcName1 = "source-101";

    if (protoVersion != 0)
    {
      params.setProperty(DatabusHttpHeaders.PROTOCOL_VERSION_PARAM, Integer.toString(protoVersion));
    }
    params.setProperty(RegisterRequestProcessor.SOURCES_PARAM, Integer.toString(srcId1));

    final StringBuilder responseStr = new StringBuilder();
    ChunkedWritableByteChannel chunkedWritableByteChannel = EasyMock.createMock(ChunkedWritableByteChannel.class);
    // We should write out proto-version as 3 if none was specified in the input, otherwise match the proto version
    chunkedWritableByteChannel.addMetadata(EasyMock.eq(DatabusHttpHeaders.DBUS_CLIENT_RELAY_PROTOCOL_VERSION_HDR),
                                           protoVersion != 0 ? EasyMock.eq(protoVersion): EasyMock.eq(3));
    EasyMock.expectLastCall().times(1);
    chunkedWritableByteChannel.write(EasyMock.anyObject(ByteBuffer.class));
    EasyMock.expectLastCall().andAnswer(new IAnswer<Object>()
    {
      @Override
      public Object answer()
          throws Throwable
      {
        Charset charset = Charset.forName("UTF-8");
        CharsetDecoder decoder = charset.newDecoder();
        responseStr.append(decoder.decode((ByteBuffer)EasyMock.getCurrentArguments()[0]));
        return responseStr.length();
      }
    });
    EasyMock.replay(chunkedWritableByteChannel);


    DatabusRequest mockReq = EasyMock.createMock(DatabusRequest.class);
    EasyMock.expect(mockReq.getParams()).andReturn(params).anyTimes();
    EasyMock.expect(mockReq.getResponseContent()).andReturn(chunkedWritableByteChannel);
    EasyMock.replay(mockReq);

    LogicalSource lsrc1 = new LogicalSource(srcId1, srcName1);
    SourceIdNameRegistry mockSrcIdReg = EasyMock.createMock(SourceIdNameRegistry.class);
    EasyMock.expect(mockSrcIdReg.getSource(srcId1)).andReturn(lsrc1).anyTimes();
    EasyMock.replay(mockSrcIdReg);

    SchemaRegistryService mockSchemaReg = EasyMock.createMock(SchemaRegistryService.class);
    EasyMock.expect(mockSchemaReg.fetchAllSchemaVersionsBySourceName(srcName1)).andReturn(null);
    EasyMock.replay(mockSchemaReg);

    HttpRelay mockRelay = EasyMock.createMock(HttpRelay.class);
    EasyMock.expect(mockRelay.getHttpStatisticsCollector()).andReturn(null).anyTimes();
    EasyMock.expect(mockRelay.getSourcesIdNameRegistry()).andReturn(mockSrcIdReg).anyTimes();
    EasyMock.expect(mockRelay.getSchemaRegistryService()).andReturn(mockSchemaReg).anyTimes();
    EasyMock.replay(mockRelay);

    RegisterRequestProcessor reqProcessor = new RegisterRequestProcessor(null, mockRelay);
    reqProcessor.process(mockReq);

    ObjectMapper mapper = new ObjectMapper();
    List<RegisterResponseEntry> schemasList =
        mapper.readValue(responseStr.toString(), new TypeReference<List<RegisterResponseEntry>>() {});

    Map<Long, List<RegisterResponseEntry>> sourcesSchemasMap = RegisterResponseEntry.convertSchemaListToMap(schemasList);

    // There should be 1 entry in the map.
    Assert.assertEquals(0, sourcesSchemasMap.size());

    EasyMock.verify(mockRelay);
    EasyMock.verify(mockReq);
    EasyMock.verify(mockSchemaReg);
    EasyMock.verify(mockSrcIdReg);
  }

  @Test
  public void testNullInGetSchemas() throws Exception
  {
    testNullSchemasInGetSchemas(3);
    testNullSchemasInGetSchemas(2);
    testNullSchemasInGetSchemas(0);
  }
}
