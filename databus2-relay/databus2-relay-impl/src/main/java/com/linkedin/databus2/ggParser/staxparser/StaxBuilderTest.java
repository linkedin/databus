/*
 *
 *  *
 *  * Copyright 2013 LinkedIn Corp. All rights reserved
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package com.linkedin.databus2.ggParser.staxparser;


import com.linkedin.databus.core.DbusConstants;
import com.linkedin.databus2.ggParser.XmlStateMachine.TransactionSuccessCallBack;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.relay.config.ReplicationBitSetterConfig;
import com.linkedin.databus2.relay.config.ReplicationBitSetterStaticConfig;
import com.linkedin.databus2.schemas.SchemaRegistryService;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import org.apache.log4j.Logger;


/**
 * This class is used from the unit tests to test the parser. (They are used by unit test TestParser)
 */
public class StaxBuilderTest
{

  private static final Logger LOG = Logger.getLogger(StaxBuilderTest.class.getName());
  private FileInputStream _fileInputStream;
  private SchemaRegistryService _schemaRegistry;
  private HashMap<String, String> _tableToNamespace;
  private HashMap<String, Integer> _tableToSourceId;
  TransactionSuccessCallBack _transactionSuccessCallBack;
  ReplicationBitSetterStaticConfig _staticReplicationConfig;


  public StaxBuilderTest(String file,
                         SchemaRegistryService schemaRegistry,
                         TransactionSuccessCallBack transactionSuccessCallBack)
      throws FileNotFoundException, DatabusException
  {
    _transactionSuccessCallBack = transactionSuccessCallBack;
    _fileInputStream = new FileInputStream(file);
    _schemaRegistry = schemaRegistry;
    //Build Datastructure for table --> namespace
    _tableToNamespace = new HashMap<String, String>();
    _tableToNamespace.put("MEMBER2.TEST", "com.linkedin.events.member2.test.test");
    _tableToNamespace.put("MEMBER2.TEST2", "com.linkedin.events.member2.test.test");
    _tableToNamespace.put("MEMBER2.TEST3", "com.linkedin.events.member2.test.test");
    _tableToSourceId = new HashMap<String, Integer>();
    _tableToSourceId.put("MEMBER2.TEST", 401);
    _tableToSourceId.put("MEMBER2.TEST2", 402);
    _tableToSourceId.put("MEMBER2.TEST3", 403);
    ReplicationBitSetterConfig _config = new ReplicationBitSetterConfig();
    _config.setFieldName("test");
    _config.setSourceType("NONE");
    _staticReplicationConfig = _config.build();
  }

  public void processXml()
      throws Exception
  {
    try
    {

     //wrap the stream with fake root
      String xmlStartTag = "<?xml version=\"" + DbusConstants.XML_VERSION+ "\" encoding=\""+ DbusConstants.ISO_8859_1 +"\"?><root>";
      String xmlEndTag = "</root>";
      List xmlTagsList = Arrays.asList(new InputStream[]
                                           {
                                               new ByteArrayInputStream(xmlStartTag.getBytes(DbusConstants.ISO_8859_1)),
                                               _fileInputStream,
                                               new ByteArrayInputStream(xmlEndTag.getBytes(DbusConstants.ISO_8859_1)),
                                           });

      Enumeration<InputStream> streams = Collections.enumeration(xmlTagsList);
      SequenceInputStream seqStream = new SequenceInputStream(streams);
      XMLInputFactory factory =  XMLInputFactory.newInstance();
      factory.setProperty(XMLInputFactory.IS_COALESCING, Boolean.TRUE);
      XMLStreamReader xmlStreamReader = factory.createXMLStreamReader(seqStream);

      XmlParser parser = new XmlParser(xmlStreamReader, _schemaRegistry, _tableToNamespace, _tableToSourceId, _transactionSuccessCallBack, true,_staticReplicationConfig, seqStream);   //TODO replace _tableToNamespace this by physical sources config
      parser.start();
    }
    catch (XMLStreamException e)
    {
      LOG.error("Unable to parse the given xml stream", e);
    }
  }

  /*
  public static void main(String[] args)
      throws XMLStreamException, FileNotFoundException, DatabusException
  {

    FileSystemSchemaRegistryService.Config configBuilder = new FileSystemSchemaRegistryService.Config();
    configBuilder.setFallbackToResources(true);
    configBuilder.setSchemaDir("/Users/nsomasun/Documents/workspace/databus2/schemas_registry/");

    FileSystemSchemaRegistryService.StaticConfig config = configBuilder.build();
    FileSystemSchemaRegistryService service = FileSystemSchemaRegistryService.build(config);
    DummyTransactionCallback callback = new DummyTransactionCallback();
    StaxBuilderTest test = new StaxBuilderTest("/Users/nsomasun/xmlwithtok",service,callback);
    test.processXml();
  }      */
}
