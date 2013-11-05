package com.linkedin.databus2.ggParser.staxparser;


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

import com.linkedin.databus2.ggParser.XmlStateMachine.TransactionSuccessCallBack;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.relay.config.LogicalSourceStaticConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import com.linkedin.databus2.relay.config.ReplicationBitSetterStaticConfig;
import com.linkedin.databus2.relay.config.ReplicationBitSetterStaticConfig.MissingValueBehavior;
import com.linkedin.databus2.schemas.SchemaRegistryService;
import java.io.InputStream;
import java.util.HashMap;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import org.apache.log4j.Logger;


public class StaxBuilder
{
  private Logger _log;
  private InputStream _inputStream;
  private SchemaRegistryService _schemaRegistry;
  private HashMap<String, String> _tableToNamespace;
  private HashMap<String, Integer> _tableToSourceId;
  private final boolean _errorOnMissingFields;
  TransactionSuccessCallBack _transactionSuccessCallBack;
  XmlParser _parser;
  ReplicationBitSetterStaticConfig _replicationBitConfig;



  public StaxBuilder(
      SchemaRegistryService schemaRegistry,
      InputStream inputStream,
      PhysicalSourceStaticConfig pConfig,
      TransactionSuccessCallBack transactionSuccessCallBack)
      throws DatabusException, XMLStreamException
  {
    String lname = (pConfig != null) ? pConfig.getName() : "";
    String moduleName = StaxBuilder.class.getName() + ":" + lname;
    _log = Logger.getLogger(moduleName);
    _transactionSuccessCallBack = transactionSuccessCallBack;
    _inputStream = inputStream;
    _schemaRegistry = schemaRegistry;
    _errorOnMissingFields = pConfig.getErrorOnMissingFields();
    _replicationBitConfig = pConfig.getReplBitSetter();

    MissingValueBehavior mvb = _replicationBitConfig.getMissingValueBehavior();
    if(mvb != MissingValueBehavior.STOP_WITH_ERROR)
      _log.warn("ReplicationBit Setter Config is set to continue parsing and mark events as ("
                  + (mvb == MissingValueBehavior.TREAT_EVENT_LOCAL ? "local" : "replicated")
                  + ") when replication-identifier is missing !!");
    
    //Build Datastructure for table --> namespace  and table --> sourceId
    LogicalSourceStaticConfig[] sources = pConfig.getSources();
    _tableToNamespace = new HashMap<String, String>();
    _tableToSourceId = new HashMap<String, Integer>();

    if(sources.length == 0)
    {
      throw new DatabusException("No logical sources found! Nothing to capture here.");
    }

    for(int i = 0 ; i< sources.length ; i++)
    {
      String tableName = sources[i].getUri();
      int sourceId = sources[i].getId();
      String name = sources[i].getName();
      _log.info("Configuring to capture changes from table: " + tableName + " which has source id: "  + sourceId + " and name : " + name);
      _tableToNamespace.put(tableName, name);
      _tableToSourceId.put(tableName, sourceId);
    }

    //Construct the Xml parser
    XMLInputFactory factory =  XMLInputFactory.newInstance();
    factory.setProperty(XMLInputFactory.IS_COALESCING, Boolean.TRUE);   //Without this the state machine might break.
    XMLStreamReader xmlStreamReader = factory.createXMLStreamReader(_inputStream);
    _parser = new XmlParser(xmlStreamReader, _schemaRegistry, _tableToNamespace, _tableToSourceId, _transactionSuccessCallBack,
                              _errorOnMissingFields, _replicationBitConfig, inputStream);
   }

  /**
   * Start processing the xml trail files. The parser is started on invoking the method.
   * @throws Exception
   */
  public void processXml()
      throws Exception
  {
    try
    {
     _parser.start();
    }
    catch (XMLStreamException e)
    {
      _log.info("Unable to parse the given xml stream because of: ",e);
      throw e;
    }
  }

  public XmlParser getParser()
  {
    return _parser;
  }
}
