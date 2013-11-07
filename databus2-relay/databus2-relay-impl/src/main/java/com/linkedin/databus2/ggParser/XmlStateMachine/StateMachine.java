package com.linkedin.databus2.ggParser.XmlStateMachine;

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

import com.linkedin.databus2.relay.config.ReplicationBitSetterStaticConfig;
import com.linkedin.databus2.schemas.SchemaRegistryService;
import java.util.HashMap;
import java.util.regex.Pattern;
import javax.xml.stream.XMLStreamReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;


public class StateMachine
{

  public final static String MODULE = StateMachine.class.getName();
  public final static Logger LOG = Logger.getLogger(MODULE);

  private final Pattern _replicationValuePattern;
  private StateProcessor _processState;
  private GenericRecord _genericRecord;
  private HashMap<String, String>  _tableToSourceNameMap;
  private HashMap<String, Integer> _tableToSourceId;
  private SchemaRegistryService _schemaRegistryService;
  private ReplicationBitSetterStaticConfig _replicationBitConfig;

  TransactionState transactionState;
  DbUpdateState dbUpdateState;
  ColumnsState columnsState;
  ColumnState columnState;
  RootState rootState;
  TokensState tokensState;
  TokenState tokenState;
  ShutdownState shutdownState;

  public ReplicationBitSetterStaticConfig getReplicationBitConfig()
  {
    return _replicationBitConfig;
  }

  public Pattern getReplicationValuePattern()
  {
    return _replicationValuePattern;
  }

  public HashMap<String, Integer> getTableToSourceId()
  {
    return _tableToSourceId;
  }

  public void setTableToSourceId(HashMap<String, Integer> tableToSourceId)
  {
    _tableToSourceId = tableToSourceId;
  }

  public HashMap<String, String> getTableToSourceNameMap()
  {
    return _tableToSourceNameMap;
  }

  public SchemaRegistryService getSchemaRegistryService()
  {
    return _schemaRegistryService;
  }

  public void setGenericRecord(GenericRecord genericRecord)
  {
    _genericRecord = genericRecord;
  }

  public GenericRecord getGenericRecord()
  {
    return _genericRecord;
  }

  public StateProcessor getProcessState()
  {
    return _processState;
  }

  public void setProcessState(StateProcessor processState)
  {
    _processState = processState;
  }

  public StateMachine(SchemaRegistryService schemaRegistryService,
                      HashMap<String, String> tableMap,
                      HashMap<String, Integer> tableToSourceId,
                      TransactionSuccessCallBack transactionSuccessCallBack,
                      boolean errorOnMissingFields, ReplicationBitSetterStaticConfig replicationBitConfig)
  {
    if(!errorOnMissingFields)
      LOG.warn("The parser will NOT terminate if fields are missing in the trail file");
     transactionState = new TransactionState(transactionSuccessCallBack);
     dbUpdateState = new DbUpdateState();
     columnsState = new ColumnsState(errorOnMissingFields);
     columnState = new ColumnState();
     rootState = new RootState();
     tokensState = new TokensState();
     shutdownState = new ShutdownState();
     tokenState = new TokenState();
    _processState = rootState;
    _schemaRegistryService = schemaRegistryService;
    _tableToSourceNameMap = tableMap;
    _tableToSourceId = tableToSourceId;
    _replicationBitConfig = replicationBitConfig;
    if(_replicationBitConfig != null && _replicationBitConfig.getRemoteUpdateValueRegex() != null)
      _replicationValuePattern = Pattern.compile(_replicationBitConfig.getRemoteUpdateValueRegex());
    else
      _replicationValuePattern = null;
  }

  public void processElement(XMLStreamReader xmlStreamReader)
      throws Exception
  {
    try
    {
     _processState.processElement(this, xmlStreamReader);
    } catch (Exception e) {
      LOG.error("Got exception. Last seen txn timestamp was : (" + transactionState.getLastSeenTxnTimestampStr() + ")", e);
      throw e;
    }
  }
}
