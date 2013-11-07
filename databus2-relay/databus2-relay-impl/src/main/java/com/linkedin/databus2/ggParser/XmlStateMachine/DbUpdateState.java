package com.linkedin.databus2.ggParser.XmlStateMachine;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;

import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.relay.config.ReplicationBitSetterStaticConfig;

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

public class DbUpdateState extends AbstractStateTransitionProcessor
{



  //Attributes and values
  public static final String TABLEATTR = "table";

  public static final String UPDATEATTRNAME = "type";
  public static final String UPDATEVAL = "update";

  public static final String DELETEATTRNAME = "type";
  public static final String DELETEVAL = "delete";

  public static final String INSERTATTRNAME = "type";
  public static final String INSERTVAL = "insert";


  public static final String PREIMAGEATTRNAME = "image";
  public static final String PREIMAGEVAL = "before";

  public final static String MODULE = DbUpdateState.class.getName();
  public final static Logger LOG = Logger.getLogger(MODULE);

  //The map contains the dbupdate in the current
  private HashMap<Integer, HashSet<DBUpdateImage>> sourceDbUpdatesMap;
  private String _currentTable;
  private long _scn = TokenState.ERRORSCN;
  private DBUpdateImage.OpType _opType = DBUpdateImage.OpType.UNKNOWN;

  public DbUpdateState()
  {
    super(STATETYPE.STARTELEMENT, DBUPDATE);
  }

  public HashMap<Integer, HashSet<DBUpdateImage>> getSourceDbUpdatesMap()
  {
    if(sourceDbUpdatesMap == null)
      sourceDbUpdatesMap = new HashMap<Integer, HashSet<DBUpdateImage>>();
    return sourceDbUpdatesMap;
  }

  public void setSourceDbUpdatesMap(HashMap<Integer, HashSet<DBUpdateImage>> sourceDbUpdatesMap)
  {
    this.sourceDbUpdatesMap = sourceDbUpdatesMap;
  }


  public long getScn()
  {
    return _scn;
  }

  public void setScn(long scn)
  {
    _scn = scn;
  }

  private HashSet<DBUpdateImage> getHashSet(int sourceId)
  {
    if(sourceDbUpdatesMap == null)
      sourceDbUpdatesMap = new HashMap<Integer, HashSet<DBUpdateImage>>();

    if(sourceDbUpdatesMap.get(sourceId) == null)
      sourceDbUpdatesMap.put(sourceId,new HashSet<DBUpdateImage>());
    return sourceDbUpdatesMap.get(sourceId);
  }


  /**
   * clean up the state and generate the required data structures
   * @param stateMachine
   * @param xmlStreamReader
   */
  @Override
  public void onEndElement(StateMachine stateMachine, XMLStreamReader xmlStreamReader)
      throws DatabusException, XMLStreamException
  {
    _currentStateType = STATETYPE.ENDELEMENT;

    //TODO construct a data structure that will hold key, csn and the avro
    GenericRecord record = stateMachine.columnsState.getGenericRecord();
    long scn = stateMachine.tokensState.getScn();

    if(_scn < scn)
    {
      _scn = scn;
      if(LOG.isDebugEnabled()) LOG.debug("Setting current DbUpdates scn to "+ _scn);
    }

    /**
     * The record is empty, this will happen when we have skipped the columns state
     */
    if(record == null)
    {
      if(LOG.isDebugEnabled())
        LOG.debug("Unable to process the current dbUpdate (record was found to be empty), skipping the dbUpdate");
      onError(stateMachine, xmlStreamReader);
      return;
    }

    if(scn == TokenState.ERRORSCN)
    {
      LOG.error("Unable to find scn for the given dbUpdate");
      throw new DatabusException("Unable to find scn for the given dbUpdate, terminating the parser");
    }

    //check if the current columns state has seen any missing elements, if it has, then print the current scn associated with the dbupdate for debugging purpose
    if(stateMachine.columnsState.isSeenMissingFields())
    {
      LOG.error("There were missing fields seen in Columns section and the corresponding scn is : " + stateMachine.tokensState.getScn());
    }

    Boolean isReplicated = false;
    if(stateMachine.getReplicationBitConfig().getSourceType() == ReplicationBitSetterStaticConfig.SourceType.COLUMN)
      isReplicated = stateMachine.columnsState.isReplicated();
    else if(stateMachine.getReplicationBitConfig().getSourceType() == ReplicationBitSetterStaticConfig.SourceType.TOKEN)
      isReplicated = stateMachine.tokensState.isReplicated();
    else if(stateMachine.getReplicationBitConfig().getSourceType() == ReplicationBitSetterStaticConfig.SourceType.NONE)
      isReplicated = false;
    else
      throw new DatabusException("Unknown source type specified in replicationBitConfig, expected COLUMN or TOKEN or NONE");

    DBUpdateImage eventImage = new DBUpdateImage(stateMachine.columnsState.getKeyPairs(),
                                                 stateMachine.tokensState.getScn(),
                                                 stateMachine.columnsState.getGenericRecord(),
                                                 stateMachine.columnsState.getCurrentSchema(),
                                                 _opType,
                                                 isReplicated);
    Integer sourceId = stateMachine.getTableToSourceId().get(_currentTable);
    if(sourceId == null)
    {
      LOG.error("The table " + _currentTable + " does not have a sourceId, the current dbUpdate cannot be processed.");
      onError(stateMachine, xmlStreamReader);
      return;
    }

    if(getHashSet(sourceId) == null)
    {
      LOG.error("The hashset is empty, cannot proceed without a valid hashset");
      throw new DatabusException("Error while creating hashset for storing dbUpdates");
    }

    //The equals method of the hashset is overridden to compare only the key, thereby ensuring the latest update on the key
    if(getHashSet(sourceId) != null && getHashSet(sourceId).contains(eventImage))
      getHashSet(sourceId).remove(eventImage);
    getHashSet(sourceId).add(eventImage);

    stateMachine.columnsState.cleanUpState(stateMachine, xmlStreamReader);
    stateMachine.tokensState.cleanUpState(stateMachine,xmlStreamReader);
    xmlStreamReader.nextTag();
    setNextStateProcessor(stateMachine,xmlStreamReader);
  }

  @Override
  public void onStartElement(StateMachine stateMachine, XMLStreamReader xmlStreamReader)
      throws DatabusException, XMLStreamException
  {
    _currentStateType = STATETYPE.STARTELEMENT;
    _opType = DBUpdateImage.OpType.UNKNOWN;
    boolean isUpdate = false;
    boolean isDelete = false;
    boolean isPreImage = false;

    for(int i = 0; i < xmlStreamReader.getAttributeCount() ; i++)
    {
      if(xmlStreamReader.getAttributeName(i).getLocalPart().equals(TABLEATTR))
        _currentTable = xmlStreamReader.getAttributeValue(i);
      else if(xmlStreamReader.getAttributeName(i).getLocalPart().equalsIgnoreCase(UPDATEATTRNAME) && xmlStreamReader.getAttributeValue(i).equalsIgnoreCase(
          UPDATEVAL))
      {
        _opType = DBUpdateImage.OpType.UPDATE;
      }
      else if(xmlStreamReader.getAttributeName(i).getLocalPart().equalsIgnoreCase(DELETEATTRNAME) && xmlStreamReader.getAttributeValue(i).equalsIgnoreCase(
          DELETEVAL))
      {
        _opType = DBUpdateImage.OpType.DELETE;
      }
      else if(xmlStreamReader.getAttributeName(i).getLocalPart().equalsIgnoreCase(INSERTATTRNAME) && xmlStreamReader.getAttributeValue(i).equalsIgnoreCase(
          INSERTVAL))
      {
        _opType = DBUpdateImage.OpType.INSERT;
      }
      else if(xmlStreamReader.getAttributeName(i).getLocalPart().equalsIgnoreCase(PREIMAGEATTRNAME) && xmlStreamReader.getAttributeValue(i).equalsIgnoreCase(
          PREIMAGEVAL))
      {
        isPreImage = true;
      }
    }

    if(isPreImage) //This is the pre-image of the row, we can skip this
    {
      if(LOG.isDebugEnabled())
        LOG.debug("Skipping current dbUpdate because it's a preimage");
      skipCurrentDBupdate(stateMachine, xmlStreamReader);
      return;
    }

    if(_currentTable == null || _currentTable.length() == 0)
    {
      LOG.fatal("PROBLEM WITH XML: Dbupdate does not have any table name associated with it, stopping ");
      throw new DatabusException("Dbupdate does not have any table name associated with it, stopping");
    }

    Schema schema = StateMachineHelper.tableToSchema(_currentTable, stateMachine.getTableToSourceNameMap(), stateMachine.getSchemaRegistryService());
    if(schema == null)
    {
      if(LOG.isDebugEnabled())
        LOG.debug("This source is not configured (couldn't find namespace). Skipping to tokens, to capture scn for empty DBUpdate");

      /**
       * State jump from DBUPDATE-> TOKENS. (we skip COLUMNS).
       * At this point, we can't capture this update (because the tableName -> namespace configuration is not found), we are still interested in knowing
       * the SCN associated with the current dbUpdate. The SCNs are captured because, if it's a slow source, we need to insert EOP at some threshold (This is done by the
       * goldengate event producer). If the transaction has no updates relevant to the tables we are interested in, this will be passed to the transaction callback as an "empty" transaction with just SCNs contained.
       */
      skipToTokens(stateMachine, xmlStreamReader);
      setNextStateProcessor(stateMachine, xmlStreamReader);
      return;
    }


    stateMachine.columnsState.setCurrentSchema(schema);
    stateMachine.columnsState.setKeyPairs(new ArrayList<ColumnsState.KeyPair>());
    xmlStreamReader.nextTag();
    setNextStateProcessor(stateMachine, xmlStreamReader);
  }

  public void onError(StateMachine stateMachine, XMLStreamReader xmlStreamReader)
      throws DatabusException, XMLStreamException
  {
    stateMachine.columnsState.cleanUpState(stateMachine, xmlStreamReader);
    stateMachine.tokensState.cleanUpState(stateMachine, xmlStreamReader);
    xmlStreamReader.nextTag();
    setNextStateProcessor(stateMachine, xmlStreamReader);
  }

  @Override
  public void cleanUpState(StateMachine stateMachine, XMLStreamReader xmlStreamReader)
  {
    _scn = TokenState.ERRORSCN;
    setSourceDbUpdatesMap(null);
    _currentTable = null;
  }

  public String getCurrentTable() {
    return _currentTable;
  }

  private void skipCurrentDBupdate(StateMachine stateMachine, XMLStreamReader xmlStreamReader)
      throws XMLStreamException, DatabusException
  {
    while(xmlStreamReader.hasNext())
    {
      XmlStreamReaderHelper.checkAndMoveToNextTagSet(xmlStreamReader);
      if(xmlStreamReader.isEndElement() && xmlStreamReader.getLocalName().equals(DBUPDATE))
      {
        return;
      }
      if(xmlStreamReader.isEndElement() && xmlStreamReader.getLocalName().equals(TRANSACTION))
      {
        setNextStateProcessor(stateMachine,xmlStreamReader);
        return;
      }
    }
  }

  private void skipToTokens(StateMachine stateMachine, XMLStreamReader xmlStreamReader)
      throws XMLStreamException, DatabusException
  {
    while(xmlStreamReader.hasNext())
    {
      XmlStreamReaderHelper.checkAndMoveToNextTagSet(xmlStreamReader);
      if(xmlStreamReader.isStartElement() && xmlStreamReader.getLocalName().equals(TOKENS))
      {
        setNextStateProcessor(stateMachine,xmlStreamReader);
        return;
      }
    }
  }


  public static class DBUpdateImage
  {

    public static enum OpType{
      UPDATE,
      INSERT,
      DELETE,
      UNKNOWN
    }

    private final ArrayList<ColumnsState.KeyPair> _keyPairs;
    private final long _scn;
    private final GenericRecord _genericRecord;
    private final Schema _schema;
    private final OpType _opType;
    private boolean _isReplicated;

    public boolean isReplicated()
    {
      return _isReplicated;
    }

    public ArrayList<ColumnsState.KeyPair> getKeyPairs()
    {
      return _keyPairs;
    }

    public long getScn()
    {
      return _scn;
    }

    public GenericRecord getGenericRecord()
    {
      return _genericRecord;
    }

    public Schema getSchema()
    {
      return _schema;
    }


    public OpType getOpType()
    {
      return _opType;
    }

    public DBUpdateImage(ArrayList<ColumnsState.KeyPair> keyPairs,
                         long scn,
                         GenericRecord genericRecord,
                         Schema schema,
                         OpType opType,
                         boolean isReplicatedFlag)
        throws DatabusException
    {
      if(keyPairs.size() == 0)
        throw new DatabusException("Unable to construct DBUpdateImage because no keys were found");

      if(opType == OpType.UNKNOWN)
        throw new DatabusException("Unknown operation type, INSERT/UPDATE/DELETE not set?");
      _keyPairs = keyPairs;
      _scn = scn;
      _genericRecord = genericRecord;
      _schema = schema;
      _opType = opType;
      _isReplicated = isReplicatedFlag;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      DBUpdateImage that = (DBUpdateImage) o;

      if (_scn != that._scn) return false;
      if (!_keyPairs.equals(that._keyPairs)) return false;

      return true;
    }

    @Override
    public int hashCode()
    {
      int result = _keyPairs.hashCode();
      result = 31 * result + (int) (_scn ^ (_scn >>> 32));
      return result;
    }
  }
}
