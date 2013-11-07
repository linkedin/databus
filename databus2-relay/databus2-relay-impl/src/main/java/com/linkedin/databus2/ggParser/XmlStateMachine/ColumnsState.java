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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.producers.gg.GGEventGenerationFactory;
import com.linkedin.databus2.relay.config.ReplicationBitSetterStaticConfig;
import com.linkedin.databus2.relay.config.ReplicationBitSetterStaticConfig.MissingValueBehavior;
import com.linkedin.databus2.schemas.utils.SchemaHelper;


public class ColumnsState extends AbstractStateTransitionProcessor
{
  public final static String MODULE = ColumnsState.class.getName();
  public final static Logger LOG = Logger.getLogger(MODULE);
  private Schema _currentSchema;
  private GenericRecord _genericRecord;
  private ArrayList<KeyPair> _keyPairs;
  private final boolean _errorOnMissingFields;
  private boolean _isReplicated;
  //This flag is used to indicate if there are any missing fields seen while processing the current columns section
  public boolean _seenMissingFields = false;
  private String _currentTable;


  public ColumnsState(boolean errorOnMissingFields)
  {
    super(STATETYPE.STARTELEMENT, COLUMNSSTATE);
    _errorOnMissingFields = errorOnMissingFields;
  }

  public boolean isSeenMissingFields()
  {
    return _seenMissingFields;
  }

  public void setSeenMissingFields(boolean seenMissingFields)
  {
    _seenMissingFields = seenMissingFields;
  }


  public GenericRecord getGenericRecord()
  {
    return _genericRecord;
  }

  public void setGenericRecord(GenericRecord genericRecord)
  {
    _genericRecord = genericRecord;
  }

  public Schema getCurrentSchema()
  {
    return _currentSchema;
  }

  public void setCurrentSchema(Schema currentSchema)
  {
    _currentSchema = currentSchema;
  }

  public ArrayList<KeyPair> getKeyPairs()
  {
    return _keyPairs;
  }

  public void setKeyPairs(ArrayList<KeyPair> keyPairs)
  {
    _keyPairs = keyPairs;
  }


  @Override
  public void cleanUpState(StateMachine stateMachine, XMLStreamReader xmlStreamReader)
  {
    setKeyPairs(null);
    setCurrentSchema(null);
    setGenericRecord(null);
    setReplicated(false);
    setSeenMissingFields(false);
  }

  @Override
  public void onEndElement(StateMachine stateMachine, XMLStreamReader xmlStreamReader)
      throws Exception
  {
    _currentStateType = STATETYPE.ENDELEMENT;
    _genericRecord = generateAvroRecord(stateMachine.columnState.getEventFields(), stateMachine.getReplicationBitConfig(),stateMachine.getReplicationValuePattern());
    stateMachine.columnState.cleanUpState(stateMachine, xmlStreamReader);
    xmlStreamReader.nextTag();
    setNextStateProcessor(stateMachine, xmlStreamReader);
  }

  @Override
  public void onStartElement(StateMachine stateMachine, XMLStreamReader xmlStreamReader)
      throws DatabusException, XMLStreamException
  {

    _currentStateType = STATETYPE.STARTELEMENT;
    _currentTable = stateMachine.dbUpdateState.getCurrentTable(); // for info purposes
    stateMachine.columnState.setEventFields(new HashMap<String, ColumnState.EventField>());
    xmlStreamReader.nextTag();
    setNextStateProcessor(stateMachine,xmlStreamReader);
  }

  /**
   * Convert the eventFields read from the xml to an avro record
   * @param eventFields hashmap <database field, value>
   * @return
   */
  private GenericRecord generateAvroRecord(HashMap<String,ColumnState.EventField> eventFields, ReplicationBitSetterStaticConfig replicationBitConfig, Pattern replicationValuePattern)
      throws Exception
  {
    GenericRecord record = new GenericData.Record(getCurrentSchema());
    List<Schema.Field> fields = getCurrentSchema().getFields();
    String pkFieldName = SchemaHelper.getMetaField(getCurrentSchema(), "pk");
    if(pkFieldName == null)
      throw new DatabusException("No primary key specified in the schema");
    PrimaryKey pk = new PrimaryKey(pkFieldName);
    for(Schema.Field field : fields)
    {
      if(field.schema().getType() == Schema.Type.ARRAY)
      {
        throw new DatabusException("The gg parser cannot handle ARRAY datatypes. Found in field: "+ field);
      }
      else
      {
        //The current database field being processed (field is avro field name  and databaseFieldName is oracle field name (one to one mapping)
        String databaseFieldName = SchemaHelper.getMetaField(field, "dbFieldName");
        //Check if it's ok for this field to be null
        checkNullSafety(eventFields, pk, field, databaseFieldName, replicationBitConfig);
        //Insert the field into the generic record
        String fieldValue = insertFieldIntoRecord(eventFields, record, pkFieldName, pk, field, databaseFieldName);
        //Set the replication flag if this is a replicated event
        if(replicationBitConfig.getSourceType() == ReplicationBitSetterStaticConfig.SourceType.COLUMN &&
            isReplicationField(
            databaseFieldName,
            replicationBitConfig))
        {
          setReplicated(StateMachineHelper.verifyReplicationStatus(replicationValuePattern, fieldValue, replicationBitConfig.getMissingValueBehavior()));
        }
      }
    }

    //prettyPrint(record);     //TODO remove this -> only for debugging
    return record;
  }

  /**
   * The method takes the given field(avro+oracle name), fetches the value from the eventFields map and inserts it into the record.
   * In addition the function also constructs the primary keys for the given record
   * @param eventFields Map containing dbFieldName => fieldValue
   * @param record The record to insert the field value into
   * @param pkFieldName The name of primaryKey field (comma seperated list)
   * @param pk The primary key object stored as an class object
   * @param field The current field being processed(Avro)
   * @param databaseFieldName Field being processed(oracle name)
   * @return
   * @throws Exception
   */
  private String insertFieldIntoRecord(HashMap<String, ColumnState.EventField> eventFields,
                                       GenericRecord record,
                                       String pkFieldName,
                                       PrimaryKey pk,
                                       Schema.Field field,
                                       String databaseFieldName)
      throws DatabusException
  {
    String fieldValue = eventFields.get(databaseFieldName).getVal();
    boolean isFieldNull = eventFields.get(databaseFieldName).isNull();
    Object fieldValueObj = null;
    try{

      if(!isFieldNull)
        fieldValueObj =  GGEventGenerationFactory.stringToAvroType(fieldValue, field);
      else
        fieldValueObj = null;

      record.put(field.name(), fieldValueObj);
    }
    catch(DatabusException e)
    {
      LOG.error("Unable to process field: " + field.name());
      throw e;
    }

    constructPkeys(eventFields, pkFieldName, pk, field, databaseFieldName, fieldValueObj);
    return fieldValue;
  }

  /**
   * Constructs the primary key pair and stores it in the current state
   * @param eventFields Map containing dbFieldName => fieldValue
   * @param pkFieldName The name of primaryKey field (comma seperated list). e.g., if the schema has pk=id, member_id. Then, pkFieldName is id, member_id.
   *                    This is used only for logging purpose
   * @param pk The primary key object stored as an class object
   * @param field The current field being processed(Avro)
   * @param databaseFieldName Field being processed(oracle name)
   * @param fieldValueObj Value of the current field being processed
   * @throws DatabusException
   */
  private void constructPkeys(HashMap<String, ColumnState.EventField> eventFields,
                              String pkFieldName,
                              PrimaryKey pk,
                              Schema.Field field,
                              String databaseFieldName,
                              Object fieldValueObj)
      throws DatabusException
  {
    if(eventFields.get(databaseFieldName).isKey())
    {
      if (!pk.isPartOfPrimaryKey(field))
        throw new DatabusException("The primary key is not as expected. Expected: " + pkFieldName + " found from xml: " + field.name());
      if(fieldValueObj == null)
        throw new DatabusException("Unable to find the value of the object");
      Schema.Type pkFieldType = SchemaHelper.unwindUnionSchema(field).getType();
      KeyPair pair = new KeyPair(fieldValueObj, pkFieldType);
      _keyPairs.add(pair);
    }
  }

  /**
   * Checks if the given field can be null.
   * 1. Primary keys cannot be null
   * 2. Replication fields cannot be null
   * 3. If the errorOnMissing field config is null, then this method will throw an exception
   * @param eventFields Map containing dbFieldName => fieldValue
   * @param pk The primary key object stored as an class object
   * @param field The current field being processed(Avro)
   * @param databaseFieldName Field being processed(oracle name)
   * @throws DatabusException
   */
  private void checkNullSafety(HashMap<String, ColumnState.EventField> eventFields,
                               PrimaryKey pk,
                               Schema.Field field,
                               String databaseFieldName,
                               ReplicationBitSetterStaticConfig replicationBitConfig)
      throws DatabusException
  {
    if(eventFields.get(databaseFieldName)  == null)
    {
      LOG.error("Missing field "+ databaseFieldName + " in event from the xml trail for table " + _currentTable);
      if(!_errorOnMissingFields)   //Are we ok to accept null fields ?
      {
        //We cannot tolerate empty primary key fields, so we'll throw exception if key is null
        if (pk.isPartOfPrimaryKey(field))
          throw new DatabusException("Skip errors on missing DB Fields is true, but cannot proceed because primary key not found: " + field.name());

        //We also need the replication field, it's not optional if MissingValueBehavior == STOP_WITH_ERROR
        if(replicationBitConfig.getSourceType() == ReplicationBitSetterStaticConfig.SourceType.COLUMN
            && isReplicationField(databaseFieldName, replicationBitConfig)
            && replicationBitConfig.getMissingValueBehavior() == MissingValueBehavior.STOP_WITH_ERROR)
        {
          throw new DatabusException("Skip errors on missing DB Fields is true, but the replication field is missing, this is mandatory, cannot proceed with  " + field.name()+ " field missing");
        }

        setSeenMissingFields(true);
        //If not primary key, we create fake hash entry with a null eventfield entry
        ColumnState.EventField emptyEventField = new ColumnState.EventField(false, null, true);
        eventFields.put(databaseFieldName, emptyEventField);
      }
      else
        throw new DatabusException("Unable to find a required field " + databaseFieldName + " in the xml trail file");
    }
  }

  /**
   * The method verifies if the the dataBaseFieldName is the field used for replication identification
   * @param databaseFieldName
   * @param replicationBitConfig
   * @return
   */
  private boolean isReplicationField(String databaseFieldName, ReplicationBitSetterStaticConfig replicationBitConfig)
  {
    return databaseFieldName.equalsIgnoreCase(replicationBitConfig.getFieldName());
  }

  public boolean isReplicated()
  {
    return _isReplicated;
  }

  public void setReplicated(boolean replicated)
  {
    _isReplicated = replicated;
  }

  private void prettyPrint(GenericRecord record)
  {
    try {
      LOG.info(new JSONObject(record.toString()).toString(2));
    } catch (JSONException e) {
      LOG.error("Unable to parser json: The Json created by the generator is not valid!", e);
    }
  }

  public static class KeyPair{
    Object key;
    Schema.Type keyType;

    public Schema.Type getKeyType()
    {
      return keyType;
    }

    public Object getKey()
    {
      return key;
    }
    public KeyPair(Object key, Schema.Type keyType)
    {
      this.key = key;
      this.keyType = keyType;
    }


    @Override
    public boolean equals(Object o)
    {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      KeyPair keyPair = (KeyPair) o;

      if (!key.equals(keyPair.key)) return false;
      if (keyType != keyPair.keyType) return false;

      return true;
    }

    @Override
    public int hashCode()
    {
      int result = key.hashCode();
      result = 31 * result + keyType.hashCode();
      return result;
    }

  }
}

