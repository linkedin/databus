package com.linkedin.databus2.producers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Encoder;
import org.apache.log4j.Logger;

import com.linkedin.databus.core.DbusConstants;
import com.linkedin.databus.core.DbusEventBufferAppendable;
import com.linkedin.databus.core.DbusEventInfo;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.UnsupportedKeyException;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.producers.ds.DbChangeEntry;
import com.linkedin.databus2.producers.ds.KeyPair;
import com.linkedin.databus2.producers.ds.PrimaryKeySchema;
import com.linkedin.databus2.relay.config.ReplicationBitSetterStaticConfig;
import com.linkedin.databus2.relay.config.ReplicationBitSetterStaticConfig.SourceType;
import com.linkedin.databus2.schemas.SchemaId;
import com.linkedin.databus2.schemas.utils.SchemaHelper;

public class OpenReplicatorAvroEventFactory
{
  /** Avro schema for the generated events. */
  protected final Schema _eventSchema;

  /** Unique schema ID for the _eventSchema. */
  protected final byte[] _schemaId;

  /** Source ID for this event source. */
  protected final int _sourceId;

  /** Physical source id */
  protected final int _pSourceId;

  /** PartitionFunction used to generate the event partition based on event key. */
  protected final PartitionFunction _partitionFunction;

  /** Logger for error and debug messages. */
  private final Logger _log = Logger.getLogger(getClass());

  /** key Schema. */
  private final PrimaryKeySchema _pKeySchema ;

  /** Replication BitSetter StaticConfig **/
  private final ReplicationBitSetterStaticConfig _replSetterConfig;

  private final Pattern _replBitSetterPattern;

  public static final String MODULE = OpenReplicatorAvroEventFactory.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);


  public OpenReplicatorAvroEventFactory(int sourceId, int pSourceId,
                                       String eventSchema, PartitionFunction partitionFunction,
                                       ReplicationBitSetterStaticConfig replSetterConfig)
  throws DatabusException
  {
    _sourceId = sourceId;
    _pSourceId = pSourceId;
    _eventSchema = Schema.parse(eventSchema);
    _schemaId = SchemaHelper.getSchemaId(eventSchema);
    _partitionFunction = partitionFunction;

    _replSetterConfig = replSetterConfig;

    if ((null != _replSetterConfig) && (SourceType.COLUMN.equals(_replSetterConfig.getSourceType())))
      _replBitSetterPattern = Pattern.compile(replSetterConfig.getRemoteUpdateValueRegex());
    else
      _replBitSetterPattern = null;

    String keyName = SchemaHelper.getMetaField(_eventSchema, "pk");

    if(keyName == null)
    {
      throw new DatabusException("The event schema is missing the required field \"key\".");
    }

    _pKeySchema = new PrimaryKeySchema(keyName);
  }


  public int createAndAppendEvent(DbChangeEntry changeEntry,
                                   DbusEventBufferAppendable eventBuffer,
                                   boolean enableTracing,
                                   DbusEventsStatisticsCollector dbusEventsStatisticsCollector)
          throws EventCreationException, UnsupportedKeyException, DatabusException
  {
    Object keyObj = obtainKey(changeEntry);

    //Construct the Databus Event key, determine the key type and construct the key
    DbusEventKey eventKey = new DbusEventKey(keyObj);

    short lPartitionId = _partitionFunction.getPartition(eventKey);

    //Get the md5 for the schema
    SchemaId schemaId = SchemaId.createWithMd5(changeEntry.getSchema());

    byte[] payload = serializeEvent(changeEntry.getRecord());

    DbusEventInfo eventInfo = new DbusEventInfo(changeEntry.getOpCode(),
                                                changeEntry.getScn(),
                                                (short)_pSourceId,
                                                lPartitionId,
                                                changeEntry.getTimestampInNanos(),
                                                (short)_sourceId,
                                                schemaId.getByteArray(),
                                                payload,
                                                enableTracing,
                                                false);

    boolean success = eventBuffer.appendEvent(eventKey, eventInfo, dbusEventsStatisticsCollector);

    return success ? payload.length : -1;
  }

  protected byte[] serializeEvent(GenericRecord record)
      throws EventCreationException
  {
    // Serialize the row
    byte[] serializedValue;
    try
    {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      Encoder encoder = new BinaryEncoder(bos);
      GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(record.getSchema());
      writer.write(record, encoder);
      serializedValue = bos.toByteArray();
    }
    catch(IOException ex)
    {
      throw new EventCreationException("Failed to serialize the Avro GenericRecord", ex);
    }
    catch(RuntimeException ex)
    {
      // Avro likes to throw RuntimeExceptions instead of checked exceptions when serialization fails.
      _log.error("Exception for record: " + record + " with schema: " + record.getSchema().getFullName());
      throw new EventCreationException("Failed to serialize the Avro GenericRecord", ex);
    }

    return serializedValue;
  }

  public int getSourceId()
  {
    return _sourceId;
  }

  /**
   * Given a DBImage, returns the key
   * If it is a single key, it returns the object if it is LONG /INT / STRING
   * For compound key, it casts the fields as String, delimits the fields and returns the appended string
   * @param dbChangeEntry The post-image of the event
   * @return Actual key object
   * @throws DatabusException
   */
  private Object obtainKey(DbChangeEntry dbChangeEntry)
      throws DatabusException
  {
    if (null == dbChangeEntry) {
      throw new DatabusException("DBUpdateImage is null");
    }
    List<KeyPair> pairs = dbChangeEntry.getPkeys();
    if (null == pairs || pairs.size() == 0) {
      throw new DatabusException("There do not seem to be any keys");
    }

    if (pairs.size() == 1) {
      Object key = pairs.get(0).getKey();
      Schema.Type pKeyType = pairs.get(0).getKeyType();
      Object keyObj = null;
      if (pKeyType == Schema.Type.INT)
      {
        if (key instanceof Integer)
        {
          keyObj = key;
        }
        else
        {
          throw new DatabusException(
              "Schema.Type does not match actual key type (INT) "
                  + key.getClass().getName());
        }

      } else if (pKeyType == Schema.Type.LONG)
      {
        if (key instanceof Long)
        {
          keyObj = key;
        }
        else
        {
          throw new DatabusException(
              "Schema.Type does not match actual key type (LONG) "
                  + key.getClass().getName());
        }

        keyObj = key;
      }
      else
      {
        keyObj = key;
      }

      return keyObj;
    } else {
      // Treat multiple keys as a separate case to avoid unnecessary casts
      Iterator<KeyPair> li = pairs.iterator();
      StringBuilder compositeKey = new StringBuilder();
      while (li.hasNext())
      {
        KeyPair kp = li.next();
        Schema.Type pKeyType = kp.getKeyType();
        Object key = kp.getKey();
        if (pKeyType == Schema.Type.INT)
        {
          if (key instanceof Integer)
            compositeKey.append(kp.getKey().toString());
          else
            throw new DatabusException(
                "Schema.Type does not match actual key type (INT) "
                    + key.getClass().getName());
        }
        else if (pKeyType == Schema.Type.LONG)
        {
          if (key instanceof Long)
            compositeKey.append(key.toString());
          else
            throw new DatabusException(
                "Schema.Type does not match actual key type (LONG) "
                    + key.getClass().getName());
        }
        else
        {
          compositeKey.append(key);
        }

        if (li.hasNext()) {
          // Add the delimiter for all keys except the last key
          compositeKey.append(DbusConstants.COMPOUND_KEY_DELIMITER);
        }
      }
      return compositeKey.toString();
    }
  }
}

