package com.linkedin.databus2.producers;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;

import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.BinlogEventV4Header;
import com.google.code.or.binlog.impl.event.AbstractBinlogEventV4;
import com.google.code.or.binlog.impl.event.DeleteRowsEvent;
import com.google.code.or.binlog.impl.event.DeleteRowsEventV2;
import com.google.code.or.binlog.impl.event.FormatDescriptionEvent;
import com.google.code.or.binlog.impl.event.QueryEvent;
import com.google.code.or.binlog.impl.event.RotateEvent;
import com.google.code.or.binlog.impl.event.TableMapEvent;
import com.google.code.or.binlog.impl.event.UpdateRowsEvent;
import com.google.code.or.binlog.impl.event.UpdateRowsEventV2;
import com.google.code.or.binlog.impl.event.WriteRowsEvent;
import com.google.code.or.binlog.impl.event.WriteRowsEventV2;
import com.google.code.or.binlog.impl.event.XidEvent;
import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Pair;
import com.google.code.or.common.glossary.Row;
import com.google.code.or.common.glossary.column.Int24Column;
import com.google.code.or.common.glossary.column.LongColumn;
import com.google.code.or.common.glossary.column.LongLongColumn;
import com.google.code.or.common.glossary.column.NullColumn;
import com.google.code.or.common.glossary.column.ShortColumn;
import com.google.code.or.common.glossary.column.TinyColumn;
import com.linkedin.databus.core.DatabusRuntimeException;
import com.linkedin.databus.core.DatabusThreadBase;
import com.linkedin.databus.core.DbusOpcode;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.producers.ds.DbChangeEntry;
import com.linkedin.databus2.producers.ds.KeyPair;
import com.linkedin.databus2.producers.ds.PerSourceTransaction;
import com.linkedin.databus2.producers.ds.PrimaryKeySchema;
import com.linkedin.databus2.producers.ds.Transaction;
import com.linkedin.databus2.producers.util.Or2AvroConvert;
import com.linkedin.databus2.schemas.NoSuchSchemaException;
import com.linkedin.databus2.schemas.SchemaRegistryService;
import com.linkedin.databus2.schemas.VersionedSchema;
import com.linkedin.databus2.schemas.utils.SchemaHelper;

/**
 * Open Replicator callback implementation
 *
 * The main callback API is {{@link #onEvents(BinlogEventV4)()}.
 *
 * This class is responsible for converting Bin log events to Avro records using schemaRegistry and calling an application callback
 * to let the application generate DbusEvent and append to EventBuffer.
 */
class ORListener extends DatabusThreadBase implements BinlogEventListener
{
  /**
   * Application Callback from this class to process one transaction
   */
  public interface TransactionProcessor
  {
    /**
     * Callback to process one transaction
     * @param txn
     * @throws DatabusException if unable to process the event. Relay will stop processing further
     * events if this problem happens
     */
    public abstract void onEndTransaction(Transaction txn) throws DatabusException;
  };

  /** Transaction object containing the current transaction that is being built from the Binlog **/
  private Transaction _transaction = null;

  /** Track current file number for generating SCN **/
  private int _currFileNum;

  /** Logger Service for this class **/
  private final Logger _log;

  /** Bin Log File Prefix used to extract file numbers in RotateEvents **/
  private final String _binlogFilePrefix;

  /** Schema Registry Service **/
  private final SchemaRegistryService _schemaRegistryService;

  /** Table URI to Source Id Map */
  private final Map<String, Short> _tableUriToSrcIdMap;
  /** Table URI to Source Name Map */
  private final Map<String, String> _tableUriToSrcNameMap;

  /** Transaction Processor **/
  private final TransactionProcessor _txnProcessor;

  /** Size in bytes in bin log for the transaction **/
  private long _currTxnSizeInBytes = 0;

  /** Txn End Timestamp (nanos) as seen in the binlog **/
  private long _currTxnTimestamp = 0;

  /** Nano sec timestamp at which the current timestamp begin seen **/
  private long _currTxnStartReadTimestamp = 0;

  /** Flag to indicate the begining of the txn is seen. Used to indicate  **/
  private boolean _isBeginTxnSeen = false;

  /** Track all the table map events, cleared when the binlog rotated **/
  private final Map<Long, TableMapEvent> _tableMapEvents = new HashMap<Long, TableMapEvent>();
  
  /** Transaction into buffer thread */
  private final TransactionWriter _transactionWriter;

  /** Shared queue to transfer binlog events from OpenReplicator to ORlistener thread **/
  private BlockingQueue<BinlogEventV4> _binlogEventQueue = null;

  /** Milli sec timeout for _binlogEventQueue operation **/
  private long _queueTimeoutMs = 100L;
  
  /** correct unsigned int type*/
  public static final int TINYINT_MAX_VALUE = 256;
  public static final int SMALLINT_MAX_VALUE = 65536;
  public static final int MEDIUMINT_MAX_VALUE = 16777216;
  public static final long INTEGER_MAX_VALUE = 4294967296L;
  public static final BigInteger BIGINT_MAX_VALUE = new BigInteger("18446744073709551616");

  private String _curSourceName;
  private boolean _ignoreSource = false;

  public ORListener(String name,
                    int currentFileNumber,
                    Logger log,
                    String binlogFilePrefix,
                    TransactionProcessor txnProcessor,
                    Map<String, Short> tableUriToSrcIdMap,
                    Map<String, String> tableUriToSrcNameMap,
                    SchemaRegistryService schemaRegistryService,
                    int maxQueueSize,
                    long queueTimeoutMs)
  {
    super("ORListener_" + name);
    _log = log;
    _txnProcessor = txnProcessor;
    _binlogFilePrefix = binlogFilePrefix;
    _tableUriToSrcIdMap = tableUriToSrcIdMap;
    _tableUriToSrcNameMap = tableUriToSrcNameMap;
    _schemaRegistryService = schemaRegistryService;
    _currFileNum = currentFileNumber;
    _binlogEventQueue = new LinkedBlockingQueue<BinlogEventV4>(maxQueueSize);
    _queueTimeoutMs = queueTimeoutMs;
    _transactionWriter = new TransactionWriter(maxQueueSize, queueTimeoutMs, txnProcessor);
    _transactionWriter.start();
  }

  @Override
  public void onEvents(BinlogEventV4 event)
  {
    boolean isPut = false;
    do {
      try {
        isPut = _binlogEventQueue.offer(event, _queueTimeoutMs, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        _log.error("failed to put binlog event to binlogEventQueue event: " + event, e);
      }
    } while (!isPut && !isShutdownRequested());
  }

  private void processTableMapEvent(TableMapEvent tme)
  {
    _tableMapEvents.put(tme.getTableId(), tme);

    _curSourceName = tme.getDatabaseName().toString().toLowerCase() + "." + tme.getTableName().toString().toLowerCase();
    startSource(_curSourceName);
  }

  private void startXtion(QueryEvent e)
  {
    _currTxnStartReadTimestamp = System.nanoTime();
    _log.info("startXtion" + e);
    if ( _transaction == null)
    {
      _transaction = new Transaction();
    }
    else
    {
      throw new DatabusRuntimeException("Got startXtion without an endXtion for previous transaction");
    }
  }

  /**
   * Per {@link http://code.google.com/p/open-replicator/source/browse/trunk/open-replicator/src/main/java/com/google/code/or/binlog/impl/event/XidEvent.java}
   * XidEvent signals a commit
   */
  private void endXtion(AbstractBinlogEventV4 e)
  {
    _currTxnTimestamp = e.getHeader().getTimestamp() * 1000000L;
    long txnReadLatency = System.nanoTime() - _currTxnStartReadTimestamp;
    boolean em = ((e instanceof QueryEvent) || ( e instanceof XidEvent));
    if (! em)
    {
      throw new DatabusRuntimeException("endXtion should be called with either QueryEvent of XidEvent");
    }
    _transaction.setSizeInBytes(_currTxnSizeInBytes);
    _transaction.setTxnNanoTimestamp(_currTxnTimestamp);
    _transaction.setTxnReadLatencyNanos(txnReadLatency);

    if(_ignoreSource) {
      long scn = scn(_currFileNum, (int)e.getHeader().getPosition());
      _transaction.setIgnoredSourceScn(scn);
    }

    try
    {
      _transactionWriter.addTransaction(_transaction);
    }
    finally
    {
      reset();

      if ( _log.isDebugEnabled())
      {
        _log.debug("endXtion" + e);
      }
    }
  }

  private void rollbackXtion(QueryEvent e)
  {
    reset();
    _log.info("rollbackXtion" + e);
  }

  private void reset()
  {
    _transaction = null;
    _currTxnSizeInBytes = 0;
  }

  private void startSource(String newTableName)
  {
    Short srcId = _tableUriToSrcIdMap.get(newTableName);
    _ignoreSource = null == srcId;
    if (_ignoreSource) {
      LOG.info("Ignoring source: " + newTableName);
      return;
    }

    LOG.info("Starting source: " + newTableName);
    assert (_transaction != null);
    if (_transaction.getPerSourceTransaction(srcId) == null)
    {
      _transaction.mergePerSourceTransaction(new PerSourceTransaction(srcId));
    }
  }

  private void deleteRows(DeleteRowsEventV2 dre)
  {
    if (_ignoreSource) {
      LOG.info("Ignoring delete rows for " + _curSourceName);
      return;
    }
    LOG.info("DELETE FROM " + _curSourceName);
    frameAvroRecord(dre.getTableId(), dre.getHeader(), dre.getRows(), DbusOpcode.DELETE);
  }

  private void deleteRows(DeleteRowsEvent dre)
  {
    if (_ignoreSource) {
      LOG.info("Ignoring delete rows for " + _curSourceName);
      return;
    }
    LOG.info("DELETE FROM " + _curSourceName);
    frameAvroRecord(dre.getTableId(), dre.getHeader(), dre.getRows(), DbusOpcode.DELETE);
  }

  private void updateRows(UpdateRowsEvent ure)
  {
    if (_ignoreSource) {
      LOG.info("Ignoring update rows for " + _curSourceName);
      return;
    }
    List<Pair<Row>> lp = ure.getRows();
    List<Row> lr =  new ArrayList<Row>(lp.size());
    for (Pair<Row> pr: lp)
    {
      Row r = pr.getAfter();
      lr.add(r);
    }
    if (lr.size() > 0) {
      LOG.info("UPDATE " + _curSourceName + ": " + lr.size());
      frameAvroRecord(ure.getTableId(), ure.getHeader(), lr, DbusOpcode.UPSERT);
    }
  }

  private void updateRows(UpdateRowsEventV2 ure)
  {
    if (_ignoreSource) {
      LOG.info("Ignoring update rows for " + _curSourceName);
      return;
    }
    List<Pair<Row>> lp = ure.getRows();
    List<Row> lr =  new ArrayList<Row>(lp.size());
    for (Pair<Row> pr: lp)
    {
      Row r = pr.getAfter();
      lr.add(r);
    }
    if (lr.size() > 0) {
      LOG.info("UPDATE " + _curSourceName + ": " + lr.size());
      frameAvroRecord(ure.getTableId(), ure.getHeader(), lr, DbusOpcode.UPSERT);
    }
  }

  private void insertRows(WriteRowsEvent wre)
  {
    if (_ignoreSource) {
      LOG.info("Ignoring insert rows for " + _curSourceName);
      return;
    }
    LOG.info("INSERT INTO " + _curSourceName);
    frameAvroRecord(wre.getTableId(), wre.getHeader(), wre.getRows(), DbusOpcode.UPSERT);
  }

  private void insertRows(WriteRowsEventV2 wre)
  {
    if (_ignoreSource) {
      LOG.info("Ignoring insert rows for " + _curSourceName);
      return;
    }
    LOG.info("INSERT INTO " + _curSourceName);
    frameAvroRecord(wre.getTableId(), wre.getHeader(), wre.getRows(), DbusOpcode.UPSERT);
  }

  private void frameAvroRecord(long tableId, BinlogEventV4Header bh, List<Row> rl, final DbusOpcode doc)
  {
    try
    {
      final long timestampInNanos = bh.getTimestamp() * 1000000L;
      final long scn = scn(_currFileNum, (int)bh.getPosition());
      final boolean isReplicated = false;
      final TableMapEvent tme = _tableMapEvents.get(tableId);
      String tableName = tme.getDatabaseName().toString().toLowerCase() + "." + tme.getTableName().toString().toLowerCase();
      VersionedSchema vs = _schemaRegistryService.fetchLatestVersionedSchemaBySourceName(_tableUriToSrcNameMap.get(tableName));
      Schema schema = vs.getSchema();

      if ( _log.isDebugEnabled())
        _log.debug("File Number :" + _currFileNum + ", Position :" + (int)bh.getPosition() + ", SCN =" + scn);

      for(Row r: rl)
      {
        List<Column> cl = r.getColumns();
        GenericRecord gr = new GenericData.Record(schema);
        generateAvroEvent(vs, cl, gr);

        List<KeyPair> kps = generateKeyPair(gr, vs);

        DbChangeEntry db = new DbChangeEntry(scn, timestampInNanos, gr, doc, isReplicated, schema, kps);
        _transaction.getPerSourceTransaction(_tableUriToSrcIdMap.get(tableName)).mergeDbChangeEntrySet(db);
      }
    } catch (NoSuchSchemaException ne)
    {
      throw new DatabusRuntimeException(ne);
    } catch (DatabusException de)
    {
      throw new DatabusRuntimeException(de);
    }
  }

  private List<KeyPair> generateKeyPair(GenericRecord gr, VersionedSchema versionedSchema)
      throws DatabusException
  {
    Object o = null;
    Schema.Type st = null;
    List<Field> pkFieldList = versionedSchema.getPkFieldList();
    if(pkFieldList.isEmpty())
    {
      String pkFieldName = SchemaHelper.getMetaField(versionedSchema.getSchema(), "pk");
	  if (pkFieldName == null)
	  {
		throw new DatabusException("No primary key specified in the schema");
	  }
	  PrimaryKeySchema pkSchema = new PrimaryKeySchema(pkFieldName);
	  List<Schema.Field> fields = versionedSchema.getSchema().getFields();
	  for (int i = 0; i < fields.size(); i++)
	  {
		Schema.Field field = fields.get(i);
		if (pkSchema.isPartOfPrimaryKey(field))
		{
		  pkFieldList.add(field);
		}
	  }
    }
    List<KeyPair> kpl = new ArrayList<KeyPair>();
    for (Field field : pkFieldList)
    {
      o = gr.get(field.name());
      st = field.schema().getType();
      KeyPair kp = new KeyPair(o, st);
      kpl.add(kp);
    }
    if (kpl == null || kpl.isEmpty())
    {
      String pkFieldName = SchemaHelper.getMetaField(versionedSchema.getSchema(), "pk");
      StringBuilder sb = new StringBuilder();
      for (Schema.Field f : versionedSchema.getSchema().getFields())
      {
        sb.append(f.name()).append(",");
      }
      throw new DatabusException("pk is assigned to " + pkFieldName + " but fieldList is " + sb.toString());
    }
    return kpl;
  }

  private void generateAvroEvent(VersionedSchema vs, List<Column> cols, GenericRecord record)
      throws DatabusException
  {
    // Get Ordered list of field by dbFieldPosition
    List<Schema.Field> orderedFields = SchemaHelper.getOrderedFieldsByDBFieldPosition(vs);

    // Build Map<AvroFieldType, Columns>
    if (orderedFields.size() != cols.size())
    {
      throw new DatabusException("Mismatch in db schema vs avro schema");
    }

    int cnt = 0;
    Map<String, Column> avroFieldCol = new HashMap<String, Column>();

    for(Schema.Field field : orderedFields)
    {
      avroFieldCol.put(field.name(), cols.get(cnt));
      cnt++;
    }

    for(Schema.Field field : orderedFields)
    {
      if(field.schema().getType() == Schema.Type.ARRAY)
      {
        throw new DatabusException("The parser cannot handle ARRAY datatypes. Found in field: "+ field);
      }
      else
      {
        // The current database field being processed
        // (field is avro field name  and databaseFieldName is oracle field name (one to one mapping)
        String databaseFieldName = SchemaHelper.getMetaField(field, "dbFieldName").toLowerCase();
        _log.debug("databaseFieldName = " + databaseFieldName);
        //Insert the field into the generic record
        insertFieldIntoRecord(avroFieldCol, record, databaseFieldName, field);
      }
    }
    if ( _log.isDebugEnabled())
    {
      _log.debug("Generic record = " + record);
    }
  }

  /**
   * Given the following :
   * 1. A row data as a map of (dbFieldName, Column) data)
   * 2. A generic record to populate
   * 3. dbFieldName
   * 4. avroFieldName
   *
   * The method locates the Column for the dbFieldName, extracts the data as a Java Object,
   * inserts into the generic record with avroFieldName.name() as the key
   */
  private void insertFieldIntoRecord(
                                     Map<String, Column> eventFields,
                                     GenericRecord record,
                                     String dbFieldName,
                                     Schema.Field avroField)
                                         throws DatabusException
  {
    String f = avroField.name();
    Column fieldValue = eventFields.get(f);
    boolean isFieldNull = (fieldValue == null);
    Object fieldValueObj = null;
    try
    {
      if (! isFieldNull)
        fieldValueObj =  orToAvroType(fieldValue, avroField);
      else
        fieldValueObj = null;

      record.put(avroField.name(), fieldValueObj);
    }
    catch(DatabusException e)
    {
      _log.error("Unable to process field: " + avroField.name());
      throw e;
    }
    return;
  }

  /**
   * Given a OR Column, it returns a corresponding Java object that can be inserted into
   * AVRO record
 * @param avroField 
   */
  private Object orToAvroType(Column s, Field avroField)
      throws DatabusException
  {
	try
	{
	  return Or2AvroConvert.convert(s, avroField);
	}
	catch (Exception e)
	{
	  throw new DatabusRuntimeException("Unknown MySQL type in the event" + s.getClass() + " Object = " + s, e);
	}
  }

  /**
   * Creates the SCN from logId and offset
   *   SCN = {          Logid           }{         Offset         }
   *         |--- High Order 32 bits ---||--- Low Order 32 bits --|
   * @param logId
   * @param offset
   * @return
   */
  public static long scn(int logId, int offset)
  {
    long scn = logId;
    scn <<= 32;
    scn |= offset;
    return scn;
  }

  @Override
  public void run() {
    List<BinlogEventV4> eventList = new ArrayList<BinlogEventV4>();
    BinlogEventV4 event;
    while (!isShutdownRequested())
    {
      if (isPauseRequested())
      {
        LOG.info("Pause requested for ORListener. Pausing !!");
        signalPause();
        LOG.info("Pausing. Waiting for resume command");
        try
        {
          awaitUnPauseRequest();
        }
        catch (InterruptedException e)
        {
          _log.info("Interrupted !!");
        }
        LOG.info("Resuming ORListener !!");
        signalResumed();
        LOG.info("ORListener resumed !!");
      }

      eventList.clear();
      int eventNumber = _binlogEventQueue.drainTo(eventList);
      if (eventNumber == 0)
      {
        try
        {
          event = _binlogEventQueue.poll(_queueTimeoutMs, TimeUnit.MILLISECONDS);
          if (event != null)
          {
            eventList.add(event);
            eventNumber = eventList.size();
          }
        }
        catch (InterruptedException e)
        {
          _log.info("Interrupted when poll from _binlogEventQueue!!");
        }
      }
      for (int i = 0; i < eventNumber; i++)
      {
        event = eventList.get(i);
        if (event == null)
        {
          _log.error("Received null event");
          continue;
        }

        try {
          // Beginning of Txn
          if (event instanceof QueryEvent)
          {
            QueryEvent qe = (QueryEvent)event;
            String sql = qe.getSql().toString();
            if ("BEGIN".equalsIgnoreCase(sql))
            {
              _isBeginTxnSeen = true;
              _log.info("BEGIN sql: " + sql);
              _currTxnSizeInBytes = event.getHeader().getEventLength();
              startXtion(qe);
              continue;
            }
          }
          else if (event instanceof RotateEvent)
          {
            RotateEvent re = (RotateEvent) event;
            String fileName = re.getBinlogFileName().toString();
            _log.info("File Rotated : FileName :" + fileName + ", _binlogFilePrefix :" + _binlogFilePrefix);
            String fileNumStr = fileName.substring(fileName.lastIndexOf(_binlogFilePrefix) + _binlogFilePrefix.length() + 1);
            _currFileNum = Integer.parseInt(fileNumStr);
            _tableMapEvents.clear();
            continue;
          }

          if ( ! _isBeginTxnSeen )
          {
            if (_log.isDebugEnabled())
            {
              _log.debug("Skipping event (" + event
                  + ") as this is before the start of first transaction");
            }
            continue;
          }

          _currTxnSizeInBytes += event.getHeader().getEventLength();

          if (event instanceof QueryEvent)
          {
            QueryEvent qe = (QueryEvent)event;
            String sql = qe.getSql().toString();
            if ("COMMIT".equalsIgnoreCase(sql))
            {
              _log.debug("COMMIT sql: " + sql);
              endXtion(qe);
              continue;
            }
            else if ("ROLLBACK".equalsIgnoreCase(sql))
            {
              _log.debug("ROLLBACK sql: " + sql);
              rollbackXtion(qe);
              continue;
            }
            else
            {
              // Ignore DDL statements for now
              _log.debug("Likely DDL statement sql: " + sql);
              continue;
            }
          }
          else if (event instanceof XidEvent)
          {
            XidEvent xe = (XidEvent)event;
            long xid = xe.getXid();
            _log.debug("Treating XID event with xid = " + xid + " as commit for the transaction");
            endXtion(xe);
            continue;
          }
          else if (event instanceof FormatDescriptionEvent)
          {
            // we don't need process this event
            _log.info("received FormatDescriptionEvent event");
            continue;
          }
          else if (event instanceof WriteRowsEvent)
          {
            WriteRowsEvent wre = (WriteRowsEvent)event;
            insertRows(wre);
          }
          else if (event instanceof WriteRowsEventV2)
          {
            WriteRowsEventV2 wre = (WriteRowsEventV2)event;
            insertRows(wre);
          }
          else if (event instanceof UpdateRowsEvent)
          {
            UpdateRowsEvent ure = (UpdateRowsEvent)event;
            updateRows(ure);
          }
          else if (event instanceof UpdateRowsEventV2)
          {
            UpdateRowsEventV2 ure = (UpdateRowsEventV2)event;
            updateRows(ure);
          }
          else if (event instanceof DeleteRowsEventV2)
          {
            DeleteRowsEventV2 dre = (DeleteRowsEventV2)event;
            deleteRows(dre);
          }
          else if (event instanceof DeleteRowsEvent)
          {
            DeleteRowsEvent dre = (DeleteRowsEvent)event;
            deleteRows(dre);
          }
          else if (event instanceof TableMapEvent)
          {
            TableMapEvent tme = (TableMapEvent)event;
            processTableMapEvent(tme);
          }
          else
          {
            _log.warn("Skipping !! Unknown OR event e: " + event);
            continue;
          }

          if (_log.isDebugEnabled())
          {
            _log.debug("e: " + event);
          }
        } catch (Exception e) {
          _log.error("failed to process binlog event, event: " + event, e);
        }
      }
    }
    _log.info("ORListener Thread done");
    doShutdownNotify();
  }

  public void shutdownAll()
  {
    if(this.isAlive())
    {
      this.shutdown();
    }
    if (_transactionWriter != null && _transactionWriter.isAlive())
    {
      _transactionWriter.shutdown();
    }
  }
}
