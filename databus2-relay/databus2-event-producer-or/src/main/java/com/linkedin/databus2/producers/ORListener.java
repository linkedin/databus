package com.linkedin.databus2.producers;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
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
import com.google.code.or.common.glossary.column.BitColumn;
import com.google.code.or.common.glossary.column.BlobColumn;
import com.google.code.or.common.glossary.column.DateColumn;
import com.google.code.or.common.glossary.column.DatetimeColumn;
import com.google.code.or.common.glossary.column.DecimalColumn;
import com.google.code.or.common.glossary.column.DoubleColumn;
import com.google.code.or.common.glossary.column.EnumColumn;
import com.google.code.or.common.glossary.column.FloatColumn;
import com.google.code.or.common.glossary.column.Int24Column;
import com.google.code.or.common.glossary.column.LongColumn;
import com.google.code.or.common.glossary.column.LongLongColumn;
import com.google.code.or.common.glossary.column.NullColumn;
import com.google.code.or.common.glossary.column.SetColumn;
import com.google.code.or.common.glossary.column.ShortColumn;
import com.google.code.or.common.glossary.column.StringColumn;
import com.google.code.or.common.glossary.column.TimeColumn;
import com.google.code.or.common.glossary.column.TimestampColumn;
import com.google.code.or.common.glossary.column.TinyColumn;
import com.google.code.or.common.glossary.column.YearColumn;
import com.linkedin.databus.core.DatabusRuntimeException;
import com.linkedin.databus.core.DatabusThreadBase;
import com.linkedin.databus.core.DbusOpcode;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.producers.ds.DbChangeEntry;
import com.linkedin.databus2.producers.ds.KeyPair;
import com.linkedin.databus2.producers.ds.PerSourceTransaction;
import com.linkedin.databus2.producers.ds.PrimaryKeySchema;
import com.linkedin.databus2.producers.ds.Transaction;
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

  /** Track current table Id as reported in bin logs  */
  private long _currTableId = -1;

  /** Track current table name as reported in bin logs */
  private String _currTableName = "";

  /** Transaction object containing the current transaction that is being built from the Binlog **/
  private Transaction _transaction = null;

  /** Batch of events corresponding to single source (table) in the current transaction **/
  private PerSourceTransaction _perSourceTransaction = null;

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

  private BlockingQueue<BinlogEventV4> _binlogEventQueue = null;

  public ORListener(String name,
                    int currentFileNumber,
                    Logger log,
                    String binlogFilePrefix,
                    TransactionProcessor txnProcessor,
                    Map<String, Short> tableUriToSrcIdMap,
                    Map<String, String> tableUriToSrcNameMap,
                    SchemaRegistryService schemaRegistryService,
                    int maxQueueSize)
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
  }

  @Override
  public void onEvents(BinlogEventV4 event)
  {
    boolean isPut = false;
    do {
      try {
        isPut = _binlogEventQueue.offer(event, 100, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        _log.error("failed to put binlog event to binlogEventQueue event: " + event, e);
      }
    } while (!isPut && !isShutdownRequested());
  }

  private void processTableMapEvent(TableMapEvent tme)
  {
    String newTableName = tme.getDatabaseName().toString().toLowerCase() + "." + tme.getTableName().toString().toLowerCase();
    long newTableId = tme.getTableId();

    final boolean areTableNamesEqual = _currTableName.equals(newTableName);
    final boolean areTableIdsEqual = (_currTableId == newTableId);
    final boolean didTableNameChange = !(areTableNamesEqual && areTableIdsEqual);
    final boolean errorTransition = (areTableNamesEqual && !areTableIdsEqual) || (!areTableNamesEqual && areTableIdsEqual);

    if (_currTableName.isEmpty() && (_currTableId == -1))
    {
      // First TableMapEvent for the transaction. Indicates the first event in the transaction is yet to come
      startSource(newTableName, newTableId);
    }
    else if (didTableNameChange)
    {
      // Event will come for a new source. Invoke an endSource on currTableName, and a startSource on newTableName
      endSource();
      startSource(newTableName, newTableId);
    }
    else
    {
      _log.error("Unexpected : TableMap Event obtained :" + tme);
      throw new DatabusRuntimeException("Unexpected : TableMap Event obtained :" +
          " _currTableName = " + _currTableName +
          " _curTableId = " + _currTableId +
          " newTableName = " + newTableName +
          " newTableId = " + newTableId);
    }

    if (errorTransition)
    {
      throw new DatabusRuntimeException("TableName and TableId should change simultaneously or not" +
          " _currTableName = " + _currTableName +
          " _curTableId = " + _currTableId +
          " newTableName = " + newTableName +
          " newTableId = " + newTableId);
    }
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
    try
    {
      _txnProcessor.onEndTransaction(_transaction);
    } catch (DatabusException e3)
    {
      _log.error("Got exception in the transaction handler ",e3);
      throw new DatabusRuntimeException(e3);
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
    _currTableName = "";
    _currTableId = -1;
    _perSourceTransaction = null;
    _transaction = null;
    _currTxnSizeInBytes = 0;
  }

  private void startSource(String newTableName, long newTableId)
  {
    _currTableName = newTableName;
    _currTableId = newTableId;
    if (_perSourceTransaction == null)
    {
      Short srcId = _tableUriToSrcIdMap.get(_currTableName);

      if (null == srcId)
      {
        throw new DatabusRuntimeException("Could not find a matching logical source for table Uri (" + _currTableName + ")" );
      }
      assert(_transaction != null);
      _perSourceTransaction = new PerSourceTransaction(srcId);
      _transaction.mergePerSourceTransaction(_perSourceTransaction);
    }
    else
    {
      throw new DatabusRuntimeException("Seems like a startSource has been received without an endSource for previous source");
    }
  }

  private void endSource()
  {
    if (_perSourceTransaction != null)
    {
      _perSourceTransaction = null;
    }
    else
    {
      throw new DatabusRuntimeException("_perSourceTransaction should not be null in endSource()");
    }
  }

  private void deleteRows(DeleteRowsEventV2 dre)
  {
    List<Row> lp = dre.getRows();
    frameAvroRecord(dre.getHeader(), lp, DbusOpcode.DELETE);
  }

  private void deleteRows(DeleteRowsEvent dre)
  {
    List<Row> lp = dre.getRows();
    frameAvroRecord(dre.getHeader(), lp, DbusOpcode.DELETE);
  }

  private void updateRows(UpdateRowsEvent ure)
  {
    List<Pair<Row>> lp = ure.getRows();
    List<Row> lr =  new ArrayList<Row>(lp.size());
    for (Pair<Row> pr: lp)
    {
      Row r = pr.getAfter();
      lr.add(r);
    }
    frameAvroRecord(ure.getHeader(), lr, DbusOpcode.UPSERT);
  }

  private void updateRows(UpdateRowsEventV2 ure)
  {
    List<Pair<Row>> lp = ure.getRows();
    List<Row> lr =  new ArrayList<Row>(lp.size());
    for (Pair<Row> pr: lp)
    {
      Row r = pr.getAfter();
      lr.add(r);
    }
    frameAvroRecord(ure.getHeader(), lr, DbusOpcode.UPSERT);
  }

  private void insertRows(WriteRowsEvent wre)
  {
    frameAvroRecord(wre.getHeader(), wre.getRows(), DbusOpcode.UPSERT);
  }

  private void insertRows(WriteRowsEventV2 wre)
  {
    frameAvroRecord(wre.getHeader(), wre.getRows(), DbusOpcode.UPSERT);
  }

  private void frameAvroRecord(BinlogEventV4Header bh, List<Row> rl, final DbusOpcode doc)
  {
    try
    {
      final long timestampInNanos = bh.getTimestamp() * 1000000L;
      final long scn = scn(_currFileNum, (int)bh.getPosition());
      final boolean isReplicated = false;
      VersionedSchema vs = _schemaRegistryService.fetchLatestVersionedSchemaBySourceName(_tableUriToSrcNameMap.get(_currTableName));
      Schema schema = vs.getSchema();

      if ( _log.isDebugEnabled())
        _log.debug("File Number :" + _currFileNum + ", Position :" + (int)bh.getPosition() + ", SCN =" + scn);

      for(Row r: rl)
      {
        List<Column> cl = r.getColumns();
        GenericRecord gr = new GenericData.Record(schema);
        generateAvroEvent(schema, cl, gr);

        List<KeyPair> kps = generateKeyPair(cl, schema);

        DbChangeEntry db = new DbChangeEntry(scn, timestampInNanos, gr, doc, isReplicated, schema, kps);
        _perSourceTransaction.mergeDbChangeEntrySet(db);
      }
    } catch (NoSuchSchemaException ne)
    {
      throw new DatabusRuntimeException(ne);
    } catch (DatabusException de)
    {
      throw new DatabusRuntimeException(de);
    }
  }

  private List<KeyPair> generateKeyPair(List<Column> cl, Schema schema)
      throws DatabusException
  {

    Object o = null;
    Schema.Type st = null;

    // Build PrimaryKeySchema
    String pkFieldName = SchemaHelper.getMetaField(schema, "pk");
    if(pkFieldName == null)
    {
      throw new DatabusException("No primary key specified in the schema");
    }

    PrimaryKeySchema pkSchema = new PrimaryKeySchema(pkFieldName);
    List<Schema.Field> fields = schema.getFields();
    List<KeyPair> kpl = new ArrayList<KeyPair>();
    int cnt = 0;
    for(Schema.Field field : fields)
    {
      if (pkSchema.isPartOfPrimaryKey(field))
      {
        o = cl.get(cnt).getValue();
        st = field.schema().getType();
        KeyPair kp = new KeyPair(o, st);
        kpl.add(kp);
      }
      cnt++;
    }

    return kpl;
  }

  private void generateAvroEvent(Schema schema, List<Column> cols, GenericRecord record)
      throws DatabusException
  {
    // Get Ordered list of field by dbFieldPosition
    List<Schema.Field> orderedFields = SchemaHelper.getOrderedFieldsByMetaField(schema, "dbFieldPosition", new Comparator<String>() {

      @Override
      public int compare(String o1, String o2)
      {
        Integer pos1 = Integer.parseInt(o1);
        Integer pos2 = Integer.parseInt(o2);

        return pos1.compareTo(pos2);
      }
    });

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
        fieldValueObj =  orToAvroType(fieldValue);
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
   */
  private Object orToAvroType(Column s)
      throws DatabusException
  {
    if (s instanceof BitColumn)
    {
      // This is in  byte order
      BitColumn bc = (BitColumn) s;
      byte[] ba = bc.getValue();
      ByteBuffer b = ByteBuffer.wrap(ba);
      return b;
    }
    else if (s instanceof BlobColumn)
    {
      BlobColumn bc = (BlobColumn) s;
      byte[] ba = bc.getValue();
      return ByteBuffer.wrap(ba);
    }
    else if (s instanceof DateColumn)
    {
      DateColumn dc =  (DateColumn) s;
      Date d = dc.getValue();
      Long l = d.getTime();
      return l;
    }
    else if (s instanceof DatetimeColumn)
    {
      DatetimeColumn dc = (DatetimeColumn) s;
      Date d = dc.getValue();
      Long t1 = (d.getTime()/1000) * 1000; //Bug in OR for DateTIme and Time data-types. MilliSeconds is not available for these columns but is set with currentMillis() wrongly.
      return t1;
    }
    else if (s instanceof DecimalColumn)
    {
      DecimalColumn dc = (DecimalColumn) s;
      _log.info("dc Value is :" + dc.getValue());
      String s1 = dc.getValue().toString(); // Convert to string for preserving precision
      _log.info("Str : " + s1);
      return s1;
    }
    else if (s instanceof DoubleColumn)
    {
      DoubleColumn dc = (DoubleColumn) s;
      Double d = dc.getValue();
      return d;
    }
    else if (s instanceof EnumColumn)
    {
      EnumColumn ec = (EnumColumn) s;
      Integer i = ec.getValue();
      return i;
    }
    else if (s instanceof FloatColumn)
    {
      FloatColumn fc = (FloatColumn) s;
      Float f = fc.getValue();
      return f;
    }
    else if (s instanceof Int24Column)
    {
      Int24Column ic = (Int24Column) s;
      Integer i = ic.getValue();
      return i;
    }
    else if (s instanceof LongColumn)
    {
      LongColumn lc = (LongColumn) s;
      Integer i = lc.getValue();
      return i;
    }
    else if (s instanceof LongLongColumn)
    {
      LongLongColumn llc = (LongLongColumn) s;
      Long l = llc.getValue();
      return l;
    }
    else if (s instanceof NullColumn)
    {
      return null;
    }
    else if (s instanceof SetColumn)
    {
      SetColumn sc = (SetColumn) s;
      Long l = sc.getValue();
      return l;
    }
    else if (s instanceof ShortColumn)
    {
      ShortColumn sc = (ShortColumn) s;
      Integer i = sc.getValue();
      return i;
    }
    else if (s instanceof StringColumn)
    {
      StringColumn sc = (StringColumn) s;
      String str = new String(sc.getValue(), Charset.defaultCharset());
      return str;
    }
    else if (s instanceof TimeColumn)
    {
      TimeColumn tc = (TimeColumn) s;
      Time t = tc.getValue();
      /**
       * There is a bug in OR where instead of using the default year as 1970, it is using 0070.
       * This is a temporary measure to resolve it by working around at this layer. The value obtained from OR is subtracted from "0070-00-01 00:00:00"
       */
      Calendar c = Calendar.getInstance();
      c.set(70, 0, 1, 0, 0, 0);
      // round off the milli-seconds as TimeColumn type has only seconds granularity but Calendar implementation
      // includes milli-second (System.currentTimeMillis() at the time of instantiation)
      long rawVal = (c.getTimeInMillis()/1000) * 1000;
      long val2 = (t.getTime()/1000) * 1000;
      long offset = val2 - rawVal;
      return offset;
    }
    else if (s instanceof TimestampColumn)
    {
      TimestampColumn tsc = (TimestampColumn) s;
      Timestamp ts = tsc.getValue();
      Long t = ts.getTime();
      return t;
    }
    else if (s instanceof TinyColumn)
    {
      TinyColumn tc = (TinyColumn) s;
      Integer i = tc.getValue();
      return i;
    }
    else if (s instanceof YearColumn)
    {
      YearColumn yc = (YearColumn) s;
      Integer i = yc.getValue();
      return i;
    }
    else
    {
      throw new DatabusRuntimeException("Unknown MySQL type in the event" + s.getClass() + " Object = " + s);
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
}
