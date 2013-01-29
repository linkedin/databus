package com.linkedin.databus.bootstrap.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.linkedin.databus.bootstrap.utils.BootstrapAuditTableReader.ResultSetEntry;
import com.linkedin.databus.bootstrap.utils.BootstrapSrcDBEventReader.PrimaryKeyTxn;
import com.linkedin.databus.client.DbusEventAvroDecoder;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus2.producers.db.MonitoredSourceInfo;
import com.linkedin.databus2.schemas.FileSystemSchemaRegistryService;
import com.linkedin.databus2.schemas.SchemaRegistryService;
import com.linkedin.databus2.schemas.VersionedSchema;
import com.linkedin.databus2.schemas.VersionedSchemaSet;
import com.linkedin.databus2.schemas.utils.SchemaHelper;
import com.linkedin.databus2.util.DBHelper;

public class BootstrapAuditMain
{
    public static final String MODULE = BootstrapAuditMain.class.getName();
    public static final Logger LOG = Logger.getLogger(MODULE);

    static
    {
      BasicConfigurator.configure();
      Logger.getRootLogger().setLevel(Level.INFO);
    }

	/**
	 * @param args
	 */
	public static void main(String[] args)
	    throws Exception
	{
	   BootstrapSeederMain.init(args);

	   BootstrapSeederMain.StaticConfig staticConfig = BootstrapSeederMain.getStaticConfig();

	   int interval = staticConfig.getController().getCommitInterval();

	   int sourceChunkSize = staticConfig.getController().getNumRowsPerQuery();

	   List<MonitoredSourceInfo> sources = BootstrapSeederMain.getSources();
	   BootstrapDBSeeder seeder = BootstrapSeederMain.getSeeder();
	   BootstrapSrcDBEventReader seedController = BootstrapSeederMain.getReader();

	   Map<String, String> pKeyNameMap = seedController.getpKeyNameMap();
	   Map<String,DbusEventKey.KeyType> pKeyTypeMap = seedController.getpKeyTypeMap();

	   for(MonitoredSourceInfo source : sources)
	   {
	     short srcId = source.getSourceId();

	                             new ConcurrentHashMap<Long,ResultSetEntry>();
	     OracleTableReader oracleReader = null;
	     MySQLTableReader mySQLReader = null;

	     try
	     {

	    	 SchemaRegistryService schemaRegistry = FileSystemSchemaRegistryService.build(staticConfig.getSchemaRegistry().getFileSystem());
	    	 Map<Short,String> schemaSet= schemaRegistry.fetchAllSchemaVersionsByType(source.getSourceName());
	    	 VersionedSchemaSet vSchemaSet = new VersionedSchemaSet();
	    	 Iterator<Map.Entry<Short,String>> it = schemaSet.entrySet().iterator();
	    	 while (it.hasNext()) {
	    		 Map.Entry<Short,String> pairs = it.next();
	    		 Schema s = Schema.parse(pairs.getValue());
	    		 VersionedSchema vs = new VersionedSchema(s.getFullName(),pairs.getKey(), s);
	    		 vSchemaSet.add(vs);
	    	 }
	       
	    	 /* Try and identify the schema key */
	    	 VersionedSchema vschema = schemaRegistry.fetchLatestVersionedSchemaByType(source.getSourceName());
	    	 Schema schema = Schema.parse(vschema.getSchema().toString());
	    	 LOG.info("Schema =" + vschema.getSchema() + "version=" + vschema.getVersion() + " name=" + vschema.getSchemaBaseName());

	    	 /*Determine type of field txn */
	    	 Field txnFieldType = schema.getField("txn");
	    	 if (txnFieldType==null) 
	    	 {
	    		 throw new Exception ("Unable to find field called 'txn'. Cannot proceeed\n");
	    	 }
	    	 Type txnType = SchemaHelper.getAnyType(txnFieldType);
	    	 
	    	 /* Determine primary key of schema. This is assumed to be invariant across versions */
	    	 String keyOverrideName = SchemaHelper.getMetaField(schema, "pk");
	    	 String keyColumnName = "key";
	    	 if (null != keyOverrideName)
	    	 {
	    		 keyColumnName = keyOverrideName;
	    	 }
	    	 Field pkeyField = schema.getField(keyColumnName);
	    	 if ( null == pkeyField) {
	    		 keyColumnName ="id";
	    		 pkeyField = schema.getField("id");
	    	 }
	    	 if (null == pkeyField)
	    	 {
	    		 throw new Exception("Unable to get the primary key for schema. Schema is :" + schema);
	    	 }

	    	 DbusEventAvroDecoder decoder = new DbusEventAvroDecoder(vSchemaSet);

	    	 BootstrapAuditTester auditor = new BootstrapAuditTester(schema);
	    	 List<BootstrapAuditTester> auditors = new ArrayList<BootstrapAuditTester>();
	    	 auditors.add(auditor);

	    	 oracleReader= new OracleTableReader(
	    			 BootstrapSeederMain.getDataStore().getConnection(),
	    			 BootstrapSrcDBEventReader.getTableName(source),
	    			 pkeyField,
	    			 SchemaHelper.getMetaField(pkeyField, "dbFieldName"),
	    			 SchemaHelper.getAnyType(pkeyField),
	    			 sourceChunkSize,
	    			 seedController.getPKIndex(source),
	    			 seedController.getQueryHint(source));

	    	 mySQLReader = new MySQLTableReader(
	    			 seeder.getConnection(),
	    			 seeder.getTableName(srcId),
	    			 pkeyField,
	    			 "id", //THis is the primary key always for bootstrapDB
	    			 Type.LONG,
	    			 interval);
	    	 
	    	 long bootstrapStartRow=1;
	    	 long bootstrapInc = 10;

	    	 TableComparator comparator = new TableComparator(oracleReader,
	    			 mySQLReader,
	    			 auditor,
	    			 decoder,
	    			 interval,
	    			 pKeyNameMap.get(source.getEventView()),
	    			 pKeyTypeMap.get(source.getEventView()),
	    			 txnType,
	    			 bootstrapStartRow,
	    			 bootstrapInc);

	    	 boolean success=false;
	    	 if (BootstrapSeederMain.getValidationType().equals("point"))
	    	 {
	    		 success = comparator.compareRecordsPoint();
	    	 } 
			 else if (BootstrapSeederMain.getValidationType().equals("pointBs"))
			 {
		     	 success = comparator.compareRecordsPointBs();
		  	 }
	    	 else 
	    	 {
	    		 success = comparator.compareRecordsNew();
	    	 }

	    	 if ( success )
	    		 LOG.info("Audit completed successfully");
	    	 else
	    		 LOG.error("Audit FAILED !!! ");
	     } catch (Exception ex) {
	    	 LOG.error("Caught an exception ex", ex);
	    	 throw ex;
	     }	 finally {
	    	 if ( null != oracleReader)
	    		 oracleReader.close();
	     }
	   }
	   DBHelper.close(seeder.getConnection());
	}

	public static class AuditStats implements Runnable {


 	   private long _numProcessed = 0;
 	   private long _numKeyEqual = 0;
 	   private long _numKeyAbsentInBootstrap = 0;
 	   private long _numKeyAbsentInOracle = 0;
 	   private long _numDataEqual = 0;
 	   private long _numOlderTxnInOracle = 0;
 	   private long _numOlderTxnInBootstrap = 0;
 	   
 	   private boolean _shutdown = false;
 	   final private int _intervalInSec ;
	   private long _numError = 0;
 	    
 	   public AuditStats () 
 	   {
 		   this (60);
 	   }
 	    public AuditStats(int intervalSec) 
 	    {
 	    	_intervalInSec = intervalSec;
 	    }
 	    
 	    
		@Override
		public void run() 
		{
			while (!_shutdown)
			{
				print();
				try {
					Thread.sleep(_intervalInSec*1000);
				} catch (InterruptedException e) {
					shutdown();
				}
			}
		}
		
		public void print()
		{
			LOG.info("AuditStats\n" + this);
		}
		
		public String toString()
		{
			StringBuilder  s = new StringBuilder(512);
			s.append(" numProcessed=").append(_numProcessed)
			.append(" numKeyEqual=").append(_numKeyEqual)
			.append(" numDataEqual=").append(_numDataEqual)
			.append(" numKeyAbsentInOracle=").append(_numKeyAbsentInOracle)
			.append(" numKeyAbsentInBootstrap=").append(_numKeyAbsentInBootstrap)
			.append(" numOlderTxnInOracle=").append(_numOlderTxnInOracle)
			.append(" numOlderTxnInBootstrap=").append(_numOlderTxnInBootstrap)
			.append(" numError=").append(_numError);
			return s.toString();
		}
		
		
		public synchronized void shutdown()
		{
			_shutdown=true;
		}
		public long getNumProcessed() {
			return _numProcessed;
		}
		public long getNumKeyEqual() {
			return _numKeyEqual;
		}
		public long getNumKeyAbsentInBootstrap() {
			return _numKeyAbsentInBootstrap;
		}
		public long getNumKeyAbsentInOracle() {
			return _numKeyAbsentInOracle;
		}
		public long getNumDataEqual() {
			return _numDataEqual;
		}
		public long getNumOlderTxnInOracle() {
			return _numOlderTxnInOracle;
		}
		public long getNumOlderTxnInBootstrap() {
			return _numOlderTxnInBootstrap;
		}
		public boolean isShutdown() {
			return _shutdown;
		}
		public int getIntervalInSec() {
			return _intervalInSec;
		}
		public long getNumError() 
		{
			return _numError;
		}
		public synchronized void incNumProcessed() {
			_numProcessed++;
		}
		public synchronized void incNumnKeyAbsentInOracle() {
			_numKeyAbsentInOracle++;
		}
		public synchronized void incNumKeyAbsentInBootstrap() {
			_numKeyAbsentInBootstrap++;
		}
		public synchronized void incNumOlderTxnInBootstrap() {
			_numOlderTxnInBootstrap++;
		}
		public synchronized void incNumOlderTxnInOracle() {
			_numOlderTxnInOracle++;
		}
		public synchronized void incNumDataEqual() {
			_numDataEqual++;
		}
		public synchronized void incNumKeyEqual() {
			_numKeyEqual++;
		}
		public synchronized void incNumError() {
			_numError++;
		}
		
		
		 
	}
	
	public  static  class TableComparator
	{
	   private final OracleTableReader _srcReader;
	   private final MySQLTableReader _destReader;
	   private final DbusEventAvroDecoder _decoder;
	   private final BootstrapAuditTester _auditor;
	   private final int          _interval;
	   private final String         _pKey;
	   private final DbusEventKey.KeyType  _pKeyType;
	   private final Type _txnType;
	   private long _bootstrapStartRow = -1 ;
	   private long _bootstrapRowIncrement = 1;

	   public TableComparator(OracleTableReader     srcReader,
	                          MySQLTableReader      destReader,
	                          BootstrapAuditTester  auditor,
	                          DbusEventAvroDecoder  decoder,
	                          int                   interval,
	                          String                pKey,
	                          DbusEventKey.KeyType  pKeyType,
	                          Type txnType,
	                          long bootstrapStartRow ,
	                          long bootstrapRowIncrement
	                          )
	   {
	     _srcReader = srcReader;
	     _destReader = destReader;
	     _decoder = decoder;
	     _interval = interval;
	     _auditor = auditor;
	     _pKey = pKey;
	     _pKeyType = pKeyType;
	     _txnType = txnType;
	     _bootstrapStartRow = bootstrapStartRow;
	     _bootstrapRowIncrement = bootstrapRowIncrement;
	   }
	   

	   public boolean compareRecordsPointBs() throws SQLException
	   {
		   AuditStats stats = new AuditStats();
		   Thread statsThread = new Thread(stats);
		   try
		   {
			   ResultSet srcRs = null;
			   ResultSet destRs = null;
			   /*
			   long srcNumRows = _srcReader.getNumRecords(_pKey);
			   long destNumRows = _destReader.getNumRecords("id");

			   LOG.info("Current Number of Rows in Source DB :" + srcNumRows);
			   LOG.info("Current Number of Rows in Destination DB :" + destNumRows);
			*/
			   statsThread.start();
			
			   String lastSrcKey="0";
			   boolean oracleDone = true;
			   boolean done = false;
			   while (!done)
			   {

				   //Read the next chunks from src and dest db's
				   if (oracleDone)
				   {
					   done =true;
					   DBHelper.close(srcRs);
					   srcRs = _srcReader.getRecords(lastSrcKey);
				   }
				   
				  
				   oracleDone = !srcRs.next();
				   done  = done && oracleDone;
				   if (!oracleDone)
				   {
					   stats.incNumProcessed();
					   /* For getting next row in   boostrap db and oracle */
					   /* The keys on which the query result set is ordered */
					   lastSrcKey = getKey(srcRs);
					   //read one record from BS
					   DBHelper.close(destRs);
					   destRs = _destReader.getRecord(lastSrcKey);
					   boolean found = destRs.next();
					   //Compare txn_id, key of both oracle and bs sources; 
					   //src_txn (oracle) , dst_txn(bootstrap) : destKey  is the key field in bs and srcKey is corresponding one in oracle
					   if (found)
					   {
						   String destKey = destRs.getString(3);
						   stats.incNumKeyEqual();
						   /* For reading txnid, key which will form the basis of comparison */
						   long srcTxnId = srcRs.getLong(2);
						   long dstTxnId = getDestTxnId(destRs);

						   //assumption: txnids are monotonically increasing
						   if (srcTxnId == dstTxnId) 
						   {
							   boolean result = _auditor.compareRecord(srcRs,destRs,_decoder);
							   if (result)
							   {
								   stats.incNumDataEqual();
							   }
						   }
						   else if (srcTxnId < dstTxnId)
						   {
							   stats.incNumOlderTxnInOracle();
						   }
						   else
						   {
							   //older txn in bootstrap;
							   stats.incNumOlderTxnInBootstrap();
						   }
					   }
					   else 
					   {
						   LOG.info("Absent in bootstrap: " + lastSrcKey);
						   //key present in oracle but not in bootstrap;
						   stats.incNumKeyAbsentInBootstrap();
					   }
				   }
				   
			   }
			   LOG.info("Done with audit- end of stream reached\n");
			   stats.shutdown();
			   statsThread.join();
			   return stats.getNumProcessed()==stats.getNumDataEqual();
		   }
		   catch (SQLException e) 
		   {
			   stats.shutdown();
			   statsThread.interrupt();
			   throw e;
		   }
		   catch (InterruptedException e) 
		   {
			   	
		   }
		   LOG.info("Done with audit- end of stream reached\n");
		   return stats.getNumProcessed()==stats.getNumDataEqual();
		   
	   }


	   public boolean compareRecordsPoint() throws SQLException
	   {
		   AuditStats stats = new AuditStats();
		   Thread statsThread = new Thread(stats);
		   try
		   {
			   ResultSet srcRs = null;
			   ResultSet destRs = null;

			   statsThread.start();

			   long lastDestId = _bootstrapStartRow;
			   boolean bootstrapDone = true;
			   boolean done = false;
			   while (!done)
			   {

				   if (bootstrapDone)
				   {
					   done= true;
					   DBHelper.close(destRs);
					   destRs = _destReader.getRecords(lastDestId + _bootstrapRowIncrement);
				   }

				   bootstrapDone = !destRs.next();

				   done  = done && bootstrapDone;
				   if (!bootstrapDone)
				   {
					   stats.incNumProcessed();
					   /* For getting next row in   boostrap db and oracle */
					   lastDestId = destRs.getLong(1);

					   String destKey = destRs.getString(3);
					   //read that one record from Oracle
					   DBHelper.close(srcRs);
					   srcRs = _srcReader.getRecord(destKey);
					   boolean found = srcRs.next();

					   if (found)
					   {
						   stats.incNumKeyEqual();
						   /* For reading txnid, key which will form the basis of comparison */
						   long srcTxnId = srcRs.getLong(2);
						   long dstTxnId = getDestTxnId(destRs);
						   if (dstTxnId != 0)
						   {
							   //assumption: txnids are monotonically increasing
							   if (srcTxnId == dstTxnId) 
							   {
								   boolean result = _auditor.compareRecord(srcRs,destRs,_decoder);
								   if (result)
								   {
									   stats.incNumDataEqual();
								   }
							   }
							   else if (srcTxnId < dstTxnId)
							   {
								   //older  txn in oracle - this cannot happen
								   stats.incNumOlderTxnInOracle();
							   }
							   else
							   {
								   //older txn in bootstrap;
								   stats.incNumOlderTxnInBootstrap();
							   }
						   } 
						   else
						   {
							   stats.incNumError();
						   }
					   }
					   else
					   {	
						   //key present in bootstrap but not in oracle;
						   stats.incNumnKeyAbsentInOracle();
					   }
				   }
			   }
			   LOG.info("Done with audit- end of stream reached\n");
			   stats.shutdown();
			   statsThread.join();
			   return stats.getNumProcessed()==stats.getNumDataEqual();
		   }
		   catch (SQLException e) 
		   {
			   stats.shutdown();
			   statsThread.interrupt();
			   throw e;
		   }
		   catch (InterruptedException e) 
		   {

		   }
		   LOG.info("Done with audit- end of stream reached\n");
		   return stats.getNumProcessed()==stats.getNumDataEqual();
	   }
	   
	   public boolean compareRecordsNew() throws SQLException
	   {
		   AuditStats stats = new AuditStats();
		   Thread statsThread = new Thread(stats);
		   try
		   {
			   ResultSet srcRs = null;
			   ResultSet destRs = null;
			   /*
			   long srcNumRows = _srcReader.getNumRecords(_pKey);
			   long destNumRows = _destReader.getNumRecords("id");

			   LOG.info("Current Number of Rows in Source DB :" + srcNumRows);
			   LOG.info("Current Number of Rows in Destination DB :" + destNumRows);
			*/
			   statsThread.start();
			
			   long lastDestId = -1;
			   String lastSrcKey="0";
			   int compareResult = 0;
			   boolean oracleDone = true;
			   boolean bootstrapDone = true;
			   boolean done = false;
			   while (!done)
			   {

				   //Read the next chunks from src and dest db's
				   if (oracleDone)
				   {
					   done =true;
					   DBHelper.close(srcRs);
					   srcRs = _srcReader.getRecords(lastSrcKey);
				   }
				   
				   if (bootstrapDone)
				   {
					   done= true;
					   DBHelper.close(destRs);
					   destRs = _destReader.getRecords(lastDestId + 1);
				   }
				   
				   //Iterate over two streams in 'key' order
				   if (compareResult <= 0)
				   {
					   oracleDone = !srcRs.next();
				   }
				   if (compareResult >= 0) 
				   {
					   bootstrapDone = !destRs.next();
				   }
				   
				   done  = done && (bootstrapDone || oracleDone);
				   if (!bootstrapDone && !oracleDone)
				   {
					   stats.incNumProcessed();
					   /* For getting next row in   boostrap db and oracle */
					   lastDestId = destRs.getLong(1);
					   /* The keys on which the query result set is ordered */
					   lastSrcKey = getKey(srcRs);
					   String destKey = destRs.getString(3);

					   compareResult = keyCompare(lastSrcKey,destKey);
					   //Compare txn_id, key of both oracle and bs sources; 
					   //src_txn (oracle) , dst_txn(bootstrap) : destKey  is the key field in bs and srcKey is corresponding one in oracle
					   if (compareResult == 0)
					   {
						   stats.incNumKeyEqual();
						   /* For reading txnid, key which will form the basis of comparison */
						   long srcTxnId = srcRs.getLong(2);
						   long dstTxnId = getDestTxnId(destRs);

						   //assumption: txnids are monotonically increasing
						   if (srcTxnId == dstTxnId) 
						   {
							   boolean result = _auditor.compareRecord(srcRs,destRs,_decoder);
							   if (result)
							   {
								   stats.incNumDataEqual();
							   }
						   }
						   else if (srcTxnId < dstTxnId)
						   {
							   //older  txn in oracle - this cannot happen
							   stats.incNumOlderTxnInOracle();
						   }
						   else
						   {
							   //older txn in bootstrap;
							   stats.incNumOlderTxnInBootstrap();
						   }
					   }
					   else if (compareResult < 0)
					   {
						   LOG.info("Absent in bootstrap: " + lastSrcKey);
						   //key present in oracle but not in bootstrap;
						   stats.incNumKeyAbsentInBootstrap();
					   }
					   else
					   {	
						   LOG.info("Absent in oracle: " + destKey);
						   //key present in bootstrap but not in oracle;
						   stats.incNumnKeyAbsentInOracle();
					   }
				   }
				   else if (!bootstrapDone && oracleDone)
				   {
					   compareResult=-1;
				   }	
				   else if (!oracleDone && bootstrapDone) 
				   {
					   compareResult=1;
				   }
				   else
				   {
					   compareResult=0;
				   }
			   }
			   LOG.info("Done with audit- end of stream reached\n");
			   stats.shutdown();
			   statsThread.join();
			   return stats.getNumProcessed()==stats.getNumDataEqual();
		   }
		   catch (SQLException e) 
		   {
			   stats.shutdown();
			   statsThread.interrupt();
			   throw e;
		   }
		   catch (InterruptedException e) 
		   {
			   	
		   }
		   LOG.info("Done with audit- end of stream reached\n");
		   return stats.getNumProcessed()==stats.getNumDataEqual();
	   }
	   
	   protected String getKey(ResultSet srcRs) throws SQLException
	   {
		   if (_pKeyType == DbusEventKey.KeyType.LONG)
		   {
			   Long key = srcRs.getLong(_pKey);
			   return key.toString();
		   }
		   return srcRs.getString(_pKey);
	   }
	   
	   protected int keyCompare(String keySrc, String keyDst)
	   {
		   if (_pKeyType==DbusEventKey.KeyType.LONG) 
		   {
			   long srcKeyLong = Long.parseLong(keySrc);
			   long destKeyLong = Long.parseLong(keyDst);
			   return (int) (srcKeyLong - destKeyLong);
		   } 
		   return keySrc.compareTo(keyDst);
	   }
	   
	   private long getDestTxnId(ResultSet bsRes)  throws SQLException {
		  GenericRecord avroRec =  _auditor.getGenericRecord(bsRes, _decoder);
		  if (avroRec == null)
		  {
			  LOG.error("No avro record skipping");
			  return 0;
		  }
		  Object txnId =  avroRec.get("txn");
		  if (txnId == null) 
		  {
			  LOG.error("Could not find a field called 'txn' in avro event in bootstrap db");
			  return 0;
		  }
		  switch (_txnType)
		  {
		  	case LONG:
		  		if (txnId instanceof Integer)
		  		{
		  			Integer i= (Integer) txnId;
		  			return i.longValue();
		  		}
		  		else if (txnId instanceof Long)
		  		{
		  			return (Long) txnId;	
		  		}
		  	case INT:
		  		Integer i = (Integer) txnId;
		  		return i.longValue();
		  	default:
		  		return 0;
		  }
	   }
	}


       

	   public static class MySQLTableReader
	   extends BootstrapAuditTableReader
	   {
		   PreparedStatement _stmt = null;
		   
		   public MySQLTableReader(
				   Connection conn,
				   String tableName,
				   Field  pkeyField,
				   String pkeyName,
				   Type   pkeyType,
				   int interval) throws SQLException
		   {
			   super(conn,tableName,pkeyField,pkeyName,pkeyType,interval);
			   StringBuilder sql = new StringBuilder();
			   sql.append("select id,scn,srckey,val from " ).append(_tableName);
			   sql.append(" where srckey= ?");
			   _stmt = _conn.prepareStatement(sql.toString());

		   }

		   public ResultSet getRecord(String lastSrcKey) throws SQLException {
			   ResultSet rs = null;
			   try
			   {
				   _stmt.setString(1,lastSrcKey);
				   //stmt.setFetchSize(10000);
				   //stmt.setMaxRows(10);
				   rs = _stmt.executeQuery();
			   } catch ( SQLException sqlEx) {
				   DBHelper.close(rs, null, null);
				   throw sqlEx;
			   }
			   return rs;
		   }

		@Override
		   public PreparedStatement getFetchStmt(long fromId)
				   throws SQLException
				   {
			   StringBuilder sql = new StringBuilder();

			   sql.append(" select id, scn, srckey, val from ").append(_tableName);
			   sql.append(" where id >= ?");
			   sql.append(" limit ?");
			   String stmtStr =  sql.toString();

			   LOG.info("MySQL Query =" + stmtStr + ",FromID =" + fromId + ",Limit=" + (_interval));
			   PreparedStatement stmt = _conn.prepareStatement(stmtStr);

			   stmt.setLong(1, fromId);
			   stmt.setLong(2, _interval);

			   return stmt;
				   }

		   @Override
		   public PreparedStatement getFetchStmt(String from)
				   throws SQLException
				   {
			   throw new RuntimeException("This method is not expected to be called !!");
				   }


		
	   }

	   public static class OracleTableReader
	   extends BootstrapAuditTableReader
	   {
		   private Map<Field,String>   _dbFieldMap;
		   private Map<Field,Type>     _dbFieldTypeMap;
		   private final DbusEventKey.KeyType _dbusKeyType;
		   private final String        _pkIndex;
		   private final String        _queryHint;
		   PreparedStatement _stmt = null;
           Method _setLobPrefetchSizeMethod = null;
           Class _oraclePreparedStatementClass = null;
		   public OracleTableReader(
				   Connection conn,
				   String tableName,
				   Field  pkeyField,
				   String pkeyName,
				   Type   pkeyType,
				   int    interval,
				   String pkIndex,
				   String queryHint) throws SQLException
				   {
			   super(conn,tableName, pkeyField,pkeyName,pkeyType,interval);

			   if ( (pkeyType == Type.INT) || (pkeyType == Type.LONG) )
			   {
				   _dbusKeyType = DbusEventKey.KeyType.LONG;
			   } else {
				   _dbusKeyType = DbusEventKey.KeyType.STRING;
			   }

			   _pkIndex = pkIndex;
			   _queryHint = queryHint;
			   String sql =  generatePointQuery(_tableName,_pkeyName, _pkIndex, _queryHint);

			   _stmt = _conn.prepareStatement(sql);

			   LOG.info("Tablename=" + tableName);
			   LOG.info("Point Oracle Query =" + sql);

			   try
			   {
				   File file = new File("ojdbc6-11.2.0.2.0.jar");
				   URL ojdbcJarFile = file.toURL();
				   URLClassLoader cl = URLClassLoader.newInstance(new URL[]{ojdbcJarFile});
				   _oraclePreparedStatementClass = cl.loadClass("oracle.jdbc.OraclePreparedStatement");
				   _setLobPrefetchSizeMethod = _oraclePreparedStatementClass.getMethod("setLobPrefetchSize", int.class);
			   } catch (Exception e)
			   {
				   LOG.error("Exception raised while trying to get oracle methods", e);
				   throw new SQLException(e.getMessage());
			   }
				   }

		   public String generatePointQuery(String table, String keyName, String pkIndex, String queryHint)
		   {
			   StringBuilder sql = new StringBuilder();

			   if ( (null == queryHint) || ( queryHint.isEmpty()))
				   sql.append("select /*+ INDEX(src ").append(pkIndex).append(") */ ");
			   else
				   sql.append("select /*+ " + queryHint + " */ ");

			   sql.append(keyName).append( " keyn,");
			   sql.append(" txn txnid, src.* ").append(" from ");
			   sql.append(table);
			   sql.append(" src");
			   sql.append(" where src." + keyName + " = ?");
			   return sql.toString();
		   }

		   public ResultSet getRecord(String destKey) throws SQLException {
			   ResultSet rs = null;
			   try
			   {

				   _stmt.setString(1,destKey);
				   //stmt.setFetchSize(10000);
				   //stmt.setMaxRows(10);
				   rs = _stmt.executeQuery();
			   } catch ( SQLException sqlEx) {
				   DBHelper.close(rs, null, null);
				   throw sqlEx;
			   }
			   return rs;
		   }

		   @Override
		   public PreparedStatement getFetchStmt(long fromId)
				   throws SQLException
				  
		  {
			   String sql =  BootstrapSrcDBEventReader.generateEventQuery2(_tableName,_pkeyName, _dbusKeyType,_pkIndex, _queryHint);
			   LOG.info("Oracle Query =" + sql);

			   PreparedStatement stmt = _conn.prepareStatement(sql);
			   stmt.setLong(1,fromId);
			   stmt.setLong(2, _interval);
			   Object ds = _oraclePreparedStatementClass.cast(stmt);
			   try
			   {
			   _setLobPrefetchSizeMethod.invoke(ds, 1000);
			   } catch (Exception e)
			   {
				   LOG.error("Error in setLobPrefetchSizeMethod" + e.getMessage());
				   throw new SQLException(e.getMessage());
			   }
			   
			   return stmt;
		  }

		   @Override
		   public PreparedStatement getFetchStmt(String from)
				   throws SQLException
				   
		   {
			   String sql =  BootstrapSrcDBEventReader.generateEventQuery2(_tableName, _pkeyName, _dbusKeyType,_pkIndex, _queryHint);
			   
			   LOG.info("Oracle Query =" + sql);

			   PreparedStatement stmt = _conn.prepareStatement(sql);
			   stmt.setString(1,from);
			   stmt.setLong(2, _interval);
			   Object ds = _oraclePreparedStatementClass.cast(stmt);
			   try
			   {
				   _setLobPrefetchSizeMethod.invoke(ds, 1000);
			   } catch (Exception e)
			   {
				   throw new SQLException("Unable to set Lob prefetch size", e);
			   }
			   return stmt;
			}

	   }

	   public static class KeyTxnReader
	   {
		   private final File _file;
		   private final BufferedReader _reader;

		   public KeyTxnReader(File file)
		   {
			   try
			   {
				   _file = file;
				   _reader = new BufferedReader(new FileReader(file));
			   } catch (IOException ioe) {
				   LOG.error("KeyTxnReader error: " + ioe.getMessage(), ioe);
				   throw new RuntimeException(ioe);
			   }
		   }


		   public boolean readNextEntry(PrimaryKeyTxn entry)
				   throws IOException
				   {
			   String line =  _reader.readLine();

			   if ( null == line )
				   return false;

			   entry.readFrom(line);
			   return true;
				   }

		   public void close()
		   {
			   try
			   {
				   _reader.close();
			   } catch (IOException ioe) {
				   LOG.error("KeyTxnReader error: " + ioe.getMessage(), ioe);
			   }
		   }
	   }

	}


