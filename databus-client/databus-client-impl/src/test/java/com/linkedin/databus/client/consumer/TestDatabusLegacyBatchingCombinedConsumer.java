package com.linkedin.databus.client.consumer;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.client.DbusEventAvroDecoder;
import com.linkedin.databus.client.SingleSourceSCN;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DatabusEventHolder;
import com.linkedin.databus.client.pub.DatabusLegacyConsumerCallback;
import com.linkedin.databus.client.pub.DatabusLegacyEventConverter;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus2.schemas.VersionedSchema;
import com.linkedin.databus2.schemas.VersionedSchemaSet;
import com.linkedin.databus2.schemas.utils.SchemaHelper;
import com.linkedin.events.bizfollow.bizfollow.BizFollow;

public class TestDatabusLegacyBatchingCombinedConsumer 
{
	  private VersionedSchemaSet _schemaSet;
	  private ByteBuffer _eventByteBuffer;
	  private DbusEventAvroDecoder _eventDecoder;
	  private DbusEvent _event;

	  @BeforeClass
	  public void beforeClass() throws Exception
	  {
	    Logger.getRootLogger().setLevel(Level.ERROR);
	    //Logger.getRootLogger().setLevel(Level.INFO);

	    _schemaSet = new VersionedSchemaSet();
	    //TODO HIGH user real version here
	    _schemaSet.add(new VersionedSchema(BizFollow.SCHEMA$.getFullName(), (short)1, BizFollow.SCHEMA$));

	    _eventDecoder = new DbusEventAvroDecoder(_schemaSet);

	    BizFollow bfRecord = new BizFollow();
	    ByteArrayOutputStream bfRecordOut = new ByteArrayOutputStream();

	    BinaryEncoder binaryEncoder = new BinaryEncoder(bfRecordOut);
	    GenericDatumWriter<BizFollow> avroWriter = new GenericDatumWriter<BizFollow>(BizFollow.SCHEMA$);
	    avroWriter.write(bfRecord, binaryEncoder);
	    binaryEncoder.flush();

	    byte[] bfRecordBytes = bfRecordOut.toByteArray();

	    _eventByteBuffer = ByteBuffer.allocate(1000).order(DbusEvent.byteOrder);
	    DbusEvent.serializeEvent(new DbusEventKey(1), (short)0, (short)1, System.nanoTime(), (short)1,
	                             SchemaHelper.getSchemaId(BizFollow.SCHEMA$.toString()),
	                             bfRecordBytes, false, _eventByteBuffer);
	    _event = new DbusEvent(_eventByteBuffer, 0);
	    
	  }
	  
	  @Test
	  public synchronized void testErrorProcessing() throws Exception
	  {
	    int streamBatchSize = 2;

	    BatchingDatabusCombinedConsumer.Config configBuilder =
	    									new BatchingDatabusCombinedConsumer.Config();
	    configBuilder.setBatchingLevel(BatchingDatabusCombinedConsumer.StaticConfig.BatchingLevel.SOURCES.toString());
	    configBuilder.setStreamBatchSize(streamBatchSize);
	    configBuilder.setBootstrapBatchSize(streamBatchSize);
	    TestLegacyConsumer consumer = new TestLegacyConsumer();
	    TestConverter converter = new TestConverter();
	    DatabusLegacyBatchingCombinedConsumerAdapter< TestV1BizfollowDTO, BizFollow> testConsumer1 = 
	    		      new DatabusLegacyBatchingCombinedConsumerAdapter<TestV1BizfollowDTO, BizFollow>(consumer,true, converter, configBuilder, BizFollow.class);
	    
	    // Consumer returned error
	    consumer.retError = true;
	    testConsumer1.onStartConsumption();
	    testConsumer1.onStartDataEventSequence(new SingleSourceSCN(1, 1));
	    testConsumer1.onStartSource(BizFollow.class.getCanonicalName(), BizFollow.SCHEMA$);
	    ConsumerCallbackResult result1 = testConsumer1.onDataEvent(_event, _eventDecoder);
	    ConsumerCallbackResult result2 = testConsumer1.onBootstrapEvent(_event, _eventDecoder);
		org.testng.Assert.assertEquals(result1, ConsumerCallbackResult.SUCCESS, "Success ConsumerCallback expected");
		org.testng.Assert.assertEquals(testConsumer1.getStreamEventHoderList().size(),1, "EventHolders Size check");
		org.testng.Assert.assertEquals(result2, ConsumerCallbackResult.SUCCESS, "Success ConsumerCallback expected");
		org.testng.Assert.assertEquals(testConsumer1.getBootstrapEventHolderList().size(),1, "EventHolders Size check");

	    result1 = testConsumer1.onDataEvent(_event, _eventDecoder);
	    result2 = testConsumer1.onBootstrapEvent(_event, _eventDecoder);
		org.testng.Assert.assertEquals(result1, ConsumerCallbackResult.ERROR, "Error ConsumerCallback expected");
		org.testng.Assert.assertEquals(testConsumer1.getStreamEventHoderList().size(),0, "EventHolders Size check");
		org.testng.Assert.assertEquals(result2, ConsumerCallbackResult.ERROR, "Error ConsumerCallback expected");
		org.testng.Assert.assertEquals(testConsumer1.getBootstrapEventHolderList().size(),0, "EventHolders Size check");


		//converter threw exception
		consumer.retError = false;
		converter.throwException = true;
		result1 = testConsumer1.onDataEvent(_event, _eventDecoder);
		result2 = testConsumer1.onBootstrapEvent(_event, _eventDecoder);
		org.testng.Assert.assertEquals(result1, ConsumerCallbackResult.SUCCESS, "Success ConsumerCallback expected");
		org.testng.Assert.assertEquals(testConsumer1.getStreamEventHoderList().size(),1, "EventHolders Size check");
		org.testng.Assert.assertEquals(result2, ConsumerCallbackResult.SUCCESS, "Success ConsumerCallback expected");
		org.testng.Assert.assertEquals(testConsumer1.getBootstrapEventHolderList().size(),1, "EventHolders Size check");


		result1 = testConsumer1.onDataEvent(_event, _eventDecoder);
		result2 = testConsumer1.onBootstrapEvent(_event, _eventDecoder);
		org.testng.Assert.assertEquals(result1, ConsumerCallbackResult.ERROR, "Error ConsumerCallback expected");
		org.testng.Assert.assertEquals(testConsumer1.getStreamEventHoderList().size(),0, "EventHolders Size check");
		org.testng.Assert.assertEquals(result2, ConsumerCallbackResult.ERROR, "Error ConsumerCallback expected");
		org.testng.Assert.assertEquals(testConsumer1.getBootstrapEventHolderList().size(),0, "EventHolders Size check");
	  }
	  
	  @Test
	  public synchronized void testSourcesLevelBatching() throws Exception
	  {
	    int streamBatchSize = 13;

	    BatchingDatabusCombinedConsumer.Config configBuilder =
	    									new BatchingDatabusCombinedConsumer.Config();
	    configBuilder.setBatchingLevel(BatchingDatabusCombinedConsumer.StaticConfig.BatchingLevel.SOURCES.toString());
	    configBuilder.setStreamBatchSize(streamBatchSize);
	    TestLegacyConsumer consumer = new TestLegacyConsumer();
	    TestConverter converter = new TestConverter();
	    DatabusLegacyBatchingCombinedConsumerAdapter< TestV1BizfollowDTO, BizFollow> testConsumer1 = 
	    		      new DatabusLegacyBatchingCombinedConsumerAdapter<TestV1BizfollowDTO, BizFollow>(consumer,true, converter, configBuilder, BizFollow.class);
	    
	    testConsumer1.onStartConsumption();
	    testConsumer1.onStartDataEventSequence(new SingleSourceSCN(1, 1));
	    testConsumer1.onStartSource(BizFollow.class.getCanonicalName(), BizFollow.SCHEMA$);
	    testConsumer1.onDataEvent(_event, _eventDecoder);
	    testConsumer1.onDataEvent(_event, _eventDecoder);
	    testConsumer1.onEndSource("source1", null);
	    testConsumer1.onStartDataEventSequence(new SingleSourceSCN(1, 2));
	    testConsumer1.onStartSource(BizFollow.class.getCanonicalName(), BizFollow.SCHEMA$);
	    testConsumer1.onDataEvent(_event, _eventDecoder);
	    testConsumer1.onDataEvent(_event, _eventDecoder);
	    testConsumer1.onDataEvent(_event, _eventDecoder);
	    testConsumer1.onEndDataEventSequence(new SingleSourceSCN(1, 2));
	    testConsumer1.onStartDataEventSequence(new SingleSourceSCN(1, 3));
	    testConsumer1.onStartSource(BizFollow.class.getCanonicalName(), BizFollow.SCHEMA$);
	    for (int i = 0; i < 3 * streamBatchSize + 2; ++i)
	      testConsumer1.onDataEvent(_event, _eventDecoder);
	    testConsumer1.onStopConsumption();

	    List<List<DatabusEventHolder<TestV1BizfollowDTO>>> streamBatchSizes = consumer.getStreamEvents();
	    org.testng.Assert.assertEquals(streamBatchSizes.size(), 6, "correct number of batches");
	    org.testng.Assert.assertEquals(streamBatchSizes.get(0).size(), 2, "correct batch size");
	    org.testng.Assert.assertEquals(streamBatchSizes.get(1).size(), 3, "correct batch size");
	    org.testng.Assert.assertEquals(streamBatchSizes.get(2).size(), streamBatchSize, "correct batch size");
	    org.testng.Assert.assertEquals(streamBatchSizes.get(3).size(), streamBatchSize, "correct batch size");
	    org.testng.Assert.assertEquals(streamBatchSizes.get(4).size(), streamBatchSize, "correct batch size");
	    org.testng.Assert.assertEquals(streamBatchSizes.get(5).size(), 2, "correct batch size");
	    
	    DatabusEventHolder<BizFollow> exp = converter.lastV2Event;
	    
	    for (List<DatabusEventHolder<TestV1BizfollowDTO>> l : streamBatchSizes)
	    {
	    	for ( DatabusEventHolder<TestV1BizfollowDTO> h : l)
	    	{
	    		org.testng.Assert.assertEquals(h.getScn(),exp.getScn(), "SCN check for holder :" + h);
	    		org.testng.Assert.assertEquals(h.getTimestamp(),exp.getTimestamp(), "Timestamp check for holder :" + h);
	    		org.testng.Assert.assertEquals(h.getKey(), exp.getKey(),"Key check for holder :" + h);
	    		org.testng.Assert.assertEquals(h.getEvent().companyId, exp.getEvent().companyId, "Company Id check for holder :" + h);
	    		org.testng.Assert.assertEquals(h.getEvent().memberId, exp.getEvent().memberId, "Member Id check for holder :" + h);
	    		org.testng.Assert.assertEquals(h.getEvent().id, exp.getEvent().id, "Id check for holder :" + h);
	    		org.testng.Assert.assertEquals(h.getEvent().source, exp.getEvent().source, "Source check for holder :" + h);
	    	}
	    }
	  }
	  
	  
	  @Test
	  public synchronized void testWindowsLevelBatching() throws Exception
	  {
	    int bstBatchSize = 13;

	    BatchingDatabusCombinedConsumer.Config configBuilder =
	        new BatchingDatabusCombinedConsumer.Config();
	    configBuilder.setBatchingLevel(BatchingDatabusCombinedConsumer.StaticConfig.BatchingLevel.WINDOWS.toString());
	    configBuilder.setBootstrapBatchSize(bstBatchSize);
	    TestLegacyConsumer consumer = new TestLegacyConsumer();
	    TestConverter converter = new TestConverter();
	    DatabusLegacyBatchingCombinedConsumerAdapter< TestV1BizfollowDTO, BizFollow> testConsumer1 = 
	    		      new DatabusLegacyBatchingCombinedConsumerAdapter<TestV1BizfollowDTO, BizFollow>(consumer,true, converter, configBuilder, BizFollow.class);
	   
	    testConsumer1.onStartBootstrap();
	    testConsumer1.onStartBootstrapSequence(new SingleSourceSCN(1, 1));
	    testConsumer1.onStartBootstrapSource(BizFollow.class.getCanonicalName(), BizFollow.SCHEMA$);
	    testConsumer1.onBootstrapEvent(_event, _eventDecoder);
	    testConsumer1.onBootstrapEvent(_event, _eventDecoder);
	    testConsumer1.onEndBootstrapSource("source1", null);
	    testConsumer1.onStartBootstrapSource(BizFollow.class.getCanonicalName(), BizFollow.SCHEMA$);
	    testConsumer1.onBootstrapEvent(_event, _eventDecoder);
	    testConsumer1.onBootstrapEvent(_event, _eventDecoder);
	    testConsumer1.onBootstrapEvent(_event, _eventDecoder);
	    testConsumer1.onEndBootstrapSequence(new SingleSourceSCN(1, 1));
	    testConsumer1.onStartBootstrapSequence(new SingleSourceSCN(1, 3));
	    testConsumer1.onStartBootstrapSource(BizFollow.class.getCanonicalName(), BizFollow.SCHEMA$);
	    for (int i = 0; i < 3 * bstBatchSize + 2; ++i)
	      testConsumer1.onBootstrapEvent(_event, _eventDecoder);
	    testConsumer1.onStopBootstrap();

	    List<List<DatabusEventHolder<TestV1BizfollowDTO>>> bstBatchSizes = consumer.getBootstrapEvents();
	    org.testng.Assert.assertEquals(bstBatchSizes.size(), 5, "correct number of batches");
	    org.testng.Assert.assertEquals(bstBatchSizes.get(0).size(), 5, "correct batch size");
	    org.testng.Assert.assertEquals(bstBatchSizes.get(1).size(), bstBatchSize, "correct batch size");
	    org.testng.Assert.assertEquals(bstBatchSizes.get(2).size(), bstBatchSize, "correct batch size");
	    org.testng.Assert.assertEquals(bstBatchSizes.get(3).size(), bstBatchSize, "correct batch size");
	    org.testng.Assert.assertEquals(bstBatchSizes.get(4).size(), 2, "correct batch size");
	    
	    DatabusEventHolder<BizFollow> exp = converter.lastV2Event;
	    
	    for (List<DatabusEventHolder<TestV1BizfollowDTO>> l : bstBatchSizes)
	    {
	    	for ( DatabusEventHolder<TestV1BizfollowDTO> h : l)
	    	{
	    		org.testng.Assert.assertEquals(h.getScn(),exp.getScn(), "SCN check for holder :" + h);
	    		org.testng.Assert.assertEquals(h.getTimestamp(),exp.getTimestamp(), "Timestamp check for holder :" + h);
	    		org.testng.Assert.assertEquals(h.getKey(), exp.getKey(),"Key check for holder :" + h);
	    		org.testng.Assert.assertEquals(h.getEvent().companyId, exp.getEvent().companyId, "Company Id check for holder :" + h);
	    		org.testng.Assert.assertEquals(h.getEvent().memberId, exp.getEvent().memberId, "Member Id check for holder :" + h);
	    		org.testng.Assert.assertEquals(h.getEvent().id, exp.getEvent().id, "Id check for holder :" + h);
	    		org.testng.Assert.assertEquals(h.getEvent().source, exp.getEvent().source, "Source check for holder :" + h);
	    	}
	    }
	  }
	  
	  @Test
	  public synchronized void testFullLevelBatching() throws Exception
	  {
	    int bstBatchSize = 10;

	    BatchingDatabusCombinedConsumer.Config configBuilder =
	        new BatchingDatabusCombinedConsumer.Config();
	    configBuilder.setBatchingLevel(BatchingDatabusCombinedConsumer.StaticConfig.BatchingLevel.FULL.toString());
	    configBuilder.setBootstrapBatchSize(bstBatchSize);
	    TestLegacyConsumer consumer = new TestLegacyConsumer();
	    TestConverter converter = new TestConverter();
	    DatabusLegacyBatchingCombinedConsumerAdapter< TestV1BizfollowDTO, BizFollow> testConsumer1 = 
	    		      new DatabusLegacyBatchingCombinedConsumerAdapter<TestV1BizfollowDTO, BizFollow>(consumer,true, converter, configBuilder, BizFollow.class);
	  
	    testConsumer1.onStartBootstrap();
	    testConsumer1.onStartBootstrapSequence(new SingleSourceSCN(1, 1));
	    testConsumer1.onStartBootstrapSource(BizFollow.class.getCanonicalName(), BizFollow.SCHEMA$);
	    testConsumer1.onBootstrapEvent(_event, _eventDecoder);
	    testConsumer1.onBootstrapEvent(_event, _eventDecoder);
	    testConsumer1.onEndBootstrapSource("source1", null);
	    testConsumer1.onStartBootstrapSource(BizFollow.class.getCanonicalName(), BizFollow.SCHEMA$);
	    testConsumer1.onBootstrapEvent(_event, _eventDecoder);
	    testConsumer1.onBootstrapEvent(_event, _eventDecoder);
	    testConsumer1.onBootstrapEvent(_event, _eventDecoder);
	    testConsumer1.onEndBootstrapSequence(new SingleSourceSCN(1, 1));
	    testConsumer1.onStartBootstrapSequence(new SingleSourceSCN(1, 3));
	    testConsumer1.onStartBootstrapSource(BizFollow.class.getCanonicalName(), BizFollow.SCHEMA$);
	    for (int i = 0; i < 3 * bstBatchSize + 2; ++i)
	      testConsumer1.onBootstrapEvent(_event, _eventDecoder);
	    testConsumer1.onStopBootstrap();

	    List<List<DatabusEventHolder<TestV1BizfollowDTO>>> bstBatchSizes = consumer.getBootstrapEvents();
	    org.testng.Assert.assertEquals(bstBatchSizes.size(), 4, "correct number of batches");
	    org.testng.Assert.assertEquals(bstBatchSizes.get(0).size(), bstBatchSize, "correct batch size");
	    org.testng.Assert.assertEquals(bstBatchSizes.get(1).size(), bstBatchSize, "correct batch size");
	    org.testng.Assert.assertEquals(bstBatchSizes.get(2).size(), bstBatchSize, "correct batch size");
	    org.testng.Assert.assertEquals(bstBatchSizes.get(3).size(), 7, "correct batch size");
	    
	    DatabusEventHolder<BizFollow> exp = converter.lastV2Event;
	    
	    for (List<DatabusEventHolder<TestV1BizfollowDTO>> l : bstBatchSizes)
	    {
	    	for ( DatabusEventHolder<TestV1BizfollowDTO> h : l)
	    	{
	    		org.testng.Assert.assertEquals(h.getScn(),exp.getScn(), "SCN check for holder :" + h);
	    		org.testng.Assert.assertEquals(h.getTimestamp(),exp.getTimestamp(), "Timestamp check for holder :" + h);
	    		org.testng.Assert.assertEquals(h.getKey(), exp.getKey(),"Key check for holder :" + h);
	    		org.testng.Assert.assertEquals(h.getEvent().companyId, exp.getEvent().companyId, "Company Id check for holder :" + h);
	    		org.testng.Assert.assertEquals(h.getEvent().memberId, exp.getEvent().memberId, "Member Id check for holder :" + h);
	    		org.testng.Assert.assertEquals(h.getEvent().id, exp.getEvent().id, "Id check for holder :" + h);
	    		org.testng.Assert.assertEquals(h.getEvent().source, exp.getEvent().source, "Source check for holder :" + h);
	    	}
	    }
	  }
	  
	public static class TestV1BizfollowDTO
	{
		private Integer id;
		private Integer memberId;
		private Integer companyId;
		private Integer source;
		
		public Integer getId() {
			return id;
		}
		public void setId(Integer id) {
			this.id = id;
		}
		public Integer getMemberId() {
			return memberId;
		}
		public void setMemberId(Integer memberId) {
			this.memberId = memberId;
		}
		public Integer getCompanyId() {
			return companyId;
		}
		public void setCompanyId(Integer companyId) {
			this.companyId = companyId;
		}
		public Integer getSource() {
			return source;
		}
		public void setSource(Integer source) {
			this.source = source;
		}
	}
	
	public static class TestConverter
	     implements DatabusLegacyEventConverter<TestV1BizfollowDTO, BizFollow>
	{
		protected DatabusEventHolder<BizFollow>  lastV2Event;
		protected boolean throwException = false;
		
		@Override
		public DatabusEventHolder<TestV1BizfollowDTO> convert(
				DatabusEventHolder<BizFollow> event) 
		{
			if (throwException)
				throw new RuntimeException("Test Runtime Exception");
			
			TestV1BizfollowDTO v1dto = new TestV1BizfollowDTO();
			DatabusEventHolder<TestV1BizfollowDTO> eventHolder = new DatabusEventHolder<TestV1BizfollowDTO>();
			eventHolder.setEvent(v1dto);
			eventHolder.setKey(event.getKey());
			eventHolder.setScn(event.getScn());
			eventHolder.setTimestamp(event.getTimestamp());
			
			v1dto.setId(event.getEvent().id);
			v1dto.setCompanyId(event.getEvent().companyId);
			v1dto.setMemberId(event.getEvent().memberId);
			v1dto.setSource(event.getEvent().source);
			
			lastV2Event = event;
			
			return eventHolder;
		}

		@Override
		public List<DatabusEventHolder<TestV1BizfollowDTO>> convert(
				List<DatabusEventHolder<BizFollow>> events) 
		{	
			List<DatabusEventHolder<TestV1BizfollowDTO>> v1List = new ArrayList<DatabusEventHolder<TestV1BizfollowDTO>>();
			for ( DatabusEventHolder<BizFollow> e: events)
			{
				v1List.add(convert(e));
			}
			return v1List;
		}		
	}
	
	
	public static class TestLegacyConsumer
	   implements DatabusLegacyConsumerCallback<TestV1BizfollowDTO>
	{
		private List<List<DatabusEventHolder<TestV1BizfollowDTO>>> streamEvents = new ArrayList<List<DatabusEventHolder<TestV1BizfollowDTO>>>();
		private List<List<DatabusEventHolder<TestV1BizfollowDTO>>> bootstrapEvents = new ArrayList<List<DatabusEventHolder<TestV1BizfollowDTO>>>();

		protected boolean retError;
		
		public void clear()
		{
			streamEvents.clear();
			bootstrapEvents.clear();
		}
				
		public List<List<DatabusEventHolder<TestV1BizfollowDTO>>> getStreamEvents() 
		{
			return streamEvents;
		}

		public List<List<DatabusEventHolder<TestV1BizfollowDTO>>> getBootstrapEvents() 
		{
			return bootstrapEvents;
		}


		@Override
		public ConsumerCallbackResult onEvent(
				DatabusEventHolder<TestV1BizfollowDTO> event) 
		{
			if (retError)
				return ConsumerCallbackResult.ERROR;
			
			List<DatabusEventHolder<TestV1BizfollowDTO>> l = new ArrayList<DatabusEventHolder<TestV1BizfollowDTO>>();
			l.add(event);
			streamEvents.add(l);
			return ConsumerCallbackResult.SUCCESS;
		}

		@Override
		public ConsumerCallbackResult onEventsBatch(
				List<DatabusEventHolder<TestV1BizfollowDTO>> events) 
		{	
			if (retError)
				return ConsumerCallbackResult.ERROR;
			
			streamEvents.add(events);
			return ConsumerCallbackResult.SUCCESS;
		}

		@Override
		public ConsumerCallbackResult onBootstrapEvent(
				DatabusEventHolder<TestV1BizfollowDTO> event) 
		{
			if (retError)
				return ConsumerCallbackResult.ERROR;
			
			List<DatabusEventHolder<TestV1BizfollowDTO>> l = new ArrayList<DatabusEventHolder<TestV1BizfollowDTO>>();
			l.add(event);
			bootstrapEvents.add(l);
			return ConsumerCallbackResult.SUCCESS;
		}

		@Override
		public ConsumerCallbackResult onBootstrapEventsBatch(
				List<DatabusEventHolder<TestV1BizfollowDTO>> events) 
		{
			if (retError)
				return ConsumerCallbackResult.ERROR;
			
			bootstrapEvents.add(events);
			return ConsumerCallbackResult.SUCCESS;
		}		
	}
}
