package com.linkedin.databus.client.consumer;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.client.DbusEventAvroDecoder;
import com.linkedin.databus.client.SingleSourceSCN;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus2.schemas.VersionedSchema;
import com.linkedin.databus2.schemas.VersionedSchemaSet;
import com.linkedin.databus2.schemas.utils.SchemaHelper;
import com.linkedin.events.bizfollow.bizfollow.BizFollow;


public class TestBatchingDatabusCombinedConsumer
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

  @AfterClass
  public void afterClass()
  {
  }

  @Test
  public void testSourcesLevelBatching() throws Exception
  {
    int streamBatchSize = 13;

    BatchingDatabusCombinedConsumer.Config configBuilder =
        new BatchingDatabusCombinedConsumer.Config();
    configBuilder.setBatchingLevel(BatchingDatabusCombinedConsumer.StaticConfig.BatchingLevel.SOURCES.toString());
    configBuilder.setStreamBatchSize(streamBatchSize);
    BatchingDatabusCombinedConsumer.StaticConfig config = configBuilder.build();

    TestBatchingConsumer testConsumer1 = new TestBatchingConsumer(config);
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

    ArrayList<Integer> streamBatchSizes = testConsumer1.getStreamBatchSizes();
    org.testng.Assert.assertEquals(streamBatchSizes.size(), 6, "correct number of batches");
    org.testng.Assert.assertEquals(streamBatchSizes.get(0).intValue(), 2, "correct batch size");
    org.testng.Assert.assertEquals(streamBatchSizes.get(1).intValue(), 3, "correct batch size");
    org.testng.Assert.assertEquals(streamBatchSizes.get(2).intValue(), streamBatchSize, "correct batch size");
    org.testng.Assert.assertEquals(streamBatchSizes.get(3).intValue(), streamBatchSize, "correct batch size");
    org.testng.Assert.assertEquals(streamBatchSizes.get(4).intValue(), streamBatchSize, "correct batch size");
    org.testng.Assert.assertEquals(streamBatchSizes.get(5).intValue(), 2, "correct batch size");
  }

  @Test
  public void testWindowsLevelBatching() throws Exception
  {
    int bstBatchSize = 13;

    BatchingDatabusCombinedConsumer.Config configBuilder =
        new BatchingDatabusCombinedConsumer.Config();
    configBuilder.setBatchingLevel(BatchingDatabusCombinedConsumer.StaticConfig.BatchingLevel.WINDOWS.toString());
    configBuilder.setBootstrapBatchSize(bstBatchSize);
    BatchingDatabusCombinedConsumer.StaticConfig config = configBuilder.build();

    TestBatchingConsumer testConsumer1 = new TestBatchingConsumer(config);
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

    ArrayList<Integer> bstBatchSizes = testConsumer1.getBootstrapBatchSizes();
    org.testng.Assert.assertEquals(bstBatchSizes.size(), 5, "correct number of batches");
    org.testng.Assert.assertEquals(bstBatchSizes.get(0).intValue(), 5, "correct batch size");
    org.testng.Assert.assertEquals(bstBatchSizes.get(1).intValue(), bstBatchSize, "correct batch size");
    org.testng.Assert.assertEquals(bstBatchSizes.get(2).intValue(), bstBatchSize, "correct batch size");
    org.testng.Assert.assertEquals(bstBatchSizes.get(3).intValue(), bstBatchSize, "correct batch size");
    org.testng.Assert.assertEquals(bstBatchSizes.get(4).intValue(), 2, "correct batch size");
  }

  @Test
  public void testFullLevelBatching() throws Exception
  {
    int bstBatchSize = 10;

    BatchingDatabusCombinedConsumer.Config configBuilder =
        new BatchingDatabusCombinedConsumer.Config();
    configBuilder.setBatchingLevel(BatchingDatabusCombinedConsumer.StaticConfig.BatchingLevel.FULL.toString());
    configBuilder.setBootstrapBatchSize(bstBatchSize);
    BatchingDatabusCombinedConsumer.StaticConfig config = configBuilder.build();

    TestBatchingConsumer testConsumer1 = new TestBatchingConsumer(config);
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

    ArrayList<Integer> bstBatchSizes = testConsumer1.getBootstrapBatchSizes();
    org.testng.Assert.assertEquals(bstBatchSizes.size(), 4, "correct number of batches");
    org.testng.Assert.assertEquals(bstBatchSizes.get(0).intValue(), bstBatchSize, "correct batch size");
    org.testng.Assert.assertEquals(bstBatchSizes.get(1).intValue(), bstBatchSize, "correct batch size");
    org.testng.Assert.assertEquals(bstBatchSizes.get(2).intValue(), bstBatchSize, "correct batch size");
    org.testng.Assert.assertEquals(bstBatchSizes.get(3).intValue(), 7, "correct batch size");
  }

}

class TestBatchingConsumer extends BatchingDatabusCombinedConsumer<BizFollow>
{

  private final ArrayList<Integer> _streamBatchSizes;
  private final ArrayList<Integer> _bootstrapBatchSizes;

  public TestBatchingConsumer(BatchingDatabusCombinedConsumer.StaticConfig config)
  {
    super(config, BizFollow.class);
    _streamBatchSizes = new ArrayList<Integer>();
    _bootstrapBatchSizes = new ArrayList<Integer>();
  }

  @Override
  protected ConsumerCallbackResult onDataEventsBatch(List<BizFollow> eventsBatch)
  {
    _streamBatchSizes.add(eventsBatch.size());
    return eventsBatch.size() <= getStaticConfig().getStreamBatchSize() ?
           ConsumerCallbackResult.SUCCESS :
           ConsumerCallbackResult.ERROR;
  }

  @Override
  protected ConsumerCallbackResult onBootstrapEventsBatch(List<BizFollow> eventsBatch)
  {
    _bootstrapBatchSizes.add(eventsBatch.size());
    return eventsBatch.size() <= getStaticConfig().getBootstrapBatchSize() ?
           ConsumerCallbackResult.SUCCESS :
           ConsumerCallbackResult.ERROR;
  }

  public ArrayList<Integer> getStreamBatchSizes()
  {
    return _streamBatchSizes;
  }

  public ArrayList<Integer> getBootstrapBatchSizes()
  {
    return _bootstrapBatchSizes;
  }

  public void reset()
  {
    _streamBatchSizes.clear();
    _bootstrapBatchSizes.clear();
  }

}
