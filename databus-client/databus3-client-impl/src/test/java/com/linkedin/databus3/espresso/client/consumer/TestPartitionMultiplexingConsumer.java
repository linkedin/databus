package com.linkedin.databus3.espresso.client.consumer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.client.DbusEventAvroDecoder;
import com.linkedin.databus.client.SingleSourceSCN;
import com.linkedin.databus.client.data_model.MultiPartitionSCN;
import com.linkedin.databus.client.pub.DatabusClientException;
import com.linkedin.databus.client.pub.DatabusCombinedConsumer;
import com.linkedin.databus.client.pub.DatabusV3Consumer;
import com.linkedin.databus.client.pub.RegistrationId;
import com.linkedin.databus.client.test.CallbackTrackingDatabusConsumer;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventBuffer.AllocationPolicy;
import com.linkedin.databus.core.DbusEventBuffer.DbusEventIterator;
import com.linkedin.databus.core.DbusEventInfo;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.DbusOpcode;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus.core.util.NamedThreadFactory;
import com.linkedin.databus.core.util.RngUtils;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.schemas.VersionedSchema;
import com.linkedin.databus2.schemas.VersionedSchemaSet;
import com.linkedin.databus2.schemas.utils.SchemaHelper;
import com.linkedin.databus2.test.ConditionCheck;
import com.linkedin.databus2.test.TestUtil;
import com.linkedin.databus3.espresso.client.consumer.PartitionMultiplexingConsumer.PartitionConsumer;
import com.linkedin.databus3.espresso.client.data_model.EspressoSubscriptionUriCodec;

public class TestPartitionMultiplexingConsumer
{
  static final Schema TABLEA_SCHEMA =
      Schema.parse("{\"name\":\"tableA\",\"type\":\"record\",\"fields\":[{\"name\":\"s\",\"type\":\"string\"}]}");
  static final byte[] TABLEA_SCHEMAID = SchemaHelper.getSchemaId(TABLEA_SCHEMA.toString());
  static DbusEventBuffer.StaticConfig _bufCfg;

  @BeforeClass
  public void setupClass() throws InvalidConfigException
  {
    TestUtil.setupLogging(true, null, Level.INFO);

    //create standard relay buffer config
    DbusEventBuffer.Config bufCfgBuilder = new DbusEventBuffer.Config();
    bufCfgBuilder.setAllocationPolicy(AllocationPolicy.HEAP_MEMORY.toString());
    bufCfgBuilder.setMaxSize(100000);
    bufCfgBuilder.setScnIndexSize(128);
    bufCfgBuilder.setReadBufferSize(1);

    _bufCfg = bufCfgBuilder.build();
  }

  @Test
  /** Test simple ticket acquisition, validation and release */
  public void testTicketAcquisitionHappyPath() throws InterruptedException, DatabusClientException
  {
    Logger log = Logger.getLogger("TestPartitionMultiplexingConsumer.testTicketAcquisitionHappyPath");
    DatabusSubscription sub1 =
        DatabusSubscription.createPhysicalPartitionReplicationSubscription(
            new PhysicalPartition(1, "test"));
    PartitionMultiplexingConsumer testConsumer =
        new PartitionMultiplexingConsumer(new RegistrationId("test1"), null, log, null, 1000,
                                          false, false, sub1);
    final PartitionMultiplexingConsumer.PartitionConsumer p1 =
        testConsumer.getPartitionConsumer(sub1);
    Assert.assertNotNull(p1);

    PartitionMultiplexingConsumer.PartitionTicket ticket =
        testConsumer.acquireTicket(p1);

    //new ticket checks
    Assert.assertNotNull(ticket);
    Assert.assertEquals(ticket.getRenewCnt(), 0);
    Assert.assertEquals(ticket.getOwner(), p1);
    Assert.assertTrue(ticket.getHoldPeriodMs() >= 0);
    Assert.assertTrue(ticket.getTimeoutRemainingMs() <= 1000);
    Assert.assertTrue(ticket.isValid());

    //sleep for a period < the expiry
    TestUtil.sleep(200);
    Assert.assertTrue(ticket.isValid());
    Assert.assertTrue(ticket.getHoldPeriodMs() >= 200);
    Assert.assertTrue(ticket.getTimeoutRemainingMs() <= 800);

    //validate the ticket (should trigger a renew)
    Assert.assertTrue(ticket.validate());
    Assert.assertEquals(ticket.getRenewCnt(), 1);
    Assert.assertTrue(ticket.getTimeoutRemainingMs() > 800);
    Assert.assertTrue(ticket.getTimeoutRemainingMs() <= 1000);
    Assert.assertTrue(ticket.getHoldPeriodMs() >= 200);

    //sleep some more
    TestUtil.sleep(300);
    Assert.assertTrue(ticket.isValid());
    Assert.assertTrue(ticket.getHoldPeriodMs() >= 500);
    Assert.assertTrue(ticket.getTimeoutRemainingMs() <= 700);

    //validate the ticket (should trigger a renew)
    Assert.assertTrue(ticket.validate());
    Assert.assertEquals(ticket.getRenewCnt(), 2);
    Assert.assertTrue(ticket.getTimeoutRemainingMs() > 700);
    Assert.assertTrue(ticket.getTimeoutRemainingMs() <= 1000);
    Assert.assertTrue(ticket.getHoldPeriodMs() >= 500);

    //close this ticket
    ticket.close();
    Assert.assertTrue(!ticket.isValid());
    Assert.assertTrue(!ticket.validate());
  }

  @Test
  /** Test ticket acquisition and timeout */
  public void testTicketAcquisitionTimeout() throws InterruptedException, DatabusClientException
  {
    Logger log = Logger.getLogger("TestPartitionMultiplexingConsumer.testTicketAcquisitionTimeout");
    DatabusSubscription sub1 =
        DatabusSubscription.createPhysicalPartitionReplicationSubscription(
            new PhysicalPartition(1, "test"));
    PartitionMultiplexingConsumer testConsumer =
        new PartitionMultiplexingConsumer(new RegistrationId("test1"), null, log, null, 1000,
                                          false, true, sub1);
    final PartitionMultiplexingConsumer.PartitionConsumer p1 =
        testConsumer.getPartitionConsumer(sub1);
    Assert.assertNotNull(p1);

    PartitionMultiplexingConsumer.PartitionTicket ticket =
        testConsumer.acquireTicket(p1);
    Assert.assertNotNull(ticket);

    //new ticket checks
    Assert.assertNotNull(ticket);
    Assert.assertEquals(ticket.getRenewCnt(), 0);
    Assert.assertEquals(ticket.getOwner(), p1);
    Assert.assertTrue(ticket.getHoldPeriodMs() >= 0);
    Assert.assertTrue(ticket.getTimeoutRemainingMs() <= 1000);
    Assert.assertTrue(ticket.isValid());

    ////////////////// Next we try to timeout a ticket
    TestUtil.sleep(1100);
    Assert.assertTrue(!ticket.isValid());
    Assert.assertTrue(ticket.getHoldPeriodMs() >= 1100);
    Assert.assertTrue(ticket.getTimeoutRemainingMs() <= 0);
    Assert.assertTrue(!ticket.validate());
  }

  @Test
  /** Attempts for a ticket acquisition from two threads. The first thread holds the ticket
   * for a while but releases it before the timeout. */
  public void testConcurrentTicketAcquisitionHappyPath()
      throws InterruptedException, DatabusClientException
  {
    Logger log = Logger.getLogger("TestPartitionMultiplexingConsumer.testConcurrentTicketAcquisitionHappyPath");
    DatabusSubscription sub1 =
        DatabusSubscription.createPhysicalPartitionReplicationSubscription(
            new PhysicalPartition(1, "test"));
    DatabusSubscription sub2 =
        DatabusSubscription.createPhysicalPartitionReplicationSubscription(
            new PhysicalPartition(2, "test"));
    final PartitionMultiplexingConsumer testConsumer =
        new PartitionMultiplexingConsumer(new RegistrationId("test1"), null, log, null, 1000, false,
                                          false, sub1, sub2);
    final PartitionMultiplexingConsumer.PartitionConsumer p1 =
        testConsumer.getPartitionConsumer(sub1);
    final PartitionMultiplexingConsumer.PartitionConsumer p2 =
        testConsumer.getPartitionConsumer(sub2);
    PartitionMultiplexingConsumer.PartitionTicket ticket =
        testConsumer.acquireTicket(p1);

    //new ticket checks
    Assert.assertNotNull(ticket);
    Assert.assertEquals(ticket.getRenewCnt(), 0);
    Assert.assertEquals(ticket.getOwner(), p1);
    Assert.assertTrue(ticket.getHoldPeriodMs() >= 0);
    Assert.assertTrue(ticket.getTimeoutRemainingMs() <= 1000);
    Assert.assertTrue(ticket.isValid());

    //start a concurrent thread trying to also acquire a ticket
    final AtomicBoolean ticket2Success = new AtomicBoolean(false);
    Thread acq2 = new Thread(new Runnable()
    {
      @Override
      public void run()
      {
        //should block waiting for a ticket
        try
        {
          PartitionMultiplexingConsumer.PartitionTicket ticket2 =
              testConsumer.acquireTicket(p2);
          ticket2Success.set(null != ticket2);
        }
        catch (InterruptedException e)
        {
          //whatever
        }
      }
    }, "ticket 2 acquisition");
    acq2.setDaemon(true);
    acq2.start();

    TestUtil.sleep(100);
    //the first ticket is still valid and the second thread should still be waiting
    Assert.assertTrue(ticket.isValid());
    Assert.assertTrue(ticket.getHoldPeriodMs() >= 100);
    Assert.assertTrue(ticket.getTimeoutRemainingMs() <= 900);
    Assert.assertTrue(acq2.isAlive());
    Assert.assertTrue(!ticket2Success.get());

    //revalidate the first ticket
    Assert.assertTrue(ticket.validate());
    Assert.assertEquals(ticket.getRenewCnt(), 1);
    Assert.assertTrue(ticket.getTimeoutRemainingMs() > 900);
    Assert.assertTrue(ticket.getTimeoutRemainingMs() <= 1000);
    Assert.assertTrue(ticket.getHoldPeriodMs() >= 100);
    Assert.assertTrue(acq2.isAlive());
    Assert.assertTrue(!ticket2Success.get());

    TestUtil.sleep(100);
    //the first ticket is still valid and the second thread should still be waiting
    Assert.assertTrue(ticket.isValid());
    Assert.assertEquals(ticket.getRenewCnt(), 1);
    Assert.assertTrue(ticket.getHoldPeriodMs() >= 200);
    Assert.assertTrue(ticket.getTimeoutRemainingMs() <= 900);
    Assert.assertTrue(acq2.isAlive());
    Assert.assertTrue(!ticket2Success.get());

    ticket.close();
    //wait for the other guy to get the ticket
    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check()
      {
        return ticket2Success.get();
      }
    }, "waiting for ticket 2 acquisition", 100, log);
  }

  @Test
  /** Attempts for a ticket acquisition and the reacquisition for the same consumer */
  public void testConcurrentTicketReAcquisition()
      throws InterruptedException, DatabusClientException
  {
    Logger log = Logger.getLogger("TestPartitionMultiplexingConsumer.testConcurrentTicketAcquisitionHappyPath");
    DatabusSubscription sub1 =
        DatabusSubscription.createPhysicalPartitionReplicationSubscription(
            new PhysicalPartition(1, "test"));
    DatabusSubscription sub2 =
        DatabusSubscription.createPhysicalPartitionReplicationSubscription(
            new PhysicalPartition(2, "test"));
    final PartitionMultiplexingConsumer testConsumer =
        new PartitionMultiplexingConsumer(new RegistrationId("test1"), null, log, null, 1000, false,
                                          false, sub1, sub2);
    final PartitionMultiplexingConsumer.PartitionConsumer p1 =
        testConsumer.getPartitionConsumer(sub1);
    final PartitionMultiplexingConsumer.PartitionConsumer p2 =
        testConsumer.getPartitionConsumer(sub2);
    PartitionMultiplexingConsumer.PartitionTicket ticket =
        testConsumer.acquireTicket(p1);

    //new ticket checks
    Assert.assertNotNull(ticket);
    Assert.assertEquals(ticket.getRenewCnt(), 0);
    Assert.assertEquals(ticket.getOwner(), p1);
    Assert.assertTrue(ticket.getHoldPeriodMs() >= 0);
    Assert.assertTrue(ticket.getTimeoutRemainingMs() <= 1000);
    Assert.assertTrue(ticket.isValid());

    //start a concurrent thread trying to also acquire a ticket
    final AtomicBoolean ticket2Success = new AtomicBoolean(false);
    Thread acq2 = new Thread(new Runnable()
    {
      @Override
      public void run()
      {
        //should block waiting for a ticket
        try
        {
          PartitionMultiplexingConsumer.PartitionTicket ticket2 =
              testConsumer.acquireTicket(p2);
          ticket2Success.set(null != ticket2);
        }
        catch (InterruptedException e)
        {
          //whatever
        }
      }
    }, "ticket 2 acquisition");
    acq2.setDaemon(true);
    acq2.start();

    TestUtil.sleep(100);
    //the first ticket is still valid and the second thread should still be waiting
    Assert.assertTrue(ticket.isValid());
    Assert.assertTrue(ticket.getHoldPeriodMs() >= 100);
    Assert.assertTrue(ticket.getTimeoutRemainingMs() <= 900);
    Assert.assertTrue(acq2.isAlive());
    Assert.assertTrue(!ticket2Success.get());

    //reacquire
    PartitionMultiplexingConsumer.PartitionTicket ticket2 =
        testConsumer.acquireTicket(p1);
    Assert.assertTrue(ticket == ticket2);//same object
    Assert.assertTrue(ticket.isValid());
    Assert.assertEquals(ticket.getRenewCnt(), 1);
    Assert.assertTrue(ticket.getHoldPeriodMs() >= 100);
    Assert.assertTrue(ticket.getTimeoutRemainingMs() > 950);
    Assert.assertTrue(acq2.isAlive());
    Assert.assertTrue(!ticket2Success.get());

    ticket.close();
    //wait for the other guy to get the ticket
    TestUtil.assertWithBackoff(new ConditionCheck()
    {
      @Override
      public boolean check()
      {
        return ticket2Success.get();
      }
    }, "waiting for ticket 2 acquisition", 100, log);
  }

  @Test
  /** Attempts for a ticket acquisition from two threads. The first thread holds the ticket
   * for a while but releases it before the timeout. */
  public void testConcurrentTicketAcquisitionTimeout()
      throws InterruptedException, DatabusClientException
  {
    Logger log = Logger.getLogger("TestPartitionMultiplexingConsumer.testConcurrentTicketAcquisitionTimeout");
    DatabusSubscription sub1 =
        DatabusSubscription.createPhysicalPartitionReplicationSubscription(
            new PhysicalPartition(1, "test"));
    DatabusSubscription sub2 =
        DatabusSubscription.createPhysicalPartitionReplicationSubscription(
            new PhysicalPartition(2, "test"));
    final PartitionMultiplexingConsumer testConsumer =
        new PartitionMultiplexingConsumer(new RegistrationId("test1"), null, log, null, 1000, false,
                                          false, sub1, sub2);
    final PartitionMultiplexingConsumer.PartitionConsumer p1 =
        testConsumer.getPartitionConsumer(sub1);
    Assert.assertNotNull(p1);
    final PartitionMultiplexingConsumer.PartitionConsumer p2 =
        testConsumer.getPartitionConsumer(sub2);
    Assert.assertNotNull(p2);

    PartitionMultiplexingConsumer.PartitionTicket ticket =
        testConsumer.acquireTicket(p1);

    //new ticket checks
    Assert.assertNotNull(ticket);
    Assert.assertEquals(ticket.getRenewCnt(), 0);
    Assert.assertEquals(ticket.getOwner(), p1);
    Assert.assertTrue(ticket.getHoldPeriodMs() >= 0);
    Assert.assertTrue(ticket.getTimeoutRemainingMs() <= 1000);
    Assert.assertTrue(ticket.isValid());

    //start a concurrent thread trying to also acquire a ticket
    final AtomicBoolean ticket2Success = new AtomicBoolean(false);
    Thread acq2 = new Thread(new Runnable()
    {
      @Override
      public void run()
      {
        //should block waiting for a ticket
        try
        {
          PartitionMultiplexingConsumer.PartitionTicket ticket2 =
              testConsumer.acquireTicket(p2);
          ticket2Success.set(null != ticket2);
        }
        catch (InterruptedException e)
        {
          //whatever
        }
      }
    }, "ticket 2 acquisition");
    acq2.setDaemon(true);
    acq2.start();

    //keep validating ticket 1; second thread should wait
    for (int i = 1; i <= 30; ++i)
    {
      TestUtil.sleep(50);
      Assert.assertTrue(ticket.validate());
      //the first ticket is still valid and the second thread should still be waiting
      Assert.assertTrue(ticket.isValid());
      Assert.assertEquals(ticket.getRenewCnt(), i);
      Assert.assertTrue(ticket.getHoldPeriodMs() >= 50 * i);
      Assert.assertTrue(ticket.getTimeoutRemainingMs() >= 990);
      Assert.assertTrue(ticket.getTimeoutRemainingMs() <= 1000);
      Assert.assertTrue(acq2.isAlive());
      Assert.assertTrue(!ticket2Success.get());
    }

    //timeout
    TestUtil.sleep(1100);
    Assert.assertTrue(!ticket.isValid());

    //second guy should get it
    Assert.assertTrue(ticket2Success.get());

    TestUtil.sleep(1100);
    Assert.assertTrue(!testConsumer._currentTicket.isValid());

    ticket.close();
  }

  @Test
  /** Attempts for a ticket acquisition from many threads. Each thread holds a ticket for a while
   * and releases it. */
  public void testConcurrentTicketAcquisitionMany()
      throws InterruptedException, DatabusClientException
  {
    Logger log = Logger.getLogger("TestPartitionMultiplexingConsumer.testConcurrentTicketAcquisitionMany");
    //log.setLevel(Level.DEBUG);
    final PartitionMultiplexingConsumer testConsumer =
        new PartitionMultiplexingConsumer(new RegistrationId("test1"), null, log, null, 500,
                                          false, false);

    final int partNum = 10;
    final PartitionMultiplexingConsumer.PartitionConsumer[] pconsumer =
        new PartitionMultiplexingConsumer.PartitionConsumer[partNum];
    TestPartitionTicketAcquirer[] consRunnable = new TestPartitionTicketAcquirer[partNum];
    for (int i = 0; i < partNum; ++i)
    {
      DatabusSubscription sub =
          DatabusSubscription.createPhysicalPartitionReplicationSubscription(
              new PhysicalPartition(i, "test"));
      pconsumer[i] =
          testConsumer.createPartitionConsumer(sub);
      consRunnable[i] = new TestPartitionTicketAcquirer(testConsumer, pconsumer[i], 90, log);
    }

    ExecutorService exec =
        Executors.newFixedThreadPool(100,
                                     new NamedThreadFactory("testConcurrentTicketAcquisitionMany",
                                                            true));
    //Note that we rely on a fair locking policy; otherwise, it is not guaranteed in which order
    //are the tickets going to be given out
    Future<?>[] call = new Future<?>[partNum];
    for (int i = 0; i < partNum; ++i)
    {
      call[i] = exec.submit(consRunnable[i]);
    }

    //the first 6 tickets should be granted; the rest should time out
    for (int i = 0; i < partNum; ++i)
    {
      try
      {
        call[i].get(600, TimeUnit.MILLISECONDS);
      }
      catch (ExecutionException e)
      {
          Assert.fail("call " + i + " execution failure: " + e.getMessage(), e);
      }
      catch (TimeoutException e)
      {
        Assert.fail("call " + i + " timeout failure: " + e);
      }
      Assert.assertTrue(call[i].isDone(), "ticket " + i);
      Assert.assertTrue(!call[i].isCancelled(), "ticket " + i);
      Assert.assertTrue(consRunnable[i].getTicketSuccess(), "ticket " + i);
    }
  }

  @Test
  public void testManySubsPerPartition() throws Exception
  {
    //It looks like the unit test class load not always loads all classes so we
    //have to force it explicitly
    EspressoSubscriptionUriCodec.getInstance();

    Logger log = Logger.getLogger("TestPartitionMultiplexingConsumer.testManySubsPerPartition");
    //log.setLevel(Level.DEBUG);
    DatabusSubscription sub1 = DatabusSubscription.createFromUri("espresso://MASTER/testDb/1/tableA");
    DatabusSubscription sub2 = DatabusSubscription.createFromUri("espresso://MASTER/testDb/2/tableA");
    DatabusSubscription sub3 = DatabusSubscription.createFromUri("espresso://MASTER/testDb/3/tableA");
    DatabusSubscription sub4 = DatabusSubscription.createFromUri("espresso://MASTER/testDb/2/tableB");
    DatabusSubscription sub5 = DatabusSubscription.createFromUri("espresso://MASTER/testDb/3/tableC");
    DatabusSubscription sub6 = DatabusSubscription.createFromUri("espresso://MASTER/testDb/4");

    final PartitionMultiplexingConsumer testConsumer1 =
        new PartitionMultiplexingConsumer(new RegistrationId("test1"), null, log, null, 500, false,
                                          false, sub1, sub2, sub3, sub4, sub5, sub6);

    List<DatabusSubscription> part0Subs = testConsumer1.getPartitionSubs(new PhysicalPartition(0, "testDb"));
    Assert.assertNull(part0Subs);
    Assert.assertNull(testConsumer1.getPartitionConsumer(new PhysicalPartition(0, "testDb")));

    List<DatabusSubscription> part1Subs = testConsumer1.getPartitionSubs(new PhysicalPartition(1, "testDb"));
    Assert.assertEquals(part1Subs.size(), 1);
    Assert.assertEquals(part1Subs.get(0), sub1);
    Assert.assertNotNull(testConsumer1.getPartitionConsumer(sub1));;
    Assert.assertEquals(testConsumer1.getPartitionConsumer(sub1),
                        testConsumer1.getPartitionConsumer(new PhysicalPartition(1, "testDb")));

    List<DatabusSubscription> part2Subs = testConsumer1.getPartitionSubs(new PhysicalPartition(2, "testDb"));
    Assert.assertEquals(part2Subs.size(), 2);
    Assert.assertEquals(part2Subs.get(0), sub2);
    Assert.assertEquals(part2Subs.get(1), sub4);
    Assert.assertNotNull(testConsumer1.getPartitionConsumer(sub2));
    Assert.assertNotNull(testConsumer1.getPartitionConsumer(sub4));
    Assert.assertEquals(testConsumer1.getPartitionConsumer(sub2),
                        testConsumer1.getPartitionConsumer(new PhysicalPartition(2, "testDb")));
    Assert.assertEquals(testConsumer1.getPartitionConsumer(sub2),
                        testConsumer1.getPartitionConsumer(sub4));

    List<DatabusSubscription> part3Subs = testConsumer1.getPartitionSubs(new PhysicalPartition(3, "testDb"));
    Assert.assertEquals(part3Subs.size(), 2);
    Assert.assertEquals(part3Subs.get(0), sub3);
    Assert.assertEquals(part3Subs.get(1), sub5);
    Assert.assertNotNull(testConsumer1.getPartitionConsumer(sub3));
    Assert.assertNotNull(testConsumer1.getPartitionConsumer(sub5));
    Assert.assertEquals(testConsumer1.getPartitionConsumer(sub3),
                        testConsumer1.getPartitionConsumer(new PhysicalPartition(3, "testDb")));
    Assert.assertEquals(testConsumer1.getPartitionConsumer(sub3),
                        testConsumer1.getPartitionConsumer(sub5));

    List<DatabusSubscription> part4Subs = testConsumer1.getPartitionSubs(new PhysicalPartition(4, "testDb"));
    Assert.assertEquals(part4Subs.size(), 1);
    Assert.assertEquals(part4Subs.get(0), sub6);
    Assert.assertNotNull(testConsumer1.getPartitionConsumer(sub6));
    Assert.assertEquals(testConsumer1.getPartitionConsumer(sub6),
                        testConsumer1.getPartitionConsumer(new PhysicalPartition(4, "testDb")));

    List<DatabusSubscription> part5Subs = testConsumer1.getPartitionSubs(new PhysicalPartition(5, "testDb"));
    Assert.assertNull(part5Subs);
    Assert.assertNull(testConsumer1.getPartitionConsumer(new PhysicalPartition(5, "testDb")));
  }

  @Test
  public void testPartitionConsumerCallbacksSeq() throws Exception
  {
    //It looks like the unit test class load not always loads all classes so we
    //have to force it explicitly
    EspressoSubscriptionUriCodec.getInstance();

    Logger log = Logger.getLogger("TestPartitionMultiplexingConsumer.testManySubsPerPartition");
    //log.setLevel(Level.DEBUG);
    DatabusSubscription sub0 = DatabusSubscription.createFromUri("espresso://MASTER/testDb/0/tableA");
    DatabusSubscription sub1 = DatabusSubscription.createFromUri("espresso://MASTER/testDb/1/tableA");
    DatabusSubscription sub2 = DatabusSubscription.createFromUri("espresso://MASTER/testDb/2/tableA");

    final TPMCMultiPartitionTestConsumer userCons =
        new TPMCMultiPartitionTestConsumer(null, log, true);
    final PartitionMultiplexingConsumer testConsumer1 =
        new PartitionMultiplexingConsumer(new RegistrationId("test1"), userCons, log, null, 500, false,
                                          false, sub0, sub1, sub2);

    final int partNum = 3;
    final int bufWinNum = 5;
    final int winEventNum = 5;
    final PartitionMultiplexingConsumer.PartitionConsumer cons[] =
        new PartitionMultiplexingConsumer.PartitionConsumer[partNum];
    final DbusEventBuffer[] buf = new DbusEventBuffer[partNum];
   for (int i = 0; i < partNum; ++i)
   {
     cons[i]= testConsumer1.getPartitionConsumer(new PhysicalPartition(i, "testDb"));
     Assert.assertNotNull(cons[i]);

     buf[i] = new DbusEventBuffer(_bufCfg);
     buf[i].start(0);
     DbusEventInfo[] einfo = createSampleTableAEvents(bufWinNum * winEventNum, (short)i);
     for (int w = 0; w < bufWinNum; ++w)
     {
       buf[i].startEvents();
       for (int j = 0; j < winEventNum; ++j)
       {
         DbusEventKey key = new DbusEventKey(("key" + j).getBytes("UTF-8"));
         buf[i].appendEvent(key, einfo[w * winEventNum + j], null);
       }
       buf[i].endEvents( 10 + (w + 1) * (i + 1));
     }
   }

   final VersionedSchemaSet schemaSet = new VersionedSchemaSet();
   schemaSet.add(new VersionedSchema(TABLEA_SCHEMA.getFullName(), (short)1, TABLEA_SCHEMA));
   final DbusEventAvroDecoder eventDecoder = new DbusEventAvroDecoder(schemaSet);
   final MultiPartitionSCN scn = new MultiPartitionSCN(partNum);
   final ArrayList<String> expCbs = new ArrayList<String>();

   final DbusEventIterator[] iter = new DbusEventIterator[partNum];
   final TestPartitionGenerator[] gens = new TestPartitionGenerator[partNum];
   for (int p = 0; p < partNum; ++p)
   {
     iter[p] = buf[p].acquireIterator("iter" + p);
     Assert.assertTrue(iter[p].hasNext());
     Assert.assertTrue(iter[p].next().isEndOfPeriodMarker()); //skip over first EOW
     gens[p] = new TestPartitionGenerator(p, cons, iter, expCbs, scn, eventDecoder, winEventNum,
                                          false);
     cons[p].onStartConsumption();
   }

   expCbs.add("onStartConsumption()");
   for (int w = 0; w < bufWinNum; ++w)
   {

     for (int i = 0; i < partNum; ++i)
     {
       int p = (i + w) % partNum;

       long seq = 10 + (w + 1) * (p + 1);
       gens[p].startPartitionWindow(seq);
       gens[p].generateTableData(seq);
       gens[p].endPartitionWindow(seq);
     }
   }
   cons[partNum - 1].onError(new DatabusException());
   PhysicalPartitionConsumptionException ex =
       new PhysicalPartitionConsumptionException(new PhysicalPartition(partNum - 1, "testDb"),
                                                 new DatabusException());
   expCbs.add("onError(" + PhysicalPartitionConsumptionException.class.getName() +
              ":" + ex.getMessage() + ")");

   expCbs.add("onStopConsumption()");

   for (int i = 0; i < partNum; ++i)
   {
     cons[i].onStopConsumption();
   }

   List<String> cbs = userCons.getCallbacks();
   Assert.assertEquals(cbs.size(), expCbs.size());
   for (int i = 0; i < expCbs.size(); ++i)
   {
     Assert.assertEquals(cbs.get(i), expCbs.get(i), "cb[" + i + "]" );
   }

   Assert.assertTrue(cbs.get(cbs.size() - 2).contains(Integer.toString(partNum - 1)));
  }

  @Test
  public void testPartitionConsumerCallbacksPar() throws Exception
  {
    //It looks like the unit test class load not always loads all classes so we
    //have to force it explicitly
    EspressoSubscriptionUriCodec.getInstance();

    Logger log = Logger.getLogger("TestPartitionMultiplexingConsumer.testManySubsPerPartition");
    //log.setLevel(Level.DEBUG);
    DatabusSubscription sub0 = DatabusSubscription.createFromUri("espresso://MASTER/testDb/0/tableA");
    DatabusSubscription sub1 = DatabusSubscription.createFromUri("espresso://MASTER/testDb/1/tableA");
    DatabusSubscription sub2 = DatabusSubscription.createFromUri("espresso://MASTER/testDb/2/tableA");

    final TPMCMultiPartitionTestConsumer userCons =
        new TPMCMultiPartitionTestConsumer(null, log, true);
    final PartitionMultiplexingConsumer testConsumer1 =
        new PartitionMultiplexingConsumer(new RegistrationId("test1"), userCons, log, null, 500, false,
                                          false, sub0, sub1, sub2);

    final int partNum = 3;
    final int bufWinNum = 5;
    final int winEventNum = 5;
    final PartitionMultiplexingConsumer.PartitionConsumer cons[] =
        new PartitionMultiplexingConsumer.PartitionConsumer[partNum];
    final DbusEventBuffer[] buf = new DbusEventBuffer[partNum];
   for (int i = 0; i < partNum; ++i)
   {
     cons[i]= testConsumer1.getPartitionConsumer(new PhysicalPartition(i, "testDb"));
     Assert.assertNotNull(cons[i]);

     buf[i] = new DbusEventBuffer(_bufCfg);
     buf[i].start(0);
     DbusEventInfo[] einfo = createSampleTableAEvents(bufWinNum * winEventNum, (short)i);
     for (int w = 0; w < bufWinNum; ++w)
     {
       buf[i].startEvents();
       for (int j = 0; j < winEventNum; ++j)
       {
         DbusEventKey key = new DbusEventKey(("key" + j).getBytes("UTF-8"));
         buf[i].appendEvent(key, einfo[w * winEventNum + j], null);
       }
       buf[i].endEvents( 10 + w);
     }
   }

   final ExecutorService exec =
       Executors.newFixedThreadPool(partNum,
                                    new NamedThreadFactory("testPartitionConsumerCallbacksPar", true));

   final VersionedSchemaSet schemaSet = new VersionedSchemaSet();
   schemaSet.add(new VersionedSchema(TABLEA_SCHEMA.getFullName(), (short)1, TABLEA_SCHEMA));
   final DbusEventAvroDecoder eventDecoder = new DbusEventAvroDecoder(schemaSet);
   final MultiPartitionSCN scn = new MultiPartitionSCN(partNum);
   final ArrayList<String> expCbs = new ArrayList<String>();

   final DbusEventIterator[] iter = new DbusEventIterator[partNum];
   final TestPartitionGenerator[] gens = new TestPartitionGenerator[partNum];
   for (int p = 0; p < partNum; ++p)
   {
     iter[p] = buf[p].acquireIterator("iter" + p);
     Assert.assertTrue(iter[p].hasNext());
     Assert.assertTrue(iter[p].next().isEndOfPeriodMarker()); //skip over first EOW
     gens[p] = new TestPartitionGenerator(p, cons, iter, expCbs, scn, eventDecoder, winEventNum,
                                          false);
     cons[p].onStartConsumption();
   }

   expCbs.add("onStartConsumption()");
   for (int w = 0; w < bufWinNum; ++w)
   {
     final long seq = 10 + w;

     final int p0 = (0 + w) % partNum;
     //get a lock
     gens[p0].startPartitionWindow(seq);

     Future<?>[] futures = new Future<?>[partNum];

     //queue up other partitions
     for (int i = 1; i < partNum; ++i)
     {
       final int p = (i + w) % partNum;
       futures[p] = exec.submit(new Runnable()
      {
        @Override
        public void run()
        {
          gens[p].startPartitionWindow(seq);
          gens[p].generateTableData(seq);
          gens[p].endPartitionWindow(seq);
        }
      });
     }

     //finish the first partition
     gens[p0].generateTableData(seq);
     gens[p0].endPartitionWindow(seq);

     //finishing the first partition should unblock the rest
     for (int i = 1; i < partNum; ++i)
     {
       final int p = (i + w) % partNum;
       futures[p].get(1000, TimeUnit.MILLISECONDS);
       Assert.assertTrue(futures[p].isDone());
     }
   }
   expCbs.add("onStopConsumption()");

   for (int i = 0; i < partNum; ++i)
   {
     cons[i].onStopConsumption();
   }

   List<String> cbs = userCons.getCallbacks();
   Assert.assertEquals(cbs.size(), expCbs.size());
   for (int i = 0; i < expCbs.size(); ++i)
   {
     Assert.assertEquals(cbs.get(i), expCbs.get(i), "cb[" + i + "]" );
   }
  }

  class TestPartitionGenerator
  {
    final int p;
    final PartitionMultiplexingConsumer.PartitionConsumer[] cons;
    final DbusEventIterator[] iter;
    final ArrayList<String> expCbs;
    final MultiPartitionSCN scn;
    final DbusEventAvroDecoder eventDecoder;
    final int winEventNum;
    final boolean _eowReleasesLock;

    public TestPartitionGenerator(int p,
                                    PartitionConsumer[] cons,
                                    DbusEventIterator[] iter,
                                    ArrayList<String> expCbs,
                                    MultiPartitionSCN scn,
                                    DbusEventAvroDecoder eventDecoder,
                                    int winEventNum,
                                    boolean eowReleasesLock)
    {
      super();
      this.p = p;
      this.cons = cons;
      this.iter = iter;
      this.expCbs = expCbs;
      this.scn = scn;
      this.eventDecoder = eventDecoder;
      this.winEventNum = winEventNum;
      _eowReleasesLock = eowReleasesLock;
    }

    void endPartitionWindow(final long seq)
    {
       cons[p].onEndDataEventSequence(new SingleSourceSCN(p, seq));
       expCbs.add("onEndDataEventSequence(" + scn + ")");
       expCbs.add("onCheckpoint(" + scn + ")");
       if (!_eowReleasesLock) Assert.assertTrue(cons[p].ensureMyTicket());
       cons[p].onCheckpoint(new SingleSourceSCN(p, seq));
    }

    void generateTableData(final long seq)
    {
      cons[p].onStartSource(TABLEA_SCHEMA.getFullName(), TABLEA_SCHEMA);
       expCbs.add("onStartSource(" + TABLEA_SCHEMA.getFullName() + ")");

       for (int j = 0; j < winEventNum; ++j)
       {

         Assert.assertTrue(iter[p].hasNext());
         DbusEvent e = iter[p].next();
         Assert.assertTrue(!e.isCheckpointMessage());
         cons[p].onDataEvent(e, eventDecoder);
         expCbs.add("onDataEvent(" + e + ")");

         if (j == 5)
         {
           cons[p].onRollback(new SingleSourceSCN(p, seq));
           expCbs.add("onRollback(" + scn + ")");
         }
       }
       Assert.assertTrue(iter[p].hasNext());
       Assert.assertTrue(iter[p].next().isEndOfPeriodMarker()); //skip over EOW

       cons[p].onEndSource(TABLEA_SCHEMA.getFullName(), TABLEA_SCHEMA);
       expCbs.add("onEndSource(" + TABLEA_SCHEMA.getFullName() + ")");
    }

    void startPartitionWindow(final long seq)
    {
       cons[p].onStartDataEventSequence(new SingleSourceSCN(p, seq));
       scn.partitionSeq(p, seq);
       expCbs.add("onStartDataEventSequence(" + scn + ")");
    }
  }

  static DbusEventInfo[] createSampleTableAEvents(int eventsNum, short part) throws IOException
  {
    Random rng = new Random();
    DbusEventInfo[] result = new DbusEventInfo[eventsNum];
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(TABLEA_SCHEMA);
    for (int i = 0; i < eventsNum; ++i)
    {
      GenericRecord r = new GenericData.Record(TABLEA_SCHEMA);
      String s = RngUtils.randomString(rng.nextInt(100));
      r.put("s", s);
      ByteArrayOutputStream baos = new ByteArrayOutputStream(s.length() + 100);
      BinaryEncoder out = new BinaryEncoder(baos);
      try
      {
        writer.write(r, out);
        out.flush();

        result[i] = new DbusEventInfo(DbusOpcode.UPSERT, 1, part, part,
                                      System.nanoTime(),
                                      (short)1, TABLEA_SCHEMAID,
                                      baos.toByteArray(), false, true);
      }
      finally
      {
        baos.close();
      }
    }

    return result;
  }

}

class TestPartitionTicketAcquirer implements Runnable
{

  public final PartitionMultiplexingConsumer.PartitionConsumer _owner;
  public final AtomicBoolean _ticketSuccess;
  public final PartitionMultiplexingConsumer _parent;
  public final long _sleepMs;
  public final Logger _log;

  public TestPartitionTicketAcquirer(PartitionMultiplexingConsumer parent,
                                     PartitionConsumer owner, long sleepMs,
                                     Logger log)
  {
    super();
    _parent = parent;
    _owner = owner;
    _ticketSuccess = new AtomicBoolean(false);
    _sleepMs = sleepMs;
    String loggerSuffix = "." + owner.getPartition().getName() + "_" + owner.getPartition().getId();
    _log = null == log  ? Logger.getLogger(getClass().toString() + loggerSuffix)
                        : Logger.getLogger(log.getName() + loggerSuffix);
  }

  @Override
  public void run()
  {
    try
    {
      _log.debug("start ");
      PartitionMultiplexingConsumer.PartitionTicket ticket = _parent.acquireTicket(_owner);
      _log.debug("ticket out");

      Assert.assertNotNull(ticket, "no ticket");
      _log.debug("has ticket");
      Assert.assertEquals(ticket.getRenewCnt(), 0);
      Assert.assertEquals(ticket.getOwner(), _owner);
      Assert.assertTrue(ticket.isValid());

      TestUtil.sleep(_sleepMs / 2);

      Assert.assertTrue(ticket.validate());
      _log.debug("ticket validated");
      //the first ticket is still valid and the second thread should still be waiting
      Assert.assertTrue(ticket.isValid());
      Assert.assertEquals(ticket.getRenewCnt(), 1);

      TestUtil.sleep(_sleepMs / 2);
      ticket.close();
      _log.debug("ticket closed");
      _ticketSuccess.set(true);
    }
    catch (InterruptedException e)
    {
      throw new RuntimeException(e);
    }

  }

  public boolean getTicketSuccess()
  {
    return _ticketSuccess.get();
  }

}

class TPMCMultiPartitionTestConsumer extends CallbackTrackingDatabusConsumer implements DatabusV3Consumer
{
  final boolean _canBootstrap;

  public TPMCMultiPartitionTestConsumer(DatabusCombinedConsumer delegate, Logger log,
                                        boolean canBootstrap)
  {
    super(delegate, log);
    _canBootstrap = canBootstrap;
  }

  @Override
  public boolean canBootstrap()
  {
    return _canBootstrap;
  }

}
