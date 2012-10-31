package com.linkedin.databus3.espresso.client;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.databus.client.SingleSourceSCN;
import com.linkedin.databus.client.pub.SCN;
import com.linkedin.databus.core.DbusEvent;

public class TestRelayFlusher
{
  public static class FakeEventProducer extends Thread
  {
    FlushConsumer _consumer;
    int _interEventLatency;
    int _totalEvents;
    int _diff;
    long _startSCN;
    
    public FakeEventProducer(FlushConsumer consumer, int interEventLatency, int totalEvents, int diff, long startSCN)
    {
      _totalEvents = totalEvents;
      _diff = diff;
      _startSCN = startSCN;
      _consumer = consumer;
      _interEventLatency = interEventLatency;
    }
    
    public void run()
    {
      for(int i = 0;i < _totalEvents; i++)
      {
        long sequence = _startSCN + i*_diff;
        ByteBuffer bb = ByteBuffer.allocate(1000);
        DbusEvent e = new DbusEvent(bb,0);
        e.setSequence(sequence);
        _consumer.onDataEvent(e, null);
        SCN endSCN = new SingleSourceSCN(0 , sequence);
        _consumer.onEndDataEventSequence(endSCN);
        try
        {
          Thread.sleep(_interEventLatency);
        } catch (InterruptedException e1)
        {
          // TODO Auto-generated catch block
          e1.printStackTrace();
        }
      }
    }
  }
  @Test
  public void testWaitforSCN() throws InterruptedException
  {
    FlushConsumer consumer = new FlushConsumer(10000, "TestDB", 22);
    FakeEventProducer p = new FakeEventProducer(consumer, 100, 10, 100, 9900);
    p.start();
    
    boolean timeout = consumer.waitForTargetSCN(2000, TimeUnit.MILLISECONDS);
    Assert.assertTrue(timeout);
    Assert.assertTrue(consumer.targetSCNReached());
    Assert.assertTrue(consumer.getCurrentMaxSCN() >= 10000);
    p.interrupt();
  }
  
  @Test
  public void testTimeout() throws InterruptedException
  {
    FlushConsumer consumer = new FlushConsumer(10000, "TestDB", 22);
    FakeEventProducer p = new FakeEventProducer(consumer, 100, 10, 20, 9900);
    p.start();
    
    boolean timeout = consumer.waitForTargetSCN(200, TimeUnit.MILLISECONDS);
    Assert.assertFalse(timeout);
    Assert.assertFalse(consumer.targetSCNReached());
    Assert.assertTrue(consumer.getCurrentMaxSCN() < 10000);
    p.interrupt();
  }
}
