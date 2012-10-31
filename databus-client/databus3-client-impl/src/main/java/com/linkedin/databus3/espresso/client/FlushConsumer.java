package com.linkedin.databus3.espresso.client;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.log4j.Logger;

import com.linkedin.databus.client.SingleSourceSCN;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DatabusCombinedConsumer;
import com.linkedin.databus.client.pub.DatabusV3Consumer;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.client.pub.SCN;
import com.linkedin.databus.core.DbusEvent;

public class FlushConsumer implements DatabusCombinedConsumer, DatabusV3Consumer
{
  public static final String MODULE = FlushConsumer.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);
  
  public static final String FLUSH_CONSUMER_REG_PREFIX  = FlushConsumer.class.getSimpleName();

  final long _targetSCNSequence;
  volatile boolean _targetSCNReached = false;
  String _dbName;
  int _partitionId;
  volatile long _currentMaxSCN = 0;
  CountDownLatch _latch = new CountDownLatch(1);
  
  public FlushConsumer(long targetSCN, String dbName, int partitionId)
  {
    _targetSCNSequence = targetSCN;
    _dbName = dbName;
    _partitionId = partitionId;
  }
  
  public boolean targetSCNReached()
  {
    return _targetSCNReached;
  }
  
  public long getCurrentMaxSCN()
  {
    return _currentMaxSCN;
  }
  
  public CountDownLatch getWaitLatch()
  {
    return _latch;
  }
  
  public boolean waitForTargetSCN(long timeout, TimeUnit unit) throws InterruptedException
  {
    return _latch.await((int)timeout, unit);
  }
  
  @Override
  public ConsumerCallbackResult onStartConsumption()
  {
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onStopConsumption()
  {
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onStartDataEventSequence(SCN startScn)
  {
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onEndDataEventSequence(SCN endScn)
  {
    if(endScn instanceof SingleSourceSCN)
    {
      long sequence = ((SingleSourceSCN)endScn).getSequence();
      _currentMaxSCN = sequence;
      if( sequence == _targetSCNSequence)
      {
        LOG.info(_dbName + "_" + _partitionId + " Target SCN " + _targetSCNSequence + " is reached");
        _targetSCNReached = true;
        _latch.countDown();
      }
      else if(sequence > _targetSCNSequence)
      {
        LOG.info(_dbName + "_" + _partitionId + " Target SCN " + _targetSCNSequence + " has passed, current sequence: " + sequence); 
        _targetSCNReached = true;
        _latch.countDown();
      }
    }
    else
    {
      LOG.error("endSCN is not in type SingleSourceSCN");
    }
    
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onRollback(SCN rollbackScn)
  {
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onStartSource(String source, Schema sourceSchema)
  {
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onEndSource(String source, Schema sourceSchema)
  {
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onDataEvent(DbusEvent e,
      DbusEventDecoder eventDecoder)
  {
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onCheckpoint(SCN checkpointScn)
  {
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onError(Throwable err)
  {
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onStartBootstrap()
  {
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onStopBootstrap()
  {
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onStartBootstrapSequence(SCN startScn)
  {
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onEndBootstrapSequence(SCN endScn)
  {
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onStartBootstrapSource(String sourceName,
      Schema sourceSchema)
  {
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onEndBootstrapSource(String name,
      Schema sourceSchema)
  {
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onBootstrapEvent(DbusEvent e,
      DbusEventDecoder eventDecoder)
  {
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onBootstrapRollback(SCN batchCheckpointScn)
  {
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onBootstrapCheckpoint(SCN checkpointScn)
  {
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public ConsumerCallbackResult onBootstrapError(Throwable err)
  {
    return ConsumerCallbackResult.SUCCESS;
  }

  @Override
  public boolean canBootstrap()
  {
    return false;
  }
}
