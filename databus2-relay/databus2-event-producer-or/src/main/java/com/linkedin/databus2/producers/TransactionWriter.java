package com.linkedin.databus2.producers;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.databus.core.DatabusRuntimeException;
import com.linkedin.databus.core.DatabusThreadBase;
import com.linkedin.databus2.producers.ORListener.TransactionProcessor;
import com.linkedin.databus2.producers.ds.Transaction;

public class TransactionWriter extends DatabusThreadBase
{
  private final BlockingQueue<Transaction> transactionQueue;
  private final TransactionProcessor txnProcessor;
  private final long queueTimeoutMs;

  private final Logger log = LoggerFactory.getLogger(getClass());

  public TransactionWriter(int maxQueueSize, long queueTimeoutMs, TransactionProcessor txnProcessor)
  {
    super("transactionWriter");
    this.txnProcessor = txnProcessor;
    this.queueTimeoutMs = queueTimeoutMs;
    transactionQueue = new LinkedBlockingQueue<Transaction>(maxQueueSize);
  }

  public void addTransaction(Transaction transaction)
  {
    boolean isPut = false;
    do
    {
      try
      {
        isPut = transactionQueue.offer(transaction, this.queueTimeoutMs, TimeUnit.MILLISECONDS);
      }
      catch (InterruptedException e)
      {
        _log.error("failed to put transaction to eventQueue,will retry!");
      }
    } while (!isPut && !isShutdownRequested());
  }

  @Override
  public void run()
  {
    List<Transaction> transactionList = new ArrayList<Transaction>();
    Transaction transaction = null;
    while (!isShutdownRequested())
    {
      if (isPauseRequested())
      {
        LOG.info("Pause requested for TransactionWriter. Pausing !!");
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
        LOG.info("Resuming TransactionWriter !!");
        signalResumed();
        LOG.info("TransactionWriter resumed !!");
      }
      transactionList.clear();
      int transactionNum = transactionQueue.drainTo(transactionList);
      if (transactionNum == 0)
      {
        try
        {
          transaction = transactionQueue.poll(queueTimeoutMs, TimeUnit.MILLISECONDS);
          if (transaction != null)
          {
            transactionList.add(transaction);
            transactionNum = transactionList.size();
          }
        }
        catch (InterruptedException e)
        {
          log.error("Interrupted when poll from transactionEventQueue!!");
        }
      }
      if (transactionNum <= 0)
      {
        continue;
      }
      for (int i = 0; i < transactionNum; i++)
      {
        transaction = transactionList.get(i);
        if (transaction == null)
        {
          log.error("received null transaction");
          continue;
        }
        try
        {
          txnProcessor.onEndTransaction(transaction);
        }
        catch (Exception e)
        {
          log.error("Got exception in the transaction handler ", e);
          throw new DatabusRuntimeException("Got exception in the transaction handler ", e);
        }
        finally
        {

        }
      }
    }
    log.info("transactionWriter thread done!");
    doShutdownNotify();
  }

}
