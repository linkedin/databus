package com.linkedin.databus.bootstrap.producer;

import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.linkedin.databus.bootstrap.producer.BootstrapApplierThread.sourcePositions;

public class ProducerNotification
{
  public static final String MODULE = ProducerNotification.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);
  
  private boolean _waiting = false;
    
  public synchronized void startWaiting(sourcePositions pos) throws InterruptedException, SQLException
  {
    _waiting = true;

    pos.refresh();
    
    LOG.info("Entering wait for more events from DatabusBootstrapProducer");
    
    while (_waiting)
    {
      wait();
    }
    
    LOG.info("Leaving wait to apply more events from DatabusBootstrapProducer");
    _waiting = false;
  }
  
  public synchronized void stopWaiting()
  {
    if (_waiting)
    {
      notifyAll();
      LOG.info("Notified applier to exit wait state");
    }
  }
}
