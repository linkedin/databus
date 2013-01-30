package com.linkedin.databus.bootstrap.producer;
/*
 *
 * Copyright 2013 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/


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
