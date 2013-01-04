package com.linkedin.databus.client.consumer;

import com.linkedin.databus.client.pub.DatabusBootstrapConsumer;
import com.linkedin.databus.client.pub.DatabusStreamConsumer;

/**
 * A that delegates all class to another consumer. This class is
 * meant to overriden with specific implementations of some of the callbacks while it will take
 * care of the rest of the callbacks.
 * */
public class SelectingDatabusCombinedConsumer extends DelegatingDatabusCombinedConsumer
{

  public SelectingDatabusCombinedConsumer(DatabusStreamConsumer streamDelegate)
  {
    super(streamDelegate, null);
  }

  public SelectingDatabusCombinedConsumer(DatabusBootstrapConsumer bootstrapDelegate)
  {
    super(null, bootstrapDelegate);
  }
  
  public boolean canBootstrap()
  {
	  if (_bootstrapDelegate != null)
		  return true;
	  else
		  return false;
  }

}
