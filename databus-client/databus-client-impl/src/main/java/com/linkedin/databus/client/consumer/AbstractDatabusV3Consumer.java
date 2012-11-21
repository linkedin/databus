package com.linkedin.databus.client.consumer;

import com.linkedin.databus.client.pub.DatabusV3Consumer;

/** Default implementation of {@link DatabusV3Consumer} interface.*/
public class AbstractDatabusV3Consumer extends AbstractDatabusCombinedConsumer
                                       implements DatabusV3Consumer
{

  @Override
  public boolean canBootstrap()
  {
    return false;
  }

}
