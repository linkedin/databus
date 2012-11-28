package com.linkedin.databus3.espresso.client.test;

import com.linkedin.databus3.espresso.client.ClientSensorFactory;
import com.linkedin.healthcheck.pub.spring.SensorRegistry;

public class EspressoClientSensorFactory extends ClientSensorFactory
{

  public EspressoClientSensorFactory(SensorRegistry sensorRegistry,
                                     EspressoTestDatabusClient client)
  {
    
    // TODO - index should be passed as an argument
    super(sensorRegistry, client.getClientImpl());
  }

}
