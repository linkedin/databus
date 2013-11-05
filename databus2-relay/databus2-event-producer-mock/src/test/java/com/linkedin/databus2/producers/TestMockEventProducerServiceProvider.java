package com.linkedin.databus2.producers;

import org.apache.log4j.Level;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus2.test.TestUtil;

/** Tests {@link MockEventProducerServiceProvider} */
public class TestMockEventProducerServiceProvider
{

  @BeforeClass
  public void setupClass()
  {
    TestUtil.setupLogging(true, null, Level.ERROR);
  }

  @Test
  /** Verifies that the provider is automatically loaded */
  public void testAutoLoading()
  {
    EventProducerServiceProvider provider =
        RelayEventProducersRegistry.getInstance().getEventProducerServiceProvider(MockEventProducerServiceProvider.SCHEME);
    Assert.assertNotNull(provider);
    Assert.assertEquals(provider.getClass(), MockEventProducerServiceProvider.class);
  }
}
