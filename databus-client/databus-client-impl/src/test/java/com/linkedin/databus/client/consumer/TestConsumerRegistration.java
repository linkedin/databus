package com.linkedin.databus.client.consumer;

import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.databus.client.pub.DatabusStreamConsumer;

public class TestConsumerRegistration {

	@Test
	public void testSingleConsumer()
	throws Exception
	{
		DatabusStreamConsumer logConsumer = new LoggingConsumer();
		List<String> sources = new ArrayList<String>();
		ConsumerRegistration<DatabusStreamConsumer> consumerReg =
				new ConsumerRegistration<DatabusStreamConsumer>(logConsumer, sources, null);

		Assert.assertEquals(logConsumer, consumerReg.getConsumer());
		return;
	}

	@Test
	public void testMultipleConsumers()
	throws Exception
	{
		DatabusStreamConsumer logConsumer1 = new LoggingConsumer();
		DatabusStreamConsumer logConsumer2 = new LoggingConsumer();
		List<DatabusStreamConsumer> lcs = new ArrayList<DatabusStreamConsumer>();
		lcs.add(logConsumer1);
		lcs.add(logConsumer2);

		List<String> sources = new ArrayList<String>();
		ConsumerRegistration<DatabusStreamConsumer> consumerReg =
				new ConsumerRegistration<DatabusStreamConsumer>(lcs, sources, null);

		DatabusStreamConsumer cons = consumerReg.getConsumer();
		boolean condition = logConsumer1.equals(cons) || logConsumer2.equals(cons);
		Assert.assertEquals(condition, true);
		return;
	}

}
