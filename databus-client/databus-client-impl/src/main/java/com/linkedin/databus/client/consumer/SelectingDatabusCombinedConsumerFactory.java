package com.linkedin.databus.client.consumer;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.databus.client.pub.DatabusBootstrapConsumer;
import com.linkedin.databus.client.pub.DatabusStreamConsumer;

public class SelectingDatabusCombinedConsumerFactory {

	static public List<SelectingDatabusCombinedConsumer> convertListOfStreamConsumers(List<DatabusStreamConsumer> dscs)
	{
		List<SelectingDatabusCombinedConsumer> lsdcc = new ArrayList<SelectingDatabusCombinedConsumer>();
		for (DatabusStreamConsumer dsc : dscs )
		{
			SelectingDatabusCombinedConsumer sdsc = new SelectingDatabusCombinedConsumer(dsc);
			lsdcc.add(sdsc);
		}
		return lsdcc;
	}
	
	static public List<SelectingDatabusCombinedConsumer> convertListOfBootstrapConsumers(List<DatabusBootstrapConsumer> dscs)
	{
		List<SelectingDatabusCombinedConsumer> lsdcc = new ArrayList<SelectingDatabusCombinedConsumer>();
		for (DatabusBootstrapConsumer dsc : dscs )
		{
			SelectingDatabusCombinedConsumer sdsc = new SelectingDatabusCombinedConsumer(dsc);
			lsdcc.add(sdsc);
		}
		return lsdcc;
	}

}
