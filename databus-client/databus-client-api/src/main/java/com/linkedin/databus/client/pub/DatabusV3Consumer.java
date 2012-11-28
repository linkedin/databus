package com.linkedin.databus.client.pub;

/**
 * This interface defines the callbacks used by Databus Espresso client library to inform Databus 
 * Espresso consumers about events in the Databus stream from a Databus relay.
 *
 * This is forward compatible Databus Espresso Consumer interface that external consumers need to 
 * adhere to.
 *
 * @author pganti
 */
public interface DatabusV3Consumer extends DatabusCombinedConsumer
{

	/**
	 * 
	 * DatabusV3Consumer is a combined consumer API for streaming and bootstrapping
	 * 
	 * @return true, consumer can actually bootstrap 
	 *         false, if it cannot
	 */
	public boolean canBootstrap();
}
