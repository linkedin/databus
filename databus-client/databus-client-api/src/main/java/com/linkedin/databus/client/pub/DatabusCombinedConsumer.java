package com.linkedin.databus.client.pub;

/**
 * A convenience interface for consumers which implement both the {@link DatabusStreamConsumer} and
 * {@link DatabusBootstrapConsumer}.
 */
public interface DatabusCombinedConsumer extends DatabusStreamConsumer, DatabusBootstrapConsumer
{
	/**
	 * @return true, if this consumer-callback instance can consume bootstrap/catchup events
	 *         false, if it cannot
	 */
	public boolean canBootstrap();

}
