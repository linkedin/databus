package com.linkedin.databus2.relay;

import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus2.core.DatabusException;

/** Factory for instantiating relays in standard setups like Oracle relay, Espresso relay, RandomGen
 * relay, etc. */
public interface RelayFactory
{
  HttpRelay createRelay() throws DatabusException;
}
