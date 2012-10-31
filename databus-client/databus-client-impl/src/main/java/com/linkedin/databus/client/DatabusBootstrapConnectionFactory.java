package com.linkedin.databus.client;

import java.io.IOException;

import com.linkedin.databus.client.netty.RemoteExceptionHandler;
import com.linkedin.databus.client.pub.ServerInfo;
import com.linkedin.databus.core.async.ActorMessageQueue;

public interface DatabusBootstrapConnectionFactory
{
  DatabusBootstrapConnection createConnection(ServerInfo relay,
                                              ActorMessageQueue callback,
                                              RemoteExceptionHandler remoteExceptionHandler)
                                              throws IOException;
}
