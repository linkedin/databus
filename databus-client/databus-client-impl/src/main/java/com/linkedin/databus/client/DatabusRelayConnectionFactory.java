package com.linkedin.databus.client;

import java.io.IOException;

import com.linkedin.databus.client.netty.RemoteExceptionHandler;
import com.linkedin.databus.client.pub.ServerInfo;
import com.linkedin.databus.core.async.ActorMessageQueue;

public interface DatabusRelayConnectionFactory
{
  DatabusRelayConnection createRelayConnection(ServerInfo relay,
                                               ActorMessageQueue callback,
                                               RemoteExceptionHandler remoteExceptionHandler)
                                               throws IOException;
}
