package com.linkedin.databus.client;

import com.linkedin.databus.client.pub.ServerInfo;
import com.linkedin.databus.core.Checkpoint;


public interface DatabusBootstrapConnectionStateMessage extends DatabusStreamConnectionStateMessage
{
  void switchToStartScnRequestError();
  void switchToStartScnResponseError();
  void switchToStartScnSuccess(Checkpoint cp, DatabusBootstrapConnection bootstrapConnection, ServerInfo serverInfo);
  void switchToStartScnRequestSent();
  void switchToTargetScnRequestError();
  void switchToTargetScnResponseError();
  void switchToTargetScnSuccess();
  void switchToTargetScnRequestSent();
  void switchToBootstrapDone();
}
