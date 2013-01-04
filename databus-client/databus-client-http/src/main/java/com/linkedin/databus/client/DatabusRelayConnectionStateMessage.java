package com.linkedin.databus.client;
import java.util.List;
import java.util.Map;

import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus2.core.container.request.RegisterResponseEntry;


public interface DatabusRelayConnectionStateMessage extends DatabusStreamConnectionStateMessage
{
  void switchToSourcesRequestError();
  void switchToSourcesRequestSent();
  void switchToSourcesResponseError();
  void switchToSourcesSuccess(List<IdNamePair> result);
  void switchToRegisterRequestError();
  void swichToRegisterRequestSent();
  void switchToRegisterResponseError();
  void switchToRegisterSuccess(Map<Long, List<RegisterResponseEntry>> result);
  void switchToStreamRequestSent();
  void switchToBootstrapRequested();
}
