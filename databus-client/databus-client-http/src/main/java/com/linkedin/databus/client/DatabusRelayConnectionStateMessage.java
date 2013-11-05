package com.linkedin.databus.client;
/*
 *
 * Copyright 2013 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/

import java.util.List;
import java.util.Map;

import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus2.core.container.request.RegisterResponseEntry;
import com.linkedin.databus2.core.container.request.RegisterResponseMetadataEntry;


public interface DatabusRelayConnectionStateMessage extends DatabusStreamConnectionStateMessage
{
  void switchToSourcesRequestError();
  void switchToSourcesRequestSent();
  void switchToSourcesResponseError();
  void switchToSourcesSuccess(List<IdNamePair> result, String hostName, String svcName);
  void switchToRegisterRequestError();
  void swichToRegisterRequestSent();
  void switchToRegisterResponseError();
  void switchToRegisterSuccess(Map<Long, List<RegisterResponseEntry>> sourcesSchemas,
                               Map<Long, List<RegisterResponseEntry>> keysSchemas,
                               List<RegisterResponseMetadataEntry> metadataSchemas);
  void switchToStreamRequestSent();
  void switchToBootstrapRequested();
}
