/**
 *
 */
package com.linkedin.databus.bootstrap.api;
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


import java.sql.ResultSet;

import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;

public interface BootstrapEventCallback
{
  // it shall be called for each row fetched from database
  BootstrapEventProcessResult onEvent(ResultSet rs, DbusEventsStatisticsCollector statsCollector) throws BootstrapProcessingException;

  void onCheckpointEvent(Checkpoint ckpt, DbusEventsStatisticsCollector curStatsCollector);
}
