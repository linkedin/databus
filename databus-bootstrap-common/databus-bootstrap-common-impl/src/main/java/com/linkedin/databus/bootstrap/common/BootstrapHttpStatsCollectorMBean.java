package com.linkedin.databus.bootstrap.common;
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


import com.linkedin.databus.bootstrap.monitoring.server.mbean.DbusBootstrapHttpStatsMBean;
import com.linkedin.databus.core.Checkpoint;

public interface BootstrapHttpStatsCollectorMBean
{

  void reset();

  boolean isEnabled();

  void setEnabled(boolean enabled);

  /** Obtains the mbean used to collect statistics about total bootstrap HTTP calls */
  DbusBootstrapHttpStatsMBean getTotalStats();

  /** Obtains the stats monitoring bean for a given peer id */
  DbusBootstrapHttpStatsMBean getPeerStats(String peer);

  /**MUTATORS*/

  /** set metrics pertaining to bootstrap call */
  void registerBootStrapReq(Checkpoint cp,long latency,long size) ;
  void registerStartSCNReq(long latency);
  void registerTargetSCNReq(long latency);

  /** set metrics pertaining to err bootstrap req */
  void registerErrBootstrap();
  void registerErrStartSCN();
  void registerErrTargetSCN();
  void registerErrSqlException();
  void registerErrDatabaseTooOld();

}
