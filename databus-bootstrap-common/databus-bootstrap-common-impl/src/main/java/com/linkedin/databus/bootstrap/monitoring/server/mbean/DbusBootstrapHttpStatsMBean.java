package com.linkedin.databus.bootstrap.monitoring.server.mbean;
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


import com.linkedin.databus.core.Checkpoint;

/**
 * MBean interface for the collector for Bootstrap HTTP monitoring mbeans.
 */
public interface DbusBootstrapHttpStatsMBean
{

  /**
   * Reset object
   */
  void reset();

  /**
   *  Metric collection enabled or not
   *   */
  boolean isEnabled();

  /** enable/disable metric collection */
  void setEnabled(boolean enabled);

  /** number of successful bootstrap requests (snapshot+catchup)  */
   long getNumReqBootstrap() ;
  /** number of successful snapshot bootstrap requests  */
  long getNumReqSnapshot();
  /** number of successful catchup bootstrap requests  */
    long getNumReqCatchup();
  /** number of erroneous bootstrap requests */
    long getNumErrReqBootstrap();
  /** number of requests where bootstrap db is too old */
    long getNumErrReqDatabaseTooOld();
  /** number of erroneous requests due to sql exception  */
    long getNumErrSqlException();
  /** number of successful startSCN requests */
    long getNumReqStartSCN();
  /** number of successful targetSCN requests */
    long getNumReqTargetSCN();
  /** number of erroneous targetSCN requests */
    long getNumErrStartSCN();
  /** number of erroneous targetSCN requests */
    long getNumErrTargetSCN();
  /** time taken by successful snapshot bootstrap requests */
    long getLatencySnapshot();
  /** time taken by successful catchup bootstrap requests */
    long getLatencyCatchup();
  /** time taken by successful startSCN requests */
    long getLatencyStartSCN();
  /** time taken by successful targetSCN requests */
    long getLatencyTargetSCN();
  /** requested batch size in bootstrap requests */
    long getSizeBatch();
  /** minimum scn seen in bootstrap requests */
    long getMinBootstrapSCN();
  /** maximum scn seen in  bootstrap requests */
    long getMaxBootstrapSCN();

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
