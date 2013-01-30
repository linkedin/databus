package com.linkedin.databus2.core.container.monitoring.mbean;
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



/** Statistics about the network container */
public interface ContainerStatsMBean
{
  // ************** GETTERS *********************

  /** number of running io threads */
  int getIoThreadRate();

  /** max number of io threads since last reset */
  int getIoThreadMax();

  /** number of active io threads */
  int getIoActiveThreadRate();

  /** number of scheduled io tasks */
  long getIoTaskCount();

  /** max number of scheduled io tasks since last reset */
  int getIoTaskMax();

  /** number of running worker threads */
  int getWorkerThreadRate();

  /** max number of worker threads since last reset */
  int getWorkerThreadMax();

  /** number of active worker threads */
  int getWorkerActiveThreadRate();

  /** number scheduled worker tasks */
  long getWorkerTaskCount();

  /** max number scheduled worker tasks since last reset */
  int getWorkerTaskMax();

  /** total number of errors */
  long getErrorCount();

  /** number of uncaught errors */
  long getErrorUncaughtCount();

  /** number of request processing errors */
  long getErrorRequestProcessingCount();

  // ****************** MUTATORS *********************

  /** Resets the statistics. */
  void reset();

}
