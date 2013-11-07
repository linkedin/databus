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



/**
 * Statistics about the network container.
 *
 * The I/O threadpool is handed off to Netty for its internal use (handling
 * connections and whatnot, near the beginning of the pipeline).  The worker
 * threadpool is used at the other end of the pipeline, to service the
 # incoming requests and produce the actual responses.
 */
public interface ContainerStatsMBean
{
  // ************** GETTERS *********************

  /** number of active (running) I/O threads */
  int getIoThreadRate();

  /** max number of I/O threads seen during a metrics update */
  int getIoThreadMax();

  /** number of scheduled I/O tasks since startup (approximate) */
  long getIoTaskCount();

  /** max number of scheduled I/O tasks in a metric-update interval */
  int getIoTaskMax();

  /** number of I/O tasks currently waiting in queue for a thread */
  int getIoTaskQueueSize();

  /** number of active (running) worker threads */
  int getWorkerThreadRate();

  /** max number of worker threads seen during a metrics update */
  int getWorkerThreadMax();

  /** number of scheduled worker tasks since startup (approximate) */
  long getWorkerTaskCount();

  /** max number of scheduled worker tasks in a metric-update interval */
  int getWorkerTaskMax();

  /** number of worker tasks currently waiting in queue for a thread */
  int getWorkerTaskQueueSize();

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
