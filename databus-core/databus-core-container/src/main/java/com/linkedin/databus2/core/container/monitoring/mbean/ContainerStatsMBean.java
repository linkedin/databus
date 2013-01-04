package com.linkedin.databus2.core.container.monitoring.mbean;


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
