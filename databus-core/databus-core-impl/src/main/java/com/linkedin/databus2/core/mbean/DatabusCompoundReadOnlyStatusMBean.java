package com.linkedin.databus2.core.mbean;

import java.util.List;

public interface DatabusCompoundReadOnlyStatusMBean extends DatabusReadOnlyStatusMBean
{
  /** The list of names of children components not started yet */
  public List<String> getInitializing();

  /** The number of children components not started yet */
  public int getNumInitializing();

  /** The list of names of children components that are currently suspended because of an error */
  public List<String> getSuspended();

  /** The number of children components that are currently suspended because of an error */
  public int getNumSuspended();

  /** The list of names of children components that are currently manually paused */
  public List<String> getPaused();

  /** The number of children components that are currently manually paused */
  public int getNumPaused();

  /** The list of names of children components that are shutdown */
  public List<String> getShutdown();

  /** The number of children components that are shutdown */
  public int getNumShutdown();

  /** The list of names of children components that are currently retrying because of an error */
  public List<String> getRetryingOnError();

  /** The number of children components that are currently retrying because of an error */
  public int getNumRetryingOnError();
}
