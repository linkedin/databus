package com.linkedin.databus2.core.mbean;

import java.util.List;

import com.linkedin.databus.core.CompoundDatabusComponentStatus;

public class DatabusCompoundReadOnlyStatus extends DatabusReadOnlyStatus
                                           implements DatabusCompoundReadOnlyStatusMBean
{
  protected CompoundDatabusComponentStatus _compoundStatus;

  public DatabusCompoundReadOnlyStatus(String name,
                                       CompoundDatabusComponentStatus status,
                                       long ownerId)
  {
    super(name, status, ownerId);
    _compoundStatus = status;
  }

  @Override
  public List<String> getInitializing()
  {
    return _compoundStatus.getInitializing();
  }

  @Override
  public int getNumInitializing()
  {
    return _compoundStatus.getInitializing().size();
  }

  @Override
  public List<String> getSuspended()
  {
    return _compoundStatus.getSuspended();
  }

  @Override
  public int getNumSuspended()
  {
    return _compoundStatus.getSuspended().size();
  }

  @Override
  public List<String> getPaused()
  {
    return _compoundStatus.getPaused();
  }

  @Override
  public int getNumPaused()
  {
    return _compoundStatus.getPaused().size();
  }

  @Override
  public List<String> getShutdown()
  {
    return _compoundStatus.getShutdown();
  }

  @Override
  public int getNumShutdown()
  {
    return _compoundStatus.getShutdown().size();
  }

  @Override
  public List<String> getRetryingOnError()
  {
    return _compoundStatus.getRetryingOnError();
  }

  @Override
  public int getNumRetryingOnError()
  {
    return _compoundStatus.getRetryingOnError().size();
  }

}
