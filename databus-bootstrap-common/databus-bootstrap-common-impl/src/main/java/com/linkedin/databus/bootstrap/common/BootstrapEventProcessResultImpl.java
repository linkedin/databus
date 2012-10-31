package com.linkedin.databus.bootstrap.common;

import com.linkedin.databus.bootstrap.api.BootstrapEventProcessResult;

public class BootstrapEventProcessResultImpl implements BootstrapEventProcessResult
{
  private boolean _isClientBufferLimitExceeded;
  private boolean _isDropped;
  private long _processedRowCount;
  
  public BootstrapEventProcessResultImpl(long processedRowCount, boolean isLimitExceeded, boolean dropped)
  {
    _isClientBufferLimitExceeded = isLimitExceeded;
    _processedRowCount = processedRowCount;
    _isDropped = dropped;
  }
  
  @Override
  public long getProcessedRowCount()
  {
    return _processedRowCount;
  }

  @Override
  public boolean isClientBufferLimitExceeded()
  {
    return _isClientBufferLimitExceeded;
  }
  
  @Override
  public boolean isDropped()
  {
    return _isDropped;
  }
  

  @Override
  public String toString()
  {
    return new String("isLimitExcceed=" + _isClientBufferLimitExceeded +
                      "; processedRowCount=" + _processedRowCount);
  }
}
