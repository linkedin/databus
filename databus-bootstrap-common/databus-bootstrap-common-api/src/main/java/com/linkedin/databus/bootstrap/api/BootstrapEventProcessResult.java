package com.linkedin.databus.bootstrap.api;

public interface BootstrapEventProcessResult
{
  public long getProcessedRowCount();
  public boolean isClientBufferLimitExceeded();
  public boolean isDropped();
}
