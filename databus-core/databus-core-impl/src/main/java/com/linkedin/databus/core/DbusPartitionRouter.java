package com.linkedin.databus.core;


public interface DbusPartitionRouter
{
  public DbusPartitionId  getPartitionId(DbusEvent event);
}
