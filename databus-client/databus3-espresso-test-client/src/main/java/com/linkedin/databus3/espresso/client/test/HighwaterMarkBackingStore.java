package com.linkedin.databus3.espresso.client.test;

import org.I0Itec.zkclient.DataUpdater;

/**
 * 
 * @author sjagadis
 * Backing store for storing key-value properties
 * @param <T>
 */
public interface HighwaterMarkBackingStore<T>
{
  public void put(String key, DataUpdater<T> updater) throws Exception;
  public void put(String path, T updatedProperty) throws Exception;
  public T get(String key) throws Exception;
}
