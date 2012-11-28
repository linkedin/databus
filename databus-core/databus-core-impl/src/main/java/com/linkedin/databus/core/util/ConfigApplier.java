package com.linkedin.databus.core.util;


public interface ConfigApplier<T>
{

  /**
   * Applies a new config
   * @param oldConfig           the old config being changed;
   */
  void applyNewConfig(T oldConfig);

  /** Compares with another configuration for equality */
  boolean equalsConfig(T otherConfig);
}
