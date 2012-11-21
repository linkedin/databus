package com.linkedin.databus.core.util;

/**
 * Used for creation and verification of configs
 * @author cbotev
 *
 * @param <C>       the type of configs to be created
 */
public interface ConfigBuilder<C>
{

  C build() throws InvalidConfigException;
}
