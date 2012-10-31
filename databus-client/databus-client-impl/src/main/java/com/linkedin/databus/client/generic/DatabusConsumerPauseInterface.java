package com.linkedin.databus.client.generic;

/**
 * @author dzhang
 * Create a pause/resume interface for consumer testing
 */
public interface DatabusConsumerPauseInterface
{
  public void pause();
  public void resume();
  public void waitIfPaused();
}
