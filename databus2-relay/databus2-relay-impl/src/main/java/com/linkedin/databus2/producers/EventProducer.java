/*
 * $Id: EventProducer.java 260802 2011-04-14 19:10:09Z cbotev $
 */
package com.linkedin.databus2.producers;

/**
 * @author Jemiah Westerman<jwesterman@linkedin.com>
 * @version $Revision: 260802 $
 */
public interface EventProducer
{
  String getName();

  long getSCN();

  void start(long sinceSCN);

  boolean isRunning();

  boolean isPaused();

  void unpause();

  void pause();

  void shutdown();
  
  void waitForShutdown() throws InterruptedException, IllegalStateException;
  
  void waitForShutdown(long timeout) throws InterruptedException, IllegalStateException;

}
