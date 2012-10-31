package com.linkedin.databus.core.async;

/**
 * An interface for sending messages to an actor.
 * @author cbotev
 *
 */
public interface ActorMessageQueue
{
  /**
   * Add the message to the actor's message queue.
   * @param message
   */
  public void enqueueMessage(Object message);
}
