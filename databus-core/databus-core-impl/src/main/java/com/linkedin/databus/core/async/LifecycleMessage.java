package com.linkedin.databus.core.async;


/**
 * This class represents a message  sent to a {@link ActorMessageQueue} to control its lifecycle.
 *
 * <p>The lifecycle messages are as follows:
 * <ul>
 *   <li>START - start the event processing loop of the actor</li>
 *   <li>SHUTDOWN - shutdown the actor with an optional error reason</li>
 *   <li>PAUSE - temporarily stop the event processing loop of the actor because of client request</li>
 *   <li>SUSPEND_ON_ERROR - temporarily stop the event processing loop of the actor because of an
 *       error</li>
 *   <li>RESUME - resume the execution of the actor after being paused or suspended</li>
 * </ul>
 *
 * <p>The lifecycle can be represented using the following regex
 *
 * <p>Lifecycle ::= START ((PAUSE | SUSPEND_ON_ERROR) RESUME)* (PAUSE | SUSPEND_ON_ERROR)? SHUTDOWN
 *
 * @author cbotev
 *
 */
public class LifecycleMessage
{
  public enum TypeId
  {
    START,
    SHUTDOWN,
    PAUSE,
    SUSPEND_ON_ERROR,
    RESUME
  }

  private Throwable _lastError = null;
  private TypeId _typeId = null;

  /** Creates a new START message */
  public static LifecycleMessage createStartMessage()
  {
    return (new LifecycleMessage()).switchToStart();
  }

  /** Creates a new SHUTDOWN message with no cause */
  public static LifecycleMessage createShutdownMessage()
  {
    return (new LifecycleMessage()).switchToShutdown(null);
  }

  /**
   * Creates a new SHUTDOWN message with the specified reason
   * @param  reason     the error that caused the shutdown
   **/
  public static LifecycleMessage createShutdownMessage(Throwable reason)
  {
    return (new LifecycleMessage()).switchToShutdown(reason);
  }

  /** Creates a new PAUSE message */
  public static LifecycleMessage createPauseMessage()
  {
    return (new LifecycleMessage()).switchToPause();
  }

  /**
   * Creates a new SUSPEND_ON_ERROR message with the specified reason
   * @param  reason         the error that caused the suspend
   */
  public static LifecycleMessage createSuspendOnErroMessage(Throwable reason)
  {
    return (new LifecycleMessage()).switchToSuspendOnError(reason);
  }

  /** Creates a new RESUME message */
  public static LifecycleMessage createResumeMessage()
  {
    return (new LifecycleMessage()).switchToResume();
  }

  /** Returns that caused the current state (SHUTDOWN or SUSPEND) */
  public Throwable getLastError()
  {
    return _lastError;
  }

  /** Returns the current type of the message */
  public TypeId getTypeId()
  {
    return _typeId;
  }

  /**
   * Reuses the current object and switches it to a START message
   * @return this message object
   */
  public LifecycleMessage switchToStart()
  {
    _typeId = TypeId.START;
    return this;
  }

  /**
   * Reuses the current object and switches it to a SHUTDOWN message
   * @param  reason     the error that caused the shutdown
   * @return this message object
   */
  public LifecycleMessage switchToShutdown(Throwable reason)
  {
    _typeId = TypeId.SHUTDOWN;
    _lastError = reason;
    return this;
  }

  /**
   * Reuses the current object and switches it to a PAUSE message
   * @return this message object
   */
  public LifecycleMessage switchToPause()
  {
    _typeId = TypeId.PAUSE;
    return this;
  }

  /**
   * Reuses the current object and switches it to a new SUSPEND_ON_ERROR message
   * @param  reason     the error that caused the suspend
   * @return this message object
   */
  public LifecycleMessage switchToSuspendOnError(Throwable reason)
  {
    _typeId = TypeId.SUSPEND_ON_ERROR;
    _lastError = reason;
    return this;
  }

  /**
   * Reuses the current object and switches it to a RESUME message
   * @return this message object
   */
  public LifecycleMessage switchToResume()
  {
    _typeId = TypeId.RESUME;
    return this;
  }


  @Override
  public String toString()
  {
    return _typeId.toString();
  }
}
