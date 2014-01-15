package com.linkedin.databus.core.async;
/*
 *
 * Copyright 2013 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/


import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.log4j.Logger;

import com.linkedin.databus.core.DatabusComponentStatus;
import com.linkedin.databus2.core.BackoffTimerStaticConfig;

/**
 * A default implementation for an actor which let it be run in a thread and provides
 * controls for its lifecycle.
 */
public abstract class AbstractActorMessageQueue implements Runnable, ActorMessageQueue
{
  public static final String MODULE = AbstractActorMessageQueue.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  public static final int MAX_QUEUED_MESSAGE_HISTORY_SIZE = 100;
  public static final int MAX_QUEUED_MESSAGES = 10;
  public static final long MESSAGE_QUEUE_POLL_TIMEOUT_MS = 100;

  private final String _name;
  private final Queue<Object> _messageQueue = new ArrayDeque<Object>(MAX_QUEUED_MESSAGES);
  protected final CircularFifoBuffer _messageProcessedHistory = new CircularFifoBuffer(MAX_QUEUED_MESSAGE_HISTORY_SIZE);
  volatile boolean _hasMessages;

  private final Lock _controlLock = new ReentrantLock(true);
  private final Condition _shutdownCondition = _controlLock.newCondition();
  private final Condition _newStateCondition = _controlLock.newCondition();
  private volatile LifecycleMessage _shutdownRequest = null;
  protected LifecycleMessage _currentLifecycleState;
  protected final DatabusComponentStatus _componentStatus;

  private final MessageQueueFilter pauseFilter = new DefaultPauseFilter();
  private final MessageQueueFilter suspendFilter = new DefaultSuspendFilter();
  private final MessageQueueFilter shutdownFilter = new DefaultShutdownFilter();

  private long _numEnqueuedMessages = 0;
  private final boolean _enablePullerMessageQueueLogging;
  public AbstractActorMessageQueue(String name, BackoffTimerStaticConfig errorRetriesConf)
  {
	  this(name, errorRetriesConf,false);
  }

  public AbstractActorMessageQueue(String name,
		  						   BackoffTimerStaticConfig errorRetriesConf,
		  						   boolean enablePullerMessageQueueLogging)
  {
    super();
    _name = name;
    _currentLifecycleState = null;
    _componentStatus = new DatabusComponentStatus(name, errorRetriesConf);
    _enablePullerMessageQueueLogging = enablePullerMessageQueueLogging;
    _hasMessages = false;
  }

  public AbstractActorMessageQueue(String name)
  {
    this(name,BackoffTimerStaticConfig.UNLIMITED_RETRIES);
  }

  /**
   * This method is called by the StatMachine thread after it exits out of the main loop in AbstractActorMessageQueue.run() and
   * is about to shutdown.
   * After this method returns, the State Machine's status will be set to SHUTDOWN and other waiting threads (for this shutdown) will be signaled.
   * Subclasses must implement this method to cleanup their state for shutdown
   */
  protected abstract void onShutdown();

  /**
   * This method is called by the StatMachine thread after it exits out of the main loop in AbstractActorMessageQueue.run()
   */
  private void performShutdown()
  {
    onShutdown();
    LOG.info(getName() + " shutdown.");
    clearQueue(shutdownFilter);
  }

  protected void onResume()
  {
  }

  public final boolean doExecuteAndChangeState(Object message)
  {
	  boolean success = false;

	  try
	  {
		  success = executeAndChangeState(message);
	  } catch (RuntimeException re) {
		  LOG.error("Stopping because of runtime exception :", re);
		  success = false;
	  }

	  _messageProcessedHistory.add(message);
	  _numEnqueuedMessages++;

	  if ( ! success )
	  {
		  LOG.info("Message Queue History (earliest first) at end:" + getMessageHistoryLog());
	  } else if (_numEnqueuedMessages%MAX_QUEUED_MESSAGE_HISTORY_SIZE == 0) {
		  if (_enablePullerMessageQueueLogging)
		  {
			  LOG.info("Message Queue History (earliest first) :" + getMessageHistoryLog());
		  } else if (LOG.isDebugEnabled()) {
			  LOG.debug("Message Queue History (earliest first) :" + getMessageHistoryLog());
		  }
	  }

	  return success;
  }


  protected boolean executeAndChangeState(Object message)
  {
    boolean success = true;

    if (message instanceof LifecycleMessage)
    {
      LifecycleMessage lcMessage = (LifecycleMessage)message;

      switch (lcMessage.getTypeId())
      {
        case START: doStart(lcMessage); break;
        case PAUSE: doPause(lcMessage); break;
        case SUSPEND_ON_ERROR: doSuspendOnError(lcMessage); break;
        case RESUME: doResume(lcMessage); break;
        case SHUTDOWN:
        {
          LOG.error("Shutdown message is seen in the queue but not expected : Message :" + lcMessage);
          success = false;
          break;
        }
        default:
        {
          LOG.error("Unknown Lifecycle message in RelayPullThread: " + lcMessage.getTypeId());
          success = false;
          break;
        }
      }
    }
    else
    {
      LOG.error("Unknown message of type " + message.getClass().getName() + ": " + message.toString());
      success = false;
    }

    return success;
  }

  protected void doResume(LifecycleMessage lcMessage)
  {
    LOG.info(getName() + ": resuming");
    _componentStatus.resume();
    onResume();
  }

  protected void doSuspendOnError(LifecycleMessage lcMessage)
  {
    final Throwable lastError = lcMessage.getLastError();
    if (null != lastError)
    {
      LOG.info(getName() + ": suspending due to " + lastError, lastError);
    }
    else
    {
      LOG.info(getName() + ": suspending");
    }
	 if (LOG.isDebugEnabled())
		 LOG.debug(" because of message: " + lcMessage.getLastError());
    _componentStatus.suspendOnError(lcMessage.getLastError());
    clearQueue(suspendFilter);
  }

  protected void doPause(LifecycleMessage lcMessage)
  {
    LOG.info(getName() + ": pausing");
    _componentStatus.pause();
    clearQueue(pauseFilter);
  }

  protected void doStart(LifecycleMessage lcMessage)
  {
    LOG.info(getName() + ": starting");
    _componentStatus.start();
  }

  @Override
  public void run()
  {
    boolean isDebugEnabled = LOG.isDebugEnabled();

    Object nextState = null;

    boolean running = true;

    try
    {
      while (running && !checkForShutdownRequest())
      {
        nextState = pollNextState();
        if (null == nextState)
        {
          running = false;
        }
        else
        {
          if (isDebugEnabled) LOG.debug(getName() + ": new state: " + nextState.toString());
          running = doExecuteAndChangeState(nextState);
        }
      }
    }
    catch (Exception e)
    {
      LOG.error(getName() + ": stopping because of unhandled exception: ", e);
      running = false;
    }

    if (isDebugEnabled)
    {
        StringBuilder sb = new StringBuilder(10240);
        sb.append(getName());
        sb.append(": message queue at exit:");
        while (null != (nextState = _messageQueue.poll()))
        {
            sb.append(nextState.toString());
            sb.append(' ');
        }

        LOG.debug(sb.toString());
    }

    try
    {
      performShutdown();
    } finally {
      _controlLock.lock();
      try
      {
        _componentStatus.shutdown();
        _shutdownCondition.signalAll();
      }
      finally
      {
        _controlLock.unlock();
      }

      LOG.info("Message Queue History (earliest first) at shutdown:" + getMessageHistoryLog());
      if (isDebugEnabled) LOG.debug(getName() + ": exited message loop.");
    }
  }

  /*
   * Atomically filters the message queue and enqueues the passed message
   */
  public void enqueueMessageAfterFilter(Object message, MessageQueueFilter filter)
  {
	  try
	  {
		  _controlLock.lock();
		  clearQueue(filter);
		  enqueueMessage(message);
	  } finally {
		  _controlLock.unlock();
	  }
  }

  /**
   * Preprocess message before enqueueing
   *
   * @param message Message to be enqueued
   * @return processed message
   */
  protected Object preEnqueue(Object message)
  {
	  return message;
  }

  @Override
  public void enqueueMessage(Object message)
  {
    if (null == message)
    {
      LOG.warn("Attempt to queue empty state");
      return;
    }

    _controlLock.lock();

    try
    {
      message = preEnqueue(message);

      if (_componentStatus.getStatus() == DatabusComponentStatus.Status.SHUTDOWN)
      {
        LOG.warn(getName() + ": shutdown: ignoring " + message.toString());
      }
      else if (checkForShutdownRequest())
      {
        LOG.warn(getName() + ": shutdown requested: ignoring " + message.toString());
      }
      else if ((_componentStatus.getStatus() == DatabusComponentStatus.Status.PAUSED)  &&
                  (! shouldRetainMessageOnPause(message)))
      {
        LOG.warn(getName() + ": ignoring message while paused: " + message.toString());
      }
      else if ((_componentStatus.getStatus() == DatabusComponentStatus.Status.SUSPENDED_ON_ERROR)  &&
              (! shouldRetainMessageOnSuspend(message)))
      {
        LOG.warn(getName() + ": ignoring message while suspended_on_error: " + message.toString());
      }
      else
      {
        boolean offerSuccess = _messageQueue.offer(message);
        if (!offerSuccess) LOG.error(getName() + ": adding a new state failed: " + message.toString()
                                     + "; queue.size=" + _messageQueue.size());

        if (1 == _messageQueue.size()) _newStateCondition.signalAll();
        _hasMessages = true;
      }

//      LOG.info(getName() + ": " + _messageQueue.toString());
    }
    finally
    {
      _controlLock.unlock();
    }
  }

  public void shutdown()
  {
    LOG.info(getName() + ": shutdown requested.");
    _shutdownRequest = LifecycleMessage.createShutdownMessage();
  }

  public void awaitShutdown()
  {
    LOG.info(getName() + ": waiting for shutdown" );
    _controlLock.lock();
    try
    {
      LOG.info(getName() + ": status at shutdown: " + _componentStatus.getStatus());
      LOG.info(getName() + ": queue at shutdown: " + _messageQueue);
      while (_componentStatus.getStatus() != DatabusComponentStatus.Status.SHUTDOWN &&
             _componentStatus.getStatus() != DatabusComponentStatus.Status.INITIALIZING)
      {
        _shutdownCondition.awaitUninterruptibly();
      }
    }
    finally
    {
      _controlLock.unlock();
    }
  }

  public String getName()
  {
    return _name;
  }

  public boolean isShutdown()
  {
    _controlLock.lock();
    try
    {
      return _componentStatus.getStatus() == DatabusComponentStatus.Status.SHUTDOWN;
    }
    finally
    {
      _controlLock.unlock();
    }
  }

  private Object pollNextState()
  {
    Object nextState = null;

    _controlLock.lock();
    try
    {
      while (! checkForShutdownRequest() && _messageQueue.isEmpty())
      {
        try
        {
          _newStateCondition.await(MESSAGE_QUEUE_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException ie){}
      }
      if (! checkForShutdownRequest())
      {
        nextState = _messageQueue.poll();
        _hasMessages = _messageQueue.size() > 0;
      }
    }
    finally
    {
      _controlLock.unlock();
    }

    return nextState;
  }

  public DatabusComponentStatus getComponentStatus()
  {
    return _componentStatus;
  }

  public boolean checkForShutdownRequest()
  {
    return null != _shutdownRequest;
  }

  public String getQueueListString()
  {
    StringBuilder sb = new StringBuilder(100);
    getQueueListString(sb);

    return sb.toString();
  }

  public void getQueueListString(StringBuilder sb)
  {
    _controlLock.lock();
    try
    {
      sb.append(getName());
      sb.append(" queue: ");
      sb.append(_messageQueue.toString());
    }
    finally
    {
      _controlLock.unlock();
    }
  }

  protected void clearQueue(MessageQueueFilter filter)
  {
	  try
	  {
		  _controlLock.lock();

		  Iterator<Object> itr = _messageQueue.iterator();

		  while (itr.hasNext())
		  {
			  Object msg = itr.next();

			  boolean retain = filter.shouldRetain(msg);

			  if (! retain)
				  itr.remove();
		  }
	  } finally {
		  _controlLock.unlock();
	  }
  }

  /**
   * By default, all messages except lifecycle messages are cleared on pause
   */
  protected void clearMessageQueueOnPause()
  {
	  clearQueue(new DefaultPauseFilter());
  }

  /**
   * By default, all messages except life-cycle messages are cleared on Suspend_On_Error
   */
  protected void clearMessageQueueOnSuspend()
  {
	  clearQueue(new DefaultSuspendFilter());
  }

  /**
   * By default, all messages are cleared on shutdown
   */
  protected void clearMessageQueueOnShutdown()
  {
	  clearQueue(new DefaultShutdownFilter());
  }


  /**
   * Filter Interface for clearing Message Queue
   */
  public interface MessageQueueFilter
  {
	  public boolean shouldRetain(Object msg);
  }

  /**
   * Filter for clearing Message Queue on shutdown
   */
  private class DefaultPauseFilter implements MessageQueueFilter
  {

	@Override
	public boolean shouldRetain(Object msg)
	{
		return shouldRetainMessageOnPause(msg);
	}
  }

  /**
   * Filter for clearing Message Queue on Suspend_on_Error
   */
  private class DefaultSuspendFilter implements MessageQueueFilter
  {
	@Override
	public boolean shouldRetain(Object msg)
	{
		return shouldRetainMessageOnSuspend(msg);
	}
  }

  /**
   * Filter for clearing Message Queue on shutdown
   */
  private class DefaultShutdownFilter implements MessageQueueFilter
  {
	@Override
	public boolean shouldRetain(Object msg) {
		return shouldRetainMessageOnShutdown(msg);
	}
  }

  /**
   * By default, all messages except lifecycle messages are cleared on pause
   */
  protected boolean shouldRetainMessageOnPause(Object msg)
  {
	  if ( msg instanceof LifecycleMessage)
		return true;

	  return false;
  }


  /**
   * By default, all messages except lifecycle messages are cleared on suspend
   */
  protected boolean shouldRetainMessageOnSuspend(Object msg)
  {
	  if ( msg instanceof LifecycleMessage)
		return true;

	  return false;
  }


  /**
   * By default, all messages are cleared on shutdown
   */
  protected boolean shouldRetainMessageOnShutdown(Object msg)
  {
	  return false;
  }

  public Queue<Object> getMessageQueue()
  {
	return _messageQueue;
  }

  public String getMessageHistoryLog()
  {
    return _messageProcessedHistory.toString();
  }

  protected boolean hasMessages()
  {
    return _hasMessages;
  }

}
