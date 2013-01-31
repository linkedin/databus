package com.linkedin.databus.bootstrap.common;
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


import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

public abstract class BootstrapProducerThreadBase
	extends Thread	
{   
	protected final Logger _log;

	protected Lock _controlLock = new ReentrantLock(true);
	protected Condition _shutdownCondition = _controlLock.newCondition();
	protected Condition _pauseCondition = _controlLock.newCondition();
	protected Condition _resumeCondition = _controlLock.newCondition();
	protected Condition _resumeRequestCondition = _controlLock.newCondition();
	protected boolean _shutdownRequested = false;
	protected boolean _shutdown = false;
	protected boolean _pauseRequested = false;
	protected boolean _paused = false;
	protected boolean _resumeRequested = false;
	
	public BootstrapProducerThreadBase(String name)
	{
		super(name);
        setDaemon(true);
		_log = Logger.getLogger(getClass().getName() + "." + name);
	}

	public void pauseAsynchronously()
	{
		_log.info("Pause requested");
		_controlLock.lock();
		try
		{
			_pauseRequested = true;
		}
		finally
		{
			_controlLock.unlock();
		}
	}

	public void pause() 
			throws InterruptedException
	{
		pauseAsynchronously();
		awaitPause();
	}

	public void unpauseAsynchronously()
	{
		_log.info("Resume requested");
		_controlLock.lock();
		try
		{
			_resumeRequested = true;
			_resumeRequestCondition.signal();

		}
		finally
		{
			_controlLock.unlock();
		}
	}

	public void unpause() 
		throws InterruptedException
	{
		unpauseAsynchronously();
		awaitUnPause();
	}
	
	public void signalPause()
	{
		_controlLock.lock();
		try
		{
			_paused = true;
			_pauseCondition.signal();
		} finally {
			_controlLock.unlock();
		}
	}
	
	/**
	 * Notify that this thread has resumed.
	 */
	public void signalResumed()
	{
		_controlLock.lock();
		try
		{
			_paused = false;
			_resumeRequested = true;
			_resumeCondition.signal();
		} finally {
			_controlLock.unlock();
		}
	}
	
	public void shutdownAsynchronously()
	{
		_log.info("Shutdown requested");
		_controlLock.lock();
		try
		{
			_shutdownRequested = true;
		}
		finally
		{
			_controlLock.unlock();
		}
	}

	public void shutdown()
	{
		shutdownAsynchronously();
		awaitShutdownUniteruptibly();
	}

	public boolean isPauseRequested()
	{
		_controlLock.lock();
		try
		{
			return _pauseRequested;
		}
		finally
		{
			_controlLock.unlock();
		}
	}

	public boolean isPaused()
	{
		_controlLock.lock();
		try
		{
			return _paused;
		}
		finally
		{
			_controlLock.unlock();
		}
	}
	
	public boolean isUnPauseRequested()
	{
		_controlLock.lock();
		try
		{
			return _resumeRequested;
		}
		finally
		{
			_controlLock.unlock();
		}
	}

	public boolean isUnPaused()
	{
		_controlLock.lock();
		try
		{
			return ! _paused;
		}
		finally
		{
			_controlLock.unlock();
		}
	}
	
	public boolean isShutdownRequested()
	{
		_controlLock.lock();
		try
		{
			return _shutdownRequested;
		}
		finally
		{
			_controlLock.unlock();
		}
	}

	public boolean isShutdown()
	{
		_controlLock.lock();
		try
		{
			return _shutdownRequested;
		}
		finally
		{
			_controlLock.unlock();
		}
	}
		
	/** Awaits interruptibly for the thread to pause */
	public void awaitPause() throws InterruptedException
	{
		_log.info("Waiting to be paused");
		_controlLock.lock();
		try
		{
			while (! _paused) _pauseCondition.await();
			_pauseRequested = false;
		}
		finally
		{
			_controlLock.unlock();
		}
		_log.info("Paused: true");
	}

	/** Awaits interruptibly for the thread to unpause */
	public void awaitUnPause() throws InterruptedException
	{
		_log.info("Waiting for resumption");
		_controlLock.lock();
		try
		{
			while (_paused) _resumeCondition.await();
			_resumeRequested = false;
		}
		finally
		{
			_controlLock.unlock();
		}
		_log.info("Resumed: true");
	}
	
	/** Awaits interruptibly for the thread to be unpause */
	public void awaitUnPauseRequest() throws InterruptedException
	{
		_log.info("Waiting to be requested for resume");
		_controlLock.lock();
		try
		{
			while (!_resumeRequested) _resumeRequestCondition.await();
		}
		finally
		{
			_controlLock.unlock();
		}
		_log.info("Resume Requested: true");
	}
	
	/**
	 * Awaits interruptibly for the thread to pause or until time out.
	 * @return true if the pause happened and false if there was a time out
	 * */
	public boolean awaitPause(long timeout, TimeUnit timeUnit) throws InterruptedException
	{
		_log.info("Waiting for pause with timeout");
		boolean success;
		_controlLock.lock();
		try
		{
			while (! _paused) 
			{
				boolean successfulWait = _pauseCondition.await(timeout, timeUnit);
				if (_log.isDebugEnabled())
					_log.debug("Await Condition returned :" + successfulWait);
			}
			success = _paused;
		}
		finally
		{
			_controlLock.unlock();
		}

		_log.info("Paused: " + success);
		return success;
	}
	
	/**
	 * Awaits interruptibly for the thread to resume or until time out.
	 * @return true if the resume happened and false if there was a time out
	 * */
	public boolean awaitUnPause(long timeout, TimeUnit timeUnit) throws InterruptedException
	{
		_log.info("Waiting for resume with timeout");
		boolean success;
		_controlLock.lock();
		try
		{
			while (_paused) {
				boolean successfulWait = _resumeCondition.await(timeout, timeUnit);
				if (_log.isDebugEnabled())
					_log.debug("Await Condition returned :" + successfulWait);
			}
			success = !_paused;
		}
		finally
		{
			_controlLock.unlock();
		}

		_log.info("UnPaused: " + success);
		return success;
	}
		
	
	/** Awaits interruptibly for the thread to shutdown */
	public void awaitShutdown() throws InterruptedException
	{
		_log.info("Waiting for shutdown");
		_controlLock.lock();
		try
		{
			while (! _shutdown) _shutdownCondition.await();
		}
		finally
		{
			_controlLock.unlock();
		}
		_log.info("Shutdown: true");
	}


	/** Awaits interruptibly for the thread to shutdown */
	public void awaitShutdownUniteruptibly()
	{
		_log.info("Waiting for shutdown uninteruptibly");
		boolean keepOnWaiting = true;
		while (keepOnWaiting)
		{
			try
			{
				awaitShutdown();
				keepOnWaiting = false;
			}
			catch (InterruptedException ie) {}
		}
		_log.info("Shutdown: true");
	}

	/**
	 * Awaits interruptibly for the thread to shutdown or until time out.
	 * @return true if the shutdown happened and false if there was a time out
	 * */
	public boolean awaitShutdownUninteruptibly(long timeout, TimeUnit timeUnit)
	{
		_log.info("Waiting for shutdown uninteruptibly with timeout");
		boolean success;
		_controlLock.lock();
		try
		{
			long startTime = System.nanoTime();
			long timeoutNanos = timeUnit.toNanos(timeout);
			while (! _shutdown)
			{
				try
				{
					long elapsed = (System.nanoTime() - startTime);
					if (elapsed >= timeoutNanos) break;
					success = _shutdownCondition.await(timeoutNanos - elapsed, timeUnit);
				}
				catch (InterruptedException ie){}
			}
			success = _shutdown;
		}
		finally
		{
			_controlLock.unlock();
		}

		_log.info("Shutdown: " + success);
		return success;
	}
	
	/**
	 * Caller is notifying that it is shutting down
	 */
	public void doShutdownNotify()
	{
	    _controlLock.lock();
	    try
	    {
	      _log.info("Signalling shutdown !!");	
	      _shutdown = true;
	      _shutdownCondition.signalAll();
	    }
	    finally
	    {
	      _controlLock.unlock();
	    }
	}
}
