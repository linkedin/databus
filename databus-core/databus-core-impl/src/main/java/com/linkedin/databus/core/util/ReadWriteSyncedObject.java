package com.linkedin.databus.core.util;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This object allows to add optional read/write synchronization to another object
 * @author cbotev
 *
 */
public class ReadWriteSyncedObject
{
  private final ReadWriteLock _readWriteLock;

  protected ReadWriteSyncedObject(boolean threadSafe)
  {
    _readWriteLock = threadSafe ? new ReentrantReadWriteLock(true) : null;
  }

  public boolean isThreadSafe()
  {
    return null != _readWriteLock;
  }

  /**
   * Obtains a read lock if required. If the lock is acquired in a relation with another lock,
   * it will do so in a way that avoids deadlocks.
   * @param  otherLock          the other lock; can be null; otherwise, must be locked
   * @return the read lock or null if not required
   */
  protected Lock acquireReadLock(Lock otherLock)
  {
    Lock result = null;
    if (null != _readWriteLock)
    {
      result = _readWriteLock.readLock();
      lockSafe(result, otherLock);
    }

    return result;
  }

  /**
   * Obtains a read lock if required.
   * @return the read lock or null if not required
   */
  protected Lock acquireReadLock()
  {
    if (null == _readWriteLock) return null;
    Lock readLock = _readWriteLock.readLock();
    readLock.lock();
    return readLock;
  }

  /**
   * Obtains a write lock if required. If the lock is acquired in a relation with another lock,
   * it will do so in a way that avoids deadlocks.
   * @param  otherLock          the other lock; can be null; otherwise, must be locked
   * @return the write lock or null if not required
   */
  protected Lock acquireWriteLock(Lock otherLock)
  {
    Lock result = null;
    if (null != _readWriteLock)
    {
      result = _readWriteLock.writeLock();
      lockSafe(result, otherLock);
    }

    return result;
  }

  /**
   * Obtains a write lock if required.
   * @return the write lock or null if not required
   */
  protected Lock acquireWriteLock()
  {
    return acquireWriteLock(null);
  }

  private void lockSafe(Lock thisLock, Lock otherLock)
  {
    if (null == otherLock)
    {
      thisLock.lock();
    }
    else
    {
      boolean done = false;
      while (!done)
      {
        done = thisLock.tryLock();
        if (!done)
        {
          otherLock.unlock();
          otherLock.lock();
        }
      }
    }
  }

  /**
   * Releases a previously acquired lock
   * @param  lock           the lock; if null, does nothing
   * */
  protected void releaseLock(Lock lock)
  {
    if (null != lock) lock.unlock();
  }

}
