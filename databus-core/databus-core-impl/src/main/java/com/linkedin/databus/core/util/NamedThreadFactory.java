package com.linkedin.databus.core.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedThreadFactory implements ThreadFactory
{
  private final AtomicInteger _tCnt = new AtomicInteger(0);
  private final String _namePrefix;
  private final boolean _spawnDaemons;

  public NamedThreadFactory(String namePrefix, boolean spawnDaemons)
  {
    _namePrefix = namePrefix + "-";
    _spawnDaemons = spawnDaemons;
  }

  public NamedThreadFactory(String namePrefix)
  {
    this(namePrefix, false);
  }

  @Override
  public Thread newThread(Runnable r)
  {
    Thread result = new Thread(r, _namePrefix + _tCnt.incrementAndGet());
    result.setDaemon(_spawnDaemons);
    return result;
  }
}
