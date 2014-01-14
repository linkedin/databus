package com.linkedin.databus.core.util;
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


import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.Schema;
import org.apache.log4j.Logger;

import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventBufferAppendable;
import com.linkedin.databus.core.DbusEventBufferMult;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.KeyTypeNotImplementedException;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus2.relay.config.LogicalSourceConfig;


public class DatabusEventRandomProducer extends Thread implements DatabusEventProducer
{
    public static final String MODULE = DatabusEventRandomProducer.class.getName();
    public static final Logger LOG = Logger.getLogger(MODULE);

    protected static final int MILLISECONDS_IN_NANOS = 1000000;
    protected static final double NANOSECONDS_IN_A_SECOND = 1000000000.0;

    protected final CountDownLatch _generationStopped;

    protected final DbusEventBufferMult _dbusEventBuffer;
    protected List<IdNamePair> _sources;
    protected List<Schema> _schemas = null;
    protected Map<Long,byte[]> _schemaIds= null;
    protected double _tickInNanos;
    protected long _duration;
    protected long _startScn;
    protected final AtomicBoolean _stopGeneration = new AtomicBoolean(true);
    protected DbusEventsStatisticsCollector _statsCollector = null;
    protected final AtomicBoolean _suspendGeneration = new AtomicBoolean(true);
    protected int _minLength;
    protected int _maxLength;
    protected int _maxEventsPerWindow;
    protected int _minEventsPerWindow;
    protected String _generationPattern;
    protected long _totalGenerationTime;
    protected long _numEventsGenerated;
    protected RateMonitor _rateMonitor;
    protected long _generateBaseTime = System.currentTimeMillis()*1000000; // see getCurrentNanoTime()
    protected long _generateBaseNanoTime = System.nanoTime();

    // stop after generated certain number of events
    protected final AtomicLong _numEventsToGenerate = new AtomicLong(Long.MAX_VALUE);

    // key range
    protected final AtomicLong _keyMin = new AtomicLong(0);
    protected final AtomicLong _keyMax = new AtomicLong(Long.MAX_VALUE);
    protected final StaticConfig _config;

    // generate events until the total event size reach a percentage of the Event buffers Maxsize
    // e.g., 110 will caused the buffer to wrap around
    protected final AtomicInteger _percentOfBufferToGenerate = new AtomicInteger(Integer.MAX_VALUE);

    Map<Integer, Integer> genEventsPerSource = new HashMap<Integer, Integer> (100);
    protected final Random _realRng = new Random(); //different random numbers on different invocations


    public DatabusEventRandomProducer(DbusEventBufferMult dbuf, long startScn, int eventsPerSecond,
        long durationInMilliseconds,List<IdNamePair> sources,StaticConfig config)
    {
      this(dbuf,startScn,eventsPerSecond, durationInMilliseconds, sources,null,config);
    }

    public DatabusEventRandomProducer(DbusEventBufferMult dbuf, long startScn, int eventsPerSecond,
                                      long durationInMilliseconds, List<IdNamePair> sources,Map<Long,byte[]> schemaIds) {
      this(dbuf,startScn,eventsPerSecond, durationInMilliseconds, sources,schemaIds,null);
    }


    public DatabusEventRandomProducer(DbusEventBufferMult dbuf, StaticConfig config)
    {
      super("DatabusEventRandomProducer");

      if ( null == config)
      {
        try
        {
          config = (new Config()).build();
        } catch (InvalidConfigException ice) {
          throw new RuntimeException(ice);
        }
      }

      _config = config;
      _statsCollector = new DbusEventsStatisticsCollector(1,"dummy",false, false, null);
      _generationStopped = new CountDownLatch(1);
      this._dbusEventBuffer = dbuf;
      this._duration = config.getDuration();
      this._sources = config.getIdNameList();
      this._startScn = config.getStartScn();
      this.setDaemon(true);
      this._minLength = config.getMinLength();
      this._maxLength = config.geMaxLength();
      this._minEventsPerWindow = config.getMinEventsPerWindow();
      this._maxEventsPerWindow = config.getMaxEventsPerWindow();
      LOG.info("Sources: " + _sources + ",duration:" + _duration + ",tickInMS:" + _tickInNanos + ",startScn" + _startScn);
      LOG.info("minEventsPerWindow:" + _minEventsPerWindow + ",maxEventsPerWindow:" + _maxEventsPerWindow);
      this._tickInNanos = NANOSECONDS_IN_A_SECOND/config.getEventRate();
      LOG.info("Will wait for " + _tickInNanos + " nanoseconds per event produced");

      for(IdNamePair p : _sources) genEventsPerSource.put(p.getId().intValue(), 0);
    }

    public DatabusEventRandomProducer(DbusEventBufferMult dbuf, long startScn, int eventsPerSecond,
                                      long durationInMilliseconds, List<IdNamePair> sources,Map<Long,byte[]> schemaIds,
                                      StaticConfig config) {
      super("DatabusEventRandomProducer");

      if ( null == config)
      {
        try
        {
          config = (new Config()).build();
        } catch (InvalidConfigException ice) {
          throw new RuntimeException(ice);
        }
      }

      _config = config;
      _generationStopped = new CountDownLatch(1);
      this._dbusEventBuffer = dbuf;
      this._schemaIds = schemaIds;
      this._tickInNanos = NANOSECONDS_IN_A_SECOND/eventsPerSecond;
      this._duration = durationInMilliseconds;
      this._sources = sources;
      this._startScn = startScn;
      this.setDaemon(true);
      this._minLength = config.getMinLength();
      this._maxLength = config.geMaxLength();
      this._maxEventsPerWindow = config.getMaxEventsPerWindow();
      this._minEventsPerWindow = config.getMinEventsPerWindow();
      this._generationPattern = config.getGenerationPattern();

      LOG.info("Sources: " + sources + ",duration:" + _duration + ",tickInMS:" + _tickInNanos + ",startScn" + startScn);
      LOG.info("Will wait for " + _tickInNanos + " nanoseconds per event produced");
    }

    public long produceNRandomEvents(long startScn, long currentTime, int numberOfEvents, List<IdNamePair> sources, long keyMin, long keyMax, int minLength, int maxLength, List<Schema> schemas) throws KeyTypeNotImplementedException {
      long endScn = startScn + 1 + (RngUtils.randomPositiveLong() % 100L); // random number between startScn and startScn + 100;
      long scnDiff = endScn - startScn;
      long maxScn = startScn;

      int numSources = sources.size();
      int eventsPerSource = numberOfEvents/numSources + 1;

      if (eventsPerSource <= 0 )
        eventsPerSource = 1;

      _dbusEventBuffer.startAllEvents();
      assert endScn > startScn;
      if (LOG.isDebugEnabled()) {
        LOG.debug("endScn = " + endScn + " startScn = " + startScn);
      }

      byte[] defaultSchemaId = "abcdefghijklmnop".getBytes(Charset.defaultCharset());

    boolean enableTracing = (RngUtils.randomPositiveLong()%100L <= 1);  // trace 1% samples
    for (int i = 0; i < numberOfEvents; ++i) {
      DbusEventKey key = new DbusEventKey(RngUtils.randomPositiveLong(keyMin, keyMax)); // random key between 0 and 100M
      long scn = startScn + (i / scnDiff);
      //short srcId = sources.get((Integer) (RngUtils.randomPositiveShort() % sources.size())).getId().shortValue();
      short srcId = sources.get(i/eventsPerSource).getId().shortValue();
      byte[] schemaId=(_schemaIds != null) ? _schemaIds.get((long) srcId) : defaultSchemaId;

      genEventsPerSource.put((int)srcId, genEventsPerSource.get((int)srcId) + 1);

      String value = null;
      int rnd = RngUtils.randomPositiveShort();
      int length = minLength + rnd % (maxLength - minLength);

      if (LOG.isDebugEnabled()) {
        LOG.debug("Creating random record with SCN =" + scn + " and length =" + length);
      }

      value = RngUtils.randomString(length); // 1K of row data

      //short lPartitionId = (short) (key.getLongKey() % Short.MAX_VALUE);
      short lPartitionId = LogicalSourceConfig.DEFAULT_LOGICAL_SOURCE_PARTITION;
      short pPartitionId =  _dbusEventBuffer.getPhysicalPartition(srcId).getId().shortValue();
      DbusEventBufferAppendable buf = _dbusEventBuffer.getDbusEventBufferAppendable(srcId);
      boolean appended = buf.appendEvent(key, pPartitionId, lPartitionId, currentTime, srcId, schemaId,
                                  value.getBytes(Charset.defaultCharset()), enableTracing, _statsCollector);
      assert appended == true;

      maxScn = Math.max(scn, maxScn);
      if (LOG.isDebugEnabled() && (i % 7 == 0))
        LOG.debug("Produce: srcId=" + srcId + ";pSid=" + _dbusEventBuffer.getPhysicalPartition(srcId) +
                  ";scn=" + scn + "; buf(hc)=" + buf.hashCode());
    }
    // for debug purposes - collect stats how many events for each srcId were generated
    StringBuilder s = new StringBuilder(100);
    for(Entry<Integer, Integer> e : genEventsPerSource.entrySet()) {
      s.append(e.getKey() + ":" + e.getValue() + ",");
    }
    LOG.info("generated sources/events = " + s.toString());

    _dbusEventBuffer.endAllEvents(maxScn, getCurrentNanoTime(), _statsCollector);
    if (LOG.isDebugEnabled()) LOG.debug("Produce:window:" + maxScn);
    assert maxScn >= startScn;
    return maxScn;
  }


  public String getRateOfProduction()
  {
    if (_totalGenerationTime <= 0) throw new IllegalStateException("Producer hasnt been started yet !!");

    double rate = ((double)_numEventsGenerated * 1000 * 1000000)/_totalGenerationTime;

    String msg = "\"Rate\":" + rate + ",\"NumEvents\":" + _numEventsGenerated + ",\"TotalTime\":" + _totalGenerationTime ;
    return msg;
  }

  public double getProductionRate()
  {
    if (_totalGenerationTime <= 0) throw new IllegalStateException("Producer hasnt been started yet !!");

    return _rateMonitor.getRate();
    //return ((double)_numEventsGenerated * 1000 * 1000000)/_totalGenerationTime;
  }

  @Override
  public boolean startGeneration(long startScn, int eventsPerSecond, long durationInMilliseconds,
                                 long numEventToGenerate, int percentOfBufferToGenerate,
                                 long keyMin,
                                 long keyMax,
                                 List<IdNamePair> sources,
                                 DbusEventsStatisticsCollector statsCollector)
  {
    if (! _stopGeneration.getAndSet(false))
    {
      return false;
    }

    for(IdNamePair p : sources) genEventsPerSource.put(p.getId().intValue(), 0);

    _statsCollector = statsCollector;
    _tickInNanos = NANOSECONDS_IN_A_SECOND/eventsPerSecond;
    _duration = durationInMilliseconds;
    _sources = sources;
    _startScn = startScn;
    _keyMin.set(keyMin);
    _keyMax.set(keyMax);
    _numEventsToGenerate.set(numEventToGenerate);
    _percentOfBufferToGenerate.set(percentOfBufferToGenerate);
    LOG.info("Will wait for " + _tickInNanos + " nanoseconds per event produced");

    start();

    return true;
  }

  @Override
  public void stopGeneration()
  {
    synchronized(this)
    {
      _stopGeneration.set(true);
      this.notifyAll();
    }
  }

  public void stopGenerationAndWait()
  {
    stopGeneration();
    while ( true)
    {
      try
      {
        _generationStopped.await();
        break;
      } catch ( InterruptedException ie) {
      }
    }
  }
  @Override
  public boolean checkRunning()
  {
    return ((!_stopGeneration.get()) && (!_suspendGeneration.get()));
  }

  @Override
  public void suspendGeneration()
  {
    synchronized(this)
    {
      _suspendGeneration.set(true);
    }
  }

  @Override
  public void resumeGeneration(long numEventToGenerate, int percentOfBufferToGenerate, long  keyMin,  long keyMax)
  {
    synchronized(this)
    {
      _suspendGeneration.set(false);
      _numEventsToGenerate.set(numEventToGenerate);
      _percentOfBufferToGenerate.set(percentOfBufferToGenerate);
      _keyMin.set(keyMin);
      _keyMax.set(keyMax);
      this.notifyAll();
    }
  }

  public void fillBuffer(int eventBatchSize) {
	  long maxScn = 0;
	  long currTime = getCurrentNanoTime();
      try {
    	  maxScn = produceNRandomEvents(_startScn, currTime, eventBatchSize, _sources, _keyMin.get(), _keyMax.get(), _minLength, _maxLength, _schemas);
    	  assert(maxScn >= _startScn);
    	  _startScn = maxScn + 1;
	} catch (KeyTypeNotImplementedException e) {
		e.printStackTrace();
	}
  }

  @Override
  public void run() {
    _stopGeneration.set(false);
    _suspendGeneration.set(false);
    long startTime = System.nanoTime();
    long startTime0 = startTime;
    long realStartTime = System.currentTimeMillis() * 1000000L;
    _numEventsGenerated = 0;
    int eventBatchSize = 10;
    long currTime = startTime;
    long sleepTimes = 0;
    long sleepTime = 0;
    long firstScn = _startScn;
    long maxScn;
    long numEventsGeneratedAfterResume = 0;
    long eventBufferSize = 0;  // get the free space at the beginning as the buffer size
    long sizeDataEventsBeforeResume = 0;
    long currSizeDataEvents = _statsCollector.getTotalStats().getSizeDataEvents() * _statsCollector.getTotalStats().getNumDataEvents();

    maxScn = firstScn;

    /* fix firstSCN setting and figure minimum free buffers size available */
    long minSpace = Long.MAX_VALUE;
    for(DbusEventBuffer buf : _dbusEventBuffer.bufIterable()) {
      if (buf.getMinScn() < 0)
        buf.start(firstScn-1);

      long tmpMinSpace = buf.getBufferFreeSpace();
      if(tmpMinSpace < minSpace)
        minSpace = tmpMinSpace;
    }
    if(_dbusEventBuffer.bufsNum() > 0)
      eventBufferSize = minSpace;

    try
    {
      _rateMonitor = new RateMonitor("RandomProducer");
      _rateMonitor.start();
      while (!_stopGeneration.get() )
      {
        LOG.info("Resume. currTime (ms) = " + currTime/MILLISECONDS_IN_NANOS + ", startTime (ms) = " + startTime/MILLISECONDS_IN_NANOS +
                 ", elapseTime = " + (currTime - startTime)/MILLISECONDS_IN_NANOS +
                 ", _duration=" + _duration +
                 ", numEventsToGenerate = " + _numEventsToGenerate.get() +
                 ", StartScn = " + firstScn + ", _minEventsPerWindow=" + _minEventsPerWindow +
                 ", _maxEventsPerWindow =" + _maxEventsPerWindow +
                 ", _keyMin =" + _keyMin +
                 ", _keyMax =" + _keyMax +
                 ", _minLength=" + _minLength +
                 ", _maxLength=" + _maxLength +
                 ", numEvents = " + numEventsGeneratedAfterResume + ", eventBufferSize = " + eventBufferSize +
                 ", sources = " + _sources.size());

        while (!_stopGeneration.get() && !_suspendGeneration.get() && (currTime - startTime < _duration*MILLISECONDS_IN_NANOS)
            && (numEventsGeneratedAfterResume < _numEventsToGenerate.get())
            && (currSizeDataEvents - sizeDataEventsBeforeResume < _percentOfBufferToGenerate.get()/100.0*eventBufferSize))
        {

          eventBatchSize = _minEventsPerWindow + RngUtils.randomPositiveShort(_realRng)%(_maxEventsPerWindow - _minEventsPerWindow);
          long before = System.nanoTime();
          _rateMonitor.resume();
          maxScn = produceNRandomEvents(firstScn, realStartTime + (currTime - startTime0), eventBatchSize, _sources, _keyMin.get(), _keyMax.get(), _minLength, _maxLength, _schemas);
          assert(maxScn >= firstScn);
          _rateMonitor.ticks(eventBatchSize + 1);
          _rateMonitor.suspend();
          currTime = System.nanoTime();
          currSizeDataEvents = _statsCollector.getTotalStats().getSizeDataEvents() * _statsCollector.getTotalStats().getNumDataEvents();
          firstScn = maxScn +1;
          _numEventsGenerated += eventBatchSize;
          numEventsGeneratedAfterResume += eventBatchSize;
          _totalGenerationTime += (currTime - before);

          long nextTime = (long) (startTime + numEventsGeneratedAfterResume * _tickInNanos);

          if (nextTime > currTime)
          {
            try {
              ++sleepTimes;
              sleepTime += (nextTime - currTime);
              long milliseconds = (nextTime - currTime) / MILLISECONDS_IN_NANOS;
              int nanoseconds = (int) ((nextTime - currTime) - (milliseconds* MILLISECONDS_IN_NANOS));
              Thread.sleep(milliseconds, nanoseconds);
            } catch (InterruptedException e) {
              e.printStackTrace();
              return;
            }
            currTime = System.nanoTime();
          }
        }
        LOG.info("Suspended. currTime (ms) = " + currTime/MILLISECONDS_IN_NANOS + ", startTime (ms) = " + startTime/MILLISECONDS_IN_NANOS +
                 ", elapseTime = " + (currTime - startTime)/MILLISECONDS_IN_NANOS +
                 ", numEvents = " + numEventsGeneratedAfterResume + ", eventBufferSize = " + eventBufferSize +
                 ", currEventSize = " + currSizeDataEvents + ", startEventSize = " + sizeDataEventsBeforeResume +
                 ", EventSizeDelta = " + (currSizeDataEvents - sizeDataEventsBeforeResume));
        LOG.info(getRateOfProduction());

        //Suspend till resumed
        synchronized(this)
        {
          if ( !_stopGeneration.get() )
          {
            boolean doWait = false;
            // Checking again for suspend inside synchronized block to make sure we didn't miss resume outside the synchronized block.
            if( ! _suspendGeneration.get())
            {
              if ( currTime - startTime >= _duration * MILLISECONDS_IN_NANOS
                  || (numEventsGeneratedAfterResume >= _numEventsToGenerate.get())
                  || (currSizeDataEvents - sizeDataEventsBeforeResume >= _percentOfBufferToGenerate.get()/100.0*eventBufferSize))
              {
                // Completed this round. Suspending myself till someone calls resume or stop
                doWait = true;
                _suspendGeneration.set(true);
              } else {
                // case when someone called suspend and resumed immediately before I reached here
                doWait = false;
              }
            } else {
              // User requested suspension
              doWait = true;
            }

            while (doWait && (!_stopGeneration.get()) && (_suspendGeneration.get()))
            {
              try
              {
                this.wait();
              } catch (InterruptedException ie) {
                LOG.info("Got Interrupted during suspension: " + ie.getMessage());
              }
            }
            // reset startTime to wall clock
            startTime = System.nanoTime();
            currTime = startTime;
            // reset the event size and number of events
            numEventsGeneratedAfterResume = 0;
            sizeDataEventsBeforeResume = currSizeDataEvents;
          }
        }
      }
      _stopGeneration.set(true);
    }
    catch (RuntimeException e)
    {
      LOG.error("event generation error:" + e.getMessage(), e);
    }
    catch (KeyTypeNotImplementedException e)
    {
      LOG.error("event generation error:" + e.getMessage(), e);
    }
    finally
    {
      LOG.info("Produced " + _numEventsGenerated + " events in " + (currTime - startTime) + " nanoseconds.");
      LOG.info("Slept a total of " + sleepTimes + " times for a duration of " + sleepTime + " nanoseconds.");
      LOG.info("Busy time = " + (currTime - startTime - sleepTime) + " nanoseconds.");
      _rateMonitor.stop();
      _generationStopped.countDown();
    }
  }

  public void setStatsCollector(DbusEventsStatisticsCollector statsCollector){
    _statsCollector = statsCollector;
  }

  public long getCurrentNanoTime() {
    return _generateBaseTime + (System.nanoTime()-_generateBaseNanoTime);
  }


  public static class StaticConfig
  {
    public int getMinLength() {
      return _minLength;
    }

    public int geMaxLength() {
      return _maxLength;
    }

    public int getMaxEventsPerWindow() {
      return _maxEventsPerWindow;
    }

    public int getMinEventsPerWindow() {
      return _minEventsPerWindow;
    }

    public String getGenerationPattern()
    {
      return _generationPattern;
    }

    public long getStartScn() {
      return _startScn;
    }

    public int getEventRate() {
      return _eventRate;
    }

    public long getDuration() {
      return _duration;
    }

    public StaticConfig(long startScn, int eventRate, long duration,
                        List<IdNamePair> idNameList,
                        int minLength, int maxLength, int minEventsPerWindow,
                        int maxEventsPerWindow, String generationPattern,
                        long eventRngSeed)
    {
      super();
      this._startScn = startScn;
      this._eventRate = eventRate;
      this._duration = duration;
      this._idNameList = idNameList;
      this._minLength = minLength;
      this._maxLength = maxLength;
      this._minEventsPerWindow = minEventsPerWindow;
      this._maxEventsPerWindow = maxEventsPerWindow;
      this._generationPattern = generationPattern;
      _eventRngSeed = eventRngSeed;
      LOG.debug("Constructor: IDNameList:" + idNameList);
    }

    public List<IdNamePair> getIdNameList() {
      return _idNameList;
    }

    public long getEventRngSeed()
    {
      return _eventRngSeed;
    }

    protected long _startScn;
    protected int  _eventRate;
    protected long _duration;
    protected List<IdNamePair> _idNameList;
    protected int _minLength;
    protected int _maxLength;
    protected int _minEventsPerWindow;
    protected int _maxEventsPerWindow;
    protected String _generationPattern;
    private final long _eventRngSeed;
  }


  public static class Config implements ConfigBuilder<StaticConfig>
  {
    public Config()
    {
      super();
      minLength = 1000;
      maxLength = 1001;
      minEventsPerWindow = 10;
      maxEventsPerWindow  = 11;
      generationPattern = "RandomOnly";
    }

    public int getMinLength() {
      return minLength;
    }

    public void setMinLength(int minLength) {
      this.minLength = minLength;
    }

    public int getMaxLength() {
      return maxLength;
    }

    public void setMaxLength(int maxLength) {
      LOG.info("maxLength:" + maxLength);
      this.maxLength = maxLength;
    }

    public int getMinEventsPerWindow() {
      return minEventsPerWindow;
    }

    public void setMinEventsPerWindow(int minEventsPerWindow) {
      LOG.info("minEventsPerWindow:" + minEventsPerWindow);
      this.minEventsPerWindow = minEventsPerWindow;
    }

    public int getMaxEventsPerWindow() {
      return maxEventsPerWindow;
    }

    public void setMaxEventsPerWindow(int maxEventsPerWindow) {
      LOG.info("maxEventsPerWindow:" + maxEventsPerWindow);
      this.maxEventsPerWindow = maxEventsPerWindow;
    }

    public String getGenerationPattern() {
      return generationPattern;
    }

    public void setGenerationPattern(String generationPattern) {
      LOG.info("GenerationPattern:" + generationPattern);
      this.generationPattern = generationPattern;
    }

    @Override
    public StaticConfig build() throws InvalidConfigException
    {

      List<IdNamePair> idNameList = new ArrayList<IdNamePair>();

      if ( sourceIdMapStr != null )
      {
        String[] tokens = sourceIdMapStr.split("[,]");

        for (int i = 0; i < tokens.length; i++)
        {
          String[] items = tokens[i].split("[:]");

          if (items.length != 2)
            throw new InvalidConfigException("SourceIdMap Config (" + sourceIdMapStr + ") invalid !!");

          IdNamePair pair = new IdNamePair();
          pair.setId(Long.parseLong(items[0].trim()));
          pair.setName(items[1].trim());
          LOG.debug("SrcId Entry:" + items[0] + "," + items[1]);
          idNameList.add(pair);
        }
      }

      //TODO add verification for the config
      return new StaticConfig(startScn, eventRate, duration,
                              idNameList, minLength, maxLength,
                              minEventsPerWindow, maxEventsPerWindow, generationPattern,
                              _eventRngSeed);
    }

    public long getStartScn() {
      return startScn;
    }

    public void setStartScn(long startScn) {
      LOG.info("startScn:" + startScn);
      this.startScn = startScn;
    }

    public long getDuration() {
      return duration;
    }

    public void setDuration(long duration) {
      LOG.info("Duration:" + duration);
      this.duration = duration;
    }

    public int getEventRate() {
      return eventRate;
    }

    public void setEventRate(int eventRate) {
      LOG.info("EventRate:"+ eventRate);
      this.eventRate = eventRate;
    }

    public String getSourceIdMap() {
      return sourceIdMapStr;
    }

    public void setSourceIdMap(String sourceIdMapStr) {
      LOG.info("sourceIdMapStr:" + sourceIdMapStr);
      this.sourceIdMapStr = sourceIdMapStr;
    }

    public long getEventRngSeed()
    {
      return _eventRngSeed;
    }

    public void setEventRngSeed(long eventRngSeed)
    {
      _eventRngSeed = eventRngSeed;
    }

    protected long startScn;
    protected int eventRate;
    protected long duration;
    protected String sourceIdMapStr;
    protected int minLength;
    protected int maxLength;
    protected int minEventsPerWindow;
    protected int maxEventsPerWindow;
    protected String generationPattern;
    private long _eventRngSeed = -1;
  }

}
