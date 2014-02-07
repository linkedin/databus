/*
 * Copyright 2010-2012 Coda Hale and Yammer, Inc.
 * Copyright 2014 LinkedIn, Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Retrieved from https://github.com/codahale/metrics/ on 2013-12-20.  Modified
 * to support aggregation, which requires read/write access both to the full
 * '_values' object (including rescaling and bulk updating) and to '_startTime'.
 * Also modified to support use of the StatUtils package in Apache Commons' Math
 * project for percentile/quantile calculations; in particular, use doubles rather
 * than longs for data values, and omit sorting the data values since StatUtils
 * necessarily does so itself.
 *
 * Ultimately the plan is to contribute this back to the public metrics-core repo on
 * github, but at present the changes don't fit terribly well with the existing class
 * design.  For example, the Reservoir interface currently assumes long data values;
 * several of the internal variables specific to the ExponentiallyDecayingReservoir
 * implementation are exposed; and the mergeability feature isn't sufficiently abstracted
 * to be immediately extensible to the other kinds of metrics-core reservoirs.
 */

package com.codahale.metrics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.lang.Math.exp;
import static java.lang.Math.min;

import com.codahale.metrics.Clock;
import com.codahale.metrics.ThreadLocalRandom;

/**
 * An exponentially-decaying random reservoir of {@code double}s.  Uses Cormode et al.'s
 * forward-decaying priority reservoir sampling method to produce a statistically
 * representative sampling reservoir, exponentially biased toward newer entries.
 *
 * @see <a href="http://dimacs.rutgers.edu/~graham/pubs/papers/fwddecay.pdf">
 *   Cormode et al., "Forward Decay: A Practical Time Decay Model for Streaming Systems."
 *   ICDE '09: Proceedings of the 2009 IEEE International Conference on Data Engineering (2009)</a>
 */
public class MergeableExponentiallyDecayingReservoir // implements Reservoir  [no longer implements update(long)]
{
    public static final int DEFAULT_SIZE = 1028;
    public static final double DEFAULT_ALPHA = 0.015;  // units = 1/sec

    private static final long RESCALE_THRESHOLD = TimeUnit.HOURS.toNanos(1);
    private static final long CURRENT_NEXT_SCALE_TIME = Long.MIN_VALUE;

    private final ConcurrentSkipListMap<Double, Double> _values;
    private final ReentrantReadWriteLock _lock;
    private final double _alpha;
    private final int _size;
    private final AtomicLong _count;  // accurate only until reservoir size is reached; thereafter mostly inaccurate
    private volatile long _startTime;
    private final AtomicLong _nextScaleTime;
    private final Clock _clock;

    /**
     * Creates a new {@link MergeableExponentiallyDecayingReservoir} of 1028 elements, which
     * offers a 99.9% confidence level with a 5% margin of error assuming a normal distribution,
     * and an alpha factor of 0.015, which heavily biases the reservoir to the past 5 minutes of
     * measurements.
     */
    public MergeableExponentiallyDecayingReservoir() {
        this(DEFAULT_SIZE, DEFAULT_ALPHA);
    }

    /**
     * Creates a new {@link MergeableExponentiallyDecayingReservoir}.
     *
     * @param size  the number of samples to keep in the sampling reservoir
     * @param alpha the exponential decay factor; the higher this is, the more biased the reservoir
     *              will be towards newer values
     */
    public MergeableExponentiallyDecayingReservoir(int size, double alpha) {
        this(size, alpha, Clock.defaultClock());
    }

    /**
     * Creates a new {@link MergeableExponentiallyDecayingReservoir}.
     *
     * @param size  the number of samples to keep in the sampling reservoir
     * @param alpha the exponential decay factor; the higher this is, the more biased the reservoir
     *              will be towards newer values
     */
    public MergeableExponentiallyDecayingReservoir(int size, double alpha, Clock clock) {
        _values = new ConcurrentSkipListMap<Double, Double>();
        _lock = new ReentrantReadWriteLock();
        _alpha = alpha;
        _size = size;
        _clock = clock;
        _count = new AtomicLong(0);
        _startTime = currentTimeInSeconds();
        _nextScaleTime = new AtomicLong(clock.getTick() + RESCALE_THRESHOLD);
    }

//  @Override
    public int size() {
        return (int) min(_size, _count.get());
    }

//  @Override
    public void update(double value) {
        update(value, currentTimeInSeconds());
    }

    /**
     * Adds an old value with a fixed timestamp to the reservoir.  This method appears to use the
     * "priority sampling" approach from page 7 of the Cormody et al. paper.  (The only weird part
     * is that the paper talks about sampling <i>without</i> replacement, yet the code is clearly
     * replacing lower-priority values.  Perhaps the distinction is the non-replacement of values
     * with the exact same priority?  The code does avoid doing that (via putIfAbsent()).)
     *
     * @param value     the value to be added
     * @param timestamp the epoch timestamp of {@code value} in seconds
     */
    public void update(double value, long timestamp) {
        rescaleIfNeeded();
        lockForRegularUsage();
        try {
            final double priority = weight(timestamp - _startTime) / ThreadLocalRandom.current()
                                                                                     .nextDouble();
            // TODO/FIXME:  why is this unconditional?  if newCount > size, should be decremented again...
            final long newCount = _count.incrementAndGet();
            if (newCount <= _size) {
                _values.put(priority, value);
            } else {
                Double first = _values.firstKey();
                if (first < priority && _values.putIfAbsent(priority, value) == null) {
                    // ensure we always remove an item
                    while (_values.remove(first) == null) {
                        first = _values.firstKey();
                    }
                }
                // _count.set(_size); ?  [TODO/FIXME:  cheap; shouldn't hurt; better than not setting]
            }
        } finally {
            unlockForRegularUsage();
        }
    }

    /**
     * Merges another reservoir into this one by (1) choosing consistent landmark (_startTime) value;
     * (2) rescaling the reservoir with the older landmark value to the newer one; and (3) adding the
     * other's higher-priority key/value pairs to this one's _values map, throwing out lower-priority
     * entries as in the normal update() method above.
     *
     * @param value     the value to be added
     * @param timestamp the epoch timestamp of {@code value} in seconds
     */
    public void merge(MergeableExponentiallyDecayingReservoir other)
    {
      if (other == null)
        return;
      ConcurrentNavigableMap<Double, Double> otherReversedMap = null;  // alternatively, array of <K,V> entries?
      final long now = _clock.getTick();
      final long otherStartTime = other.getLandmark();  // 1-second granularity
      if (otherStartTime < _startTime)
      {
        // other is older, so need to rescale other's data to match ours
        otherReversedMap = other.rescale(now, CURRENT_NEXT_SCALE_TIME, _startTime);
      }
      else
      {
        // get other's data here (as close as possible to time of getLandmark() call):  small race condition, but
        // given one-hour rescale granularity and fact that other is more recently rescaled, shouldn't actually be
        // a problem
        otherReversedMap = other.getDescendingMap();  // _values.descendingMap()

        if (otherStartTime > _startTime)
        {
          // other is newer; need to rescale our data to match, even if haven't yet reached _nextScaleTime
          rescale(now, _nextScaleTime.get(), otherStartTime);
        }
      }

      // both halves of merge now have same landmark (startTime) value, and we have a reverse-iterable
      // view of other's map => can do actual merge on apples-to-apples basis
      lockForRegularUsage();
      try
      {
        long approxCount = _count.get();
        // iterate over other's entries from highest priority to lowest
        for (Double priority : otherReversedMap.keySet())
        {
          Double value = otherReversedMap.get(priority);
          if (++approxCount <= _size)
          {
            _values.put(priority, value);
          }
          else
          {
            Double lowestPriority = _values.firstKey();
            if (lowestPriority < priority)
            {
              if (_values.putIfAbsent(priority, value) == null)
              {
                // ensure we always remove an item
                while (_values.remove(lowestPriority) == null)
                {
                  lowestPriority = _values.firstKey();
                }
              }
            }
            else  // hit break-even point:  this and all future "other" priorities are equal to or lower than
                  // lowest already present in map, so no point in continuing to iterate:  discard in bulk
            {
              break;
            }
          }
        }

        // in principle, other threads might have been updating _values concurrently, so set _count to the
        // true value, and trim the map if we've exceeded our target size
        long trueCount = _values.size();  // O(n)
        if (trueCount > _size)
        {
          for (long i = _size; i < trueCount; ++i)
          {
            Double lowestPriority = _values.firstKey();
            while (_values.remove(lowestPriority) == null)
            {
              lowestPriority = _values.firstKey();
            }
          }
          trueCount = _size;
        }
        _count.set(trueCount);
      } finally {
        unlockForRegularUsage();
      }
    }

    public long getLandmark()
    {
      lockForRegularUsage();
      try
      {
        return _startTime;
      }
      finally
      {
        unlockForRegularUsage();
      }
    }

    public ConcurrentNavigableMap<Double, Double> getDescendingMap()
    {
      lockForRegularUsage();
      try
      {
        return _values.descendingMap();
      }
      finally
      {
        unlockForRegularUsage();
      }
    }

    // for use with StatUtils.percentile() and StatUtils.max(), which presumably do their own sorting
    public double[] getUnsortedValues()
    {
      lockForRegularUsage();
      try
      {
        Collection<Double> dataValues = _values.values();
        double[] result = new double[dataValues.size()];
        int j = 0;
        for (Double dataValue : dataValues)
        {
          result[j++] = dataValue.doubleValue();
        }
        return result;
      }
      finally
      {
        unlockForRegularUsage();
      }
    }

//  @Override
//  public Snapshot getSnapshot() {
//      lockForRegularUsage();
//      try {
//          return new Snapshot(_values.values());
//      } finally {
//          unlockForRegularUsage();
//      }
//  }

    private long currentTimeInSeconds() {
        return TimeUnit.MILLISECONDS.toSeconds(_clock.getTime());
    }

    private double weight(long t) {
        return exp(_alpha * t);
    }

    // TODO:  if getTime() is comparably cheap to getTick(), use currentTimeInSeconds() for both next/now AND
    //        _startTime:  1-second granularity more than sufficient if rescaling only once per hour, and
    //        should help synch up landmark values during multiple merges => avoid rescale churn
    // [getTime() == System.currentTimeMillis()]					Windoze:  "5-6 CPU clocks"
    // [getTick() == System.nanoTime() with default clock == Clock.UserTimeClock]	"relatively expensive; can be 100+ CPU clocks"
    // [https://blogs.oracle.com/dholmes/entry/inside_the_hotspot_vm_clocks]
    //     [not much detailed info on Linux timings...]
    // [AHA, Linux:  http://bugs.sun.com/view_bug.do?bug_id=6876279  Aug 2010]
    //     [currentTimeMillis():  ~7 ns on 1.6.0_04 64-bit with AggressiveOpts, ~1600 ns on 1.6.0_14 64-bit]
    //         [1.6.0_04 included an experimental gettimeofday() cache in AggressiveOpts; later removed due to problems]
    //     [nanoTime():           ~1700 ns on both 1.6 versions; should be much better with reliable-TSC support in hw]
    private void rescaleIfNeeded() {
        final long now = _clock.getTick();
        final long next = _nextScaleTime.get();
        if (now >= next) {
            rescale(now, next);
        }
    }

    /* "A common feature of the above techniques—indeed, the key technique that
     * allows us to track the decayed weights efficiently—is that they maintain
     * counts and other quantities based on g(ti − L), and only scale by g(t − L)
     * at query time. But while g(ti −L)/g(t−L) is guaranteed to lie between zero
     * and one, the intermediate values of g(ti − L) could become very large. For
     * polynomial functions, these values should not grow too large, and should be
     * effectively represented in practice by floating point values without loss of
     * precision. For exponential functions, these values could grow quite large as
     * new values of (ti − L) become large, and potentially exceed the capacity of
     * common floating point types. However, since the values stored by the
     * algorithms are linear combinations of g values (scaled sums), they can be
     * rescaled relative to a new landmark. That is, by the analysis of exponential
     * decay in Section III-A, the choice of L does not affect the final result. We
     * can therefore multiply each value based on L by a factor of exp(−α(L′ − L)),
     * and obtain the correct value as if we had instead computed relative to a new
     * landmark L′ (and then use this new L′ at query time). This can be done with
     * a linear pass over whatever data structure is being used."
     */
    private void rescale(long now, long next) {
        if (_nextScaleTime.compareAndSet(next, now + RESCALE_THRESHOLD)) {
            lockForRescale();
            try {
                final long oldStartTime = _startTime;
                _startTime = currentTimeInSeconds();
                final ArrayList<Double> keys = new ArrayList<Double>(_values.keySet());
                for (Double key : keys) {
                    final Double value = _values.remove(key);
                    _values.put(key * exp(-_alpha * (_startTime - oldStartTime)), value);
                }

                // make sure the counter is in sync with the number of stored samples.
                _count.set(_values.size());  // TODO/FIXME:  O(n):  truly needed?  after hit _size, effectively just need boolean...
            } finally {
                unlockForRescale();
            }
        }
    }

    // Public variant of above for merging (aggregating) histograms.
    public ConcurrentNavigableMap<Double, Double> rescale(long now, long next, long startTime)
    {
      if (next == CURRENT_NEXT_SCALE_TIME)
      {
        next = _nextScaleTime.get();
      }
      if (_nextScaleTime.compareAndSet(next, now + RESCALE_THRESHOLD))
      {
        lockForRescale();
        try
        {
          final long oldStartTime = _startTime;
          _startTime = startTime;
          final ArrayList<Double> keys = new ArrayList<Double>(_values.keySet());
          for (Double key : keys)
          {
            final Double value = _values.remove(key);
            _values.put(key * exp(-_alpha * (_startTime - oldStartTime)), value);
          }

          // make sure the counter is in sync with the number of stored samples.
          //_count.set(_values.size());  // same O(n) concern as above...
          return _values.descendingMap();
        }
        finally
        {
          unlockForRescale();
        }
      }
      else
      {
        lockForRegularUsage();
        try
        {
          return _values.descendingMap();
        }
        finally
        {
          unlockForRegularUsage();
        }
      }
    }

    private void lockForRescale() {
        _lock.writeLock().lock();
    }

    private void unlockForRescale() {
        _lock.writeLock().unlock();
    }

    private void lockForRegularUsage() {
        _lock.readLock().lock();
    }

    private void unlockForRegularUsage() {
        _lock.readLock().unlock();
    }
}
