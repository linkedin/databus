package com.linkedin.databus2.core;

/**
 * Generic static config for error retries. It is used to model error retries with sleeps between
 * each retry. Allows for configuring error retries with a sleep between each of them.
 *
 * <p>Sleep is computed using the formula: S' = min(maxSleep, S * sleepIncFactor + sleepIncDelta).
 *
 * Typical use cases:
 *
 * <ul>
 *   <li>No retries: maxRetryNum = 0</li>
 *   <li>Unlimited retries: maxRetryNum < 0</li>
 *   <li>No sleep: maxSleep = 0</li>
 *   <li>Linear backoff: sleepIncFactor = 1.0, sleepIncDelta = slope of the sleep line
 *   <li>Exponential backoff (base 2): sleepIncFactor = 2.0
 * </ul>
 *
 */
public class BackoffTimerStaticConfig
{
	public static final BackoffTimerStaticConfig NO_RETRIES =
		new BackoffTimerStaticConfig(0, 0, 0, 0, 0);
	public static final BackoffTimerStaticConfig UNLIMITED_RETRIES =
		new BackoffTimerStaticConfig(0, 0, 0, 0, -1);

	private final long _initSleep;
	private final long _maxSleep;
	private final double _sleepIncFactor;
	private final long _sleepIncDelta;
	private final int _maxRetryNum;

	public BackoffTimerStaticConfig(long initSleep,
			long maxSleep,
			double sleepIncFactor,
			long sleepIncDelta,
			int maxRetryNum)
	{
		super();
		_initSleep = initSleep;
		_maxSleep = maxSleep;
		_sleepIncFactor = sleepIncFactor;
		_sleepIncDelta = sleepIncDelta;
		_maxRetryNum = maxRetryNum;
	}

	/** base sleep (when no errors) in milliseconds */
	public long getInitSleep()
	{
		return _initSleep;
	}

	/** the maximum sleep between retries in milliseconds */
	public long getMaxSleep()
	{
		return _maxSleep;
	}

	/** The exponential increase part in the sleep */
	public double getSleepIncFactor()
	{
		return _sleepIncFactor;
	}

	/** The linear increase part in the sleep */
	public long getSleepIncDelta()
	{
		return _sleepIncDelta;
	}

	/** Max number of error retries before giving up; < 0 infinite retries */
	public int getMaxRetryNum()
	{
		return _maxRetryNum;
	}

	/**
	 * Calculates how long the next sleep should be given the current sleep value. If max sleep is
	 * not 0, the method will guarantee that the next sleep is longer than the current sleep to avoid
	 * getting stuck due to number rounding.
	 *
	 * @param     curSleep        the current sleep value; < 0 indicates first sleep
	 */
	public long calcNextSleep(long curSleep)
	{
		if (curSleep < 0) return _initSleep;

		long newSleep = (long)(_sleepIncFactor * curSleep) + _sleepIncDelta;
		if (newSleep <= curSleep) newSleep = curSleep + 1;

		return Math.min(_maxSleep, newSleep) ;
	}

	@Override
	public String toString() {
		return "BackoffTimerStaticConfig [_initSleep=" + _initSleep
		+ ", _maxSleep=" + _maxSleep + ", _sleepIncFactor="
		+ _sleepIncFactor + ", _sleepIncDelta=" + _sleepIncDelta
		+ ", _maxRetryNum=" + _maxRetryNum + "]";
	}


}
