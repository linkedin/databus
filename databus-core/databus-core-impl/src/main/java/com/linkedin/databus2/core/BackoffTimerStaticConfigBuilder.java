package com.linkedin.databus2.core;
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


import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;

/** A builder for error retries static configuration */
public class BackoffTimerStaticConfigBuilder implements ConfigBuilder<BackoffTimerStaticConfig>
{
	public static final long NO_SLEEP = 0L;

	private long _initSleep      = 0;
	private long _maxSleep       = 60000; //1 min
	private double _sleepIncFactor = 2.0;
	private long _sleepIncDelta  = 1;
	private int _maxRetryNum     = 10;

	public BackoffTimerStaticConfigBuilder()
	{
	}

	public long getInitSleep()
	{
		return _initSleep;
	}

	public void setInitSleep(long initSleep)
	{
		_initSleep = initSleep;
	}

	public long getMaxSleep()
	{
		return _maxSleep;
	}

	public void setMaxSleep(long maxSleep)
	{
		_maxSleep = maxSleep;
	}

	public double getSleepIncFactor()
	{
		return _sleepIncFactor;
	}

	public void setSleepIncFactor(double sleepIncFactor)
	{
		_sleepIncFactor = sleepIncFactor;
	}

	public long getSleepIncDelta()
	{
		return _sleepIncDelta;
	}

	public void setSleepIncDelta(long sleepIncDelta)
	{
		_sleepIncDelta = sleepIncDelta;
	}

	public int getMaxRetryNum()
	{
		return _maxRetryNum;
	}

	public void setMaxRetryNum(int maxRetryNum)
	{
		_maxRetryNum = maxRetryNum;
	}

	@Override
	public BackoffTimerStaticConfig build() throws InvalidConfigException
	{
		if (0 > _initSleep) throw new InvalidConfigException("initial sleep must be non-negative:" +
				_initSleep);
		if (0 > _maxSleep) throw new InvalidConfigException("max sleep must be non-negative: " +
				_maxSleep);

		BackoffTimerStaticConfig newConfig = new BackoffTimerStaticConfig(_initSleep, _maxSleep,
				_sleepIncFactor, _sleepIncDelta, _maxRetryNum);

		//sanity check
		long secondSleep = newConfig.calcNextSleep(_initSleep);
		if (secondSleep < _initSleep || secondSleep < 0)
			throw new InvalidConfigException("sleeps are decreasing!");

		return newConfig;
	}

	@Override
	public String toString() {
		return "BackoffTimerStaticConfigBuilder [_initSleep=" + _initSleep
		+ ", _maxSleep=" + _maxSleep + ", _sleepIncFactor="
		+ _sleepIncFactor + ", _sleepIncDelta=" + _sleepIncDelta
		+ ", _maxRetryNum=" + _maxRetryNum + "]";
	}



}
