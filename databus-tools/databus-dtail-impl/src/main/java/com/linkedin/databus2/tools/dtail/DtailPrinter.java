package com.linkedin.databus2.tools.dtail;
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

import java.io.IOException;
import java.io.OutputStream;
import java.util.Formatter;

import org.apache.log4j.Logger;

import com.linkedin.databus.client.DatabusHttpClientImpl;
import com.linkedin.databus.client.consumer.AbstractDatabusCombinedConsumer;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.client.pub.SCN;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventInternalReadable;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;

public abstract class DtailPrinter extends AbstractDatabusCombinedConsumer
{
  public static final Logger LOG = Logger.getLogger(DtailPrinter.class);
  public static final String GLOBAL_STATS_FORMAT =
      "\n======== DTAILed STATISTICS =========\n" +
      "elapsed, ms    : %d\n" +
      "events         : %d\n" +
      "events/sec     : %6.2f\n" +
      "windows        : %d\n" +
      "windows/sec    : %6.2f\n" +
      "payload, total : %d\n" +
      "payload, MB/s  : %6.2f\n" +
      "avg payload,B  : %d\n" +
      "bytes, total   : %d\n" +
      "bytes, MB/s    : %6.2f\n" +
      "avg lag, ms    : %6.2f\n\n";

  public static enum PrintVerbosity
  {
    EVENT,
    SOURCE,
    WINDOW,
    LIFECYCLE
  }

  public static enum MetadataOutput
  {
    NONE,
    ONLY,
    INCLUDE
  }

  public static class StaticConfig
  {
    private final long _maxEventsNum;
    private final PrintVerbosity _printPrintVerbosity;
    private final long _maxDurationMs;
    private final boolean _printStats;

    public StaticConfig(PrintVerbosity printPrintVerbosity, long maxEventsNum, long maxDurationMs,
                        boolean printStats)
    {
      _maxEventsNum = maxEventsNum;
      _printPrintVerbosity = printPrintVerbosity;
      _maxDurationMs = maxDurationMs;
      _printStats = printStats;
    }

    public long getMaxEventsNum()
    {
      return _maxEventsNum;
    }

    public PrintVerbosity getPrintPrintVerbosity()
    {
      return _printPrintVerbosity;
    }

    public long getMaxDurationMs()
    {
      return _maxDurationMs;
    }

    public boolean isPrintStats()
    {
      return _printStats;
    }
  }

  protected static class StaticConfigBuilderBase
  {
    private long _maxEventsNum = Long.MAX_VALUE;
    private PrintVerbosity _printPrintVerbosity = PrintVerbosity.EVENT;
    private long _maxDurationMs = Long.MAX_VALUE;
    private boolean _printStats = false;

    public long getMaxEventsNum()
    {
      return _maxEventsNum;
    }

    public void setMaxEventsNum(long maxEventsNum)
    {
      _maxEventsNum = maxEventsNum;
    }

    public PrintVerbosity getPrintPrintVerbosity()
    {
      return _printPrintVerbosity;
    }

    public void setPrintPrintVerbosity(PrintVerbosity printPrintVerbosity)
    {
      _printPrintVerbosity = printPrintVerbosity;
    }

    public long getMaxDurationMs()
    {
      return _maxDurationMs;
    }

    public void setMaxDurationMs(long maxDurationMs)
    {
      _maxDurationMs = maxDurationMs;
    }

    public boolean isPrintStats()
    {
      return _printStats;
    }

    public void setPrintStats(boolean printStats)
    {
      _printStats = printStats;
    }
  }

  public static class StaticConfigBuilder extends StaticConfigBuilderBase
                                          implements ConfigBuilder<StaticConfig>

  {
    @Override
    public StaticConfig build() throws InvalidConfigException
    {
      return new StaticConfig(getPrintPrintVerbosity(), getMaxEventsNum(), getMaxDurationMs(),
                              isPrintStats());
    }
  }

  protected final DatabusHttpClientImpl _client;
  protected final StaticConfig _conf;
  protected final OutputStream _out;

  protected boolean _done = false;
  protected long _eventsNum = 0;
  protected long _winNum = 0;
  protected long _startTs = 0;
  protected long _endTs = 0;
  protected long _payloadBytes = 0;
  protected long _eventBytes = 0;
  protected long _eventLagNs = 0;

  public DtailPrinter(DatabusHttpClientImpl client, StaticConfig conf, OutputStream out)
  {
    _client = client;
    _conf = conf;
    _out = out;
  }

  public abstract ConsumerCallbackResult printEvent(DbusEventInternalReadable e, DbusEventDecoder eventDecoder);

  protected ConsumerCallbackResult processEvent(DbusEventInternalReadable e, DbusEventDecoder eventDecoder)
  {
    if (_done) return ConsumerCallbackResult.SUCCESS;

    ConsumerCallbackResult result = printEvent(e, eventDecoder);
    if (ConsumerCallbackResult.isSuccess(result))
    {
      _endTs = System.currentTimeMillis();
      ++ _eventsNum;
      _eventBytes += e.size();
      _payloadBytes += e.payloadLength();
      long lag = _endTs * 1000000 - e.timestampInNanos();
      _eventLagNs += lag;
      //System.out.println("lag: " + e.timestampInNanos() +  "->" + lag + "->" + (_eventLagNs / _eventsNum));

      if (_eventsNum >= _conf.getMaxEventsNum())
      {
        _done = true;
        LOG.info("event limit reached:" + _eventsNum);
      }

      long elapsed = _endTs - _startTs;
      //LOG.info("elapsed: " + elapsed + "; limit=" + _conf.getMaxDurationMs());
      if (elapsed > _conf.getMaxDurationMs())
      {
        _done = true;
        LOG.info("time limit reached; elapsed, ms:" + elapsed);
      }

      if (_done)
      {
        //printStats();
        LOG.info("Dtail shutting down ...");
        _client.shutdownAsynchronously();
      }
    }

    return result;
  }

  @Override
  public ConsumerCallbackResult onDataEvent(DbusEvent e, DbusEventDecoder eventDecoder)
  {
    if (!(e instanceof DbusEventInternalReadable))
    {
      throw new ClassCastException("DbusEvent not readable");
    }
    DbusEventInternalReadable er = (DbusEventInternalReadable)e;
    return processEvent(er, eventDecoder);
  }

  @Override
  public ConsumerCallbackResult onBootstrapEvent(DbusEvent e,
                                                 DbusEventDecoder eventDecoder)
  {
    if (!(e instanceof DbusEventInternalReadable))
    {
      throw new ClassCastException("DbusEvent not readable");
    }
    DbusEventInternalReadable er = (DbusEventInternalReadable)e;
    return processEvent(er, eventDecoder);
  }

  @Override
  public ConsumerCallbackResult onStartConsumption()
  {
    startConsumption();
    return super.onStartConsumption();
  }

  @Override
  public ConsumerCallbackResult onStartBootstrap()
  {
    startConsumption();
    return super.onStartBootstrap();
  }

  protected void startConsumption()
  {
    if (0 >= _startTs)
    {
      _startTs = System.currentTimeMillis();
      _endTs = _startTs;
      LOG.info("start timestamp:" + _startTs);
    }

  }

  @Override
  public ConsumerCallbackResult onStopConsumption()
  {
    if (_conf.isPrintStats())
    {
      printStats();
    }
    return super.onStopConsumption();
  }

  public void printStats()
  {
    if (0 == _winNum) _winNum = 1;
    long elapsedMs = _endTs - _startTs;
    Formatter fmt = new Formatter();
    fmt.format(GLOBAL_STATS_FORMAT, elapsedMs,
               _eventsNum, (1000.0 * _eventsNum / elapsedMs),
               _winNum, (1000.0 * _winNum / elapsedMs),
               _payloadBytes, (1000.0 * _payloadBytes / elapsedMs), _payloadBytes / _eventsNum,
               _eventBytes, (1000.0 * _eventBytes / elapsedMs),
               1.0 * _eventLagNs / (1000000L * _eventsNum));
    fmt.flush();
    fmt.close();
    String statsStr = fmt.toString();
    try
    {
      _out.write(statsStr.getBytes());
      _out.flush();
    }
    catch (IOException e)
    {
      LOG.error("unable to write stats", e);
    }
  }

  @Override
  public ConsumerCallbackResult onStartDataEventSequence(SCN startScn)
  {
    ++ _winNum;
    return super.onStartDataEventSequence(startScn);
  }

}
