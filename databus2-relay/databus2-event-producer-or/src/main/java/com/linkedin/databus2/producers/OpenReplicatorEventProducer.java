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
package com.linkedin.databus2.producers;

import java.lang.management.ManagementFactory;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.Logger;

import com.google.code.or.OpenReplicator;
import com.linkedin.databus.core.DatabusThreadBase;
import com.linkedin.databus.core.DbusEventBufferAppendable;
import com.linkedin.databus.core.UnsupportedKeyException;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus.monitoring.mbean.EventSourceStatistics;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.seq.MaxSCNReaderWriter;
import com.linkedin.databus2.producers.ORListener.TransactionProcessor;
import com.linkedin.databus2.producers.db.EventReaderSummary;
import com.linkedin.databus2.producers.db.EventSourceStatisticsIface;
import com.linkedin.databus2.producers.db.ReadEventCycleSummary;
import com.linkedin.databus2.producers.ds.DbChangeEntry;
import com.linkedin.databus2.producers.ds.PerSourceTransaction;
import com.linkedin.databus2.producers.ds.Transaction;
import com.linkedin.databus2.relay.config.LogicalSourceStaticConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import com.linkedin.databus2.schemas.SchemaRegistryService;

/**
 * Fetcher for mysql using OpenReplicator.
 *
 * Connectivity Params :
 * URI format:  mysql://username/password@mysql_host[:mysql_port]/mysql_serverid/binlog_prefix
 * binLogFilePath : <Diretory Path where the binlogs reside>
 *
 * SCN format: <4 higher order bytes file number><4 low order-bytes binlog offset>
 *
 */
public class OpenReplicatorEventProducer extends AbstractEventProducer
{
  public static final Integer DEFAULT_MYSQL_PORT = 3306;
  public static final Pattern PATH_PATTERN = Pattern.compile("/([0-9]+)/[0-9a-zA-Z-]+");

  protected final Logger _log;
  private final OpenReplicator _or;
  private String _binlogFilePrefix;
  private final MaxSCNReaderWriter _maxSCNReaderWriter;
  private final PhysicalSourceStaticConfig _physicalSourceStaticConfig;
  private final SchemaRegistryService _schemaRegistryService;
  private final String _physicalSourceName;
  private EventProducerThread _producerThread;
  private final Map<Integer, OpenReplicatorAvroEventFactory> _eventFactoryMap;
  private final DbusEventsStatisticsCollector _relayInboundStatsCollector;

  // special source to collect global data
  public static final short GLOBAL_SOURCE_ID=0;

  /** Table URI to Source Id Map */
  private final Map<String, Short> _tableUriToSrcIdMap;
  /** Table URI to Source Name Map */
  private final Map<String, String> _tableUriToSrcNameMap;

  private final List<ObjectName> _registeredMbeans = new ArrayList<ObjectName>();
  private final MBeanServer _mbeanServer = ManagementFactory.getPlatformMBeanServer();
  private final String _jmxDomain;

  //list of all sources we are interested in
  private final Map<Short, ORMonitoredSourceInfo> _monitoredSources = new HashMap<Short, ORMonitoredSourceInfo>();

  public OpenReplicatorEventProducer(List<OpenReplicatorAvroEventFactory> eventFactories,
                                     DbusEventBufferAppendable eventBuffer,
                                     MaxSCNReaderWriter maxSCNReaderWriter,
                                     PhysicalSourceStaticConfig physicalSourceStaticConfig,
                                     DbusEventsStatisticsCollector relayInboundStatsCollector,
                                     MBeanServer mbeanServer,
                                     Logger log,
                                     SchemaRegistryService schemaRegistryService,
                                     String jmxDomain)
                                         throws DatabusException, InvalidConfigException
  {
    super(eventBuffer,
          maxSCNReaderWriter,
          physicalSourceStaticConfig,
          mbeanServer);
    _maxSCNReaderWriter = maxSCNReaderWriter;
    _physicalSourceStaticConfig = physicalSourceStaticConfig;
    _physicalSourceName = physicalSourceStaticConfig.getName();
    _log = (null != log) ? log : Logger.getLogger("com.linkedin.databus2.producers.or_" +
        _physicalSourceName);

    _eventFactoryMap = new HashMap<Integer, OpenReplicatorAvroEventFactory>();
    for (OpenReplicatorAvroEventFactory s : eventFactories)
    {
      _eventFactoryMap.put(Integer.valueOf(s.getSourceId()), s);
    }

    _or = new OpenReplicator();
    _jmxDomain = jmxDomain;
    try
    {
      _binlogFilePrefix =  processUri(new URI(physicalSourceStaticConfig.getUri()), _or);
    } catch (URISyntaxException u)
    {
      throw new InvalidConfigException(u);
    }

    _schemaRegistryService = schemaRegistryService;
    _relayInboundStatsCollector = relayInboundStatsCollector;

    _tableUriToSrcIdMap = new HashMap<String, Short>();
    _tableUriToSrcNameMap = new HashMap<String, String>();

    for (LogicalSourceStaticConfig l : _physicalSourceStaticConfig.getSources())
    {
      _tableUriToSrcIdMap.put(l.getUri().toLowerCase(), l.getId());
      _tableUriToSrcNameMap.put(l.getUri().toLowerCase(), l.getName());
      ORMonitoredSourceInfo source = buildORMonitoredSourceInfo(l, _physicalSourceStaticConfig);
      _monitoredSources.put(source.getSourceId(), source);
    }

    // get one fake global source for total stats
    LogicalSourceStaticConfig logicalSourceStaticConfig = new LogicalSourceStaticConfig(GLOBAL_SOURCE_ID, _physicalSourceStaticConfig.getName(), "",
                                                                                        "constant:1", (short)0, false, null, null, null);
    ORMonitoredSourceInfo source = buildORMonitoredSourceInfo(logicalSourceStaticConfig, _physicalSourceStaticConfig);
    _monitoredSources.put(source.getSourceId(), source);
  }

  private ORMonitoredSourceInfo buildORMonitoredSourceInfo( LogicalSourceStaticConfig sourceConfig,
                                                           PhysicalSourceStaticConfig pConfig)
         throws DatabusException, InvalidConfigException
  {
    EventSourceStatistics statisticsBean = new EventSourceStatistics(sourceConfig.getName());

    ORMonitoredSourceInfo sourceInfo =
        new ORMonitoredSourceInfo(sourceConfig.getId(), sourceConfig.getName(), statisticsBean);

    registerMbeans(sourceInfo);

    return sourceInfo;
  }

  /**
   *
   * Responsible for parsing the URI and setting up OpenReplicator connectivity information
   * @param uri Open Replicator URI
   * @param or Open Replicator
   * @return Bin Log Prefix
   * @throws InvalidConfigException if URI is incorrect or missing information
   */
  public static String processUri(URI uri, OpenReplicator or) throws InvalidConfigException
  {
    String userInfo = uri.getUserInfo();
    if (null == userInfo)
      throw new InvalidConfigException("missing user info in: " + uri);
    int slashPos = userInfo.indexOf('/');
    if (slashPos < 0 )
      slashPos = userInfo.length();
    else if (0 == slashPos)
      throw new InvalidConfigException("missing user name in user info: " + userInfo);

    String userName = userInfo.substring(0, slashPos);
    String userPass = slashPos < userInfo.length() - 1 ? userInfo.substring(slashPos + 1) : null;

    String hostName = uri.getHost();

    int port = uri.getPort();
    if (port < 0) port = DEFAULT_MYSQL_PORT;

    String path = uri.getPath();
    if (null == path)
      throw new InvalidConfigException("missing path: " + uri);
    Matcher m = PATH_PATTERN.matcher(path);
    if (!m.matches())
      throw new InvalidConfigException("invalid path:" + path);

    String[] gp = m.group().split("/");
    if (gp.length != 3)
      throw new InvalidConfigException("Invalid format " + Arrays.toString(gp));
    String serverIdStr = gp[1];

    int serverId = -1;
    try
    {
      serverId = Integer.parseInt(serverIdStr);
    }
    catch (NumberFormatException e)
    {
      throw new InvalidConfigException("incorrect mysql serverid:" + serverId);
    }

    // Assign them to incoming variables
    if (null != or)
    {
      or.setUser(userName);
      if (null != userPass) or.setPassword(userPass);
      or.setHost(hostName);
      or.setPort(port);
      or.setServerId(serverId);
    }
    return gp[2];
  }

  /**
   * Returns the logid ( upper 32 bits  of the SCN )
   * For e.g., mysql-bin.000001 is said to have an id 000001
   */
  public static int logid(long scn)
  {
    // During startup, read from the first binlog file
    if (scn == -1 || scn == 0)
    {
      return 1;
    }
    return (int)((scn >> 32) & 0xFFFFFFFF);
  }

  /**
   * Returns the binlogoffset ( lower 32 bits  of the SCN )
   */
  public static int offset(long scn)
  {
    // During startup, read from offset 0 (or should it be 4)
    if (scn == -1 || scn == 0)
    {
      return 4;
    }
    return (int)(scn & 0xFFFFFFFF);
  }


  /* (non-Javadoc)
   * @see com.linkedin.databus2.producers.AbstractEventProducer#start(long)
   */
  @Override
  public synchronized void start(long sinceSCN)
  {
    long sinceSCNToUse = 0;

    if (sinceSCN > 0)
    {
      sinceSCNToUse = sinceSCN;
    }
    else
    {
      /*
       * If the maxScn is persisted in maxSCN file and is greater than 0, then honor it.
       * Else use sinceSCN passed in from above which is -1 or 0
       * Currently, both of them mean to start from the first binlog file with filenum 000001, and offset 4
       */
      if (_maxSCNReaderWriter != null)
      {
        try
        {
          long scn = _maxSCNReaderWriter.getMaxScn();
          if(scn > 0)
          {
            sinceSCNToUse = scn;
          }
        }
        catch (DatabusException e)
        {
          _log.warn("Could not read saved maxScn: Defaulting to startSCN="+ sinceSCNToUse);
        }
      }
    }
    _producerThread = new EventProducerThread(_physicalSourceName, sinceSCNToUse);
    _producerThread.start();
  }

  public class EventProducerThread extends DatabusThreadBase implements TransactionProcessor
  {
    // The scn with which the event buffer is started
    private final AtomicLong _startPrevScn = new AtomicLong(-1);

    private final long _sinceScn;

    private ORListener _orListener;

    private String _sourceName;

    private final long _reconnectIntervalMs = 2000;
    private final long _workIntervalMs = 100;

    public EventProducerThread(String sourceName, long sinceScn)
    {
      super("OpenReplicator_" + sourceName);
      _sourceName = sourceName;
      _sinceScn = sinceScn;
    }

    void initOpenReplicator(long scn)
    {
      int offset = offset(scn);
      int logid = logid(scn);

      String binlogFile = String.format("%s.%06d", _binlogFilePrefix, logid);
      // we should use a new ORListener to drop the left events in binlogEventQueue and the half processed transaction.
      _orListener = new ORListener(_sourceName, logid, _log, _binlogFilePrefix, _producerThread, _tableUriToSrcIdMap,
          _tableUriToSrcNameMap, _schemaRegistryService, 200, 100L);

      _or.setBinlogFileName(binlogFile);
      _or.setBinlogPosition(offset);
      _or.setBinlogEventListener(_orListener);

      //must set transport and binlogParser to null to drop the old connection environment in reinit case
      _or.setTransport(null);
      _or.setBinlogParser(null);

      _log.info("Connecting to OpenReplicator " + _or.getUser() + "@" + _or.getHost() + ":" + _or.getPort() + "/"
              + _or.getBinlogFileName() + "#" + _or.getBinlogPosition());
    }

    @Override
    public void run()
    {
      _eventBuffer.start(_sinceScn);
      _startPrevScn.set(_sinceScn);

      initOpenReplicator(_sinceScn);
      try
      {
        boolean started = false;
        while (!started) {
          try {
            _or.start();
            started = true;
          }
          catch (Exception e) {
            _log.error("Failed to start OpenReplicator: " + e);
            _log.warn("Sleeping for 1000 ms");
            Thread.sleep(1000);
          }
        }
        _orListener.start();
      } catch (Exception e)
      {
        _log.error("failed to start open replicator: " + e.getMessage(), e);
        return;
      }

      long lastConnectMs = System.currentTimeMillis();
      while (!isShutdownRequested())
      {
        if (isPauseRequested())
        {
          LOG.info("Pause requested for OpenReplicator. Pausing !!");
          signalPause();
          LOG.info("Pausing. Waiting for resume command");
          try
          {
            if (_orListener.isAlive())
            {
              _orListener.pause();
            }
            awaitUnPauseRequest();
          }
          catch (InterruptedException e)
          {
            _log.info("Interrupted !!");
          }
          LOG.info("Resuming OpenReplicator !!");
          if (_orListener.isAlive())
          {
            try
            {
              _orListener.unpause();
            }
            catch (InterruptedException e)
            {
              _log.info("Interrupted !!");
            }
          }
          signalResumed();
          LOG.info("OpenReplicator resumed !!");
        }

        if (!_or.isRunning() && System.currentTimeMillis() - lastConnectMs > _reconnectIntervalMs)
        {
          lastConnectMs = System.currentTimeMillis();
          try
          {
            //should stop orListener first to get the final maxScn used for init open replicator.
            _orListener.shutdownAll();
            long maxScn = _maxSCNReaderWriter.getMaxScn();
            _startPrevScn.set(maxScn);
            initOpenReplicator(maxScn);
            _or.start();
            _orListener.start();
            _log.info("start Open Replicator successfully");
          }
          catch (Exception e)
          {
            _log.error("failed to start Open Replicator", e);
            if (_or.isRunning())
            {
              try
              {
                _or.stop(10, TimeUnit.SECONDS);
              }
              catch (Exception e2)
              {
                _log.error("failed to stop Open Replicator", e2);
              }
            }
          }
        }

        try
        {
          Thread.sleep(_workIntervalMs);
        }
        catch (InterruptedException e)
        {
          _log.info("Interrupted !!");
        }
      }

      if (_or.isRunning())
      {
        try
        {
          _or.stop(10, TimeUnit.SECONDS);
        }
        catch (Exception e)
        {
          _log.error("failed to stop Open Replicator", e);
        }
      }
      _orListener.shutdownAll();

      _log.info("Event Producer Thread done");
      doShutdownNotify();
    }

    @Override
    public void onEndTransaction(Transaction txn)
        throws DatabusException
    {
      try
      {
        addTxnToBuffer(txn);
        _maxSCNReaderWriter.saveMaxScn(txn.getIgnoredSourceScn()!=-1 ? txn.getIgnoredSourceScn() : txn.getScn());
      }
      catch (UnsupportedKeyException e)
      {
        _log.fatal("Got UnsupportedKeyException exception while adding txn (" + txn + ") to the buffer", e);
        throw new DatabusException(e);
      }
      catch (EventCreationException e)
      {
        _log.fatal("Got EventCreationException exception while adding txn (" + txn + ") to the buffer", e);
        throw new DatabusException(e);
      }
    }

    /**
     * Add all txn events to Buffer
     * @param txn Transaction to be added
     * @throws EventCreationException
     */
    private void addTxnToBuffer(Transaction txn)
        throws DatabusException, UnsupportedKeyException, EventCreationException
    {
      /**
       * We skip the start scn of the relay, we have already added a EOP for this SCN in the buffer.
       * Why is this not a problem ?
       * There are two cases:
       * 1. When we use the earliest/latest scn if there is no maxScn (We don't really have a start point). So it's really OK to miss the first event.
       * 2. If it's the maxSCN, then event was already seen by the relay.
       */
      if(txn.getScn() <= _startPrevScn.get())
      {
        _log.info("Skipping this transaction, EOP already send for this event. Txn SCN =" + txn.getScn()
                                    + ", _startPrevScn=" + _startPrevScn.get());
        return;
      }

      List<PerSourceTransaction> sources = txn.getOrderedPerSourceTransactions();
      if (0 == sources.size()) {
        _log.info("Ignoring txn: " + txn);
        return;
      }

      EventSourceStatistics globalStats = getSource(GLOBAL_SOURCE_ID).getStatisticsBean();

      _eventBuffer.startEvents();

      long scn = txn.getScn();
      long timestamp = txn.getTxnNanoTimestamp();
      List<EventReaderSummary> summaries = new ArrayList<EventReaderSummary>();

      for (PerSourceTransaction t: sources )
      {
        long startDbUpdatesMs = System.currentTimeMillis();
        short sourceId = (short)t.getSrcId();
        EventSourceStatistics perSourceStats = getSource(sourceId).getStatisticsBean();
        long dbUpdatesEventsSize = 0;

        for (DbChangeEntry c : t.getDbChangeEntrySet())
        {
          int length = 0;

          try
          {
            length = _eventFactoryMap.get(t.getSrcId()).createAndAppendEvent(
                                                                  c,
                                                                  _eventBuffer,
                                                                  false,
                                                                  _relayInboundStatsCollector);
            if ( length < 0)
            {
              _log.error("Unable to append DBChangeEntry (" + c + ") to event buffer !! EVB State : " + _eventBuffer);
              throw new DatabusException("Unable to append DBChangeEntry (" + c + ") to event buffer !!");
            }
            dbUpdatesEventsSize += length;
          } catch (DatabusException e) {
            _log.error("Got databus exception :", e);
            perSourceStats.addError();
            globalStats.addEmptyEventCycle();
            throw e;
          } catch (UnsupportedKeyException e) {
            perSourceStats.addError();
            globalStats.addEmptyEventCycle();
            _log.error("Got UnsupportedKeyException :", e);
            throw e;
          } catch (EventCreationException e) {
            perSourceStats.addError();
            globalStats.addEmptyEventCycle();
            _log.error("Got EventCreationException :", e);
            throw e;
          }
          perSourceStats.addEventCycle(1, txn.getTxnReadLatencyNanos(), length, scn);
          globalStats.addEventCycle(1, txn.getTxnReadLatencyNanos(), length, scn);
        }

        long endDbUpdatesMs = System.currentTimeMillis();
        long dbUpdatesElapsedTimeMs = endDbUpdatesMs - startDbUpdatesMs;

        // Log Event Summary at logical source level
        EventReaderSummary summary = new EventReaderSummary(sourceId, _monitoredSources.get(sourceId).getSourceName(),
                                                            scn, t.getDbChangeEntrySet().size(), dbUpdatesEventsSize,-1L /* Not supported */,
                                                            dbUpdatesElapsedTimeMs,  timestamp, timestamp, -1L /* Not supported */);
        if (_eventsLog.isInfoEnabled())
        {
          _eventsLog.info(summary.toString());
        }
        summaries.add(summary);

        long tsEnd = System.currentTimeMillis();
        perSourceStats.addTimeOfLastDBAccess(tsEnd);
        globalStats.addTimeOfLastDBAccess(tsEnd);
      }
      _eventBuffer.endEvents(scn, _relayInboundStatsCollector);
      // Log Event Summary at Physical source level
      ReadEventCycleSummary summary = new ReadEventCycleSummary(_physicalSourceStaticConfig.getName(),
                                                                summaries,
                                                                scn,
                                                                -1 /* Overall time including query time not calculated */);

      if (_eventsLog.isInfoEnabled())
      {
        _eventsLog.info(summary.toString());
      }
    }
  }

  /**
   * Since we have a push architecture, this is used only for logging.
   * @see com.linkedin.databus2.producers.AbstractEventProducer#readEventsFromAllSources(long)
   */
  @Override
  protected ReadEventCycleSummary readEventsFromAllSources(long sinceSCN) throws DatabusException,
  EventCreationException,
  UnsupportedKeyException
  {
    throw new RuntimeException("Not supported !!");
  }

  /* (non-Javadoc)
   * @see com.linkedin.databus2.producers.AbstractEventProducer#isPaused()
   */
  @Override
  public synchronized boolean isPaused()
  {
    return _producerThread.isPauseRequested();
  }

  /* (non-Javadoc)
   * @see com.linkedin.databus2.producers.AbstractEventProducer#isRunning()
   */
  @Override
  public synchronized boolean isRunning()
  {
    return !_producerThread.isShutdownRequested() && !_producerThread.isPauseRequested();
  }

  /* (non-Javadoc)
   * @see com.linkedin.databus2.producers.AbstractEventProducer#unpause()
   */
  @Override
  public synchronized void unpause()
  {
    try
    {
      _producerThread.unpause();
    }
    catch (InterruptedException e)
    {
      _log.info("Interrupted while unpausing EventProducer", e);
    }
    super.unpause();
  }

  /* (non-Javadoc)
   * @see com.linkedin.databus2.producers.AbstractEventProducer#pause()
   */
  @Override
  public synchronized void pause()
  {
    try
    {
      _producerThread.pause();
    }
    catch (InterruptedException e)
    {
      _log.info("Interrupted while pausing EventProducer", e);
    }    super.pause();
  }

  /* (non-Javadoc)
   * @see com.linkedin.databus2.producers.AbstractEventProducer#shutdown()
   */
  @Override
  public synchronized void shutdown()
  {
    _producerThread.shutdown();
    super.shutdown();

    for (ObjectName name:_registeredMbeans)
    {
      try {
        _mbeanServer.unregisterMBean(name);
        _log.info("Unregistered or-source mbean: " + name);
      } catch (MBeanRegistrationException e) {
        _log.warn("Exception when unregistering or-source statistics mbean: " + name + e) ;
      } catch (InstanceNotFoundException e) {
        _log.warn("Exception when unregistering or-source statistics mbean: " + name + e) ;
      }
    }
  }

  @Override
  public List<? extends EventSourceStatisticsIface> getSources()
  {
    return new ArrayList<ORMonitoredSourceInfo>(_monitoredSources.values());
  }

  // register each source with the mbeanServer
  private void registerMbeans(ORMonitoredSourceInfo source) throws DatabusException
  {
    try
    {
      Hashtable<String,String> props = new Hashtable<String,String>();
      props.put("type", "SourceStatistics");
      props.put("name", source.getSourceName());
      ObjectName objectName = new ObjectName(_jmxDomain, props);

      if (_mbeanServer.isRegistered(objectName))
      {
        _log.warn("Unregistering old or-source statistics mbean: " + objectName);
        _mbeanServer.unregisterMBean(objectName);
      }

      _mbeanServer.registerMBean(source.getStatisticsBean(), objectName);
      _log.info("Registered or-source statistics mbean: " + objectName);
      _registeredMbeans.add(objectName);
    }
    catch(Exception ex)
    {
      _log.error("Failed to register the or-source statistics mbean for source (" + source.getSourceName() + ") due to an exception.", ex);
      throw new DatabusException("Failed to initialize or event statistics mbeans.", ex);
    }
  }

  /**
   * return MonitoredSourceInfo per source
   * @param sourceId
   * @return MonitoredSourceInof for this source id
   */
  public ORMonitoredSourceInfo getSource(short sourceId) {
    return _monitoredSources.get(sourceId);
  }
}
