package com.linkedin.databus2.relay;
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.lang.management.ManagementFactory;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.xml.stream.XMLStreamException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Encoder;
import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;

import com.linkedin.databus.core.ConcurrentAppendableCompositeFileInputStream;
import com.linkedin.databus.core.DbusConstants;
import com.linkedin.databus.core.DbusEventBufferAppendable;
import com.linkedin.databus.core.DbusEventInfo;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.DbusOpcode;
import com.linkedin.databus.core.TrailFilePositionSetter;
import com.linkedin.databus.core.UnsupportedKeyException;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus.core.util.RateControl;
import com.linkedin.databus.monitoring.mbean.EventSourceStatistics;
import com.linkedin.databus.monitoring.mbean.GGParserStatistics;
import com.linkedin.databus.monitoring.mbean.GGParserStatistics.TransactionInfo;
import com.linkedin.databus.monitoring.mbean.GGParserStatisticsMBean;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.netty.ServerContainer;
import com.linkedin.databus2.core.seq.MaxSCNReaderWriter;
import com.linkedin.databus2.ggParser.XmlStateMachine.ColumnsState.KeyPair;
import com.linkedin.databus2.ggParser.XmlStateMachine.DbUpdateState;
import com.linkedin.databus2.ggParser.XmlStateMachine.TransactionState;
import com.linkedin.databus2.ggParser.XmlStateMachine.TransactionSuccessCallBack;
import com.linkedin.databus2.ggParser.staxparser.StaxBuilder;
import com.linkedin.databus2.ggParser.staxparser.XmlParser;
import com.linkedin.databus2.producers.AbstractEventProducer;
import com.linkedin.databus2.producers.EventCreationException;
import com.linkedin.databus2.producers.PartitionFunction;
import com.linkedin.databus2.producers.db.EventReaderSummary;
import com.linkedin.databus2.producers.db.EventSourceStatisticsIface;
import com.linkedin.databus2.producers.db.GGMonitoredSourceInfo;
import com.linkedin.databus2.producers.db.GGXMLTrailTransactionFinder;
import com.linkedin.databus2.producers.db.ReadEventCycleSummary;
import com.linkedin.databus2.producers.gg.GGEventGenerationFactory;
import com.linkedin.databus2.relay.config.LogicalSourceStaticConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import com.linkedin.databus2.schemas.SchemaId;
import com.linkedin.databus2.schemas.SchemaRegistryService;

/**
 * The event producer implementation for the xml trail file based event producer.
 * The producer controls the xmlparser (start, pause, shutdown etc.)
 */
public class GoldenGateEventProducer extends AbstractEventProducer
{
  //Physical source config for which this relay is configured (should be only the golden gate (gg://) sources.
  private final PhysicalSourceStaticConfig _pConfig;
  //The schema registry service the relay uses to fetch the schemas.
  private final SchemaRegistryService _schemaRegistryService;
  private final DbusEventsStatisticsCollector _statsCollector;
  //The scn of the first event.
  //TBD : Reconcile this with sinceScn in the parent class ?
  private final AtomicLong _scn = new AtomicLong(-1);
  // The scn with which the event buffer is started
  // TBD : Reconcile this with sinceScn in the parent class ?
  private final AtomicLong _startPrevScn = new AtomicLong(-1);
  //This variable depicts the current state of the event producer
  State _currentState = State.INIT;
  //The workerthread is a thread that controls the xml parser
  private WorkerThread _worker;
  //Reentrant lock to protect pause requests
  private final Lock _pauseLock = new ReentrantLock(true);
  private final Condition _pausedCondition = _pauseLock.newCondition();
  //The hashMap holds the sourceId => Partition function
  private final HashMap<Integer,PartitionFunction>  _partitionFunctionHashMap;
  // Ensures relay reads at a controlled rate
  private RateControl _rc;

  private final GGParserStatistics _ggParserStats;

  //list of all sources we are interested in
  private final Map<Short, GGMonitoredSourceInfo> _monitoredSources = new HashMap<Short, GGMonitoredSourceInfo>();
  // special source to collect global data
  public static final short GLOBAL_SOURCE_ID=0;

  private final List<ObjectName> _registeredMbeans = new ArrayList<ObjectName>();
  private final MBeanServer _mbeanServer = ManagementFactory.getPlatformMBeanServer();

  /** DB Events Logger */
  private final Logger _eventsLog;

  private enum State
  {
    INIT, PAUSED, RUNNING, SHUTDOWN
  };

  public final Logger _log;

  /**
   *
   * @param pConfig The physical source config for which the event producer is configured.
   * @param schemaRegistryService Schema registry to fetch schemas
   * @param dbusEventBuffer An event buffer to which the producer can write/append events.
   * @param statsCollector Reporting stats
   * @param maxScnReaderWriters To read/write the maxScn from maxScn file
   * @throws DatabusException
   */
  public GoldenGateEventProducer(PhysicalSourceStaticConfig pConfig,
                                 SchemaRegistryService schemaRegistryService,
                                 DbusEventBufferAppendable dbusEventBuffer,
                                 DbusEventsStatisticsCollector statsCollector,
                                 MaxSCNReaderWriter maxScnReaderWriters)
      throws DatabusException
  {
    super(dbusEventBuffer, maxScnReaderWriters, pConfig, null);
    _pConfig = pConfig;
    _schemaRegistryService = schemaRegistryService;
    _statsCollector = statsCollector;
    _currentState = State.INIT;
    _partitionFunctionHashMap = new HashMap<Integer, PartitionFunction>();
    _eventsLog = Logger.getLogger("com.linkedin.databus2.producers.db.events." + pConfig.getName());

    if (_pConfig != null)
    {
      long eventRatePerSec = pConfig.getEventRatePerSec();
      long maxThrottleDurationInSecs = pConfig.getMaxThrottleDurationInSecs();

      if ((eventRatePerSec > 0) && (maxThrottleDurationInSecs > 0))
      {
        _rc = new RateControl(eventRatePerSec, maxThrottleDurationInSecs);
      }
      else
      {
        // Disable rate control
        _rc = new RateControl(Long.MIN_VALUE, Long.MIN_VALUE);
      }
    }

    final String MODULE = GoldenGateEventProducer.class.getName();
    _log = Logger.getLogger(MODULE + "." + getName());

    //Create a hashmap for logical source id ==> PartitionFunction, this will be used as the logical partition Id for the event creation
    // also create a list(map) of MonitoredSourceInfo objects to monitor GGEventProducer progress
    for(int i = 0; i < _pConfig.getSources().length; i++)
    {
      LogicalSourceStaticConfig logicalSourceStaticConfig = _pConfig.getSources()[i];
      GGMonitoredSourceInfo source = buildGGMonitoredSourceInfo(logicalSourceStaticConfig, _pConfig);
      _monitoredSources.put(source.getSourceId(), source);
    }

    // get one fake global source for total stats
    LogicalSourceStaticConfig logicalSourceStaticConfig = new LogicalSourceStaticConfig(GLOBAL_SOURCE_ID, _pConfig.getName(), "",
                                                                                        "constant:1", (short)0, false, null, null, null);
    GGMonitoredSourceInfo source = buildGGMonitoredSourceInfo(logicalSourceStaticConfig, _pConfig);
    _monitoredSources.put(source.getSourceId(), source);

    // create stats collector for parser
    _ggParserStats = new GGParserStatistics(_pConfig.getName());
    registerParserMbean(_ggParserStats);

  }


  public GGMonitoredSourceInfo buildGGMonitoredSourceInfo(
            LogicalSourceStaticConfig sourceConfig, PhysicalSourceStaticConfig pConfig)
                throws DatabusException, InvalidConfigException
  {
    // udpate partition mapping
    PartitionFunction partitionFunction = GGEventGenerationFactory.buildPartitionFunction(sourceConfig);
    _partitionFunctionHashMap.put((int)sourceConfig.getId(), partitionFunction);

    EventSourceStatistics statisticsBean = new EventSourceStatistics(sourceConfig.getName());

    GGMonitoredSourceInfo sourceInfo =
        new GGMonitoredSourceInfo(sourceConfig.getId(), sourceConfig.getName(), statisticsBean);

    registerMbeans(sourceInfo);

    return sourceInfo;
  }

  private void registerParserMbean(GGParserStatisticsMBean parserBean) throws DatabusException
  {
    try
    {
      Hashtable<String,String> props = new Hashtable<String,String>();
      props.put("type", "GGParserStatistics");
      props.put("name", _pConfig.getName());
      ObjectName objectName = new ObjectName(ServerContainer.JMX_DOMAIN, props);

      if (_mbeanServer.isRegistered(objectName))
      {
        _log.warn("Unregistering old ggparser statistics mbean: " + objectName);
        _mbeanServer.unregisterMBean(objectName);
      }

      _mbeanServer.registerMBean(parserBean, objectName);
      _log.info("Registered gg-parser statistics mbean: " + objectName);
      _registeredMbeans.add(objectName);
    }
    catch(Exception ex)
    {
      _log.error("Failed to register the GGparser statistics mbean for db = " + _pConfig.getName() + " due to an exception.", ex);
      throw new DatabusException("Failed to initialize GGparser statistics mbean.", ex);
    }

  }


  // register each source with the mbeanServer
  private void registerMbeans(GGMonitoredSourceInfo source) throws DatabusException
  {
    try
    {
      Hashtable<String,String> props = new Hashtable<String,String>();
      props.put("type", "SourceStatistics");
      props.put("name", source.getSourceName());
      ObjectName objectName = new ObjectName(ServerContainer.JMX_DOMAIN, props);

      if (_mbeanServer.isRegistered(objectName))
      {
        _log.warn("Unregistering old gg-source statistics mbean: " + objectName);
        _mbeanServer.unregisterMBean(objectName);
      }

      _mbeanServer.registerMBean(source.getStatisticsBean(), objectName);
      _log.info("Registered gg-source statistics mbean: " + objectName);
      _registeredMbeans.add(objectName);
    }
    catch(Exception ex)
    {
      _log.error("Failed to register the gg-source statistics mbean for source (" + source.getSourceName() + ") due to an exception.", ex);
      throw new DatabusException("Failed to initialize gg event statistics mbeans.", ex);
    }

  }

  public GGParserStatistics getParserStats() {
    return _ggParserStats;
  }

  /**
   * Returns the name of the source for which this relay is configured
   */
  @Override
  public String getName()
  {
    return (_pConfig != null) ? _pConfig.getName() : "NONE";
  }

  /**
   * Get the last scn that the relay written to the buffer.
   * Will return <=0 if called before starting the producer
   * @return
   */
  @Override
  public long getSCN()
  {
    return _scn.get();
  }

  /**
   *
   * @param sinceSCN
   */
  @Override
  public synchronized void start(long sinceSCN)
  {
    _log.info("Start golden gate evert producer requested.");
    if (_currentState == State.RUNNING)
    {
      _log.error("Thread already running! ");
      return;
    }
    _scn.set(TrailFilePositionSetter.USE_LATEST_SCN);

    if (sinceSCN > 0)
    {
      _scn.set(sinceSCN);
    }
    else
    {
      if (getMaxScnReaderWriter() != null)
      {
        try
        {
          long scn = getMaxScnReaderWriter().getMaxScn();

          //If the max scn is greater than 0, then honor it.
          if(scn > 0)
          {
            //apply the restart SCN offset
            long newScn = (scn >= _pConfig.getRestartScnOffset()) ? scn - _pConfig.getRestartScnOffset() : 0;
            _log.info("Checkpoint read = " + scn + " restartScnOffset= " + _pConfig.getRestartScnOffset() + " Adjusted SCN= " + newScn);
            if (newScn > 0)
            {
              _scn.set(newScn);
            }
          }
          else //If the max scn is set to <0, this is a special case that we use to let the trail file notifier that you want to override the default behaviour of starting with the latest scn.
          {
              _log.info("Overridding default behaviour (start with latest scn), using scn : " + scn + " to start the relay");
              if(scn != TrailFilePositionSetter.USE_EARLIEST_SCN && scn != TrailFilePositionSetter.USE_LATEST_SCN)
                throw new DatabusException("The scn you have passed is neither EARLIEST or LATEST  setting, cannot proceed with using this scn");

              _scn.set(scn);
          }

        }
        catch (DatabusException e)
        {
          _log.warn("Could not read saved maxScn: Defaulting to startSCN="
                       + _scn.get());
        }
      }
    }

    if(_worker == null)
    {
      _log.info("Starting with scn = " + _scn.get());
      _worker = new WorkerThread();
      _worker.setDaemon(true);
      _worker.start();
    }
  }

  @Override
  public boolean isRunning()
  {
    if(_currentState == State.RUNNING) return true;
    return false;
  }

  @Override
  public boolean isPaused()
  {
    if(_currentState == State.PAUSED) return true;
    return false;
  }

  // TBD : Reconcile this behavior with the pause/unpause functionality in parent class
  @Override
  public void unpause()
  {
    _log.info("Golden gate evert producer unpause requested.");
    _pauseLock.lock();
    try{
    _pauseRequested = false;
    _pausedCondition.signalAll();
    }
    catch(Exception e)
    {
      _log.error("Error while unpausing the golden gate event producer: " + e);
    }
    finally{
      _pauseLock.unlock();
    }
  }

  // TBD : Reconcile this behavior with the pause/unpause functionality in parent class
  @Override
  public void pause()
  {
    _log.info("Golden gate evert producer pause requested.");
    _pauseLock.lock();
    try{
      _pauseRequested = true;
    }
    catch(Exception e)
    {
      _log.error("Error while unpausing the golden gate event producer: " + e);
    }
    finally{
      _pauseLock.unlock();
    }
  }

  private synchronized boolean isPauseRequested()
  {
    return _pauseRequested;
  }

  // TBD : Reconcile this behavior in parent class
  @Override
  public synchronized void shutdown()
  {
    _log.info("Golden gate evert producer shutdown requested.");
    _shutdownRequested = true;

    for (ObjectName name:_registeredMbeans)
    {
      try {
        _mbeanServer.unregisterMBean(name);
        _log.info("Unregistered gg-source mbean: " + name);
      } catch (MBeanRegistrationException e) {
        _log.warn("Exception when unregistering gg-source statistics mbean: " + name + e) ;
      } catch (InstanceNotFoundException e) {
        _log.warn("Exception when unregistering gg-source statistics mbean: " + name + e) ;
      }
    }


    if (_worker != null)
    {
      if(_worker._parser == null)
      {
        _log.error("The parser is null, unable to shutdown the event producer");
        return;
      }
      _worker._parser.setShutDownRequested(true);
      _worker.interrupt();
    }

    _log.warn("Shut down request sent to thread");
  }

  // TBD : Reconcile this behavior in parent class
  @Override
  public synchronized void waitForShutdown() throws InterruptedException,
                                                    IllegalStateException
  {
    if (_currentState != State.SHUTDOWN)
    {
      if (_worker != null)
        _worker.join();
    }
  }

  @Override
  public synchronized void waitForShutdown(long timeout)
      throws InterruptedException, IllegalStateException
  {
    if (_currentState != State.SHUTDOWN)
    {
      if (_worker != null)
        _worker.join(timeout);
    }
  }

  @Override
  protected ReadEventCycleSummary readEventsFromAllSources(long sinceSCN)
  throws DatabusException, EventCreationException, UnsupportedKeyException
  {
    throw new NotImplementedException("Not implemented");
  }

  private class WorkerThread extends Thread{

    private HandleXmlCallback _xmlCallback;
    private XmlParser _parser;
    private int nullTransactions = 0;

    private class HandleXmlCallback implements TransactionSuccessCallBack
    {

      @Override
      public void onTransactionEnd(List<TransactionState.PerSourceTransactionalUpdate> dbUpdates, TransactionInfo txnInfo)
          throws DatabusException, UnsupportedKeyException
      {
        long scn = txnInfo.getScn();

        if(dbUpdates == null)
          _log.info("Received empty transaction callback with no DbUpdates with scn " + scn);
        else
          _log.info("Received transaction callback for " + dbUpdates.size() + " sources and scn " + scn);

        if(!isReadyToRun())
          return;

        try
        {
          if(dbUpdates == null) {
            checkAndInsertEOP(scn);
            _ggParserStats.addTransactionInfo(txnInfo, 0);
          } else {
            addEventToBuffer(dbUpdates, txnInfo);
          }
        }
        catch (DatabusException e)                                 //TODO upon exception, retry from the last SCN.
        {
          _ggParserStats.addError();
          _log.error("Error while adding events to buffer: " + e);
          throw e;
        }
        catch (UnsupportedKeyException e)
        {
          _ggParserStats.addError();
          _log.error("Error while adding events to buffer: " + e);
          throw e;
        }

      }
    }

      /**
       * The method inserts EOP for every 100 times //TODO update with config name
       * @param scn
       */
      private void checkAndInsertEOP(long scn)
      {
        _scn.set(scn);
        nullTransactions++;
        if(nullTransactions >= 100) //TODO add a configuration to get this value, number of null transactions before inserting EOP
        {
          _log.info("Inserting EOP in the buffer after "+ nullTransactions + " empty transactions at scn = " + scn);
          getEventBuffer().startEvents();
          getEventBuffer().endEvents(scn, _statsCollector);
          nullTransactions = 0;
        }
      }


      /**
       *
       * @return true if ready to run, false if shutdown or should not run
       */
      private boolean isReadyToRun(){

        if(_shutdownRequested)
        {
          _log.info("The parser is already shutdown");
          _currentState = GoldenGateEventProducer.State.SHUTDOWN;
          return false;
        }

        _pauseLock.lock();
        try{
          if (isPauseRequested()
              && _currentState != GoldenGateEventProducer.State.PAUSED)
          {
            _currentState = GoldenGateEventProducer.State.PAUSED;
            _log.warn("Pausing event generator");
            while (_currentState == GoldenGateEventProducer.State.PAUSED
                && !_shutdownRequested
                && isPauseRequested())
            {
              try
              {
                _pausedCondition.await();
              }
              catch (InterruptedException e)
              {
                _log.warn("Paused thread interrupted! Shutdown requested="
                    + _shutdownRequested);
              }
            }
          }
        }
        finally
        {
          _pauseLock.unlock();
        }

        if (!_shutdownRequested)
        {
          _currentState = GoldenGateEventProducer.State.RUNNING;
        }

        return true;
      }

    @Override
    public void run()
    {

      ConcurrentAppendableCompositeFileInputStream compositeInputStream = null;
      try
      {
        if(_xmlCallback == null)
          _xmlCallback = new HandleXmlCallback();

        String xmlDir = GGEventGenerationFactory.uriToGGDir(_pConfig.getUri());
        String xmlPrefix = GGEventGenerationFactory.uriToXmlPrefix(_pConfig.getUri());
        File file = new File(xmlDir);
        if(!file.exists() || !file.isDirectory())
        {
          _log.fatal("Unable to load the directory: "+ xmlDir + " it doesn't seem to be a valid directory");
          throw new DatabusException("Invalid trail file directory");
        }

        boolean parseError = false;
        do
        {
          try{
             _log.info("Using xml directory : "+ xmlDir + " and using the xml Prefix : " + xmlPrefix);
            compositeInputStream = locateScnInTrailFile(xmlDir,xmlPrefix);
            compositeInputStream.setGGParserStats(_ggParserStats);
            _log.info("Attempting to start the parser...");


            //Not a retry, first time the producer is started, in which case, start the eventBuffer with the appropriate scn
            if(!parseError)
            {
              _log.info("Starting dbusEventBuffer with _scn : " + _startPrevScn.get());
              getEventBuffer().start(_startPrevScn.get());
            }
            else
            {
                _log.warn("Umm, looks like the parser had failed, this is an retry attempt using _scn: " + _scn.get());
                _log.info("CompositeInputStream used:" + compositeInputStream);
            }

           StaxBuilder builder = new StaxBuilder(_schemaRegistryService, wrapStreamWithXmlTags(compositeInputStream), _pConfig, _xmlCallback);

            if(_log.isDebugEnabled())
              _log.debug("CompositeInputStream used:" + compositeInputStream);

            _parser = builder.getParser();
            builder.processXml(); // --> The call doesn't return after this (it starts processing the xml trail files), unless a shutdown is requested or an exception is thrown.
            parseError = false;  //--> If this code path is executed, then the shutdown has been requested
          }
          catch (XMLStreamException e)
          {
            _ggParserStats.addParsingError();

            //If the parser was in the middle of execution and an shutdown was issued, then an xmlstream exception is expected.
            if(_shutdownRequested )
            {
              parseError = false;
            }
            else
            {
              _log.error("Error while parsing the xml, will retry loading the parser", e);
              _log.info("Last scn seen before the crash: " + _scn.get());
              _log.info("CompositeInputStream used:" + compositeInputStream);
              parseError = true;
            }
          }
            finally
          {
            if(compositeInputStream != null)
              compositeInputStream.close();
          }
        }while(parseError);  //TODO && retry count (add config to control number of retires)

      }
      catch (RuntimeException e) {
        _log.info("CompositeInputStream used:" + compositeInputStream);
        _log.error("Error while parsing data, compositeInputStream shutting down the relay", e);
        _currentState = GoldenGateEventProducer.State.SHUTDOWN;
        throw e;
      }
      catch (Exception e)
      {
        _log.info("CompositeInputStream used:" + compositeInputStream);
        _log.error("Error while parsing data, compositeInputStream shutting down the relay", e);
        _currentState = GoldenGateEventProducer.State.SHUTDOWN;
        return;
      }
    }

  }

  /**
   * The method takes the an inputstream as an input and wraps it around with xml tags,
   * sets the xml encoding and xml version specified in the physical sources config.
   * @param compositeInputStream The inputstream to be wrapped with the xml tags
   * @return
   */
  private InputStream wrapStreamWithXmlTags(InputStream compositeInputStream)
  {

    String xmlVersion = _pConfig.getXmlVersion();
    String xmlEncoding = _pConfig.getXmlEncoding();
    String xmlStart = "<?xml version=\""+ xmlVersion + "\" encoding=\""+ xmlEncoding +"\"?>\n<root>";
    String xmlEnd = "</root>";
    _log.info("The xml start tag used is:" + xmlStart);
    List xmlTagsList = Arrays.asList(new InputStream[]
                                         {
                                             new ByteArrayInputStream(xmlStart.getBytes(Charset.forName(xmlEncoding))),
                                             compositeInputStream,
                                             new ByteArrayInputStream(xmlEnd.getBytes(Charset.forName(xmlEncoding))),
                                         });
    Enumeration<InputStream> streams = Collections.enumeration(xmlTagsList);
    SequenceInputStream seqStream = new SequenceInputStream(streams);
    return seqStream;
  }

  /**
   * Given an xml directory and prefix, the method identifies the file which has the scn (_scn from event producer class)
   * and returns an inputstream reader pointing to the scn location. If the scn is not found:
   * 1. If scn less than what is present in the trail file directory (minimum) - throws a fatal exception.
   * 2. If exact scn is not found, but it's greater than the minimum scn in the trail file directory, it returns the closest scn greater than _scn (from the event producer class).
   * This methods reads and modifies the _scn from the event producer class.
   * @param xmlDir The directory where the trail files are located
   * @param xmlPrefix The prefix of the xml trail files, eg. x4
   * @return
   * @throws IOException
   * @throws DatabusException
   */
  private ConcurrentAppendableCompositeFileInputStream locateScnInTrailFile(String xmlDir, String xmlPrefix)
      throws Exception
  {
    ConcurrentAppendableCompositeFileInputStream compositeInputStream = null;
    TrailFilePositionSetter.FilePositionResult filePositionResult = null;
    TrailFilePositionSetter trailFilePositionSetter = null;

    while(compositeInputStream == null)
    {

      _log.info("Requesting trail file position setter for scn: " + _scn.get());
      trailFilePositionSetter = new TrailFilePositionSetter(xmlDir,xmlPrefix, getName());
      filePositionResult = trailFilePositionSetter.locateFilePosition(_scn.get(), new GGXMLTrailTransactionFinder());
      _log.info("File position at : "+ filePositionResult);
      switch(filePositionResult.getStatus())
      {
        case ERROR:
          _log.fatal("Unable to locate the scn in the trail file.");
          throw new DatabusException("Unable to find the given scn " + _scn.get() + " in the trail files");
        case NO_TXNS_FOUND:

          //If the latest scn is not found in the trail files, then use the earliest scn.
          if(_scn.get() == TrailFilePositionSetter.USE_LATEST_SCN)
          {
            _log.info("Switching from USE_LATEST_SCN to USE_EARLIEST_SCN because no trail files were not found");
            _scn.set(TrailFilePositionSetter.USE_EARLIEST_SCN);
          }

          long noTxnsFoundSleepTime = 500;                      //TODO sleep get configuration for sleep time
          _log.info("NO_TXNS_FOUND, sleeping for "+ noTxnsFoundSleepTime + " ms before retrying");
          Thread.sleep(noTxnsFoundSleepTime);
          break;
        case EXACT_SCN_NOT_FOUND:
        {
          _log.info("Exact SCN was not found, the closest scn found was: " + filePositionResult.getTxnPos().getMinScn());
          compositeInputStream = new ConcurrentAppendableCompositeFileInputStream(xmlDir,
                                                                                  filePositionResult.getTxnPos().getFile(),
                                                                                  filePositionResult.getTxnPos().getFileOffset(),
                                                                                  new TrailFilePositionSetter.FileFilter(new File(xmlDir), xmlPrefix),
                                                                                  false);
          long foundScn = filePositionResult.getTxnPos().getMaxScn();
          /**
           * If exact scn is not found, the trail file position setter returns the next immediate available scn, i.e., the contract guarantees
           * a scn always greater than the given scn (foundscn > _scn). We use the _scn (requested scn to be found) as the prevScn to start the event buffer.
           * And the scn found as the current scn(first event in the relay).
           */
          if(foundScn <= _scn.get())
            throw new DatabusException("EXACT_SCN_NOT_FOUND, but foundScn is <= _scn ");

          _startPrevScn.set(_scn.get());
          _log.info("Changing current scn from " + _scn.get() + " to " + foundScn);
          _log.info("Planning to use prevScn " + _startPrevScn);
          _scn.set(foundScn);
          break;
        }
        case FOUND:
        {
          _log.info("Exact SCN was  found" + filePositionResult.getTxnPos().getMaxScn());
          compositeInputStream = new ConcurrentAppendableCompositeFileInputStream(xmlDir,
                                                                                  filePositionResult.getTxnPos().getFile(),
                                                                                  filePositionResult.getTxnPos().getFileOffset(),
                                                                                  new TrailFilePositionSetter.FileFilter(new File(xmlDir), xmlPrefix),
                                                                                  false);
          /**
           * The trail file position setter returns FOUND in two cases:
           * 1. MaxScn was given as input.
           * 2. Earliest or Latest scn was given as input.
           * For both the cases, we set the prevScn to the foundScn-1 and the foundScn as the currentScn.
           */
          long foundScn = filePositionResult.getTxnPos().getMaxScn();

          //Assert that if maxScn was requested, the trail file position setter has returned the exact scn (It has returned FOUND).
          if(_scn.get() >=0 && _scn.get() != foundScn)
          {
            throw new DatabusException("The exact scn was not found, but the trail file position setter has returned FOUND!");
          }

          _startPrevScn.set(foundScn - 1);
          _scn.set(foundScn);
          break;
        }
        default:
          throw new DatabusException("Unhandled file position result in switch case, terminating producer.");
      }
    }

    if(filePositionResult == null)
    {
      _log.info(trailFilePositionSetter);
      throw new DatabusException("file position Result returned by TrailFilePositionSetter is null!");
    }

    if(_scn.get() <= 0)
    {
      _log.info("The scn is <=0, using scn from file position setter:" + filePositionResult);
      _scn.set(filePositionResult.getTxnPos().getMaxScn());
    }

    return compositeInputStream;

  }

  /**
   * Given a DBImage, returns the key
   * If it is a single key, it returns the object if it is LONG /INT / STRING
   * For compound key, it casts the fields as String, delimits the fields and returns the appended string
   * @param dbUpdate The post-image of the event
   * @return Actual key object
   * @throws DatabusException
   */
  static protected Object obtainKey(DbUpdateState.DBUpdateImage dbUpdate)
      throws DatabusException
  {
    if (null == dbUpdate) {
      throw new DatabusException("DBUpdateImage is null");
    }
    List<KeyPair> pairs = dbUpdate.getKeyPairs();
    if (null == pairs || pairs.size() == 0) {
      throw new DatabusException("There do not seem to be any keys");
    }

    if (pairs.size() == 1) {
      Object key = dbUpdate.getKeyPairs().get(0).getKey();
      Schema.Type pKeyType = dbUpdate.getKeyPairs().get(0).getKeyType();
      Object keyObj = null;
      if (pKeyType == Schema.Type.INT)
      {
        if (key instanceof Integer)
        {
          keyObj = key;
        }
        else
        {
          throw new DatabusException(
              "Schema.Type does not match actual key type (INT) "
                  + key.getClass().getName());
        }

      } else if (pKeyType == Schema.Type.LONG)
      {
        if (key instanceof Long)
        {
          keyObj = key;
        }
        else
        {
          throw new DatabusException(
              "Schema.Type does not match actual key type (LONG) "
                  + key.getClass().getName());
        }

        keyObj = key;
      }
      else
      {
        keyObj = key;
      }

      return keyObj;
    } else {
      // Treat multiple keys as a separate case to avoid unnecessary casts
      Iterator<KeyPair> li = pairs.iterator();
      String compositeKey = "";
      while (li.hasNext())
      {
        KeyPair kp = li.next();
        Schema.Type pKeyType = kp.getKeyType();
        Object key = kp.getKey();
        if (pKeyType == Schema.Type.INT)
        {
          if (key instanceof Integer)
            compositeKey += kp.getKey().toString();
          else
            throw new DatabusException(
                "Schema.Type does not match actual key type (INT) "
                    + key.getClass().getName());
        }
        else if (pKeyType == Schema.Type.LONG)
        {
          if (key instanceof Long)
            compositeKey += key.toString();
          else
            throw new DatabusException(
                "Schema.Type does not match actual key type (LONG) "
                    + key.getClass().getName());
        }
        else
        {
          compositeKey += key;
        }

        if (li.hasNext()) {
          // Add the delimiter for all keys except the last key
          compositeKey += DbusConstants.COMPOUND_KEY_DELIMITER;
        }
      }
      return compositeKey;
    }
  }

  /**
   *
   * @param dbUpdates  The dbUpdates present in the current transaction
   * @param ti The meta information about the transaction. (See TransactionInfo class for more details).
   * @throws DatabusException
   * @throws UnsupportedKeyException
   */
  protected void addEventToBuffer(List<TransactionState.PerSourceTransactionalUpdate> dbUpdates, TransactionInfo ti)
      throws DatabusException, UnsupportedKeyException
  {
    if(dbUpdates.size() == 0)
      throw new DatabusException("Cannot handle empty dbUpdates");

    long scn = ti.getScn();
    long timestamp = ti.getTransactionTimeStampNs();
    EventSourceStatistics globalStats = getSource(GLOBAL_SOURCE_ID).getStatisticsBean();

    /**
     * We skip the start scn of the relay, we have already added a EOP for this SCN in the buffer.
     * Why is this not a problem ?
     * There are two cases:
     * 1. When we use the earliest/latest scn if there is no maxScn (We don't really have a start point). So it's really OK to miss the first event.
     * 2. If it's the maxSCN, then event was already seen by the relay.
     */
    if(scn == _startPrevScn.get())
    {
      _log.info("Skipping this transaction, EOP already send for this event");
      return;
    }

    getEventBuffer().startEvents();

    int eventsInTransactionCount = 0;

    List<EventReaderSummary> summaries = new ArrayList<EventReaderSummary>();

    for (int i = 0; i < dbUpdates.size(); ++i)
    {
      GenericRecord record = null;
      TransactionState.PerSourceTransactionalUpdate perSourceUpdate = dbUpdates.get(i);
      short sourceId = (short)perSourceUpdate.getSourceId();
      // prepare stats collection per source
      EventSourceStatistics perSourceStats = getSource(sourceId).getStatisticsBean();

      Iterator<DbUpdateState.DBUpdateImage> dbUpdateIterator = perSourceUpdate.getDbUpdatesSet().iterator();
      int eventsInDbUpdate = 0;
      long dbUpdatesEventsSize = 0;
      long startDbUpdatesMs = System.currentTimeMillis();

      while(dbUpdateIterator.hasNext())  //TODO verify if there is any case where we need to rollback.
      {
        DbUpdateState.DBUpdateImage dbUpdate =  dbUpdateIterator.next();

        //Construct the Databus Event key, determine the key type and construct the key
        Object keyObj = obtainKey(dbUpdate);
        DbusEventKey eventKey = new DbusEventKey(keyObj);

        //Get the logicalparition id
        PartitionFunction partitionFunction = _partitionFunctionHashMap.get((int)sourceId);
        short lPartitionId = partitionFunction.getPartition(eventKey);

        record = dbUpdate.getGenericRecord();
        //Write the event to the buffer
        if (record == null)
          throw new DatabusException("Cannot write event to buffer because record = " + record);

        if(record.getSchema() == null)
          throw new DatabusException("The record does not have a schema (null schema)");

        try
        {
          //Collect stats on number of dbUpdates for one source
          eventsInDbUpdate++;

          //Count of all the events in the current transaction
          eventsInTransactionCount++;
          // Serialize the row
          ByteArrayOutputStream bos = new ByteArrayOutputStream();
          Encoder encoder = new BinaryEncoder(bos);
          GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(
              record.getSchema());
          writer.write(record, encoder);
          byte[] serializedValue = bos.toByteArray();

          //Get the md5 for the schema
          SchemaId schemaId = SchemaId.createWithMd5(dbUpdate.getSchema());

          //Determine the operation type and convert to dbus opcode
          DbusOpcode opCode;
          if(dbUpdate.getOpType() == DbUpdateState.DBUpdateImage.OpType.INSERT || dbUpdate.getOpType() == DbUpdateState.DBUpdateImage.OpType.UPDATE)
          {
            opCode = DbusOpcode.UPSERT;
            if(_log.isDebugEnabled())
              _log.debug("The event with scn "+ scn +" is INSERT/UPDATE");
          }
          else if(dbUpdate.getOpType() == DbUpdateState.DBUpdateImage.OpType.DELETE)
          {
            opCode = DbusOpcode.DELETE;
            if(_log.isDebugEnabled())
              _log.debug("The event with scn "+ scn +" is DELETE");
          }
          else
          {
            throw new DatabusException("Unknown opcode from dbUpdate for event with scn:" + scn);
          }


          //Construct the dbusEvent info
          DbusEventInfo dbusEventInfo = new DbusEventInfo(opCode,
                                                          scn,
                                                          (short)_pConfig.getId(),
                                                          lPartitionId,
                                                          timestamp,
                                                          sourceId,
                                                          schemaId.getByteArray(),
                                                          serializedValue,
                                                          false,
                                                          false);
          dbusEventInfo.setReplicated(dbUpdate.isReplicated());

          perSourceStats.addEventCycle(1, ti.getTransactionTimeRead(), serializedValue.length, scn);
          globalStats.addEventCycle(1, ti.getTransactionTimeRead(), serializedValue.length, scn);

          long tsEnd = System.currentTimeMillis();
          perSourceStats.addTimeOfLastDBAccess(tsEnd);
          globalStats.addTimeOfLastDBAccess(tsEnd);

          //Append to the event buffer
          getEventBuffer().appendEvent(eventKey, dbusEventInfo, _statsCollector);
          _rc.incrementEventCount();
          dbUpdatesEventsSize += serializedValue.length;
        }
        catch (IOException io)
        {
          perSourceStats.addError();
          globalStats.addEmptyEventCycle();
          _log.error("Cannot create byte stream payload: " + dbUpdates.get(i).getSourceId());
        }
      }
      long endDbUpdatesMs = System.currentTimeMillis();
      long dbUpdatesElapsedTimeMs = endDbUpdatesMs - startDbUpdatesMs;

      // Log Event Summary at logical source level
      EventReaderSummary summary = new EventReaderSummary(sourceId, _monitoredSources.get(sourceId).getSourceName(),
                                                          scn, eventsInDbUpdate, dbUpdatesEventsSize,-1L /* Not supported */,
                                                          dbUpdatesElapsedTimeMs,  timestamp, timestamp, -1L /* Not supported */);
      if (_eventsLog.isInfoEnabled())
      {
        _eventsLog.info(summary.toString());
      }
      summaries.add(summary);

      if(_log.isDebugEnabled())
        _log.debug("There are "+ eventsInDbUpdate + " events seen in the current dbUpdate");
    }

    // update stats
    _ggParserStats.addTransactionInfo(ti, eventsInTransactionCount);

    // Log Event Summary at Physical source level
    ReadEventCycleSummary summary = new ReadEventCycleSummary(_pConfig.getName(),
                                                              summaries,
                                                              scn,
                                                              -1 /* Overall time including query time not calculated */);

    if (_eventsLog.isInfoEnabled())
    {
      _eventsLog.info(summary.toString());
    }

    _log.info("Writing "+ eventsInTransactionCount + " events from transaction with scn: " + scn);
    if(scn <= 0)
      throw new DatabusException("Unable to write events to buffer because of negative/zero scn: " + scn);

    getEventBuffer().endEvents(scn, _statsCollector);
    _scn.set(scn);

    if (getMaxScnReaderWriter() != null)
    {
      try
      {
        getMaxScnReaderWriter().saveMaxScn(_scn.get());
      }
      catch (DatabusException e)
      {
        _log.error("Cannot save scn = " + _scn + " for physical source = " + getName(), e);
      }
    }
  }

  protected RateControl getRateControl()
  {
    return _rc;
  }

  @Override
  public List<? extends EventSourceStatisticsIface> getSources() {
    return new ArrayList<GGMonitoredSourceInfo>(_monitoredSources.values());
  }
  /**
   * return MonitoredSourceInfo per source
   * @param sourceId
   * @return MonitoredSourceInof for this source id
   */
  public GGMonitoredSourceInfo getSource(short sourceId) {
    return _monitoredSources.get(sourceId);
  }
}
