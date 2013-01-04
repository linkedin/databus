package com.linkedin.databus.client.consumer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Formatter;

import org.apache.avro.Schema;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.linkedin.databus.client.DbusEventAvroDecoder;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DatabusCombinedConsumer;
import com.linkedin.databus.client.pub.DatabusV3Consumer;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.client.pub.SCN;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.FileBasedEventTrackingCallback;
import com.linkedin.databus.core.util.ConfigApplier;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;

/**
 * A databus stream consumer that can be used for logging/debugging purposes. The class is thread-
 * safe.
 * @author cbotev
 *
 */
public class LoggingConsumer extends DelegatingDatabusCombinedConsumer
                             implements DatabusV3Consumer
{
  public static final String MODULE = LoggingConsumer.class.getName();
  private static final Logger CLASS_LOG = Logger.getLogger(MODULE);

  private static final double NANOS_PER_MS = 1000000.0;

  public static enum Verbosity
  {
    EVENTS_ONLY,
    EVENT_WINDOWS,
    ALL
  }

  private final StaticConfig _staticConfig;
  private RuntimeConfig _runtimeConfig;
  private long _currentWindowScn;
  private long _lastWindowSeq;
  private long _lastWindowStartTs;
  private long _lastWindowEndTs;
  private long _curSourceStartTs;
  private int _curSourceEvents;
  private int _curWindowEvents;
  private StringBuffer _perSourceMsgBuilder;
  private FileBasedEventTrackingCallback _fileBasedCallback = null;

  public LoggingConsumer() throws InvalidConfigException
  {
    this(new Config());
  }

  public LoggingConsumer(Config configBuilder) throws InvalidConfigException
  {
    this(configBuilder.build());
  }

  public LoggingConsumer(StaticConfig staticConfig, String regId) throws InvalidConfigException
  {
    this(staticConfig, regId, null);
  }
  public LoggingConsumer(StaticConfig staticConfig) throws InvalidConfigException
  {
    this(staticConfig, null, (DatabusCombinedConsumer)null);
  }

  public LoggingConsumer(StaticConfig staticConfig, String regId, DatabusCombinedConsumer delegate)
         throws InvalidConfigException
  {
    super(delegate, regId == null ? null : Logger.getLogger(MODULE + ":" + regId) );

    _staticConfig = staticConfig;
    _log.info("logging listener config: " + staticConfig.toString());

    staticConfig.getRuntime().managedInstance(this);
    _runtimeConfig = staticConfig.getRuntime().build();
    _currentWindowScn = -1;
    _lastWindowStartTs = System.nanoTime();
    _lastWindowEndTs = _lastWindowStartTs;
    _curSourceEvents = 0;
    _curWindowEvents = 0;
    _lastWindowSeq = -1;
  }

  public  void enableEventFileTrace(String outputFileName) throws IOException
  {
    if (outputFileName != null)
    {
      enableEventFileTrace(outputFileName,false);
    }
  }
  
  public  void enableEventFileTrace(String outputFileName, boolean append) throws IOException
  {
    if (outputFileName != null)
    {
      _fileBasedCallback = new FileBasedEventTrackingCallback(outputFileName, append);
      _fileBasedCallback.init();
    }
  }

  public boolean isEnabled()
  {
    return _runtimeConfig.isEnabled();
  }

  public Level getLogLevel()
  {
    return _runtimeConfig.getLogLevel();
  }

  public Verbosity getVerbosity()
  {
    return _runtimeConfig.getVerbosity();
  }

  public boolean isValidityCheckEnabled()
  {
    return _runtimeConfig.isValidityCheckEnabled();
  }

  public RuntimeConfig getRuntimeConfig()
  {
    return _runtimeConfig;
  }

  private void setNewConfig(RuntimeConfig runtimeConfig)
  {
    _runtimeConfig = runtimeConfig;
  }

  @Override
  public ConsumerCallbackResult onEndDataEventSequence(SCN endScn)
  {
    ConsumerCallbackResult result = super.onEndDataEventSequence(endScn);
    return doEndDataEventSequence(endScn, result, false);
  }

  @Override
  public ConsumerCallbackResult onDataEvent(DbusEvent e, DbusEventDecoder eventDecoder)
  {
    ConsumerCallbackResult result = super.onDataEvent(e, eventDecoder);
    return doDataEvent(e, eventDecoder, result, false);
  }

  @Override
  public ConsumerCallbackResult onCheckpoint(SCN checkpointScn)
  {
    ConsumerCallbackResult result = super.onCheckpoint(checkpointScn);
    return doCheckpoint(checkpointScn, result, false);
  }

  @Override
  public ConsumerCallbackResult onEndSource(String source, Schema sourceSchema)
  {
    ConsumerCallbackResult result = super.onEndSource(source, sourceSchema);
    return doEndSource(source, sourceSchema, result, false);
  }

  @Override
  public ConsumerCallbackResult onRollback(SCN rollbackScn)
  {
    ConsumerCallbackResult result = super.onRollback(rollbackScn);
    return doRollback(rollbackScn, result, false);
  }

  @Override
  public ConsumerCallbackResult onStartDataEventSequence(SCN startScn)
  {
    startDataEventSequenceStats();
    ConsumerCallbackResult result = super.onStartDataEventSequence(startScn);
    return doStartDataEventSequence(startScn, result, false);
  }

  @Override
  public ConsumerCallbackResult onStartSource(String source, Schema sourceSchema)
  {
    startSourceStats(sourceSchema);
    ConsumerCallbackResult result = super.onStartSource(source, sourceSchema);
    return doStartSource(source, sourceSchema, result, false);
  }

  @Override
  public ConsumerCallbackResult onEndBootstrapSequence(SCN endScn)
  {
    ConsumerCallbackResult result = super.onEndBootstrapSequence(endScn);
    return doEndDataEventSequence(endScn, result, true);
  }

  @Override
  public ConsumerCallbackResult onEndBootstrapSource(String source, Schema sourceSchema)
  {
    ConsumerCallbackResult result = super.onEndBootstrapSource(source, sourceSchema);
    return doEndSource(source, sourceSchema, result, true);
  }

  @Override
  public ConsumerCallbackResult onBootstrapEvent(DbusEvent e, DbusEventDecoder eventDecoder)
  {
    ConsumerCallbackResult result = super.onBootstrapEvent(e, eventDecoder);
    return doDataEvent(e, eventDecoder, result, true);
  }

  @Override
  public ConsumerCallbackResult onBootstrapCheckpoint(SCN batchCheckpointScn)
  {
    ConsumerCallbackResult result = super.onBootstrapCheckpoint(batchCheckpointScn);
    return doCheckpoint(batchCheckpointScn, result, true);
  }

  @Override
  public ConsumerCallbackResult onStartBootstrapSequence(SCN startScn)
  {
    startDataEventSequenceStats();
    ConsumerCallbackResult result = super.onStartBootstrapSequence(startScn);
    return doStartDataEventSequence(startScn, result, true);
  }

  @Override
  public ConsumerCallbackResult onStartBootstrapSource(String source, Schema sourceSchema)
  {
    startSourceStats(sourceSchema);
    ConsumerCallbackResult result = super.onStartBootstrapSource(source, sourceSchema);
    return doStartSource(source, sourceSchema, result, true);
  }

  @Override
  public ConsumerCallbackResult onStartConsumption()
  {
    ConsumerCallbackResult result = super.onStartConsumption();
    return doStartConsumption(result, false);
  }

  @Override
  public ConsumerCallbackResult onStopConsumption()
  {
    ConsumerCallbackResult result = super.onStopConsumption();
    return doStopConsumption(result, false);
  }

  @Override
  public ConsumerCallbackResult onBootstrapRollback(SCN rollbackScn)
  {
    ConsumerCallbackResult result = super.onBootstrapRollback(rollbackScn);
    return doRollback(rollbackScn, result, true);
  }

  @Override
  public ConsumerCallbackResult onStartBootstrap()
  {
    ConsumerCallbackResult result = super.onStartBootstrap();
    return doStartConsumption(result, true);
  }

  @Override
  public ConsumerCallbackResult onStopBootstrap()
  {
    ConsumerCallbackResult result = super.onStopBootstrap();
    return doStopConsumption(result, true);
  }

  @Override
  public ConsumerCallbackResult onError(Throwable err)
  {
    ConsumerCallbackResult result = super.onError(err);
    return doError(err, result, false);
  }

  @Override
  public ConsumerCallbackResult onBootstrapError(Throwable err)
  {
    ConsumerCallbackResult result = super.onBootstrapError(err);
    return doError(err, result, true);
  }


  @Override
  public boolean canBootstrap()
  {
	  return false;
  }

  private ConsumerCallbackResult doStartConsumption(ConsumerCallbackResult result,
                                                   boolean bootstrapOn)
  {
    RuntimeConfig rtConfig = getRuntimeConfig();
    if (! rtConfig.isEnabled() || !Verbosity.ALL.equals(rtConfig.getVerbosity())) return result;

    _log.log(rtConfig.getLogLevel(), bootstrapOn ? "startBootstrap" : "startConsumption");
    return result;
  }

  private ConsumerCallbackResult doStartDataEventSequence(SCN startScn, ConsumerCallbackResult result,
                                                          boolean bootstrapOn)
  {
    RuntimeConfig rtConfig = getRuntimeConfig();

    if (! rtConfig.isEnabled() || Verbosity.EVENTS_ONLY.equals(rtConfig.getVerbosity())) return result;

    _log.log(rtConfig.getLogLevel(),
             (bootstrapOn ? "startBootstrapSequence:" : "startDataEventSequence:") +
              startScn.toString());

    return result;
  }

  private ConsumerCallbackResult doStartSource(String source, Schema sourceSchema,
                                               ConsumerCallbackResult result,
                                               boolean bootstrapOn)
  {
    RuntimeConfig rtConfig = getRuntimeConfig();

    if (! rtConfig.isEnabled() || !Verbosity.ALL.equals(rtConfig.getVerbosity())) return result;

    _log.log(rtConfig.getLogLevel(),
             (bootstrapOn ? "startBootstrapSource:" : "startSource:") + source);

    return result;
  }

  private ConsumerCallbackResult doDataEvent(DbusEvent e, DbusEventDecoder eventDecoder,
                                            ConsumerCallbackResult result,
                                            boolean bootstrapOn)
  {
    RuntimeConfig rtConfig = getRuntimeConfig();
    if (! rtConfig.isEnabled()) return result;

    // check for event validity as long as the option is NOT disabled
    if (rtConfig.isValidityCheckEnabled())
    {
      if (!e.isValid())
      {
        _log.error("invalid event received:");
        _log.error(e.toString());
      }
    }

    //for backwards compatibility
    if (bootstrapOn) _log.log(rtConfig.getLogLevel(), "onBootstrapEvent:" + e.sequence());

    _currentWindowScn = e.sequence();
    if (_fileBasedCallback != null)
    {
      _fileBasedCallback.onEvent(e);
    }

    if (_staticConfig.isLogTypedValue())
    {
      logTypedValue(e, eventDecoder, rtConfig, bootstrapOn ? "b:" : "s:");
    }

    updateEventStats(e);
    return result;
  }

  private ConsumerCallbackResult doEndSource(String source, Schema sourceSchema,
                                            ConsumerCallbackResult result,
                                            boolean bootstrapOn)
  {
    endSourceStats(sourceSchema);
    RuntimeConfig rtConfig = getRuntimeConfig();
    if (! rtConfig.isEnabled() || !Verbosity.ALL.equals(rtConfig.getVerbosity())) return result;

    _log.log(rtConfig.getLogLevel(), (bootstrapOn ? "endBootstrapSource" : "endSource:") + source);

    return result;
  }

  private ConsumerCallbackResult doCheckpoint(SCN checkpointScn, ConsumerCallbackResult result,
                                              boolean bootstrapOn)
  {
    RuntimeConfig rtConfig = getRuntimeConfig();
    if (! rtConfig.isEnabled() || Verbosity.EVENTS_ONLY.equals(rtConfig.getVerbosity())) return result;

    _log.log(rtConfig.getLogLevel(), (bootstrapOn ? "bootstrapCheckpoint:" : "Checkpoint:" ) +
             checkpointScn);

    return result;
  }

  private ConsumerCallbackResult doRollback(SCN rollbackScn, ConsumerCallbackResult result,
                                            boolean bootstrapOn)
  {
    RuntimeConfig rtConfig = getRuntimeConfig();
    if (! rtConfig.isEnabled() || !Verbosity.ALL.equals(rtConfig.getVerbosity())) return result;

    _log.log(rtConfig.getLogLevel(), (bootstrapOn ? "bootstrapRollback" : "rollback") + rollbackScn);
    return result;
  }

  private ConsumerCallbackResult doError(Throwable err, ConsumerCallbackResult result,
                                         boolean bootstrapOn)
  {
    RuntimeConfig rtConfig = getRuntimeConfig();
    _log.log(rtConfig.getLogLevel(), bootstrapOn ? "onBootstrapError" : "onError", err);
    return result;
  }

  private ConsumerCallbackResult doEndDataEventSequence(SCN endScn, ConsumerCallbackResult result,
                                                        boolean bootstrapOn)
  {
    RuntimeConfig rtConfig = getRuntimeConfig();

    if (! rtConfig.isEnabled() || Verbosity.EVENTS_ONLY.equals(rtConfig.getVerbosity())) return result;
    endDataEventSequenceStats(rtConfig, bootstrapOn);
    return result;
  }

  private void startDataEventSequenceStats()
  {
    _lastWindowStartTs = System.nanoTime();
    _curWindowEvents = 0;
    _perSourceMsgBuilder = new StringBuffer(200);
  }

  private void endDataEventSequenceStats(RuntimeConfig rtConfig, boolean bootstrapOn)
  {
    long endTs = System.nanoTime();
    StringBuilder sb = new StringBuilder(500);
    Formatter fmt = new Formatter(sb);

    long curWindowSeq = _currentWindowScn;

    fmt.format("%s: %d updates => wt:%.3f;cb:%.3f%nevents => bop=%d eop=%d %s",
               (bootstrapOn ? "bst" : "str"),
               _curWindowEvents,
               (_lastWindowStartTs - _lastWindowEndTs) / NANOS_PER_MS,
               (endTs - _lastWindowStartTs) / NANOS_PER_MS,
               _lastWindowSeq,
               curWindowSeq,
               _perSourceMsgBuilder
               );
    fmt.flush();

    _log.log(rtConfig.getLogLevel(), fmt.toString());
    fmt.close();

    _lastWindowEndTs = endTs;
    _lastWindowSeq = curWindowSeq;
    _perSourceMsgBuilder = null;
  }

  private ConsumerCallbackResult doStopConsumption(ConsumerCallbackResult result,
                                                  boolean bootstrapOn)
  {
    RuntimeConfig rtConfig = getRuntimeConfig();
    if (! rtConfig.isEnabled() || !Verbosity.ALL.equals(rtConfig.getVerbosity())) return result;

    _log.log(rtConfig.getLogLevel(), bootstrapOn ? "stopBootstrap" : "stopConsumption");
    return result;
  }

  private void startSourceStats(Schema sourceSchema)
  {
    _curSourceStartTs = System.nanoTime();
    _curSourceEvents = 0;
  }

  private void endSourceStats(Schema sourceSchema)
  {
    Formatter fmt = new Formatter(_perSourceMsgBuilder);
    fmt.format("%s=%d (%.3f ms) ",
               sourceSchema.getName(),
               _curSourceEvents, (System.nanoTime() - _curSourceStartTs) / NANOS_PER_MS);
    fmt.flush();
    fmt.close();
  }

  private void updateEventStats(DbusEvent e)
  {
    ++ _curWindowEvents;
    ++ _curSourceEvents;
  }

  protected void logTypedValue(DbusEvent e, DbusEventDecoder eventDecoder, RuntimeConfig rtConfig,
                               String phase) {
    if (eventDecoder instanceof DbusEventAvroDecoder)
    {
      try
      {
        DbusEventAvroDecoder avroDecoder = (DbusEventAvroDecoder)eventDecoder;
        ByteArrayOutputStream stringOut = new ByteArrayOutputStream();
        stringOut.write(phase.getBytes("UTF-8"));
        avroDecoder.dumpEventValueInJSON(e, stringOut);
        stringOut.flush();

        _log.log(rtConfig.getLogLevel(), stringOut.toString("UTF-8"));
      }
      catch (IOException ex)
      {
        _log.warn("typed value serialization error:" + ex.getMessage() ,ex);
      }
      catch (RuntimeException ex)
      {
        _log.warn("typed value serialization error:" + ex.getMessage() ,ex);
      }
    }
  }


public class RuntimeConfig implements ConfigApplier<RuntimeConfig>
  {
    private final boolean _enabled;
    private final Level _logLevel;
    private final Verbosity _verbosity;
    private final boolean _validityCheckEnabled;

    public RuntimeConfig(boolean enabled, Verbosity verbosity, Level logLevel, boolean validityCheckEnabled)
    {
      super();
      _enabled = enabled;
      _logLevel = logLevel;
      _verbosity = verbosity;
      _validityCheckEnabled = validityCheckEnabled;
    }

    /** A flag that indicates if the consumer is enabled */
    public boolean isEnabled()
    {
      return _enabled;
    }

    /** Logging level to be used by the consumer */
    public Level getLogLevel()
    {
      return _logLevel;
    }

    /** A flag that indicates if the consumer should check the validity of the events it gets */
    public boolean isValidityCheckEnabled()
    {
      return _validityCheckEnabled;
    }

    /**
     * The verbosity of the consumer, i.e. which events are to be logged.
     * <ul>
     *    <li>EVENTS_ONLY - only onEvent() and onBootstrapEvent() calls are logged</li>
     *    <li>EVENT_WINDOWS - all calls in EVENTS_ONLY plus start/endEventSequence() and
     *        start/endBootstrapSequence() are logged </li>
     *    <li>ALL - all calls are logged </li>
     * </ul>
     **/
    public Verbosity getVerbosity()
    {
      return _verbosity;
    }

    @Override
    public void applyNewConfig(RuntimeConfig oldConfig)
    {
      if (null == oldConfig || !equals(oldConfig))
      {
        setNewConfig(this);
      }
    }

    @Override
    public boolean equals(Object otherConfig)
    {
      if (null == otherConfig || !(otherConfig instanceof RuntimeConfig)) return false;

      return  equalsConfig((RuntimeConfig)otherConfig);
    }

    @Override
    public boolean equalsConfig(RuntimeConfig otherConfig)
    {
      if (null == otherConfig) return false;

      return isEnabled() == otherConfig.isEnabled() &&
             getLogLevel().equals(otherConfig.getLogLevel()) &&
             isValidityCheckEnabled() == otherConfig.isValidityCheckEnabled();
    }

    @Override
    public int hashCode()
    {
      return (_enabled ? 0xFFFFFFFF : 0) ^ _logLevel.hashCode() ^
             (_validityCheckEnabled ? 0xFFFFFFFF : 0);
    }
  }

  public static class RuntimeConfigBuilder implements ConfigBuilder<RuntimeConfig>
  {
    private boolean _enabled;
    private String _logLevel;
    private LoggingConsumer _managedInstance;
    private String _verbosity;
    private boolean _validityCheckEnabled;

    public RuntimeConfigBuilder()
    {
      _enabled = true;
      _logLevel = "INFO";
      _verbosity = "EVENT_WINDOWS";
      _managedInstance = null;
      _validityCheckEnabled = true;
    }

    public boolean isValidityCheckEnabled()
    {
      return _validityCheckEnabled;
    }

    public void setValidityCheckEnabled(boolean validityCheckEnabled)
    {
      _validityCheckEnabled = validityCheckEnabled;
    }

    public boolean isEnabled()
    {
      return _enabled;
    }

    public void setEnabled(boolean enabled)
    {
      _enabled = enabled;
    }

    public String getLogLevel()
    {
      return _logLevel;
    }

    public void setLogLevel(String logLevel)
    {
      _logLevel = logLevel;
    }

    public LoggingConsumer managedInstance()
    {
      return _managedInstance;
    }

    public void managedInstance(LoggingConsumer managedInstance)
    {
      _managedInstance = managedInstance;
    }

    @Override
    public RuntimeConfig build() throws InvalidConfigException
    {
      if (null == _managedInstance)
      {
        throw new InvalidConfigException("Managed logging listener not set");
      }

      Level logLevel = null;
      try
      {
        logLevel = Level.toLevel(getLogLevel());
      }
      catch (Exception e)
      {
        throw new InvalidConfigException("Invalid log level:", e);
      }

      Verbosity verbosity = null;
      try
      {
        verbosity = Verbosity.valueOf(getVerbosity());
      }
      catch (Exception e)
      {
        throw new InvalidConfigException("Invalid verbosiry:", e);
      }

      return _managedInstance.new RuntimeConfig(isEnabled(), verbosity, logLevel, isValidityCheckEnabled());
    }

    public String getVerbosity()
    {
      return _verbosity;
    }

    public void setVerbosity(String verbosity)
    {
      _verbosity = verbosity;
    }

    @Override
    public String toString()
    {
      return toJsonString();
    }

    public String toJsonString()
    {
      ObjectMapper mapper = new ObjectMapper();
      StringWriter writer = new StringWriter(100);
      try
      {
        mapper.writeValue(writer, this);
        writer.flush();
        return writer.toString();
      }
      catch (JsonGenerationException e)
      {
        CLASS_LOG.error("json error: " + e.getMessage(), e);
      }
      catch (JsonMappingException e)
      {
        CLASS_LOG.error("json error: " + e.getMessage(), e);
      }
      catch (IOException e)
      {
        CLASS_LOG.error("json i/o error: " + e.getMessage(), e);
      }

      return "";
    }
  }

  public static class StaticConfig
  {
    private final RuntimeConfigBuilder _runtime;
    private final boolean _logTypedValue;

    public StaticConfig(RuntimeConfigBuilder runtime, boolean logTypedValue)
    {
      super();
      _runtime = runtime;
      _logTypedValue = logTypedValue;
    }

    /** Runtime configuration properties*/
    public RuntimeConfigBuilder getRuntime()
    {
      return _runtime;
    }

    /** A flag that indicates if the consumer should log the typed value of data events as JSON */
    public boolean isLogTypedValue()
    {
      return _logTypedValue;
    }

    @Override
    public String toString()
    {
      return toJsonString();
    }

    public String toJsonString()
    {
      ObjectMapper mapper = new ObjectMapper();
      StringWriter writer = new StringWriter(100);
      try
      {
        mapper.writeValue(writer, this);
        writer.flush();
        return writer.toString();
      }
      catch (JsonGenerationException e)
      {
        CLASS_LOG.error("json error: " + e.getMessage(), e);
      }
      catch (JsonMappingException e)
      {
        CLASS_LOG.error("json error: " + e.getMessage(), e);
      }
      catch (IOException e)
      {
        CLASS_LOG.error("json i/o error: " + e.getMessage(), e);
      }

      return "";
    }

  }

  public static class Config implements ConfigBuilder<StaticConfig>
  {
    private RuntimeConfigBuilder _runtime;
    private boolean _logTypedValue = false;

    public Config()
    {
      _runtime = new RuntimeConfigBuilder();
    }

    public RuntimeConfigBuilder getRuntime()
    {
      return _runtime;
    }

    public void setRuntime(RuntimeConfigBuilder runtime)
    {
      _runtime = runtime;
    }

    public boolean isLogTypedValue()
    {
      return _logTypedValue;
    }

    public void setLogTypedValue(boolean logTypedValue)
    {
      _logTypedValue = logTypedValue;
    }

    @Override
    public StaticConfig build() throws InvalidConfigException
    {
      return new StaticConfig(getRuntime(), _logTypedValue);
    }

  }

}
