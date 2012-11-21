/*
 * $Id: OracleEventProducer.java 272015 2011-05-21 03:03:57Z cbotev $
 */
package com.linkedin.databus2.producers.db;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.sql.DataSource;

import com.linkedin.databus.core.DbusEventBufferAppendable;
import com.linkedin.databus.core.UnsupportedKeyException;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.netty.ServerContainer;
import com.linkedin.databus2.core.seq.MaxSCNReaderWriter;
import com.linkedin.databus2.producers.AbstractEventProducer;
import com.linkedin.databus2.producers.EventCreationException;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;

/**
 * Wraps the OracleEventsMonitor in an event producer with a thread that can be started, paused, and
 * stopped.
 *
 * @author Jemiah Westerman<jwesterman@linkedin.com>
 * @version $Revision: 272015 $
 */
public class OracleEventProducer extends AbstractEventProducer
{
  private final SourceDBEventReader _sourceDBEventReader;
  private ArrayList<ObjectName> _registeredMbeans;

  public OracleEventProducer(
    List<MonitoredSourceInfo> sources,
    DataSource dataSource,
    DbusEventBufferAppendable eventBuffer,
    boolean enableTracing,
    DbusEventsStatisticsCollector dbusEventsStatisticsCollector,
    MaxSCNReaderWriter maxScnReaderWriter,
    PhysicalSourceStaticConfig physicalSourceConfig,
    MBeanServer mbeanServer
    ) throws DatabusException
  {
    super(eventBuffer, enableTracing, dbusEventsStatisticsCollector, maxScnReaderWriter,
          physicalSourceConfig, mbeanServer);
    _registeredMbeans = new ArrayList<ObjectName>(sources.size());
    for (MonitoredSourceInfo source:sources)
    {
    	try
    	{

    		Hashtable<String,String> props = new Hashtable<String,String>();
    		props.put("type", "SourceStatistics");
    		props.put("name", source.getSourceName());
    		ObjectName objectName = new ObjectName(ServerContainer.JMX_DOMAIN, props);

    		if (mbeanServer.isRegistered(objectName))
    		{
    			_log.warn("Unregistering old source statistics mbean: " + objectName);
    			mbeanServer.unregisterMBean(objectName);
    		}

    		mbeanServer.registerMBean(source.getStatisticsBean(), objectName);
    		_log.info("Registered source statistics mbean: " + objectName);
    		_registeredMbeans.add(objectName);
    	}

    	catch(Exception ex)
    	{
    		_log.error("Failed to register the source statistics mbean for source (" + source.getSourceName() + ") due to an exception.", ex);
    		throw new DatabusException("Failed to initialize event statistics mbeans.", ex);
    	}
    }
    _sourceDBEventReader = new OracleTxlogEventReader(physicalSourceConfig.getName(), sources,
                                                      dataSource, eventBuffer, enableTracing,
                                                      dbusEventsStatisticsCollector,
                                                      maxScnReaderWriter,
                                                      physicalSourceConfig.getSlowSourceQueryThreshold(),
                                                      physicalSourceConfig.getChunkingType(),
                                                      physicalSourceConfig.getTxnsPerChunk(),
                                                      physicalSourceConfig.getScnChunkSize(),
                                                      physicalSourceConfig.getChunkedScnThreshold(),
                                                      physicalSourceConfig.getMaxScnDelayMs());

  }

  public OracleEventProducer(DbusEventBufferAppendable eventBuffer,
                             OracleTxlogEventReader oracleEventsMonitor,
                             MaxSCNReaderWriter maxScnReaderWriter,
                             PhysicalSourceStaticConfig physicalSourceConfig,
                             MBeanServer mbeanServer)
  {
    super(eventBuffer, maxScnReaderWriter, physicalSourceConfig, mbeanServer);
    _sourceDBEventReader = oracleEventsMonitor;
  }

  @Override
  public List<MonitoredSourceInfo> getSources() {
	  return _sourceDBEventReader.getSources();
  }

  @Override
  protected ReadEventCycleSummary readEventsFromAllSources(long sinceSCN)
            throws DatabusException, EventCreationException, UnsupportedKeyException
  {
    return _sourceDBEventReader.readEventsFromAllSources(sinceSCN);
  }
  
  @Override
  public synchronized void shutdown()
  {
	  for (ObjectName name:_registeredMbeans)
	  {
		  try {
			_mbeanServer.unregisterMBean(name);
			_log.info("Unregistered source mbean: " + name);
		} catch (MBeanRegistrationException e) {
			_log.warn("Exception when unregistering source statistics mbean: " + name + e) ;
		} catch (InstanceNotFoundException e) {
			_log.warn("Exception when unregistering source statistics mbean: " + name + e) ;	
		}
	  }
	  super.shutdown();
  }

  public SourceDBEventReader getSourceDBReader()
  {
	  return  _sourceDBEventReader;
  }
}
