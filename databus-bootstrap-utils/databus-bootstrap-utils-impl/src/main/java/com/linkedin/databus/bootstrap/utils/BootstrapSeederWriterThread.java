package com.linkedin.databus.bootstrap.utils;

import org.apache.log4j.Logger;

import com.linkedin.databus.bootstrap.utils.BootstrapEventBuffer.EventBufferEntry;
import com.linkedin.databus.core.util.RateMonitor;

public class BootstrapSeederWriterThread 
      extends DbusSeederBaseThread
      implements BootstrapEventBuffer.EventProcessor
{
  private static final Logger LOG = Logger.getLogger(BootstrapSeederWriterThread.class);
  private static final boolean _sDebug = LOG.isDebugEnabled(); 

  private final BootstrapDBSeeder    _seeder;
  private final BootstrapEventBuffer _buffer;
  private final RateMonitor          _mySQLWriteLatency = new RateMonitor("mySQLWriteLatency");
  
  public BootstrapSeederWriterThread(BootstrapEventBuffer buffer,
                                     BootstrapDBSeeder seeder)
  {
    super("BootstrapSeederWriterThread");
    _seeder = seeder;
    _buffer = buffer;
    
    _mySQLWriteLatency.start();;
    _mySQLWriteLatency.suspend();
  }

  @Override
  public boolean process(EventBufferEntry entry, long scn)
  {
    _rate.tick();
    
    switch (entry.getType())
    {
      case  EVENT_VALID:
                      if (_sDebug) LOG.debug("Received a new record for writing with key :" + entry.getKey());
                      _mySQLWriteLatency.resume();
                      _seeder.appendEvent(entry.getKey(), entry.getSeederChunkKey(), scn, 
                                          entry.getPhysicalPartitionId(), entry.getLogicalPartitionId(), 
                                          entry.getTimeStamp(),entry.getSrcId(), 
                                          entry.getSchemaId(), entry.getValue(), 
                                          entry.isEnableTracing(),entry.getStatsCollector());
                      _mySQLWriteLatency.suspend();
                      break;
                      
      case EVENT_EOP:
                    LOG.info("EOP received by the SeederWriterThread. ||");
                    LOG.info("Writer Rate is :" + _rate.getRate());
                    _buffer.logLatency();
                    LOG.info("MYSQL Writer Latency :" + _mySQLWriteLatency.getDuration()/1000000);
                     _seeder.endEvents(entry.getTimeStamp(),  null); //TimeStamp contains the rowId
                     break;
      
      case EVENT_EOS:
          			LOG.info("EOS received by the SeederWriterThread. ||");
          			LOG.info("Writer Rate is :" + _rate.getRate());
                    _buffer.logLatency();
                    LOG.info("MYSQL Writer Latency :" + _mySQLWriteLatency.getDuration()/1000000);
                    _seeder.endSource(scn);
                    break;
                    
      case EVENT_EOF:
                    LOG.info("EOF received by the SeederWriterThread. Stopping !!");
                    LOG.info("Writer Rate is :" + _rate.getRate());
                    _buffer.logLatency();
                    LOG.info("MYSQL Writer Latency :" + _mySQLWriteLatency.getDuration()/1000000);
                    _seeder.endSeeding();
                    _stop.set(true);
                    break;
                    
      case EVENT_ERROR:
          			LOG.error("ERROR received by the SeederWriterThread. Stopping !!");
          			LOG.info("Writer Rate is :" + _rate.getRate());
                    _buffer.logLatency();
                    LOG.info("MYSQL Writer Latency :" + _mySQLWriteLatency.getDuration()/1000000);
                    _stop.set(true);
                    break;
    }
    return true;
  }

  @Override
  public void run()
  {
    boolean success = true;
    _rate.start();
    LOG.info("MYSQL Writer Thread started !!");
    long count =0;
    while (!(_stop.get()) && success)
    {
    	count++;
        success = _buffer.readNextEvent(this);
       // LOG.info("MYSQL written record :" + count);
    }
    LOG.info("MYSQL Writer Thread done !!");
  }
}
