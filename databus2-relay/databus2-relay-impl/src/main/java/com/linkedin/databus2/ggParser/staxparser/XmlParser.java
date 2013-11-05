package com.linkedin.databus2.ggParser.staxparser;


import com.linkedin.databus.core.util.Base64;
import com.linkedin.databus2.ggParser.XmlStateMachine.StateMachine;
import com.linkedin.databus2.ggParser.XmlStateMachine.TransactionSuccessCallBack;
import com.linkedin.databus2.ggParser.XmlStateMachine.XmlStreamReaderHelper;
import com.linkedin.databus2.relay.config.ReplicationBitSetterStaticConfig;
import com.linkedin.databus2.schemas.SchemaRegistryService;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import org.apache.log4j.Logger;


public class XmlParser
{

  XMLStreamReader _xmlStreamReader;
  SchemaRegistryService _schemaRegistryService;
  StateMachine _stateMachine;
  AtomicBoolean _shutdownRequested = new AtomicBoolean(false);
  InputStream _inputStream;
  public static final Logger LOG = Logger.getLogger(XmlParser.class
                                                        .getName());
  public XmlParser(XMLStreamReader xmlStreamReader,
                   SchemaRegistryService schemaRegistry,
                   HashMap<String, String> tableMap,
                   HashMap<String, Integer> tableToSourceId,
                   TransactionSuccessCallBack _transactionSuccessCallBack,
                   boolean errorOnMissingFields,
                   ReplicationBitSetterStaticConfig replicationBitConfig,
                   InputStream inputStream)
  {
    _xmlStreamReader = xmlStreamReader;
    _schemaRegistryService = schemaRegistry;
    _stateMachine = new StateMachine(schemaRegistry, tableMap, tableToSourceId, _transactionSuccessCallBack,errorOnMissingFields,replicationBitConfig);
    _shutdownRequested.set(false);
    //The reference to the inputstream is passed just for closing the stream on shutdown.`
    _inputStream = inputStream;
  }

  public void start()
      throws Exception
  {
    XmlStreamReaderHelper.checkAndMoveNext(_xmlStreamReader); // Point to the root element
    try{
      while(!_shutdownRequested.get() && _xmlStreamReader.hasNext())
      {
        _stateMachine.processElement(_xmlStreamReader);
      }
    }
    catch (NoSuchElementException e)
    {
      /**
       * This is an expected behavior during a shutdown, the parser was reading something and we decided to close the stream from another thread.
       * We consume this exception and proceed to shutdown. (DDSDBUS-3267 for details)
       */
      if(!_shutdownRequested.get())
      {
        throw e;
      }
    }
    catch (XMLStreamException e)
    {
      /**
       * This is an expected behavior during a shutdown, the parser was reading something and we decided to close the stream from another thread.
       * We consume this exception and proceed to shutdown. We rethrow this exception (on non shutdown situation) because the event producer attempts to restart the parser with this
       * information. (DDSDBUS-3267 for details)
       */
      if(!_shutdownRequested.get())
      {
        throw e;
      }
    }
  }

  public void setShutDownRequested(boolean shutDownRequested)
  {
    _shutdownRequested.set(shutDownRequested);
    try
    {
      _xmlStreamReader.close();
    }
    catch (XMLStreamException e)
    {
      LOG.error("Error while attempting to close the parser: ");
    }
    catch(NoSuchElementException e)
    {
      LOG.info("Concurrent stream access while shutdown, it's safe to ignore this.");
    }
    catch(RuntimeException e)
    {
      LOG.error("Runtime exception while attempting shutdown, ignoring the exception", e);
    }

    try
    {
      _inputStream.close();
    }
    catch (IOException e)
    {
      LOG.error("Error while attempting to close the input stream:",  e);
    }
    catch(NoSuchElementException e)
    {
      LOG.info("Concurrent stream access while shutdown, it's safe to ignore this.");
    }
    catch(RuntimeException e)
    {
      LOG.error("Runtime exception while attempting shutdown, ignoring the exception", e);
    }

 }
}
