/**
 *
 */
package com.linkedin.databus.client.consumer;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.specific.SpecificRecord;
import org.apache.log4j.Logger;
import org.xeril.util.config.ConfigHelper;

import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DatabusEventHolder;
import com.linkedin.databus.client.pub.DatabusLegacyConsumerAdapter;
import com.linkedin.databus.client.pub.DatabusLegacyConsumerCallback;
import com.linkedin.databus.client.pub.DatabusLegacyEventConverter;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.util.InvalidConfigException;

/**
 * @author "Balaji Varadarajan<bvaradarajan@linkedin.com>"
 *
 * Consumer used for migrating Databus V1 clients to Databus V2.
 *
 * The idea is to have this class be the consumer.
 * This class delegates the conversion of V2 to V1 events and consuming V1 events to application
 * specific callbacks (DatabusLegacyEventConverter and DatabusLegacyConsumerCallback correspondingly)
 * 
 * W.r.t Batching, this class does the following steps:
 *   1. batches the eventholders without setting the V2 DTOs. 
 *   2. Once the batch reaches a fixed size, the v2 DTOs are set to respective eventHolder objects.
 *   3. The converter is called with the v2 eventholder list to convert them to v1 event holder list.
 *   4. The V1 legacy consumer callback is then called with the v1 event holder list.   
 *
 */
public class DatabusLegacyBatchingCombinedConsumerAdapter<V1, V2 extends SpecificRecord> extends
    BatchingDatabusCombinedConsumer<V2>
    implements DatabusLegacyConsumerAdapter<V1, V2>
{
  private DatabusLegacyConsumerCallback<V1> _consumer;
  private DatabusLegacyEventConverter<V1, V2> _v2ToV1Converter;
  private ArrayList<DatabusEventHolder<V2>> _streamEventHoderList = new ArrayList<DatabusEventHolder<V2>>();
  private ArrayList<DatabusEventHolder<V2>> _bootstrapEventHolderList = new ArrayList<DatabusEventHolder<V2>>();

  private final boolean _bootstrapAllowed;

  public static final String MODULE = DatabusLegacyBatchingCombinedConsumerAdapter.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  @Override
  public ConsumerCallbackResult onDataEvent(DbusEvent e, DbusEventDecoder eventDecoder)
  {
      LOG.debug("Databus2DataProvider:onDataEvent() called !!");
      handleEvent(e,_streamEventHoderList);
      return super.onDataEvent(e, eventDecoder); // batches the v2 DTO
  }

  /**
   * helper method that batches the v2 event holders (without setting the V2 DTOs). 
   * The V2 DTO object is batched in the base class.  
   * @param e : v2 dbusevent
   * @param batch : V2 EventHolderList to be added.
   */
  private void handleEvent(DbusEvent e, List<DatabusEventHolder<V2>> batch)
  {
      DatabusEventHolder<V2> v2 = new DatabusEventHolder<V2>();
      if ( e.isKeyString())
    	  v2.setKey(new DbusEventKey(e.keyBytes()));
      else
    	  v2.setKey(new DbusEventKey(e.key()));
      v2.setScn(e.sequence());
      v2.setTimestamp(e.timestampInNanos());
      batch.add(v2);
  }
  
  
  @Override
  public ConsumerCallbackResult onBootstrapEvent(DbusEvent e, DbusEventDecoder eventDecoder)
  {
      LOG.debug("Databus2DataProvider:onBootstrapEvent() called !!");

      if ( ! _bootstrapAllowed)
      {
          return ConsumerCallbackResult.SUCCESS; //Dont do any processing in bootstrap
      }

      handleEvent(e,_bootstrapEventHolderList);
      return super.onBootstrapEvent(e, eventDecoder); // batches the v2 DTO
  }

  /* (non-Javadoc)
   * @see com.linkedin.databus.client.consumer.BatchingDatabusCombinedConsumer#onDataEventsBatch(java.util.List)
   */
  @Override
  protected ConsumerCallbackResult onDataEventsBatch(List<V2> v2DTOs)
  {
	  ConsumerCallbackResult result = ConsumerCallbackResult.SUCCESS;  
	  try
	  {
		  if ( _streamEventHoderList.size() != v2DTOs.size())
		  {
			  StringBuilder msg = new StringBuilder();
			  msg.append("StreamEventsHolder Size and EventsBatch Size doesnt match !! V2 Events Batch Size : " + v2DTOs.size() + ", Sequences Size :" + _streamEventHoderList.size());
			  msg.append("\nEvents Batch :").append(v2DTOs);
			  msg.append("\nOther Payload :").append(_streamEventHoderList);
			  throw new RuntimeException(msg.toString());
		  }

		  addDTOsToBatch(_streamEventHoderList, v2DTOs);

		  List<DatabusEventHolder<V1>> v1Events = _v2ToV1Converter.convert(_streamEventHoderList);
		  result = _consumer.onEventsBatch(v1Events);
	  } catch (RuntimeException re) {
		  LOG.error("Got runtime exception while processing V2 Events.", re);
		  result = ConsumerCallbackResult.ERROR;
	  } finally {
		  _streamEventHoderList.clear();
	  }
	  return result;
  }

  private void addDTOsToBatch(List<DatabusEventHolder<V2>> eventBatch, List<V2> dtoList)
  {
    for (int i = 0; i < eventBatch.size(); i++)
    {
      DatabusEventHolder<V2> e = eventBatch.get(i);
      e.setEvent(dtoList.get(i));
    }
  }

  protected ArrayList<DatabusEventHolder<V2>> getStreamEventHoderList() 
  {
	return _streamEventHoderList;
  }

  protected ArrayList<DatabusEventHolder<V2>> getBootstrapEventHolderList() 
  {
	return _bootstrapEventHolderList;
  }

/* (non-Javadoc)
   * @see com.linkedin.databus.client.consumer.BatchingDatabusCombinedConsumer#onBootstrapEventsBatch(java.util.List)
   */
  @Override
  protected ConsumerCallbackResult onBootstrapEventsBatch(List<V2> v2DTOs)
  {
	  ConsumerCallbackResult result = ConsumerCallbackResult.SUCCESS;  

	  try
	  {
		  if ( _bootstrapEventHolderList.size() != v2DTOs.size())
		  {
			  StringBuilder msg = new StringBuilder();
			  msg.append("BootstrapEventHolderList Size and EventsBatch Size doesnt match !! V2 Event Batch Size : " + v2DTOs.size() + ", EventHolderList Size :" + _bootstrapEventHolderList.size());
			  msg.append("\nEvents Batch :").append(v2DTOs);
			  msg.append("\nOther Payload :").append(_bootstrapEventHolderList);
			  throw new RuntimeException(msg.toString());
		  }

		  addDTOsToBatch(_bootstrapEventHolderList, v2DTOs);
		  List<DatabusEventHolder<V1>> v1Events = _v2ToV1Converter.convert(_bootstrapEventHolderList);
		  result = _consumer.onBootstrapEventsBatch(v1Events);
	  } catch (RuntimeException re) {
		  LOG.error("Got runtime exception while processing V2 Events.", re);
		  result = ConsumerCallbackResult.ERROR;
	  } finally {
		  _bootstrapEventHolderList.clear();
	  }
	  return result;
  }

  public DatabusLegacyBatchingCombinedConsumerAdapter(DatabusLegacyConsumerCallback<V1> consumer,
                              boolean bootstrapAllowed,
                              DatabusLegacyEventConverter<V1, V2> v2ToV1Converter,
                              BatchingDatabusCombinedConsumer.Config batchConfig,
                              Class<V2> payloadClass)
      throws InvalidConfigException
  {
    super(batchConfig,payloadClass);
    _consumer=consumer;
    _bootstrapAllowed = bootstrapAllowed;
    _v2ToV1Converter = v2ToV1Converter;
  }

  @SuppressWarnings("unchecked")
  public DatabusLegacyBatchingCombinedConsumerAdapter(Config<V1,V2> config)
  throws Exception
  {
    this(config.getConsumer(),config.isBootstrapAllowed(), config.getV2ToV1Converter(), config.getV2BatchConfig(), (Class<V2>)(Class.forName(config.getV2EventClassName())));
  }

  public static class Config<V1, V2>
  {
    public static final boolean DEFAULT_BOOTSTRAP_ALLOWED = true;

    @Override
    public String toString() {
        return "Config [_consumer=" + _consumer + ", _bootstrapAllowed="
                + _bootstrapAllowed + ", _v2ToV1Converter="
                + _v2ToV1Converter + ", _v2BatchConfig=" + _v2BatchConfig
                + ", _v2EventClassName=" + _v2EventClassName + "]";
    }

    private DatabusLegacyConsumerCallback<V1> _consumer;
    private boolean _bootstrapAllowed = DEFAULT_BOOTSTRAP_ALLOWED;
    private DatabusLegacyEventConverter<V1, V2> _v2ToV1Converter;
    private BatchingDatabusCombinedConsumer.Config _v2BatchConfig;
    private String   _v2EventClassName;

    public String getV2EventClassName() {
        return _v2EventClassName;
    }

    public void setV2EventClassName(String v2EventClassName) {
        _v2EventClassName = v2EventClassName;
    }

    public void setV2BatchConfig(BatchingDatabusCombinedConsumer.Config v2BatchConfig)
    {
        _v2BatchConfig = v2BatchConfig;
    }

    public BatchingDatabusCombinedConsumer.Config getV2BatchConfig()
    {
        return _v2BatchConfig;
    }

    public DatabusLegacyConsumerCallback<V1> getConsumer()
    {
      return ConfigHelper.getRequired(_consumer);
    }

    public void setConsumer(DatabusLegacyConsumerCallback<V1> consumer)
    {
      _consumer = consumer;
    }

    public boolean isBootstrapAllowed()
    {
        return _bootstrapAllowed;
    }

    public void setBootstrapAllowed(boolean bootstrapAllowed)
    {
        _bootstrapAllowed = bootstrapAllowed;
    }

    public DatabusLegacyEventConverter<V1, V2> getV2ToV1Converter() {
        return _v2ToV1Converter;
    }

    public void setV2ToV1Converter(DatabusLegacyEventConverter<V1, V2> v2ToV1Converter) {
        this._v2ToV1Converter = v2ToV1Converter;
    }
  }
}
