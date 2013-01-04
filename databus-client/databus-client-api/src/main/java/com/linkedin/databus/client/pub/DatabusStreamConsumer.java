package com.linkedin.databus.client.pub;

import org.apache.avro.Schema;

import com.linkedin.databus.core.DbusEvent;

/**
 * This interface defines the callbacks used by Databus client library to inform Databus consumers
 * about events in the Databus stream from a Databus relay.
 *
 * @see DatabusBootstrapConsumer
 *
 * @author cbotev
 */
public interface DatabusStreamConsumer
{
  /**
   * Denotes the start of the databus events stream consumption
   * @return the callback result code; ERROR is treated as ERROR_FATAL and causes a hard error;
   *         CHECKPOINT is a no-op
   * @exception RuntimeException exceptions are treated as a return code ERROR_FATAL
   */
  ConsumerCallbackResult onStartConsumption();

  /**
   * Denotes the end of the databus events stream consumption
   * @return the callback result code; ERROR_FATAL and ERROR are logged but otherwise ignored since
   *         consumption is finishing anyway; CHECKPOINT is a no-op
   * @exception RuntimeException exceptions are treated as  a return code ERROR
   */
  ConsumerCallbackResult onStopConsumption();

  /**
   * Denotes the start of an event window.
   *
   * @param  startScn           the sequence number of the new event window
   * @return the callback result code; ERROR causes a rollback
   * @exception RuntimeException exceptions are treated as a return code  ERROR
   */
  ConsumerCallbackResult onStartDataEventSequence(SCN startScn);

  /**
   * Denotes the end of an event window.
   *
   * @param  endScn           the sequence number of the event window
   * @return the callback result code; ERROR causes a rollback
   * @exception RuntimeException exceptions are treated as a return code  ERROR
   */
  ConsumerCallbackResult onEndDataEventSequence(SCN endScn);

  /**
   * Denotes a rollback to the specified SCN.
   *
   * @param rollbackScn         rollbacks to the specified SCN
   * @return the callback result code; CHECKPOINT is a no-op; ERROR is logged but otherwise ignored.
   * @exception RuntimeException exceptions are treated as a return code  ERROR
   */
  ConsumerCallbackResult onRollback(SCN rollbackScn);

  /**
   * Denotes the start of a new Databus source.
   *
   * @param  source             the source name
   * @param  sourceSchema       the Avro schema used for serialization of the events
   * @return the callback result code; ERROR causes a rollback
   * @exception RuntimeException exceptions are treated as a return code  ERROR
   */
  ConsumerCallbackResult onStartSource(String source, Schema sourceSchema);

  /**
   * Denotes the end of a Databus source.
   * @param  source             the source name
   * @param  sourceSchema       the Avro schema used for serialization of the events
   * @return the callback result code; ERROR causes a rollback
   * @exception RuntimeException exceptions are treated as a return code  ERROR
   */
  ConsumerCallbackResult onEndSource(String source, Schema sourceSchema);

  /**
   * Denotes a new data event.
   *
   * <b>IMPORTANT:</b> The consumer should not save the event object passed as it is not guaranteed
   * the event contents will not be overwritten in the future. Instead the consumer should copy
   * the data payload before returning from the callback.
   *
   * @param  e                  provides access to the payload of the data event
   * @param  eventDecoder       A converter that can be used for access to the SpecificRecord
   *                            version of the event payload.
   *
   * @return the callback result code; ERROR causes a rollback
   * @exception RuntimeException exceptions are treated as a return code  ERROR
   */
  ConsumerCallbackResult onDataEvent(DbusEvent e, DbusEventDecoder eventDecoder);

  /**
   * Denotes that Databus wants to persist a checkpoint. The consumer can accept or deny the request.
   * If the consumer accepts the checkpoint, the Databus client library will attempt to restart from
   * this point in case of a failure at a later stage. Thus, the checkpoint allows the consumer to
   * make gradual progress in processing of Databus events.
   *
   * @param  checkpointScn      the current SCN to be saved in the checkpoint
   * @return true if the consumer accepts the checkpoint
   * @return the callback result code; SUCCESS and CHECKPOINT cause a checkpoint to be persisted;
   *         ERROR causes the checkpoint not to be persisted;
   * @exception RuntimeException exceptions are treated as a return code  ERROR
   */
  ConsumerCallbackResult onCheckpoint(SCN checkpointScn);


  /**
   * Denotes that Databus encountered an error. The consumer could react based on the specific error.
   *
   * @param  err  -  error encountered by Databus
   * @return the callback result code; ERROR is logged by ignored; CHECKPOINT is ignored.
   * @exception RuntimeException exceptions are treated as a return code  ERROR
   */
  ConsumerCallbackResult onError(Throwable err);
  
}
