package com.linkedin.databus.client.pub;

import org.apache.avro.Schema;

import com.linkedin.databus.core.DbusEvent;

/**
 * This interface defines the callbacks used by Databus client library to inform Databus consumers
 * about events during Databus bootstrap.
 *
 * @see DatabusStreamConsumer
 *
 * @author cbotev
 */
public interface DatabusBootstrapConsumer
{
  /**
   * Denotes the start of the bootstrap process
   *
   * @return the callback result code; ERROR is treated as ERROR_FATAL and causes a hard error;
   *         CHECKPOINT is a no-op
   * @exception RuntimeException exceptions are treated as a return code ERROR_FATAL
   */
  ConsumerCallbackResult onStartBootstrap();

  /**
   * Denotes the end of the bootstrap process
   *
   * @return the callback result code; ERROR is treated as ERROR_FATAL and causes a hard error;
   *         CHECKPOINT is a no-op
   * @exception RuntimeException exceptions are treated as a return code ERROR_FATAL
   */
  ConsumerCallbackResult onStopBootstrap();

  /**
   * Denotes the start of an bootstrap event window.
   *
   * @param  startScn           the sequence number of the new bootstrap event window
   * @return the callback result code; ERROR causes a rollback
   * @exception RuntimeException exceptions are treated as a return code  ERROR
   */
  ConsumerCallbackResult onStartBootstrapSequence(SCN startScn);

  /**
   * Denotes the end of an bootstrap event window.
   *
   * @param  endScn           the sequence number of the bootstrap event window
   * @return the callback result code; ERROR causes a rollback
   * @exception RuntimeException exceptions are treated as a return code  ERROR
   */
  ConsumerCallbackResult onEndBootstrapSequence(SCN endScn);

  /**
   * Denotes the start of the bootstrapping of a new Databus source.
   *
   * @param  sourceName             the source name
   * @param  sourceSchema       the Avro schema used for serialization of the events
   * @return the callback result code; ERROR causes a rollback
   * @exception RuntimeException exceptions are treated as a return code  ERROR
   */
  ConsumerCallbackResult onStartBootstrapSource(String sourceName, Schema sourceSchema);

  /**
   * Denotes the end of the bootstrapping of  a Databus source.
   * @return the callback result code; ERROR causes a rollback
   * @exception RuntimeException exceptions are treated as a return code  ERROR
   */
  ConsumerCallbackResult onEndBootstrapSource(String name, Schema sourceSchema);

  /**
   * Denotes a new data event during bootstrapping.
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
  ConsumerCallbackResult onBootstrapEvent(DbusEvent e, DbusEventDecoder eventDecoder);

  /**
   * Denotes the rollback to an earlier checkpoint because of an error in the processing of the
   * previous events.
   *
   * @param batchCheckpointScn
   * @return the callback result code; CHECKPOINT is a no-op; ERROR is logged but otherwise ignored.
   * @exception RuntimeException exceptions are treated as a return code  ERROR
   */
  ConsumerCallbackResult onBootstrapRollback(SCN batchCheckpointScn);

  /**
   * Denotes that Databus wants to persist a checkpoint. The consumer can accept or deny the request.
   * If the consumer accepts the checkpoint, the Databus client library will attempt to restart from
   * this point in case of a failure at a later stage. Thus, the checkpoint allows the consumer to
   * make gradual progress in processing of Databus events.
   *
   * @param  checkpointScn      the current SCN to be saved in the checkpoint
   * @return the callback result code; SUCCESS and CHECKPOINT cause a checkpoint to be persisted;
   *         ERROR causes the checkpoint not to be persisted;
   * @exception RuntimeException exceptions are treated as a return code  ERROR
   */
  ConsumerCallbackResult onBootstrapCheckpoint(SCN checkpointScn);

  /**
   * Denotes that Databus encountered an error. The consumer could react based on the specific error.
   *
   * @param  err  -  error encountered by Databus
   * @return the callback result code; ERROR is logged by ignored; CHECKPOINT is ignored.
   * @exception RuntimeException exceptions are treated as a return code  ERROR
   */
  ConsumerCallbackResult onBootstrapError(Throwable err);
}
