package com.linkedin.databus.client;

import com.linkedin.databus.core.Checkpoint;

/**
 * Message type for controlling checkpoints.
 *
 * <p>Available types:
 * <ul>
 *   <li>SET_CHECKPOINT - changes the current checkpoint</li>
 * </ul>
 *
 * @author cbotev
 *
 */
public class CheckpointMessage
{
  public enum TypeId
  {
    SET_CHECKPOINT
  }

  private TypeId _typeId;
  private Checkpoint _checkpoint;

  private CheckpointMessage(TypeId typeId, Checkpoint checkpoint)
  {
    _typeId = typeId;
    _checkpoint = checkpoint;
  }

  public static CheckpointMessage createSetCheckpointMessage(Checkpoint checkpoint)
  {
    return new CheckpointMessage(TypeId.SET_CHECKPOINT, checkpoint);
  }

  public CheckpointMessage switchToSetCheckpoint(Checkpoint checkpoint)
  {
    _typeId = TypeId.SET_CHECKPOINT;
    _checkpoint = checkpoint;

    return this;
  }

  public TypeId getTypeId()
  {
    return _typeId;
  }

  public Checkpoint getCheckpoint()
  {
    return _checkpoint;
  }

  @Override
  public String toString()
  {
    return _typeId.toString();
  }

}
