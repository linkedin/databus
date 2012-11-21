package com.linkedin.databus.client.pub;

/**
 * The result code from the execution of a consumer callback.
 *
 * <ul>
 *  <li>SUCCESS - callback finished successfully</li>
 *  <li>CHECKPOINT - callback finished successfully and consumer is ready for a checkpoint</li>
 *  <li>ERROR - callback finished unsuccessfully; the Databus library should retry the call</li>
 *  <li>ERROR_FATAL - callback finished unsuccessfully with an unrecoverable error</li>
 * </ul>
 */
public enum ConsumerCallbackResult
{
  SUCCESS(0),
  CHECKPOINT(100),
  ERROR(200),
  ERROR_FATAL(300);

  private final int _level;

  private ConsumerCallbackResult(int level)
  {
    _level = level;
  }

  public int getLevel()
  {
    return _level;
  }

  public static boolean isSuccess(ConsumerCallbackResult resultCode)
  {
    return SUCCESS == resultCode || CHECKPOINT == resultCode;
  }

  public static boolean isFailure(ConsumerCallbackResult resultCode)
  {
    return ERROR == resultCode || ERROR_FATAL == resultCode;
  }

  /** Returns the more severe result code */
  public static ConsumerCallbackResult max(ConsumerCallbackResult r1, ConsumerCallbackResult r2)
  {
    return (r1._level < r2._level) ? r2 : r1;
  }
}
