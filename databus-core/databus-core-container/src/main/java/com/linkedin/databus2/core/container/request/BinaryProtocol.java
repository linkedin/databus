package com.linkedin.databus2.core.container.request;

import java.nio.ByteOrder;

public class BinaryProtocol
{
   // Protocol constants

  /** Byte order for number serialization */
  public static final ByteOrder BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;
  public static final byte RECOVERABLE_ERROR_THRESHOLD = (byte)0;
  public static final byte UNRECOVERABLE_ERROR_THRESHOLD = (byte)-64;

  /** Maximum number of bytes of the error class name to be serialized*/
  public static final int MAX_ERROR_CLASS_LEN = 100;

  /** Maximum number of bytes of the error message to be serialized*/
  public static final int MAX_ERROR_MESSAGE_LEN = 1000;

  /** Result ok */
  public static final byte RESULT_OK = 0;

  /* Recoverable errors */
  /** Server found itself in an unexpected state while processing the request */
  public static final byte RESULT_ERR_INTERNAL_SERVER_ERROR = RECOVERABLE_ERROR_THRESHOLD - 1;

  /**
   * The producer is trying to push events for sources which may generate gaps in the event
   * sequence. */
  public static final byte RESULT_ERR_SOURCES_TOO_OLD = RECOVERABLE_ERROR_THRESHOLD - 2;

  /**
   * The producer is trying to push events for an unknown partition */
  public static final byte RESULT_ERR_UNKNOWN_PARTITION = RECOVERABLE_ERROR_THRESHOLD - 3;

  /* Unrecoverable errors */
  /** Server received unknown command */
  public static final byte RESULT_ERR_UNKNOWN_COMMAND = UNRECOVERABLE_ERROR_THRESHOLD - 1;

  /** The requested protocol version is not supported */
  public static final byte RESULT_ERR_UNSUPPORTED_PROTOCOL_VERSION = UNRECOVERABLE_ERROR_THRESHOLD - 2;

  /**
   * The server received unexpected command; generally seen when the server expects certain
   * commands in a fixed sequence. */
  public static final byte RESULT_ERR_UNEXPECTED_COMMAND = UNRECOVERABLE_ERROR_THRESHOLD - 3;

  /** One of the request parameters had invalid value */
  public static final byte RESULT_ERR_INVALID_REQ_PARAM = UNRECOVERABLE_ERROR_THRESHOLD - 4;

  /** Control event before any data event*/
  public static final byte RESULT_ERR_UNEXPECTED_CONTROL_EVENT = UNRECOVERABLE_ERROR_THRESHOLD - 5;

  /** Invalid sequence number in the event */
  public static final byte RESULT_ERR_INVALID_EVENT = UNRECOVERABLE_ERROR_THRESHOLD - 6;
}
