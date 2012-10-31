package com.linkedin.databus2.producers;

/**
 * Thrown when event creation failed for a databus source.
 *
 * @author Jemiah Westerman<jwesterman@linkedin.com>
 * @version $Revision: 153294 $
 */
public class EventCreationException
extends Exception
{
  private static final long serialVersionUID = 1L;

  private final String _sourceName;

  public EventCreationException(String sourceName)
  {
    super("Event creation failed. (source: " + sourceName + ")");
    _sourceName = sourceName;
  }

  public EventCreationException(String message, String sourceName)
  {
    super(message + " (source: " + sourceName + ")");
    _sourceName = sourceName;
  }

  public EventCreationException(String sourceName, Throwable cause)
  {
    super("Event creation failed. (source: " + sourceName + ")", cause);
    _sourceName = sourceName;
  }

  public EventCreationException(String message, String sourceName, Throwable cause)
  {
    super(message + " (source: " + sourceName + ")", cause);
    _sourceName = sourceName;
  }

  public String getSourceName()
  {
    return _sourceName;
  }
}
