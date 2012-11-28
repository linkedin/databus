package com.linkedin.databus2.core;

/** Describes what level of asserts should be evaluated */
public enum AssertLevel
{
  NONE (0),
  QUICK (100),
  MEDIUM (200),
  ALL (300);

  private final int _intValue;
  private AssertLevel(int intValue)
  {
    _intValue = intValue;
  }

  public int getIntValue()
  {
    return _intValue;
  }

  public boolean isSameOrStricter(AssertLevel other)
  {
    return _intValue >= other._intValue;
  }

  public boolean enabled()
  {
    return _intValue > NONE._intValue;
  }

  public boolean quickEnabled()
  {
    return isSameOrStricter(QUICK);
  }

  public boolean mediumEnabled()
  {
    return isSameOrStricter(MEDIUM);
  }

  public boolean allEnabled()
  {
    return isSameOrStricter(ALL);
  }
}
