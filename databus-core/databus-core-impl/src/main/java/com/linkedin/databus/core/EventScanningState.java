package com.linkedin.databus.core;

public enum EventScanningState
{
  LOOKING_FOR_FIRST_VALID_EVENT,
  IN_LESS_THAN_EQUALS_SCN_ZONE,
  FOUND_WINDOW_ZONE,
  VALID_ZONE,
  MISSED_WINDOW_ZONE
  
}
