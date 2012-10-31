package com.linkedin.databus.bootstrap.api;


/*
 * TODO : Add doc for State transition for BootstrapDB
 */
public class BootstrapProducerStatus
{
  /*
   * TODO: DDSDBUS-361 	Use Enum to track BootstrapDB Status
   */
  public static final int UNKNOWN = 0;
  public static final int NEW = 1;
  public static final int SEEDING = 2;
  public static final int SEEDING_CATCHUP = 3; 
  public static final int ACTIVE = 4;
  public static final int INACTIVE = 5;
  public static final int FELL_OFF_RELAY = 6;
  
  /*
   * Check if the Source is in active status
   * 
   * @return true if status == ACTIVE, otherwise false
   */
  public static boolean isActive(int status)
  {
	  return (ACTIVE == status);	  
  }
  
  /*
   * Check if the Source is disabled manually
   * 
   * @return true if status == INACTIVE, otherwise false
   */
  public static boolean isDisabled(int status)
  {
	  return ( INACTIVE == status);
  }
  
  /*
   * Check if the bootstrap can serve data
   * @return true if status == ACTIVE, otherwise false
   */
  public static boolean isReadyForBootstrap(int status)
  {
	  return  isActive(status);
  }
  
  /*
   * Check if the bootstrap can consume data ( in a consistent state)
   * @return true if status is ACTIVE (or) seeding_catchup, otherwise false
   */  
  public static boolean isReadyForConsumption(int status)
  {
	  return isActive(status) || (status == SEEDING_CATCHUP);
  }
  
  /*
   * Check if the bootstrap DB is being seeded
   * @return true if status is SEEDING or SEEDING_CATCHUP, otherwise false
   */
  public static boolean isBeingSeeded(int status)
  {
	  return (SEEDING  == status) || ( SEEDING_CATCHUP == status);
  }
  
  
  /*
   * Check if the bootstrap DB is in seeding Catchup phase
   * @return true id status is SEEDING_CATCHUP, otherwise false
   */
  public static boolean isSeedingCatchup(int status)
  {
	  return ( SEEDING_CATCHUP == status);
  }  
}
