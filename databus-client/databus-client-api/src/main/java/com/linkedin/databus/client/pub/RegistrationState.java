package com.linkedin.databus.client.pub;

/**
 * The allowed transitions are as follows
 * CREATED -> STARTED
 * CREATED -> DEREGISTERED
 * STARTED -> DEREGISTERED
 */

public enum RegistrationState {
	 // Default state for a registration
     CREATED,
     // Goes to this state from CREATED, when a start() is invoked
     STARTED,
     // Enters this state from either CREATED or STARTED state when a deregister() is invoked 
     DEREGISTERED
}
