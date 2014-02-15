package com.linkedin.databus.client.pub;
/*
 *
 * Copyright 2013 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/

public enum RegistrationState
{
  /**
   * Initialization state. Dbus Client library has not set up registration for its consumers yet.
   */
  INIT,
  /**
   * Consumers have been registered but consumption has not yet started.
   */
  REGISTERED,
  /**
   * Consumption has started.
   */
  STARTED,
  /**
   * Consumption is paused.
   */
  PAUSED,
  /**
   * Consumption is resumed.
   */
  RESUMED,
  /**
   * Consumption is suspended because of error.
   */
  SUSPENDED_ON_ERROR,
  /**
   * Consumption is shut down.
   */
  SHUTDOWN,
  /**
   * Client library is unregistered and removed from client's internal data structures.
   */
  DEREGISTERED;

  /**
   * @return true if consumption has not yet started
   */
  public boolean isPreStartState()
  {
    switch(this)
    {
    case INIT :
    case REGISTERED :
      return true;
    default:
      return false;
    }
  }

  /**
   * @return true if consumption has completed
   */
  public boolean isPostRunState()
  {
    switch(this)
    {
    case SHUTDOWN:
    case DEREGISTERED:
      return true;
    default:
      return false;
    }
  }

  /**
   * @return true if consumption is actively running
   */
  public boolean isRunning()
  {
    switch (this)
    {
    case STARTED:
    case PAUSED:
    case SUSPENDED_ON_ERROR:
    case RESUMED:
      return true;
    default:
      return false;
    }
  }

  /**
   * @return true if registration is actively maintained in the client library
   */
  public boolean isActiveRegistration()
  {
    switch (this)
    {
    case REGISTERED:
    case STARTED:
    case PAUSED:
    case RESUMED:
    case SUSPENDED_ON_ERROR:
    case SHUTDOWN:
      return true;
    default:
      return false;
    }
  }
};
