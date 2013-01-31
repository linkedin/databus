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


public interface DeregisterResult 
{
	  
	  /**
	   * Result of deregistration
	   */
	  public boolean isSuccess();
	  
	  /**
	   * Error Message when result code failed.
	   * @return null, if degregistration did not fail
	   *         error message otherwise
	   */
	  public String getErrorMessage();
	  
	  /**
	   * If the resultCode is success, then this API will tell if the connection shutdown happened or not  
	   */
	  public boolean isConnectionShutdown();
	  
	  
	  /**
	   * Exception, if any
	   * @return null, if degregistration did not fail
	   *        exception otherwise
	   */
	  public Exception getException();
}
