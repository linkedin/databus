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


public interface StartResult {
	  /**
	   * Result of start
	   */
	  public boolean getSuccess();
	  
	  /**
	   * Error Message when result code failed.
	   * @return null, if start did not fail
	   *         error message otherwise
	   */
	  public String getErrorMessage();
	  
	  /**
	   * Returns an exception in case of an error
	   * If there is no exception ( but there is an error ), can return null
	   * @return
	   */
	  public Exception getException();	  
	  
}
