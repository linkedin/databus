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


/**
 * An base class for all Databus client library specific exceptions.
 *
 * @author cbotev
 *
 */
public class DatabusClientException extends Exception
{
  private static final long serialVersionUID = 1L;

  /**
   * Constructs an exception object with the specified error message and cause
   * @param     message         the error message
   * @param     cause           the error cause
   */
  public DatabusClientException(String message, Throwable cause)
  {
    super(message, cause);
  }

  /**
   * Constructs an exception object with the specified error message
   * @param     message         the error message
   */
  public DatabusClientException(String message)
  {
    super(message);
  }

  /**
   * Constructs an exception object with the specified error cause
   * @param     cause           the error cause
   */
  public DatabusClientException(Throwable cause)
  {
    super(cause);
  }

}
