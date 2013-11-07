package com.linkedin.databus.core;
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


/** Exception usually denoting mismatch between the content in the
 * SCN index and the content in the event buffer */
public class OffsetNotFoundException extends Exception {

  private static final long serialVersionUID = 1L;

  public OffsetNotFoundException()
  {
    super();
  }

  public OffsetNotFoundException(String message, Throwable cause)
  {
    super(message, cause);
  }

  public OffsetNotFoundException(String message)
  {
    super(message);
  }

  public OffsetNotFoundException(Throwable cause)
  {
    super(cause);
  }

}
