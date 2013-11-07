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


/**
 * Thrown when the Scn requested is not found in the Buffer. 
 * @author sdas
 *
 */
public class ScnNotFoundException 
extends Exception 
{

  /**
   * 
   */
  private static final long serialVersionUID = 6210518999221853075L;

  public ScnNotFoundException()
  {
    super();
  }

  public ScnNotFoundException(String message, Throwable cause)
  {
    super(message, cause);
  }

  public ScnNotFoundException(String message)
  {
    super(message);
  }

  public ScnNotFoundException(Throwable cause)
  {
    super(cause);
  }
}
