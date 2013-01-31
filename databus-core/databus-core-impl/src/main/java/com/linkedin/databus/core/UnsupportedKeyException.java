/*
 * $Id: UnsupportedKeyException.java 153294 2010-12-02 20:46:45Z jwesterm $
 */
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
 * Thrown when the data type of the "key" field is not a supported type.
 *
 * @author Jemiah Westerman<jwesterman@linkedin.com>
 * @version $Revision: 153294 $
 */
public class UnsupportedKeyException
extends Exception
{
  private static final long serialVersionUID = 1L;

  public UnsupportedKeyException()
  {
    super();
  }

  public UnsupportedKeyException(String message, Throwable cause)
  {
    super(message, cause);
  }

  public UnsupportedKeyException(String message)
  {
    super(message);
  }

  public UnsupportedKeyException(Throwable cause)
  {
    super(cause);
  }
}
