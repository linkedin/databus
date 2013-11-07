/**
 * $Id: MaxSCNReader.java 33147 2007-11-18 22:29:12Z dmccutch $ */
package com.linkedin.databus2.core.seq;
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


import com.linkedin.databus2.core.DatabusException;

/**
 * Interface to read the maxSCN
 *
 * @author ypujante
 */
public interface MaxSCNReader
{
  /**
   * @return the max scn
   * @throws DatabusException if an error occurs
   */
  public long getMaxScn() throws DatabusException;
}
