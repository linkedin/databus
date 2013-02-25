/*
 * $Id: EventProducer.java 260802 2011-04-14 19:10:09Z cbotev $
 */
package com.linkedin.databus2.producers;
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
 * @author Jemiah Westerman<jwesterman@linkedin.com>
 * @version $Revision: 260802 $
 */
public interface EventProducer
{
  String getName();

  long getSCN();

  void start(long sinceSCN);

  boolean isRunning();

  boolean isPaused();

  void unpause();

  void pause();

  void shutdown();
  
  void waitForShutdown() throws InterruptedException, IllegalStateException;
  
  void waitForShutdown(long timeout) throws InterruptedException, IllegalStateException;

}
