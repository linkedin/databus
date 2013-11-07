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
 * This interface defines the callbacks used by Databus Espresso client library to inform Databus 
 * Espresso consumers about events in the Databus stream from a Databus relay.
 *
 * This is forward compatible Databus Espresso Consumer interface that external consumers need to 
 * adhere to.
 *
 * @author pganti
 */
public interface DatabusV3Consumer extends DatabusCombinedConsumer
{

	/**
	 * 
	 * DatabusV3Consumer is a combined consumer API for streaming and bootstrapping
	 * 
	 * @return true, consumer can actually bootstrap 
	 *         false, if it cannot
	 */
	public boolean canBootstrap();
}
