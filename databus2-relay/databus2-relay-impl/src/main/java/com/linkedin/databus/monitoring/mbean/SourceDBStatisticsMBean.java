package com.linkedin.databus.monitoring.mbean;
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
 *
 * @author snagaraj
 * Provides SCN related metrics of  source DB's of Databus2 Relays per logical source ; i.e. views / tables within a database
 */

public interface SourceDBStatisticsMBean
{

    /** Return source Mbean **/
    public String getSourceName();

    /** Return maximum SCN in source database */
    public long getMaxScn();

    /** Reset all statistics to zero. Typically for debugging / testing purposes. */
    public void reset();

    /** Return timestamp in seconds  of last update */
   public long getTimeSinceLastUpdate();

}
